from __future__ import annotations

import contextlib
import multiprocessing
from collections import defaultdict
from datetime import datetime, timedelta
from functools import wraps
from threading import Lock

from apscheduler.jobstores.base import JobLookupError
from pytz import utc

from ap import ListenNotifyType, close_sessions, dic_config, scheduler
from ap.common.common_utils import get_process_queue
from ap.common.constants import LOCK, PROCESS_QUEUE, JobType
from ap.common.logger import (
    log_execution_time,
    logger,
)
from bridge.common.server_config import ServerConfig

# RESCHEDULE_SECONDS
RESCHEDULE_SECONDS = 10

threadingLock = Lock()
multiprocessingLock = multiprocessing.Lock()

# running jobs dict

# check parallel running
# ex: (JobType.CSV_IMPORT, JobType.GEN_GLOBAL): False
#       csv import and gen global can not run in the same time
#       False: Don't need to check key between 2 job (key: job parameter).
#       True: Need to check key between 2 job , if key is not the same , they can run parallel

FREE_JOBS = {JobType.PROCESS_COMMUNICATE.name, JobType.IDLE_MONITORING.name}

EXCLUSIVE_JOBS = {
    JobType.DATABASE_MAINTENANCE.name,
    JobType.SCAN_MASTER.name,
    JobType.SCAN_DATA_TYPE.name,
    JobType.TRANSACTION_CLEAN.name,
    JobType.CLEAN_DATA.name,
    JobType.DEL_PROCESS.name,
}

# jobs with `_id` suffixes
EXCLUSIVE_JOBS_WITH_IDS = {
    JobType.BACKUP_DATABASE.name,
    JobType.RESTORE_DATABASE.name,
}

CONFLICT_PAIR = {
    (JobType.PULL_DB_DATA.name, JobType.PULL_DB_DATA.name),
    (JobType.PULL_DB_DATA.name, JobType.PULL_PAST_DB_DATA.name),
    (JobType.PULL_DB_DATA.name, JobType.PULL_CSV_DATA.name),
    (JobType.PULL_DB_DATA.name, JobType.PULL_PAST_CSV_DATA.name),
    (JobType.PULL_CSV_DATA.name, JobType.PULL_PAST_DB_DATA.name),
    (JobType.PULL_CSV_DATA.name, JobType.PULL_CSV_DATA.name),
    (JobType.PULL_CSV_DATA.name, JobType.PULL_PAST_CSV_DATA.name),
    (JobType.PULL_PAST_DB_DATA.name, JobType.PULL_PAST_DB_DATA.name),
    (JobType.PULL_PAST_DB_DATA.name, JobType.PULL_PAST_CSV_DATA.name),
    (JobType.PULL_PAST_CSV_DATA.name, JobType.PULL_PAST_CSV_DATA.name),
    (JobType.PULL_CSV_DATA.name, JobType.ZIP_FILE_THREAD.name),
    # import trans
    (JobType.PULL_FEATHER_DATA.name, JobType.PULL_DB_DATA.name),
    (JobType.PULL_FEATHER_DATA.name, JobType.PULL_PAST_DB_DATA.name),
    (JobType.PULL_FEATHER_DATA.name, JobType.PULL_CSV_DATA.name),
    (JobType.PULL_FEATHER_DATA.name, JobType.PULL_PAST_CSV_DATA.name),
    (JobType.PULL_FEATHER_DATA.name, JobType.ZIP_FILE_THREAD.name),
    # SYNC
    (JobType.SYNC_CONFIG.name, JobType.SYNC_TRANSACTION.name),
    (JobType.SYNC_TRANSACTION.name, JobType.SYNC_PROC_LINK.name),
    (JobType.SCAN_MASTER.name, JobType.SCAN_MASTER.name),
    (JobType.SCAN_MASTER.name, JobType.SCAN_DATA_TYPE.name),
    (JobType.SCAN_MASTER.name, JobType.PULL_DB_DATA.name),
    (JobType.SCAN_MASTER.name, JobType.PULL_PAST_DB_DATA.name),
    (JobType.SCAN_MASTER.name, JobType.PULL_PAST_CSV_DATA.name),
    (JobType.SCAN_MASTER.name, JobType.SCAN_FILE.name),
    (JobType.SCAN_MASTER.name, JobType.PULL_FEATHER_DATA.name),
    (JobType.SCAN_DATA_TYPE.name, JobType.SCAN_DATA_TYPE.name),
    (JobType.SCAN_DATA_TYPE.name, JobType.PULL_DB_DATA.name),
    (JobType.SCAN_DATA_TYPE.name, JobType.PULL_CSV_DATA.name),
    (JobType.SCAN_DATA_TYPE.name, JobType.PULL_PAST_DB_DATA.name),
    (JobType.SCAN_DATA_TYPE.name, JobType.PULL_PAST_CSV_DATA.name),
    (JobType.SCAN_DATA_TYPE.name, JobType.PULL_FEATHER_DATA.name),
}


@log_execution_time(logging_exception=True)
def scheduler_app_context(fn):
    """application context decorator for background task(scheduler)

    Arguments:
        fn {function} -- [description]

    Returns:
        [type] -- [description]
    """

    @wraps(fn)
    def inner(*args, **kwargs):
        print('--------CHECK_BEFORE_RUN---------')
        job_id = kwargs.get('_job_id')
        job_name = kwargs.get('_job_name')

        flask_app = ServerConfig.current_app
        with flask_app.app_context():
            dic_config[PROCESS_QUEUE] = get_process_queue()
            lock = dic_config[PROCESS_QUEUE][LOCK]
            with lock:
                if not scheduler_check_before_run(job_id, job_name):
                    scheduler.reschedule_job(job_id, fn, kwargs)
                    return

            try:
                result = fn(*args, **kwargs)
            except Exception as e:
                logger.exception(e)
                raise e
            finally:
                # clear running job
                if job_name not in FREE_JOBS:
                    dic_config[PROCESS_QUEUE][ListenNotifyType.RUNNING_JOB.name].pop(job_id)

                # rollback and close session to avoid database locked.
                close_sessions()

            return result

    return inner


def is_job_existed_in_exclusive_jobs_with_ids(job: str, running_jobs: list[str]):
    def extract_id_from_job(job_name: str) -> int | None:
        for exclusive_job_with_id in EXCLUSIVE_JOBS_WITH_IDS:
            if job_name.startswith(exclusive_job_with_id):
                id_from_job = job_name[len(exclusive_job_with_id) + 1 :]
                with contextlib.suppress(ValueError):
                    return int(id_from_job)
        return None

    job_id = extract_id_from_job(job)
    running_job_ids = map(extract_id_from_job, running_jobs)
    return job_id is not None and job_id in running_job_ids


def scheduler_check_before_run(job_id, job_name):
    """check if job can run parallel with other jobs"""
    job_name = str(job_name)
    dic_jobs = get_running_jobs()
    running_jobs = list(set(dic_jobs.values()))

    if is_job_existed_in_exclusive_jobs_with_ids(job_name, running_jobs):
        return False

    for _running_job_name in running_jobs:
        if job_name in EXCLUSIVE_JOBS:
            return False

        running_job_name = str(_running_job_name)
        if running_job_name in EXCLUSIVE_JOBS:
            return False

        pair1 = (job_name, running_job_name)
        pair2 = (running_job_name, job_name)
        if pair1 in CONFLICT_PAIR or pair2 in CONFLICT_PAIR:
            return False

    if job_name not in FREE_JOBS:
        dic_config[PROCESS_QUEUE][ListenNotifyType.RUNNING_JOB.name][job_id] = job_name

    return True


@log_execution_time()
def remove_jobs(target_job_names, proc_id=None):
    """remove all interval jobs

    Keyword Arguments:
        target_job_names {[type]} -- [description] (default: {None})
    """
    try:
        jobs = scheduler.get_jobs()
        for job in jobs:
            if job.name not in JobType.__members__ or JobType[job.name] not in target_job_names:
                continue

            if proc_id:
                if job.id == f'{job.name}_{proc_id}':
                    job.remove()
            else:
                job.remove()
    except JobLookupError:
        pass


def get_job_name_with_ids(target_job_names: list) -> dict:
    jobs = scheduler.get_jobs()
    job_dict = {}
    for job in jobs:
        if job.name in JobType.__members__ and JobType[job.name] in target_job_names:
            if job.name not in job_dict.keys():
                job_dict[job.name] = []

            if job.name != job.id:
                target_id = int(job.id.replace(f'{job.name}_', ''))
                job_dict[job.name].append(target_id)

    return job_dict


def add_job_to_scheduler(job_id, job_name, trigger, import_func, run_now, dic_import_param=None, replace_exist=True):
    if dic_import_param is None:
        dic_import_param = defaultdict()
    if run_now:
        scheduler.add_job(
            job_id,
            import_func,
            name=str(job_name),
            replace_existing=replace_exist,
            trigger=trigger,
            next_run_time=datetime.now().astimezone(utc) + timedelta(seconds=1),
            kwargs=dic_import_param,
        )
    else:
        scheduler.add_job(
            job_id,
            import_func,
            name=str(job_name),
            replace_existing=replace_exist,
            trigger=trigger,
            kwargs=dic_import_param,
        )


def get_running_jobs():
    dic_jobs = dic_config[PROCESS_QUEUE][ListenNotifyType.RUNNING_JOB.name]
    return dic_jobs


def is_job_running(job_id=None, job_name=None):
    dic_jobs = dic_config[PROCESS_QUEUE][ListenNotifyType.RUNNING_JOB.name]
    if job_id:
        return job_id in dic_jobs

    if job_name:
        return job_name in list(dic_jobs.values())

    return False
