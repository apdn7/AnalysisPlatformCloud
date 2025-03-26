from __future__ import annotations

import datetime as dt
import math
import re
import traceback
from typing import List

from ap import PROCESS_QUEUE, ListenNotifyType, db, dic_config
from ap.api.common.services.show_graph_database import DictToClass
from ap.common.common_utils import (
    DATE_FORMAT_STR_FACTORY_DB,
    convert_list_file_to_pickle,
    convert_time,
    get_current_timestamp,
    get_process_queue,
)
from ap.common.constants import (
    ALMOST_COMPLETE_PERCENT,
    COMPLETED_PERCENT,
    DATA_TABLE_ID,
    DATA_TABLE_NAME,
    HALF_WIDTH_SPACE,
    UNKNOWN_ERROR_TEXT,
    AnnounceEvent,
    CacheType,
    DiskUsageStatus,
    JobStatus,
    JobType,
)
from ap.common.disk_usage import get_disk_capacity_once
from ap.common.logger import log_execution_time, logger
from ap.common.memoize import memoize
from ap.common.services.error_message_handler import ErrorMessageHandler
from ap.common.timezone_utils import choose_utc_convert_func
from ap.setting_module.models import (
    CfgDataSource,
    CfgDataTable,
    CfgProcess,
    CsvImport,
    FactoryImport,
    JobManagement,
    make_session,
)

JOB_ID = 'job_id'
JOB_NAME = 'job_name'
JOB_TYPE = 'job_type'
DB_CODE = 'db_code'
PROC_CODE = 'proc_code'
PROC_ID = 'proc_id'
DB_MASTER_NAME = 'db_master_name'
DONE_PERCENT = 'done_percent'
START_TM = 'start_tm'
END_TM = 'end_tm'
DURATION = 'duration'
STATUS = 'status'
PROCESS_MASTER_NAME = 'process_master_name'
DETAIL = 'detail'
DATA_TYPE_ERR = 'data_type_error'
ERROR_MSG = 'error_msg'
previous_disk_status = DiskUsageStatus.Normal


@memoize(cache_type=CacheType.CONFIG_DATA)
def get_all_proc_shown_names():
    return {proc.id: proc.shown_name for proc in CfgProcess.get_all()}


@log_execution_time()
def get_background_jobs_service(page=1, per_page=50, sort_by='', order='', ignore_job_types=None, error_page=False):
    """
    Get background jobs from JobManagement table
    """

    jobs = JobManagement.query
    if error_page:
        jobs = jobs.filter(JobManagement.status.in_(JobStatus.failed_statuses()))

    if ignore_job_types:
        jobs = jobs.filter(JobManagement.job_type.notin_(ignore_job_types))

    if sort_by != '':
        sort_by_col = JobManagement.job_sorts(order)
        jobs = jobs.order_by(sort_by_col[sort_by])
    else:
        jobs = jobs.order_by(JobManagement.id.desc())

    jobs = jobs.paginate(page, per_page, error_out=False)
    dic_procs = get_all_proc_shown_names()
    rows = []
    for _job in jobs.items:
        dic_job = _job.as_dict()
        job = DictToClass(**dic_job)

        # get job information and send to UI
        job_name = f'{job.job_type}_{job.process_id}' if job.process_id else job.job_type

        if not error_page and job.process_id is not None and job.process_id not in dic_procs:
            # do not show deleted process in job normal page -> show only job error page
            continue

        # get process shown name
        proc_name = dic_procs.get(job.process_id)
        if not proc_name:
            proc_name = job.process_name or ''

        row = {
            JOB_ID: job.id,
            JOB_NAME: job_name,
            JOB_TYPE: job.job_type or '',
            DB_CODE: job.db_code or '',
            DB_MASTER_NAME: job.db_name or '',
            DONE_PERCENT: job.done_percent or 0,
            START_TM: job.start_tm,
            END_TM: job.end_tm or '',
            DURATION: round(job.duration, 2),
            STATUS: str(job.status),
            PROCESS_MASTER_NAME: proc_name,
            DETAIL: '',
            ERROR_MSG: job.error_msg,
        }
        rows.append(row)

    # TODO get more info from jobs ( next_page, prev_page, total_pages...)
    return rows, jobs


def send_processing_info(
    generator_func,
    job_type: JobType | str,
    db_code=None,
    data_table_id=None,
    process_id=None,
    process_name=None,
    after_success_func=None,
    is_check_disk=True,
    return_job_id=None,
    is_run_one_time=None,
    **kwargs,
):
    """send percent, status to client

    Arguments:
        job_type {JobType} -- [description]
        generator_func {[type]} -- [description]
    """
    # output
    return_val = None
    if isinstance(job_type, str):
        job_type = JobType[job_type]

    # add new job
    global previous_disk_status
    process_queue = get_process_queue()
    dic_config[PROCESS_QUEUE] = process_queue
    dic_progress = process_queue[ListenNotifyType.JOB_PROGRESS.name]
    error_msg_handler = ErrorMessageHandler()

    start_tm = dt.datetime.utcnow()
    with make_session() as session:
        job = JobManagement()
        job.job_type = job_type.name
        job.db_code = db_code

        # datasource_idの代わりに、t_job_managementテーブルにdatasource_nameが保存される
        if process_id is None:
            job.process_id = None
            job.process_name = None
        elif isinstance(process_id, int):
            job.process_id = process_id
            cfg_process = CfgProcess.get_by_id(process_id)
            if cfg_process is not None:
                job.process_name = cfg_process.name
        elif isinstance(process_id, (tuple, list)):
            names = []
            for _id in process_id:
                cfg_process = CfgProcess.get_by_id(_id)
                names.append(cfg_process.name)

            job.process_id = process_id[0]
            job.process_name = ' -> '.join(names)

        if data_table_id:
            cfg_data_table = CfgDataTable.get_by_id(data_table_id)
            data_table_name = cfg_data_table.name
            db_code = db_code if db_code else cfg_data_table.data_source_id
            data_source = CfgDataSource.get_by_id(db_code)
            job.data_table_id = data_table_id
            if cfg_data_table.table_name:
                job.db_name = f'{data_source.name}({cfg_data_table.table_name})'
            else:
                job.db_name = data_source.name

        job.status = str(JobStatus.PROCESSING)
        session.add(job)
        session.commit()
        job = DictToClass(**job.as_dict())

    # processing info
    dic_res = {
        job.id: {
            JOB_ID: job.id,
            JOB_NAME: f'{job.job_type}_{job.process_id}' if job.process_id else job.job_type,
            JOB_TYPE: job.job_type,
            DB_CODE: job.db_code or '',
            PROC_CODE: job.process_name or '',
            PROC_ID: job.process_id or process_id or '',
            DATA_TABLE_ID: job.data_table_id or data_table_id or '',
            DATA_TABLE_NAME: data_table_name if data_table_id else '',
            DB_MASTER_NAME: job.db_name or '',
            DONE_PERCENT: job.done_percent,
            START_TM: job.start_tm,
            END_TM: job.end_tm,
            DURATION: round(job.duration, 2),
            STATUS: str(job.status),
            PROCESS_MASTER_NAME: job.process_name or '',
            DETAIL: '',
        },
    }

    # time variable ( use for set start time in csv import)
    anchor_tm_csv = get_current_timestamp()
    anchor_tm_fac = get_current_timestamp()

    prev_job_info = None
    notify_data_type_error_flg = True
    while True:
        try:
            if is_check_disk or is_run_one_time:
                is_run_one_time = False
                disk_capacity = get_disk_capacity_once(_job_id=job.id)

                if disk_capacity:
                    if previous_disk_status != disk_capacity.disk_status:
                        # background_announcer.announce(disk_capacity.to_dict(), AnnounceEvent.DISK_USAGE.name)
                        dic_progress[job.id] = (disk_capacity.to_dict(), AnnounceEvent.DISK_USAGE.name)
                        previous_disk_status = disk_capacity.disk_status

                    if disk_capacity.disk_status == DiskUsageStatus.Full:
                        raise disk_capacity

            job_info = next(generator_func)

            # update job details ( csv_import , gen global...)
            if isinstance(job_info, int):
                # job percent update
                job.done_percent = job_info
            elif isinstance(job_info, dict):
                job_info[JOB_ID] = job.id
            elif isinstance(job_info, JobInfo):
                prev_job_info = job_info
                job_info.job_id = job.id
                if job_info.percent:
                    job_info.percent = round(job_info.percent, 2)
                # job percent update
                job.done_percent = job_info.percent
                # update job details (csv_import...)

                # insert import history
                if not job_info.import_type:
                    job_info.import_type = job_type.name

                if job_type is JobType.CSV_IMPORT and job_info.empty_files:
                    # empty files
                    # background_announcer.announce(job_info.empty_files, AnnounceEvent.EMPTY_FILE.name,)
                    # process_queue.put_nowait((job_info.empty_files, AnnounceEvent.EMPTY_FILE.name))
                    # process_queue.send((job_info.empty_files, AnnounceEvent.EMPTY_FILE.name))
                    dic_progress[job.id] = (job_info.empty_files, AnnounceEvent.EMPTY_FILE.name)

                # job err msg
                update_job_management_status_n_error(job.id, job_info)

                if should_save_import_csv_history(job_type):
                    detail_rec = insert_csv_import_info(job, anchor_tm_csv, job_info)
                    if detail_rec:
                        anchor_tm_csv = get_current_timestamp()

                if should_save_import_factory_history(job_type):
                    detail_rec = insert_factory_import_info(job, anchor_tm_fac, job_info, job_type)
                    if detail_rec:
                        anchor_tm_fac = get_current_timestamp()

        except StopIteration as stop_instance:
            job = update_job_management(job)

            # emit successful import data
            if prev_job_info and prev_job_info.has_record and after_success_func:
                proc_link_publish_flg = job_type in (
                    JobType.FACTORY_IMPORT,
                    JobType.CSV_IMPORT,
                )
                after_success_func(publish=proc_link_publish_flg)

            # Added. Support to get return value from generator
            return_val = stop_instance.value
            # stop while loop
            #
            break
        except Exception as e:
            # update job status
            db.session.rollback()
            message = error_msg_handler.msg_from_exception(exception=e)
            job = update_job_management(job, message)
            logger.exception(str(e))
            traceback.print_exc()
            break
        finally:
            # notify if data type error greater than 100
            if notify_data_type_error_flg and prev_job_info and prev_job_info.data_type_error_cnt > COMPLETED_PERCENT:
                # background_announcer.announce(job.db_name, AnnounceEvent.DATA_TYPE_ERR.name)
                # process_queue.put_nowait((job.db_name, AnnounceEvent.DATA_TYPE_ERR.name))
                # process_queue.send((job.db_name, AnnounceEvent.DATA_TYPE_ERR.name))
                dic_progress[job.id] = (job.db_name, AnnounceEvent.DATA_TYPE_ERR.name)

                dic_res[job.id][DATA_TYPE_ERR] = True
                notify_data_type_error_flg = False

            # emit info
            dic_res[job.id][DONE_PERCENT] = job.done_percent
            dic_res[job.id][END_TM] = job.end_tm
            dic_res[job.id][DURATION] = round((dt.datetime.utcnow() - start_tm).total_seconds(), 2)
            # background_announcer.announce(dic_res, AnnounceEvent.JOB_RUN.name)
            # process_queue.put_nowait((dic_res, AnnounceEvent.JOB_RUN.name))
            # process_queue.send((dic_res, AnnounceEvent.JOB_RUN.name))
            dic_progress[job.id] = (dic_res, AnnounceEvent.JOB_RUN.name)

    dic_res[job.id][STATUS] = str(job.status)
    # background_announcer.announce(dic_res, AnnounceEvent.JOB_RUN.name)
    # process_queue.put_nowait((dic_res, AnnounceEvent.JOB_RUN.name))
    # process_queue.send((dic_res, AnnounceEvent.JOB_RUN.name))
    dic_progress[job.id] = (dic_res, AnnounceEvent.JOB_RUN.name)
    if job.job_type == JobType.CSV_IMPORT.name:
        dic_register_progress = {
            'status': job.status,
            'process_id': job.process_id,
            'is_first_imported': False,
        }
        dic_progress[f'{job.id}_register_by_file'] = (dic_register_progress, AnnounceEvent.DATA_REGISTER.name)

    if return_job_id:
        job_id = job.id if job else None
        return return_val, job_id

    return return_val


def update_job_management(job, err=None):
    """update job status

    Arguments:
        job {[type]} -- [description]
        done_percent {[type]} -- [description]
    """
    with make_session() as meta_session:
        job = JobManagement(**job.__dict__)
        if (
            not err
            and not job.error_msg
            and (
                job.done_percent == COMPLETED_PERCENT or job.status in (JobStatus.PROCESSING.name, JobStatus.DONE.name)
            )
        ):
            job.status = JobStatus.DONE.name
            job.done_percent = 100
            job.error_msg = None
        else:
            job.status = JobStatus.FATAL.name if job.status == JobStatus.FATAL else JobStatus.FAILED.name
            job.error_msg = err or job.error_msg or UNKNOWN_ERROR_TEXT

        job.duration = round((dt.datetime.utcnow() - job.start_tm).total_seconds(), 2)
        job.end_tm = get_current_timestamp()

        meta_session.merge(job)
        job = DictToClass(**job.as_dict())

    return job


def should_save_import_csv_history(job_type):
    return job_type in (JobType.PULL_CSV_DATA, JobType.PULL_PAST_CSV_DATA)


def should_save_import_factory_history(job_type):
    return job_type in (
        JobType.PULL_DB_DATA,
        JobType.PULL_PAST_DB_DATA,
        JobType.TRANSACTION_IMPORT,
        JobType.TRANSACTION_PAST_IMPORT,
        JobType.PULL_CSV_DATA,
        JobType.PULL_PAST_CSV_DATA,
    )


def insert_csv_import_info(job, start_tm, job_info):
    """insert csv import information

    Arguments:
        job {[type]} -- [description]
        file_name {[type]} -- [description]
        imported_row {[type]} -- [description]
        error_msg {[type]} -- [description]
    """

    with make_session() as session:
        csv_import_mana = CsvImport()
        csv_import_mana.job_id = job.id
        csv_import_mana.data_table_id = job.data_table_id
        csv_import_mana.process_id = job.process_id
        csv_import_mana.file_name = convert_list_file_to_pickle(job_info.target)
        csv_import_mana.status = str(job_info.status)
        csv_import_mana.imported_row = job_info.committed_count
        csv_import_mana.error_msg = job_info.err_msg
        csv_import_mana.start_tm = job.start_tm or start_tm
        csv_import_mana.end_tm = job.end_tm or get_current_timestamp()
        session.add(csv_import_mana)

    return csv_import_mana


def insert_factory_import_info(job, start_tm, job_info, job_type):
    """
    import history (efa v2 others)

    :param job:
    :param start_tm:
    :param job_info:
    :param job_type:
    :return:
    """
    import_frm = job_info.auto_increment_start_tm or None
    import_to = job_info.auto_increment_end_tm or None

    with make_session() as session:
        fac_import_mana = FactoryImport()
        fac_import_mana.job_id = job.id
        fac_import_mana.data_table_id = job.data_table_id
        fac_import_mana.process_id = job_info.process_id
        fac_import_mana.import_type = str(job.job_type)
        fac_import_mana.import_from = import_frm
        fac_import_mana.import_to = import_to

        if job_info.cycle_start_tm:
            fac_import_mana.cycle_start_tm = convert_time(job_info.cycle_start_tm, return_string=False)

        if job_info.cycle_end_tm:
            fac_import_mana.cycle_end_tm = convert_time(job_info.cycle_end_tm, return_string=False)

        fac_import_mana.is_duplicate_checked = job_type == JobType.CSV_IMPORT.name
        fac_import_mana.status = str(job_info.status)
        fac_import_mana.imported_row = job_info.committed_count
        fac_import_mana.imported_cycle_id = int(job_info.imported_cycle_id) if job_info.imported_cycle_id else None
        fac_import_mana.error_msg = job_info.err_msg
        fac_import_mana.start_tm = start_tm
        fac_import_mana.end_tm = get_current_timestamp()
        session.add(fac_import_mana)

    return fac_import_mana


class JobInfo:
    job_id: int
    auto_increment_col_timezone: bool
    percent: int
    status: JobStatus
    row_count: int
    committed_count: int
    imported_cycle_id: int  # added 2021/06/30
    has_record: bool
    exception: Exception
    empty_files: List[str]
    data_table_id: int
    process_id: int
    dic_imported_row: dict
    import_type: str
    import_from: str
    import_to: str

    def __init__(self):
        self.job_id = None
        self.data_table_id = None
        self.target = []  # type: List[str]
        self.percent = 0
        self.status = JobStatus.PROCESSING
        self.row_count = 0
        self.imported_cycle_id = 0
        self.auto_increment_start_tm = None
        self.auto_increment_end_tm = None
        self.cycle_start_tm = None
        self.cycle_end_tm = None
        self.auto_increment_col_timezone = False
        self.empty_files = None
        self.job_type = None
        self.detail = ''

        # private
        self._exception = None
        self._committed_count = 0
        self._err_msg = None

        self.start_tm = None
        self.end_tm = None

        # 累計(Cumulative)
        self.has_record = False
        self.has_error = None
        self.data_type_error_cnt = 0

        self.process_id = None
        self.data_table_id = None

        # meta data for target files in chunk
        self.dic_imported_row = None
        self.import_type = None
        self.import_from = None
        self.import_to = None

    @property
    def committed_count(self):
        return self._committed_count

    @committed_count.setter
    def committed_count(self, val: int):
        if val:
            self.has_record = True

        self._committed_count = val

    @property
    def err_msg(self):
        return self._err_msg

    @err_msg.setter
    def err_msg(self, val: str):
        if val:
            self.has_error = True

        self._err_msg = val or None

    @property
    def exception(self):
        return self._exception

    @exception.setter
    def exception(self, val: Exception):
        if val:
            self.has_error = True

        self._exception = val or None

    def calc_percent(self, row_count, total):
        percent = row_count * COMPLETED_PERCENT / total
        percent = math.floor(percent)
        if percent >= COMPLETED_PERCENT:
            percent = ALMOST_COMPLETE_PERCENT

        self.percent = percent


def format_factory_date_to_meta_data(date_val, is_tz_col):
    if is_tz_col:
        convert_utc_func, _ = choose_utc_convert_func(date_val)
        date_val = convert_utc_func(date_val)
        date_val = date_val.replace('T', HALF_WIDTH_SPACE)
        regex_str = r'(\.)(\d{3})(\d{3})'
        date_val = re.sub(regex_str, '\\1\\2', date_val)
    else:
        date_val = convert_time(
            date_val,
            format_str=DATE_FORMAT_STR_FACTORY_DB,
            only_millisecond=True,
        )

    return date_val


@log_execution_time()
def get_job_detail_service(job_id):
    """
    Get all job details of a job
    :param job_id:
    :return:
    """
    job = db.session.query(JobManagement).filter(JobManagement.id == job_id).first()
    job_details_as_dict = {}
    if job and job.process_id:
        if job.job_type == JobType.CSV_IMPORT.name:
            job_details = CsvImport.get_error_jobs(job_id=job_id)
        else:
            job_details = FactoryImport.get_error_jobs(job_id=job_id)

        if job.error_msg:
            job_details = [job] + job_details

        for job_detail in job_details:
            job_details_as_dict[job_detail.id] = row2dict(job_detail)

    return job_details_as_dict


@log_execution_time()
def row2dict(row):
    """
    Convert SQLAlchemy returned object to dictionary
    :param row:
    :return:
    """
    object_as_dict = {}
    for column in row.__table__.columns:
        object_as_dict[column.name] = str(getattr(row, column.name))

    return object_as_dict


def update_job_management_status_n_error(job_mana_id, job_info: JobInfo):
    with make_session():
        job = JobManagement.get_by_id(job_mana_id)
        if not job.status or job_info.status.value > JobStatus[job.status].value:
            job.status = job_info.status.name

        if job_info.err_msg:
            if job.error_msg:
                job.error_msg += job_info.err_msg
            else:
                job.error_msg = job_info.err_msg

        # reset job info
        job_info.err_msg = None

    return job, job_info
