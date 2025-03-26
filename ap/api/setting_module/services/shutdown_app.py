import math
import os
import time
from datetime import datetime
from typing import Generator

from apscheduler.triggers.date import DateTrigger
from flask import request
from pytz import utc

from ap import dic_config, scheduler
from ap.common.common_utils import get_process_queue
from ap.common.constants import (
    PROCESS_QUEUE,
    SHUTDOWN_APP_TIMEOUT,
    SHUTDOWN_APP_WAITING_INTERVAL,
    AnnounceEvent,
    JobType,
    ListenNotifyType,
)
from ap.common.logger import log_execution_time, logger, logger_force_flush
from ap.common.scheduler import get_running_jobs, scheduler_app_context
from ap.common.services.import_export_config_and_master_data import pause_job_running
from ap.common.services.sse import background_announcer
from ap.setting_module.services.background_process import send_processing_info

SAFE_TO_ROLLBACK_JOBS = {JobType.PROC_LINK_COUNT.name}


@log_execution_time()
def shut_down_app():
    logger.info('///////////// SHUTDOWN APP ///////////')
    add_shutdown_app_job()

    # need to pause scheduler in main thread
    pause_job_running(remove_jobs=False)

    shutdown_function = request.environ.get('werkzeug.server.shutdown')
    if shutdown_function is not None:
        shutdown_function()


@log_execution_time()
def add_shutdown_app_job():
    shutdown_app_job_id = JobType.SHUTDOWN_APP.name
    scheduler.add_job(
        shutdown_app_job_id,
        shutdown_app_job,
        trigger=DateTrigger(run_date=datetime.now().astimezone(utc), timezone=utc),
        replace_existing=True,
        kwargs={
            '_job_id': shutdown_app_job_id,
            '_job_name': JobType.SHUTDOWN_APP.name,
        },
    )


@scheduler_app_context
def shutdown_app_job(_job_id=None, _job_name=None, *args, **kwargs):
    """scheduler job to shutdown app

    Keyword Arguments:
        _job_id {[type]} -- [description] (default: {None})
        _job_name {[type]} -- [description] (default: {None})
    """
    try:
        gen = waiting_for_shutdown(*args, **kwargs)
        send_processing_info(gen, JobType.SHUTDOWN_APP, is_check_disk=False)
    except Exception as e:
        logger.exception(e)


def handle_shutdown_app(is_main: bool = False) -> None:
    """handle `SHUTDOWN`
    Here what we do:
      - Shutdown scheduler
      - Notify front-end, so it can tell users about the shutdown
    """
    if not is_main:
        dic_config[PROCESS_QUEUE] = get_process_queue()
        dic_config[PROCESS_QUEUE][ListenNotifyType.SHUTDOWN.name][True] = True
        return

    background_announcer.announce(True, AnnounceEvent.SHUT_DOWN.name, is_main=is_main)

    # should use `stop_scheduler` function
    if scheduler.running:
        scheduler.shutdown(wait=False)

    logger_force_flush()

    os._exit(0)


def can_shutdown_app() -> bool:
    """We should shut down application if and only if there are no others job running.
    Which means only one ``SHUTDOWN_APP`` job is currently running.
    """

    shutdown_job = {JobType.SHUTDOWN_APP.name}
    running_jobs = set(get_running_jobs().values())
    logger.info(f'Check shutdown app, running jobs: {running_jobs}')

    # check if we are shutting down app
    if not shutdown_job.issubset(running_jobs):
        return False

    running_jobs -= shutdown_job

    # exclude jobs that we can force to stop without loss of data
    running_jobs -= SAFE_TO_ROLLBACK_JOBS

    return len(running_jobs) == 0


@log_execution_time()
def waiting_for_shutdown() -> Generator[int, None, None]:
    """pause scheduler and wait for all other jobs done.

    Arguments:
        proc_id {[type]} -- [description]

    Keyword Arguments:
        db_id {[type]} -- [description] (default: {None})

    Yields:
        [type] -- [description]
    """
    yield 0

    start_time = time.time()

    while True:
        time_since_shutting_down = time.time() - start_time
        if time_since_shutting_down > SHUTDOWN_APP_TIMEOUT or can_shutdown_app():
            break

        percent = min(math.ceil(time_since_shutting_down / SHUTDOWN_APP_TIMEOUT * 100), 99)
        yield percent

        time.sleep(SHUTDOWN_APP_WAITING_INTERVAL)

    handle_shutdown_app()
    yield 100
