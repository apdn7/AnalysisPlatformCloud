from datetime import datetime

from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from pytz import utc

from ap import dic_request_info, scheduler
from ap.api.setting_module.services.data_import import add_transaction_import_job
from ap.common.common_utils import add_seconds
from ap.common.constants import LAST_REQUEST_TIME, CfgConstantType, MasterDBType
from ap.common.logger import log_execution_time
from ap.common.scheduler import (
    JobType,
    add_job_to_scheduler,
    get_job_name_with_ids,
    remove_jobs,
    scheduler_app_context,
)
from ap.setting_module.models import CfgConstant, CfgDataTable, CfgDataTableColumn, CfgProcess
from bridge.models.bridge_station import BridgeStationModel
from bridge.services.csv_management import add_scan_files_job
from bridge.services.etl_services.etl_import import (
    pull_csv_job,
    pull_db_job,
    pull_past_csv_job,
    pull_past_db_job,
)


@log_execution_time()
def change_polling_all_interval_jobs(interval_sec=None, run_now=False, is_user_request: bool = False):
    """add job for csv and factory import

    Arguments:
        interval_sec {[type]} -- [description]

    Keyword Arguments:
        target_job_names {[type]} -- [description] (default: {None})
    """
    # target jobs (do not remove factory past data import)
    target_jobs = [JobType.PULL_CSV_DATA, JobType.PULL_DB_DATA, JobType.TRANSACTION_IMPORT]

    job_dict = get_job_name_with_ids(target_jobs)

    # remove jobs
    remove_jobs(target_jobs)

    if interval_sec is None:
        interval_sec = CfgConstant.get_value_by_type_first(CfgConstantType.POLLING_FREQUENCY.name, int)

    # check if not run now and interval is zero , quit
    if interval_sec == 0 and not run_now:
        return

    for job_name, target_ids in job_dict.items():
        if job_name in [JobType.PULL_CSV_DATA.name, JobType.PULL_DB_DATA.name]:
            for data_table_id in target_ids:
                cfg_data_table = CfgDataTable.get_by_id(data_table_id)
                add_pull_data_job(cfg_data_table, interval_sec=interval_sec, run_now=run_now)
        else:
            for process_id in target_ids:
                add_transaction_import_job(process_id, interval_sec=interval_sec, run_now=run_now, is_past=False)


# etl transaction -> efa / v2 / ...
def add_pull_data_job(cfg_data_table: CfgDataTable, interval_sec=None, run_now=None, import_process_id=None):
    if interval_sec:
        trigger = IntervalTrigger(seconds=interval_sec, timezone=utc)
    else:
        trigger = DateTrigger(datetime.now().astimezone(utc), timezone=utc)

    is_csv = bool(cfg_data_table.data_source.csv_detail)
    master_db_type = cfg_data_table.get_master_type()
    if master_db_type in (MasterDBType.V2.name, MasterDBType.V2_HISTORY.name) or is_csv:
        job_name = JobType.PULL_CSV_DATA.name
        job_id = f'{job_name}_{cfg_data_table.id}'
        dic_import_param = {
            '_job_id': job_id,
            '_job_name': job_name,
            '_db_id': cfg_data_table.data_source_id,
            '_data_table_id': cfg_data_table.id,
            'import_process_id': import_process_id,
        }
        add_job_to_scheduler(job_id, job_name, trigger, pull_csv_job, run_now, dic_import_param)
    else:
        job_name = JobType.PULL_DB_DATA.name
        job_id = f'{job_name}_{cfg_data_table.id}'
        dic_import_param = {
            '_job_id': job_id,
            '_job_name': job_name,
            '_db_id': cfg_data_table.data_source_id,
            '_data_table_id': cfg_data_table.id,
        }
        add_job_to_scheduler(job_id, job_name, trigger, pull_db_job, run_now, dic_import_param)


@log_execution_time()
def add_idle_monitoring_job():
    scheduler.add_job(
        JobType.IDLE_MONITORING.name,
        idle_monitoring,
        name=JobType.IDLE_MONITORING.name,
        replace_existing=True,
        trigger=IntervalTrigger(seconds=5 * 60, timezone=utc),
        kwargs={'_job_id': JobType.IDLE_MONITORING.name, '_job_name': JobType.IDLE_MONITORING.name},
        executor='threadpool',
    )

    return True


@scheduler_app_context
def idle_monitoring(_job_id=None, _job_name=None):
    """
    check if system is idle

    """
    # check last request > now() - 5 minutes
    last_request_time = dic_request_info.get(LAST_REQUEST_TIME, datetime.utcnow())
    if last_request_time > add_seconds(seconds=-5 * 60):
        return

    # pull data
    for cfg_data_table in CfgDataTable.get_all():
        add_pull_past_data_job(cfg_data_table, True)

    # import data
    with BridgeStationModel.get_db_proxy() as db_instance:
        table_names: list[str] = db_instance.list_tables()
        for process in CfgProcess.get_all():
            if process.table_name in table_names:
                add_transaction_import_job(process.id, run_now=True, is_past=True)


def add_pull_past_data_job(cfg_data_table: CfgDataTable, run_now=False):
    trigger = DateTrigger(datetime.now().astimezone(utc), timezone=utc)

    is_csv = bool(cfg_data_table.data_source.csv_detail)
    master_db_type = cfg_data_table.get_master_type()
    if master_db_type in (MasterDBType.V2.name, MasterDBType.V2_HISTORY.name) or is_csv:
        # ['登録日時', '工程名', 'ライン名']
        split_cols = CfgDataTableColumn.get_split_columns(cfg_data_table.id)
        job_name = JobType.PULL_PAST_CSV_DATA.name
        job_id = f'{job_name}_{cfg_data_table.id}'
        dic_import_param = {
            '_job_id': job_id,
            '_job_name': job_name,
            '_db_id': cfg_data_table.data_source_id,
            '_data_table_id': cfg_data_table.id,
        }
        columns = CfgDataTableColumn.get_column_names_by_data_group_types(cfg_data_table.id, split_cols)
        add_scan_files_job(data_table_id=cfg_data_table.id, columns=columns)
        add_job_to_scheduler(job_id, job_name, trigger, pull_past_csv_job, run_now, dic_import_param)
    else:
        job_name = JobType.PULL_PAST_DB_DATA.name
        job_id = f'{job_name}_{cfg_data_table.id}'
        dic_import_param = {
            '_job_id': job_id,
            '_job_name': job_name,
            '_db_id': cfg_data_table.data_source_id,
            '_data_table_id': cfg_data_table.id,
        }
        add_job_to_scheduler(job_id, job_name, trigger, pull_past_db_job, run_now, dic_import_param)

    return True
