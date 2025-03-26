import datetime as dt
from datetime import datetime

from apscheduler.triggers.date import DateTrigger
from pytz import utc

from ap.common.clean_old_data import clean_old_files
from ap.common.common_utils import get_data_path
from ap.common.constants import JobType
from ap.common.logger import log_execution_time, logger
from ap.common.scheduler import scheduler, scheduler_app_context
from ap.common.services.import_export_config_and_master_data import (
    pause_job_running,
    set_break_job_flag,
)
from ap.setting_module.services.background_process import send_processing_info
from bridge.models.archived_cycle import ArchivedCycle
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.semi_master import SemiMaster
from bridge.models.t_csv_import import CsvImport
from bridge.models.t_csv_management import CsvManagement
from bridge.models.t_factory_import import FactoryImport
from bridge.models.t_proc_data_count import ProcDataCount
from bridge.models.t_proc_link_count import ProcLinkCount


def run_clean_trans_tables_job():
    """Trigger cleaning transaction tables job
    :return:
    """
    # pause job
    pause_job_running()

    clean_job_id = f'{JobType.TRANSACTION_CLEAN.name}'
    scheduler.add_job(
        clean_job_id,
        clean_trans_data_job,
        trigger=DateTrigger(run_date=datetime.now().astimezone(utc), timezone=utc),
        replace_existing=True,
        next_run_time=dt.datetime.now().astimezone(utc),
        kwargs={'_job_id': clean_job_id, '_job_name': clean_job_id},
    )


@scheduler_app_context
def clean_trans_data_job(_job_id=None, _job_name=None, *args, **kwargs):
    """scheduler job to delete process from db

    Keyword Arguments:
        _job_id {[type]} -- [description] (default: {None})
        _job_name {[type]} -- [description] (default: {None})
    """
    gen = clean_transaction(*args, **kwargs)
    send_processing_info(gen, JobType.TRANSACTION_CLEAN, is_check_disk=False)
    # run_clean_data_job(JobType.CLEAN_DATA.name, folder=get_data_path(), num_day_ago=-1, job_repeat_sec=24 * 60 * 60)


@log_execution_time()
def clean_transaction():
    tables = (
        ArchivedCycle,
        SemiMaster,
        CsvImport,
        CsvManagement,
        FactoryImport,
        ProcDataCount,
        ProcLinkCount,
    )
    table_names = [table._table_name for table in tables]

    yield 0
    with BridgeStationModel.get_db_proxy() as db_instance:
        all_table = db_instance.list_tables()
        t_process_tables = [table_name for table_name in all_table if 't_process_' in table_name]
        table_names = table_names + t_process_tables
        percent_step = 100 // len(table_names)
        for idx, table_name in enumerate(table_names, start=1):
            sql = f'TRUNCATE TABLE {table_name} CASCADE'
            db_instance.execute_sql(sql)
            yield idx * percent_step

    logger.info('TRUNCATE ALL TRANSACTION TABLES')
    list(clean_old_files(get_data_path(), num_day_ago=-1))
    logger.info('DELETE ALL TRANSACTION FILES')
    # reset the break job constant
    set_break_job_flag(False)
    yield 100
