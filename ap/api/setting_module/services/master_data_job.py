from datetime import datetime

import pandas as pd
from apscheduler.triggers.date import DateTrigger
from pytz import utc

from ap import scheduler
from ap.common.constants import AnnounceEvent, JobType
from ap.common.logger import log_execution_time, logger
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.scheduler import add_job_to_scheduler, scheduler_app_context
from ap.common.services.sse import background_announcer
from ap.setting_module.models import CfgDataTable
from ap.setting_module.services.background_process import send_processing_info
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_data_table import CfgDataTable as BSCfgDataTable
from bridge.models.cfg_process import CfgProcess
from bridge.models.mapping_factory_machine import MappingFactoryMachine
from bridge.redis_utils.db_changed import ChangedType, publish_transaction_changed
from bridge.services.master_data_import import scan_master
from bridge.services.scan_data_type import scan_data_type


def add_scan_master_job(data_table_id):
    # method scan_master_job
    job_name = JobType.SCAN_MASTER.name
    job_id = f'{job_name}_{data_table_id}'
    trigger = DateTrigger(datetime.now().astimezone(utc), timezone=utc)
    # interval_sec = CfgConstant.get_value_by_type_first(CfgConstantType.POLLING_FREQUENCY.name, int)
    # trigger = IntervalTrigger(seconds=interval_sec, timezone=utc)
    cfg_data_table = CfgDataTable.get_by_id(data_table_id)

    dic_import_param = {
        'data_table_id': cfg_data_table.id,
        '_job_id': job_id,
        '_job_name': job_name,
        '_db_id': cfg_data_table.data_source_id,
        '_data_table_id': cfg_data_table.id,
        '_data_table_name': cfg_data_table.name,
    }

    scheduler.add_job(
        id=job_id,
        name=job_name,
        func=scan_master_job,
        replace_existing=True,
        trigger=trigger,
        next_run_time=datetime.now().astimezone(utc),
        kwargs=dic_import_param,
    )

    logger.info(f'Add Job : {job_id} DONE')


@scheduler_app_context
@log_execution_time()
def scan_master_job(
    _job_id=None,
    _job_name=None,
    _db_id=None,
    _data_table_id=None,
    _data_table_name=None,
    *args,
    **kwargs,
):
    is_for_test = kwargs.pop('is_for_test', None)

    # start job
    logger.info('{0} proc_id: {1}'.format('SCAN MASTER MAIN FUNCTION', _data_table_id))
    generator_import_factory = scan_master(*args, **kwargs)
    dict_records_count, *_ = send_processing_info(
        generator_import_factory,
        JobType.SCAN_MASTER,
        data_table_id=_data_table_id,
        is_check_disk=False,
        is_run_one_time=True,
    )

    if is_for_test:
        return

    total_records = sum(list(dict_records_count.values())) if dict_records_count else 0
    # TODO: if zero : scan master target will False , so it will be run again ( take time )
    if total_records:
        # tunghh note: version dùng GUI thì ở đây không cần publish_transaction_changed cũng được.
        # mà publish_transaction_changed lúc GUI thực hiện insert

        publish_transaction_changed(_data_table_id, ChangedType.SCAN_MASTER)
        add_job_scan_data_type(_data_table_id)
    else:
        cfg_data_table: CfgDataTable = CfgDataTable.get_by_id(_data_table_id)
        send_sse_show_process_cfg_modal([cfg_data_table.id])


def add_job_scan_data_type(data_table_id):
    job_name = JobType.SCAN_DATA_TYPE.name
    job_id = f'{job_name}_{data_table_id}'
    trigger = DateTrigger(datetime.now().astimezone(utc), timezone=utc)
    cfg_data_table: CfgDataTable = CfgDataTable.get_by_id(data_table_id)

    dic_import_param = {
        'data_table_id': cfg_data_table.id,
        '_job_id': job_id,
        '_job_name': job_name,
        '_db_id': cfg_data_table.data_source_id,
        '_data_table_id': cfg_data_table.id,
        '_data_table_name': cfg_data_table.name,
    }

    add_job_to_scheduler(job_id, job_name, trigger, scan_data_type_job, True, dic_import_param)


@scheduler_app_context
@log_execution_time()
def scan_data_type_job(
    _job_id=None,
    _job_name=None,
    _db_id=None,
    _data_table_id=None,
    _data_table_name=None,
    *args,
    **kwargs,
):
    is_for_test = kwargs.pop('is_for_test', None)

    # start job
    logger.info('{0} proc_id: {1}'.format('SCAN DATA TYPE MAIN FUNCTION', _data_table_id))
    generator = scan_data_type(*args, **kwargs)
    send_processing_info(
        generator,
        JobType.SCAN_DATA_TYPE,
        data_table_id=_data_table_id,
        is_check_disk=False,
        is_run_one_time=True,
    )

    if is_for_test:
        return

    publish_transaction_changed(_data_table_id, ChangedType.SCAN_DATA_TYPE)
    send_sse_show_process_cfg_modal([_data_table_id])


@BridgeStationModel.use_db_instance()
def send_sse_show_process_cfg_modal(data_table_ids: list[int], db_instance: PostgreSQL = None):
    rows = MappingFactoryMachine.get_process_id_with_data_table_id(db_instance, data_table_ids)
    df = pd.DataFrame(rows)
    if df.empty:
        return

    # Send SSE messages from Bridge to GUI
    cfg_process_infos = {}
    for process_id, df in df.groupby(by='process_id'):
        cfg_process = CfgProcess.get_by_id(db_instance, process_id)
        if cfg_process is None:
            continue

        table_ids = df['data_table_id'].unique().tolist()
        data_table_infos = []
        data_source_infos = []
        for data_table_id in table_ids:
            cfg_data_table = BSCfgDataTable.get_by_id(db_instance, data_table_id, is_cascade=True)
            dic_data_table = cfg_data_table.to_dict(cfg_data_table)
            dic_data_source = cfg_data_table.data_source.to_dict(cfg_data_table.data_source)
            # TODO : don't use string name
            dic_data_table['data_source'] = dic_data_source
            data_table_infos.append(dic_data_table)
            data_source_infos.append(dic_data_source)

        cfg_process_info = cfg_process.to_dict(cfg_process)
        cfg_process_info['shown_name'] = cfg_process.shown_name
        cfg_process_info['data_tables'] = data_table_infos
        cfg_process_info['data_sources'] = data_source_infos
        cfg_process_infos[process_id] = cfg_process_info

    background_announcer.announce(cfg_process_infos, AnnounceEvent.PROC_ID.name)
