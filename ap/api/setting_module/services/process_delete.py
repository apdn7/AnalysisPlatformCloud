from datetime import datetime
from operator import or_

from apscheduler.triggers.date import DateTrigger
from pytz import utc

from ap import db
from ap.common.common_utils import delete_file
from ap.common.constants import PROCESS_ID, AnnounceEvent, JobType
from ap.common.logger import log_execution_time
from ap.common.scheduler import add_job_to_scheduler, remove_jobs, scheduler_app_context
from ap.common.services.sse import background_announcer
from ap.setting_module.models import (
    AutoLink,
    CfgDataSource,
    CfgDataTable,
    CfgProcess,
    MData,
    MProcess,
    ProcLinkCount,
    make_session,
)
from ap.setting_module.services.background_process import send_processing_info
from ap.trace_data.models import ProcDataCount
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.transaction_model import TransactionData
from bridge.services.data_import import get_import_files


@scheduler_app_context
def delete_process_job(_job_id=None, _job_name=None, _process_id=None):
    """scheduler job to delete process from db

    Keyword Arguments:
        _job_id {[type]} -- [description] (default: {None})
        _job_name {[type]} -- [description] (default: {None})
    """
    gen = delete_process(_process_id)
    send_processing_info(
        gen,
        JobType.DEL_PROCESS,
        process_id=_process_id,
        is_check_disk=False,
    )
    announce_data = {PROCESS_ID: _process_id}
    background_announcer.announce(announce_data, AnnounceEvent.DEL_PROCESS.name)


@log_execution_time()
def delete_process(process_id):
    """
    delete processes
    :return:
    """
    yield 0
    # delete transaction table t_process_<process_name_sys>
    with BridgeStationModel.get_db_proxy() as db_instance:
        transaction_data_obj = TransactionData(process_id)
        transaction_data_obj.delete_process(db_instance)

    # delete config and master
    with make_session(db.session) as _:
        MData.hide_col_by_ids(process_id=process_id)
        MProcess.update_by_conditions({MProcess.deleted_at: datetime.utcnow()}, ids=[process_id])
        CfgProcess.query.filter(CfgProcess.id == process_id).delete()
        ProcLinkCount.query.filter(
            or_(ProcLinkCount.process_id == process_id, ProcLinkCount.target_process_id == process_id),
        ).delete()
        ProcDataCount.query.filter(ProcDataCount.process_id == process_id).delete()
        AutoLink.query.filter(AutoLink.process_id == process_id).delete()

    # delete feather file
    data_table_id_with_file_names = get_import_files(process_id)
    for file_names in data_table_id_with_file_names.values():
        for file_name in file_names:
            delete_file(file_name)

    yield 100


@log_execution_time()
def add_del_proc_job(process_id):
    job_name = JobType.DEL_PROCESS.name
    job_id = f'{job_name}_{process_id}'
    trigger = DateTrigger(datetime.now().astimezone(utc), timezone=utc)
    dic_import_param = {'_job_id': job_id, '_job_name': job_name, '_process_id': process_id}
    add_job_to_scheduler(job_id, job_name, trigger, delete_process_job, True, dic_import_param)


@log_execution_time()
def delete_data_table_and_relate_jobs(data_table_id):
    # delete cfg process
    deleted = CfgDataTable.delete(data_table_id)

    # remove job relate to that process
    if deleted:
        # target jobs
        target_jobs = [
            JobType.SCAN_MASTER,
            JobType.SCAN_DATA_TYPE,
            JobType.PULL_DB_DATA,
            JobType.PULL_CSV_DATA,
            JobType.PULL_PAST_CSV_DATA,
            JobType.PULL_PAST_DB_DATA,
        ]
        # remove importing job from job queue
        remove_jobs(target_jobs, data_table_id)


def del_data_source(ds_id):
    """
    delete data source
    :param ds_id:
    :return:
    """
    with make_session() as meta_session:
        ds = meta_session.query(CfgDataSource).get(ds_id)
        if not ds:
            return

        # delete data
        for data_table in ds.data_tables or []:
            delete_data_table_and_relate_jobs(data_table.id)
        meta_session.delete(ds)
