from datetime import datetime, timedelta

from apscheduler.triggers.date import DateTrigger
from pytz import utc

from ap import scheduler
from ap.common.constants import CfgConstantType, CRUDType, JobType
from ap.common.model_utils import get_dic_tablename_models
from ap.common.scheduler import scheduler_app_context
from ap.setting_module.models import (
    CfgConstant,
    ConfigDBModel,
    FactoryImport,
    MasterDBModel,
    SemiMasterDBModel,
    get_models,
    make_session,
)
from ap.setting_module.services.background_process import JobInfo, send_processing_info
from bridge.services.sync_config_master import get_config_master_changed_data
from grpc_server.sync_transaction import sync_proc_link_jobs, sync_transaction_jobs


def sync_master_jobs():
    job_name = JobType.SYNC_MASTER.name
    job_id = job_name
    scheduler.add_job(
        job_id,
        sync_master_wrap,
        name=job_name,
        replace_existing=True,
        trigger=DateTrigger(datetime.now().astimezone(utc), timezone=utc),
    )


def sync_config_jobs(call_sync_trans=False):
    job_name = JobType.SYNC_CONFIG.name
    job_id = job_name
    scheduler.add_job(
        job_id,
        sync_config_wrap,
        name=job_name,
        replace_existing=True,
        trigger=DateTrigger(datetime.now().astimezone(utc) + timedelta(seconds=10), timezone=utc),
        args=(call_sync_trans,),
    )


@scheduler_app_context
def sync_master_wrap():
    send_processing_info(sync_all_masters(), JobType.SYNC_MASTER)


@scheduler_app_context
def sync_config_wrap(call_sync_trans=False):
    send_processing_info(sync_all_configs(), JobType.SYNC_CONFIG)
    if call_sync_trans:
        sync_transaction_jobs()
        sync_proc_link_jobs()


def save_config_master_from_redis(table_name=None, crud_type=None, id=None, dict_data=None):
    """
    sync config , master
    :param table_name:
    :param crud_type:
    :param id:
    :param dict_data:
    :return:
    """
    if table_name is None:
        # TODO : need ?
        # sync_all_configs()
        # sync_all_masters()
        return

    dic_table_class = get_dic_tablename_models([ConfigDBModel, MasterDBModel, SemiMasterDBModel])
    cls = dic_table_class.get(table_name)
    if cls is None:
        return

    # TODO : not need ?
    if id is None and dict_data is None:
        sync_model(cls)
        return

    dict_data = remove_deleted_at_key(dict_data)
    with make_session() as session:
        primary_key = cls.get_primary_keys()
        if crud_type == CRUDType.DELETE.name:
            session.query(cls).filter(primary_key == id).delete()
        else:
            current_record = session.query(cls).filter(primary_key == id).first()
            if current_record:
                if str(dict_data.get(cls.updated_at.key, 0)) > str(current_record.updated_at):
                    current_record.update_by_dict(dict_data)
            else:
                record = cls.create_instance(dict_data)
                session.add(record)

        # save the latest time to constant
        CfgConstant.create_or_update_by_type(
            session,
            const_type=CfgConstantType.SYNC_MASTER_CONFIG.name,
            const_name=table_name,
            const_value=dict_data[cls.updated_at.key],
        )

    return True


# def get_changed_data(table_name):
#     """
#     get changed from Bridge Station
#     :param table_name:
#     :return:
#     """
#     updated_at = CfgConstant.get_value_by_type_name(CfgConstantType.SYNC_MASTER_CONFIG.name, table_name) or None
#     dict_data = {DB_INSTANCE: None, TABLE_NAME: table_name, UPDATED_AT: updated_at}
#     request_msg = GenericRequestParams(method=GenericGrpcFunc.SYNC_CONFIG_MASTER.name,
#                                        binParams=pickle.dumps(dict_data))
#     method_name = SettingServicer.GenericGrpcMethod.__name__
#     response = call_grpc(SettingStub, method_name, request_msg)
#     output = pickle.loads(response.binResponse)
#     return output
#
#
def save_changed_data(table_name, columns, rows, archived_columns, archived_rows, **kwargs):
    dic_table_class = get_dic_tablename_models([ConfigDBModel, MasterDBModel, SemiMasterDBModel, FactoryImport])
    cls = dic_table_class.get(table_name)
    if cls is None:
        return

    date_times = []
    with make_session() as session:
        if rows:
            date_times = cls.insert_records(columns, rows, session)

        # date_times = []
        # # update , insert
        # for row in rows or []:
        #     # dict_data = remove_deleted_at_key(dict(zip(columns, row)))
        #     dict_data = dict(zip(columns, row))
        #     record = cls(**dict_data)
        #     session.merge(record)
        #     if record.updated_at:
        #         date_times.append(record.updated_at)

        # delete ids
        if archived_columns and archived_rows:
            archived_ids = []
            for id, updated_at in archived_rows:
                # TODO: archive cycle ids
                # ids = json.load(ids)
                # archived_ids.extend(ids)
                archived_ids.append(id)
                if updated_at:
                    date_times.append(updated_at)

            cls.delete_by_ids(ids=archived_ids, session=session)

        # save the latest time to constant
        date_times = [_dt for _dt in date_times if _dt]

        if date_times:
            max_date = max(date_times)
            CfgConstant.create_or_update_by_type(
                session,
                const_type=CfgConstantType.SYNC_MASTER_CONFIG.name,
                const_name=table_name,
                const_value=max_date,
            )

    return True


def sync_model(model):
    table_name = model.__tablename__
    updated_at = CfgConstant.get_value_by_type_name(CfgConstantType.SYNC_MASTER_CONFIG.name, table_name)
    data = get_config_master_changed_data(table_name, updated_at)
    # data = get_changed_data(table_name)
    if data:
        save_changed_data(**data)

    return True


def sync_all_configs():
    yield from sync_config_master_common(
        ConfigDBModel, is_sync_factory_import=True
    )  # sync config_master and sync factory_import


def sync_all_masters():
    yield from sync_config_master_common([MasterDBModel, SemiMasterDBModel])


def sync_config_master_common(db_model, is_sync_factory_import=False):
    yield 0
    models = get_models(db_model)
    if is_sync_factory_import:
        list_models = list(models)
        list_models.append(FactoryImport)
        models = tuple(list_models)
    one_loop_percent = 100 // len(models)
    percent = 0
    job_info = JobInfo()
    for model in models:
        sync_model(model)
        percent += one_loop_percent
        job_info.percent = percent
        yield job_info

    yield 100


def remove_deleted_at_key(dic_data):
    # TODO: remove after clear all deleted_at column on Bridge Station
    key = 'deleted_at'
    if key in dic_data:
        del dic_data[key]

    return dic_data
