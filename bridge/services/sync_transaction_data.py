import pickle
from typing import List, Tuple

from ap.common.common_utils import split_grpc_limitation, split_grpc_limitation_for_archived_cycle
from ap.common.constants import COLS, FETCH_MANY_SIZE, ROWS, DataType, JobType
from ap.common.logger import log_execution_time
from ap.common.pydn.dblib.db_common import SqlComparisonOperator
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.archived_cycle import ArchivedCycle
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.t_csv_import import CsvImport
from bridge.models.t_factory_import import FactoryImport
from bridge.models.t_job_management import JobManagement
from bridge.models.t_process import Process
from grpc_server.services.grpc_service_proxy import grpc_api_stream

# (factory_imports:List[FactoryImport], cnt_imported_records:int, from_date:datetime, to_date:datetime)
from grpc_src.models.sync_transaction_pb2 import ResponseSyncBridgeToEdge

META_DATA = 'meta_data'

# (column_names:List[str], data_ints:List[Tuple(table t_data_int)], data_floats, data_datetimes, data_texts)
TRANSACTION_DATA = 'transaction_data'

# Edge Server's consumer use this. See request_for_imported_transaction
SYNC_ORDER: [List[Tuple[BridgeStationModel]]] = [(Process,), (CsvImport, FactoryImport)]

TRANSACTION_TABLE = [*SYNC_ORDER[0]]


def get_sync_order_table_name():
    list_return = []
    for _tuple in SYNC_ORDER:
        list_return.append(tuple([cls.get_original_table_name() for cls in _tuple]))
    return list_return


@log_execution_time('get_transaction_data_by_range')
def get_transaction_data_by_range(db_instance: PostgreSQL, t_proc_id, from_cycle, to_cycle):
    outputs = []
    for model_cls in ():
        cols, rows = get_records_from_t_data(db_instance, t_proc_id, from_cycle, to_cycle, model_cls)
        if rows:
            outputs.append((model_cls, cols, rows))

    return outputs


def get_import_metadata(db_instance, cfg_proc_id, request_from=None):
    tup_factory_imports = get_factory_import_by_from_cycle_id(db_instance, cfg_proc_id, request_from)
    if not tup_factory_imports:
        return None

    factory_imports, cnt_imported_records, last_cycle_id = tup_factory_imports  # unpacking

    from_cycle = request_from
    to_cycle = factory_imports[-1].imported_cycle_id
    if not factory_imports:
        meta_data = (factory_imports, cnt_imported_records, last_cycle_id, None, None)
    else:
        meta_data = (factory_imports, cnt_imported_records, last_cycle_id, from_cycle, to_cycle)
    return meta_data


def get_records_from_t_data(db_instance: PostgreSQL, process_id, get_from, get_to, model_cls):
    dic_conditions = {model_cls.Columns.process_id.name: process_id}
    if get_from and get_to:
        dic_conditions.update(
            {
                model_cls.Columns.cycle_id.name: [
                    (SqlComparisonOperator.GREATER_THAN, get_from),
                    (SqlComparisonOperator.LESS_THAN_OR_EQ, get_to),
                ],
            },
        )
    elif get_from:
        dic_conditions.update({model_cls.Columns.cycle_id.name: [(SqlComparisonOperator.GREATER_THAN, get_from)]})
    elif get_to:
        dic_conditions.update({model_cls.Columns.cycle_id.name: [(SqlComparisonOperator.LESS_THAN_OR_EQ, get_to)]})

    select_col = model_cls.Columns.get_column_names()
    # TODO: re-check use TO_CHAR
    # datetime_columns = [col.name for col in model_cls.Columns if col.value[1] == DataType.DATETIME]
    # dict_aggregate_function = {col: (AggregateFunction.TO_CHAR.name, col, add_single_quote(SQL_DATE_FORMAT_STR))
    #                            for col in datetime_columns}

    return model_cls.select_records(
        db_instance,
        dic_conditions=dic_conditions,
        select_cols=select_col,
        row_is_dict=False,
    )
    # return model_cls.select_records(db_instance, dic_conditions=dic_conditions, select_cols=select_col,
    #                                 dict_aggregate_function=dict_aggregate_function, row_is_dict=False)


def get_factory_import_by_from_cycle_id(
    db_instance: PostgreSQL,
    config_process_id,
    import_from,
    max_record=None,
) -> (List, int):
    """
    A simple version of get_factory_import_by_from_date.
    :param db_instance:
    :param proc_id: config process id
    :param import_from:
    :param import_to:
    :param max_record:
    :return:
    """
    if not max_record:
        max_record = FETCH_MANY_SIZE

    # t_process = Process.get_by_process_id(db_instance, proc_id)
    # config_process_id = t_process.config_process_id if t_process else None

    if not config_process_id:
        return None

    factory_imports = FactoryImport.get_done_histories(
        db_instance,
        config_process_id,
        import_from,
        (JobType.TRANSACTION_IMPORT.name,),
    )
    if not factory_imports:
        return None

    return filter_transaction_data_by_max_record(factory_imports, max_record)


def filter_transaction_data_by_max_record(factory_imports, max_record) -> (List, int):
    total_cnt_imported_records = sum([fi_record.imported_row for fi_record in factory_imports])
    last_cycle_id = factory_imports[-1].imported_cycle_id  # supports edge re-send request if not reach last
    cnt = 0
    _factory_imports = []  # append until sum(imported_row) greater then max_record
    if total_cnt_imported_records > max_record:
        for fi_record in factory_imports:
            if cnt > max_record:
                break
            cnt += fi_record.imported_row
            _factory_imports.append(fi_record)

        if not _factory_imports:
            _factory_imports.append(factory_imports[0])  # To make sure always have at least 1 records
        return _factory_imports, cnt, last_cycle_id
    return factory_imports, total_cnt_imported_records, last_cycle_id


def get_new_jobs_from_factory_imports(db_instance, proc_id, factory_imports, request_from):
    min_job_id = min(factory_imports, key=lambda fi: fi.job_id).job_id if factory_imports else None
    if not min_job_id:
        latest_factory_import = FactoryImport.get_latest_records_by_imported_cycle_id(
            db_instance,
            proc_id,
            request_from,
        )
        min_job_id = latest_factory_import.job_id if latest_factory_import else None
    if min_job_id:
        jobs = JobManagement.get_new_jobs(db_instance, proc_id, min_job_id, is_return_dict=True)
    else:
        jobs = JobManagement.get_by_proc_id(db_instance, proc_id, is_return_dict=True)
    return jobs


def build_grpc_response_msg(t_proc_id, column_names, data_type, imported_records, last_cycle_id):
    response = ResponseSyncBridgeToEdge(
        process_id=t_proc_id,
        column_names=column_names,
        data_type=data_type,
        imported_records=imported_records,
        last_cycle_id=last_cycle_id,
    )
    return response


def gen_response(
    t_proc_id,
    column_names,
    data_type: DataType,
    rows: List[Tuple],
    imported_records=0,
    last_cycle_id=False,
):
    data_type = str(data_type) if data_type else None
    # response_msg = build_grpc_response_msg(t_proc_id, column_names, data_type, imported_records, last_cycle_id)
    for data_chunk in split_grpc_limitation(rows):
        # response_msg.data = pickle.dumps(data_chunk)
        yield t_proc_id, column_names, data_type, imported_records, last_cycle_id, pickle.dumps(data_chunk)


@grpc_api_stream()
def get_archived_cycle(job_ids):
    # get changed data
    dic_conditions = {ArchivedCycle.Columns.job_id.name: [(SqlComparisonOperator.IN, tuple(job_ids))]}
    select_cols = [
        ArchivedCycle.Columns.process_id.name,
        ArchivedCycle.Columns.archived_ids.name,
        ArchivedCycle.Columns.updated_at.name,
    ]
    with BridgeStationModel.get_db_proxy() as db_instance:
        cols, rows = ArchivedCycle.select_records(
            db_instance,
            select_cols=select_cols,
            dic_conditions=dic_conditions,
            dic_order_by=[ArchivedCycle.Columns.job_id.name],
            row_is_dict=False,
        )
    for chunk_rows in split_grpc_limitation_for_archived_cycle(rows):
        dic_output = {COLS: cols, ROWS: chunk_rows}
        yield dic_output
