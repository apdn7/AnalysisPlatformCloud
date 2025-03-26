from ap.common.constants import DBType
from ap.common.pydn.dblib.db_common import OrderBy, SqlComparisonOperator
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_csv_column import CfgCsvColumn
from bridge.models.cfg_data_source import CfgDataSource
from bridge.models.cfg_data_source_csv import CfgDataSourceCSV
from bridge.models.cfg_data_source_db import CfgDataSourceDB
from bridge.models.cfg_data_table import CfgDataTable
from bridge.models.cfg_data_table_column import CfgDataTableColumn
from bridge.models.cfg_process import CfgProcess
from bridge.models.cfg_process_column import CfgProcessColumn
from bridge.models.cfg_trace import CfgTrace
from bridge.models.cfg_trace_key import CfgTraceKey
from bridge.models.t_job_management import JobManagement
from grpc_server.services.grpc_service_proxy import grpc_api


def remove_datetime_columns(row):
    datetime_lst = [
        BridgeStationModel.Columns.created_at.name,
        BridgeStationModel.Columns.updated_at.name,
    ]
    for col in datetime_lst:  # temp. remove datetime column, which can not be dump by json
        if col in row:
            del row[col]


@grpc_api()
def get_data_source_by_id(db_instance, id):
    ds = CfgDataSource.get_by_id(db_instance, id)
    if ds['type'] == DBType.CSV.name:
        csv_detail = CfgDataSourceCSV.get_by_id(db_instance, ds['id'], is_cascade_column=True)
        if csv_detail:
            ds['csv_detail'] = CfgDataSourceCSV.to_dict(csv_detail)
            ds['csv_detail']['csv_columns'] = [CfgCsvColumn.to_dict(col) for col in csv_detail.csv_columns]
    else:
        db_detail = CfgDataSourceDB.get_by_id(db_instance, ds['id'])
        ds['db_detail'] = CfgDataSourceDB.to_dict(db_detail)

    return ds


@grpc_api()
def get_all_cfg_process(db_instance):
    _, rows = CfgProcess.select_records(db_instance, row_is_dict=True)
    if not rows:
        return []

    for row in rows:
        cfg_data_table_columns = CfgProcessColumn.get_by_proc_id(db_instance, row['id'])
        row['columns'] = [CfgDataTableColumn.to_dict(col) for col in cfg_data_table_columns]

        traces = CfgTrace.get_traces_of_proc(db_instance, row['id'], cascade_trace_key=True)
        dict_traces = []
        for trace in traces:
            trace_keys = [CfgTraceKey.to_dict(key) for key in trace.trace_keys]
            dict_trace = CfgTrace.to_dict(trace)
            dict_trace['trace_keys'] = trace_keys
            dict_traces.append(dict_trace)

        row['traces'] = dict_traces

        remove_datetime_columns(row)
        for col in row['columns']:
            remove_datetime_columns(col)

    return rows


@grpc_api()
def get_all_process_no_nested(db_instance):
    _, rows = CfgProcess.select_records(db_instance, row_is_dict=True)
    if not rows:
        return []
    return rows


@grpc_api()
def get_cfg_data_table_by_id(db_instance, id):
    row = CfgDataTable.get_by_id(db_instance, id)
    if not row:
        return None

    row = CfgDataTable.to_dict(row)
    cfg_data_table_columns = CfgDataTableColumn.get_by_data_table_id(db_instance, row['id'])
    row['columns'] = [CfgDataTableColumn.to_dict(col) for col in cfg_data_table_columns]
    row['data_source'] = CfgDataSource.get_by_id(db_instance, row['data_source_id'])

    remove_datetime_columns(row)
    remove_datetime_columns(row['data_source'])
    for col in row['columns']:
        remove_datetime_columns(col)

    return row


@grpc_api()
def get_job_management_list(latest_time):
    dict_cond = {JobManagement.Columns.start_tm.name: [(SqlComparisonOperator.GREATER_THAN_OR_EQ, latest_time)]}
    dict_order = {JobManagement.Columns.id.name: OrderBy.ASC.name}
    with BridgeStationModel.get_db_proxy() as db_instance:
        _, rows = JobManagement.select_records(db_instance, dic_conditions=dict_cond, dic_order_by=dict_order)
        if rows:
            jobs_from_db = [JobManagement(row) for row in rows]
            return jobs_from_db
    return []


@grpc_api()
def get_job_detail_by_id(job_id):
    # todo: copy logic of get_job_detail_service
    with BridgeStationModel.get_db_proxy() as db_instance:
        job = JobManagement.get_by_id(db_instance, job_id)
        return {job_id: job}


@grpc_api()
def get_process_cfg(cfg_process_id):
    with BridgeStationModel.get_db_proxy() as db_instance:
        cfg_process = CfgProcess.get_by_process_id(db_instance, cfg_process_id)
        cfg_process = CfgProcess.to_dict(cfg_process)
        # cfg_process['data_source'] = CfgDataSource.get_by_id(db_instance, cfg_process['data_source_id'])

        cfg_data_table_columns = CfgProcessColumn.get_by_proc_id(db_instance, cfg_process['id'])
        cfg_process['columns'] = [CfgDataTableColumn.to_dict(col) for col in cfg_data_table_columns]

        remove_datetime_columns(cfg_process)
        # remove_datetime_columns(cfg_process['data_source'])
        for col in cfg_process['columns']:
            remove_datetime_columns(col)

    return cfg_process
