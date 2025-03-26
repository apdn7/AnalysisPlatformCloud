# tunghh todo remove
import pickle

from ap.common.common_utils import SQL_DATE_FORMAT_STR
from ap.common.pydn.dblib.db_common import (
    AggregateFunction,
    OrderBy,
    SqlComparisonOperator,
    add_single_quote,
)
from bridge.models.bridge_station import BridgeStationModel, MasterModel
from bridge.models.etl_mapping import EtlMapping
from bridge.models.table_relationship import insertion_order_master
from grpc_src.models.sync_master_pb2 import ResponseSyncBridgeToEdge_FactoryMode


def get_master_data_by_range(master_table_id, get_from=None, _get_to=None):  # todo: handle  _get_to ?
    gen_mst_table_info = get_master_table_info(master_table_id)
    for mst_table_info in gen_mst_table_info:
        table_id, table_name, columns, increment_column = mst_table_info
        model = [cls for cls in MasterModel.get_all_subclasses() if cls.get_original_table_name() == table_name]
        if not model:
            raise Exception(f'Model class for table {table_name} was not found')
        else:
            model = model[0]

        # issue: không cần phải select theo table etl_column. trong đó không có id
        columns = model.Columns.get_column_names()
        dic_select_params = {'select_cols': columns, 'row_is_dict': False}

        if increment_column:
            dic_cond = {increment_column: [(SqlComparisonOperator.GREATER_THAN, get_from)]} if get_from else {}
            dict_aggregate_function = {
                increment_column: (
                    AggregateFunction.TO_CHAR.name,
                    increment_column,
                    add_single_quote(SQL_DATE_FORMAT_STR),
                ),
            }  # todo: may refactor
            dic_order_by = {increment_column: OrderBy.ASC.name}
            dic_select_params['dic_conditions'] = dic_cond
            dic_select_params['dict_aggregate_function'] = dict_aggregate_function
            dic_select_params['dic_order_by'] = dic_order_by

        with BridgeStationModel.get_db_proxy() as db_instance:
            _col, master_data = model.select_records(db_instance, **dic_select_params)

        dic_response_params = {
            'process_id': table_id,
            'column_names': columns,
            'table_name': table_name,
            'data': pickle.dumps(master_data),
        }

        if increment_column:
            import_from = (
                get_from if get_from else master_data[-1][_col.index(increment_column)] if master_data else None
            )
            import_to = master_data[-1][_col.index(increment_column)] if master_data else import_from
            dic_response_params['import_from']: import_from
            dic_response_params['import_to']: import_to

        # todo split data into grpc max chunk
        yield ResponseSyncBridgeToEdge_FactoryMode(**dic_response_params)


def get_master_table_info(table_ids):
    sql, params = EtlMapping.get_records_by_data_src(data_src_id=None, table_ids=table_ids)
    with BridgeStationModel.get_db_proxy() as db_instance:
        _, elt_mapping_records = db_instance.run_sql(sql, row_is_dict=False, params=params)

    dict_etl_mapping = {}
    for etl_mapping in elt_mapping_records:
        if etl_mapping[0] in dict_etl_mapping:
            dict_etl_mapping[etl_mapping[0]].append(etl_mapping)
        else:
            dict_etl_mapping.update({etl_mapping[0]: [etl_mapping]})
    for value in dict_etl_mapping.values():
        table_id = value[0][0]  # record 0, column 0
        table_name = value[0][1]  # record 0, column 1
        columns = [rec[3] for rec in value]

        increment_column = [rec[3] for rec in value if rec[5] is True]
        increment_column = increment_column[0] if increment_column else None
        yield table_id, table_name, columns, increment_column


def get_etl_mapping():
    model_classes = list(EtlMapping.get_all_subclasses())
    model_classes.sort(key=lambda _cls: _cls.__name__)
    with BridgeStationModel.get_db_proxy() as db_instance:
        for cls in model_classes:
            cols, rows = cls.get_all_records(db_instance)
            yield cls.get_table_name(), cols, rows


def get_master_data(tables=()):
    if not tables:
        model_classes = insertion_order_master
    else:
        model_classes = [model_cls for model_cls in insertion_order_master if model_cls.get_table_name() in tables]

    # model_classes.sort(key=lambda _cls: _cls.__name__)
    with BridgeStationModel.get_db_proxy() as db_instance:
        for cls in model_classes:
            df = cls.get_all_as_df(db_instance)
            yield cls.get_table_name(), df.columns, df
