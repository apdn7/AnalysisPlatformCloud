from __future__ import annotations

import re
from functools import lru_cache
from typing import Union

import pandas as pd
from sqlalchemy.orm import scoped_session

from ap import log_execution_time
from ap.common.common_utils import get_format_padding
from ap.common.constants import (
    __NO_NAME__,
    DataGroupType,
    DataType,
    DBType,
    EFAMasterColumn,
    MasterDBType,
    ProcessCfgConst,
    RawDataTypeDB,
    Suffixes,
    V2MasterColumn,
    dict_convert_raw_data_type,
)
from ap.common.pydn.dblib.db_proxy_readonly import ReadOnlyDbProxy
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.services.jp_to_romaji_utils import to_romaji
from ap.equations.utils import get_all_functions_info
from ap.setting_module.models import (
    CfgDataSource,
    CfgDataTable,
    CfgDataTableColumn,
    CfgFilter,
    CfgProcess,
    CfgProcessColumn,
    CfgProcessFunctionColumn,
    CfgVisualization,
    MappingFactoryMachine,
    MappingProcessData,
    MData,
    MDataGroup,
    MProcess,
    MUnit,
    RFactoryMachine,
    crud_config,
    insert_or_update_config,
    make_session,
)
from ap.setting_module.schemas import (
    DataTableSchema,
    FilterSchema,
    ProcessColumnSchema,
    ProcessOnlySchema,
    ProcessSchema,
    ProcessVisualizationSchema,
)
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_data_source import EFA_TABLES
from bridge.models.cfg_data_source import CfgDataSource as BSCfgDataSource
from bridge.models.cfg_process import CfgProcess as BSCfgProcess
from bridge.models.transaction_model import TransactionData
from bridge.services.utils import get_well_known_columns


def get_all_process(session=None):
    process = CfgProcess.get_all(session) or []
    return process


def get_all_data_tables():
    cfg_data_tables = CfgDataTable.get_all() or []
    return cfg_data_tables


def get_all_functions():
    all_functions_info = get_all_functions_info()
    all_functions_info = [function_info.model_dump() for function_info in all_functions_info]
    return all_functions_info


def get_all_process_no_nested():
    process_only_schema = ProcessOnlySchema(many=True)
    processes = CfgProcess.get_all() or []
    # Not show process has shown name empty
    processes = [process for process in processes if process.shown_name != to_romaji(__NO_NAME__)]
    return process_only_schema.dump(processes, many=True)


def get_process_cfg(proc_id, db_instance: PostgreSQL = None):
    process_schema = ProcessSchema()
    if db_instance:
        process = BSCfgProcess.get_by_id(db_instance, proc_id, is_cascade=True)
    else:
        process = CfgProcess.query.get(proc_id) or {}
    if process and not process.name_en:
        process.name_en = to_romaji(process.name_jp)
    columns = process.columns
    columns = sorted(columns, key=lambda x: (x.order is not None, x.order), reverse=True)
    process.columns = columns
    return process_schema.dump(process)


def get_data_table_cfg(cfg_data_table_id):
    data_table_schema = DataTableSchema()
    data_table = CfgDataTable.query.get(cfg_data_table_id) or {}
    return data_table_schema.dump(data_table)


def get_process_columns(proc_id):
    proc_col_schema = ProcessColumnSchema(many=True)
    columns = CfgProcessColumn.query.filter(CfgProcessColumn.process_id == proc_id).all() or []
    # not show LINE_ID, EQUIP_ID, PROCESS_ID
    columns = [
        column
        for column in columns
        if (column.column_type in DataGroupType.get_column_type_show_graph() or len(column.function_details))
    ]
    # change type for column format zero as string
    for column in columns:
        if column.data_type == DataType.INTEGER.value and column.format and get_format_padding(column.format):
            column.data_type = DataType.TEXT.value

        # modify data type based on function column
        if column.function_details and column.function_details[-1].return_type:
            function_return_type = column.function_details[-1].return_type
            column.data_type = dict_convert_raw_data_type.get(function_return_type, function_return_type)

        # show boolean as text
        if column.data_type == DataType.BOOLEAN.value:
            column.data_type = DataType.TEXT.value

    columns = sorted(columns, key=lambda col: col.column_type)

    return proc_col_schema.dump(columns)


def get_process_filters(proc_id):
    proc_filter_schema = FilterSchema(many=True)
    filters = CfgFilter.query.filter(CfgFilter.process_id == proc_id).all() or []
    return proc_filter_schema.dump(filters)


def get_process_visualizations(proc_id):
    proc_vis_schema = ProcessVisualizationSchema()
    process = CfgProcess.query.get(proc_id) or {}

    return proc_vis_schema.dump(process)


def get_all_visualizations():
    return list({cfg.filter_detail_id for cfg in CfgVisualization.get_filter_ids()})


def create_or_update_process_cfg(proc_data, unused_columns=None, unchecked_proc_data=None):
    with make_session() as meta_session:
        # save process config
        process: CfgProcess = insert_or_update_config(
            meta_session=meta_session,
            data=proc_data,
            key_names=CfgProcess.id.key,
            model=CfgProcess,
        )
        meta_session.commit()

        # create column alchemy object + assign process id
        columns = proc_data[ProcessCfgConst.PROC_COLUMNS.value]
        is_has_get_date_col = False
        for proc_column in columns:  # type: CfgProcessColumn
            # transform data type
            if not proc_column.column_type:
                cfg_process_column = CfgProcessColumn.get_by_col_name(process.id, proc_column.column_name)
                proc_column.column_type = cfg_process_column.column_type
            if proc_column.raw_data_type == RawDataTypeDB.SMALL_INT.value:
                proc_column.data_type = DataType.INTEGER.value
            elif proc_column.raw_data_type in (
                RawDataTypeDB.CATEGORY.value,
                RawDataTypeDB.BIG_INT.value,
                RawDataTypeDB.BOOLEAN.value,
            ):
                proc_column.data_type = DataType.TEXT.value
            else:
                proc_column.data_type = proc_column.raw_data_type

            # update column type when user chosen another column as main:datetime column
            if is_has_get_date_col and proc_column.column_type == DataGroupType.DATA_TIME.value:
                proc_column.column_type = DataGroupType.GENERATED.value

            # if proc_column.raw_data_type in [
            #     RawDataTypeDB.INTEGER.value,
            #     RawDataTypeDB.TEXT.value,
            #     RawDataTypeDB.SMALL_INT.value,
            #     RawDataTypeDB.BIG_INT.value,
            #     RawDataTypeDB.BOOLEAN.value,
            # ] and (
            #     proc_column.column_type in DataGroupType.get_column_type_show_graph()
            #     or proc_column.function_details is not None
            # ):
            #     proc_column.is_linking_column = True
            proc_column.process_id = process.id

            if proc_column.is_get_date:
                is_has_get_date_col = True
                proc_column.column_type = DataGroupType.DATA_TIME.value

            # transform english name
            if not proc_column.name_en:
                proc_column.name_en = to_romaji(proc_column.column_name)

            # gen datetime main column
            if proc_column.id < 0 and proc_column.is_get_date:
                from bridge.services.master_data_import import gen_m_data_manual

                unit_id = MUnit.get_empty_unit_id()
                new_ids = gen_m_data_manual(
                    RawDataTypeDB.DATETIME.value,
                    unit_id,
                    proc_column.name_en,
                    proc_column.name_jp,
                    proc_column.name_local,
                    proc_column.process_id,
                    DataGroupType.GENERATED.value,
                )
                proc_column.id = new_ids['data_id']
                proc_column.bridge_column_name = f'_{proc_column.id}_{proc_column.column_raw_name}'

        # re-fill function columns & hide columns to avoid deleting it
        hide_columns = list(
            filter(
                lambda c: (
                    len(c.function_details) or c.column_type in DataGroupType.get_hide_column_type_cfg_proces_columns()
                ),
                CfgProcessColumn.get_by_process_id(process.id),
            ),
        )
        columns.extend(hide_columns)

        # save columns
        crud_config(
            meta_session=meta_session,
            data=columns,
            parent_key_names=CfgProcessColumn.process_id.key,
            key_names=[CfgProcessColumn.column_name.key],
            model=CfgProcessColumn,
        )

        # update m_data data type
        dic_m_data_records = {rec.id: rec for rec in MData.get_by_process_id(process.id, session=meta_session)}
        columns = proc_data['columns']
        for col in columns:
            m_data: MData = dic_m_data_records.get(int(col.id))
            if not m_data:
                continue

            m_data.data_type = col.raw_data_type

        # create table transaction_process
        with BridgeStationModel.get_db_proxy() as db_instance:
            transaction_data_obj = TransactionData(process.id)
            _ = transaction_data_obj.create_table(db_instance)
            dict_id_with_bridge_column_name = {
                col.id: col.bridge_column_name for col in unchecked_proc_data.get('columns') or {}
            }
            # remove column
            if len(dict_id_with_bridge_column_name):
                remove_column(db_instance, transaction_data_obj, dict_id_with_bridge_column_name)

    return process


def remove_column(db_instance, transaction_data_obj, dict_id_with_bridge_column_name):
    transaction_data_obj.delete_columns(db_instance, list(dict_id_with_bridge_column_name.values()))
    MData.hide_col_by_ids(
        data_ids=list(dict_id_with_bridge_column_name.keys()),
    )  # TODO: run by job , to avoid delete and import run the same time


def remove_m_data(meta_session, uncheck_proc_data) -> bool:
    columns = uncheck_proc_data[ProcessCfgConst.PROC_COLUMNS.value]
    if not columns:
        return

    # collect data_id base on process_column_id
    data_ids = []
    for proc_column in columns:
        data_ids.append(proc_column.id)
    MData.delete_by_ids(data_ids, meta_session)

    return len(data_ids) > 0


def create_or_update_data_table_cfg(cfg_data_table_data: CfgDataTable, is_checks=None):
    def index_is_checked(idx: int):
        return is_checks and is_checks[idx]

    with make_session() as meta_session:
        # save process config
        data_table = insert_or_update_config(
            meta_session=meta_session,
            data=cfg_data_table_data,
            key_names=CfgDataTable.id.key,
            model=CfgDataTable,
        )
        meta_session.flush()

        # create column alchemy object + assign process id
        columns = cfg_data_table_data.columns
        well_known_columns = get_well_known_columns(
            data_table.data_source.master_type or MasterDBType.OTHERS.name,
            None,
        )
        default_column_name_efa = EFAMasterColumn.get_default_column(is_key_name=False)
        default_column_name_v2 = V2MasterColumn.get_default_column(is_key_name=False)

        # for table_column in columns:
        filter_columns = []
        for index, table_column in enumerate(columns):
            table_column.data_table_id = data_table.id
            if not index_is_checked(index):
                column_value = well_known_columns.get(table_column.column_name)
                if data_table.data_source.master_type == MasterDBType.EFA.name:
                    table_column.column_name = default_column_name_efa.get(DataGroupType(column_value))
                elif data_table.data_source.master_type == MasterDBType.V2.name:
                    table_column.column_name = default_column_name_v2.get(DataGroupType(column_value))
            # transform english name
            table_column.english_name = to_romaji(table_column.english_name)
            table_column.order = index

            if not (
                not index_is_checked(index)
                and well_known_columns.get(table_column.name)
                in [DataGroupType.DATA_SERIAL.value, DataGroupType.DATA_TIME.value]
            ):
                filter_columns.append(table_column)
        columns = filter_columns

        # save columns
        crud_config(
            meta_session=meta_session,
            data=columns,
            parent_key_names=CfgDataTableColumn.data_table_id.key,
            key_names=CfgDataTableColumn.column_name.key,
            model=CfgDataTableColumn,
        )
        meta_session.commit()

        from ap.api.setting_module.services.direct_import import (
            add_dummy_masters_for_data_table_column,
        )

        if not MasterDBType.is_efa_group(data_table.get_master_type()):
            add_dummy_masters_for_data_table_column(data_table)

    return data_table


def query_database_tables(db_id, table_prefix=None):
    with make_session() as mss:
        data_source = mss.query(CfgDataSource).get(db_id)
        return query_database_tables_core(data_source, table_prefix)


@BridgeStationModel.use_db_instance()
def query_database_tables_db_instance(db_id, table_prefix=None, db_instance: PostgreSQL = None):
    data_source = BSCfgDataSource(
        BSCfgDataSource.get_by_id(db_instance, db_id),
        is_cascade=True,
        db_instance=db_instance,
    )
    return query_database_tables_core(data_source, table_prefix)


def query_database_tables_core(data_source: Union[CfgDataSource, BSCfgDataSource], table_prefix):
    if not data_source:
        return None

    detail_master_types = []
    output = {'ds_type': data_source.type, 'master_type': data_source.master_type, 'tables': []}
    # return None if CSV
    if data_source.type.lower() in [DBType.CSV.name.lower(), DBType.V2.name.lower()]:
        if data_source.csv_detail.directory:
            detail_master_types.append(MasterDBType.V2.name)
        if data_source.csv_detail.second_directory:
            detail_master_types.append(MasterDBType.V2_HISTORY.name)
        output['detail_master_types'] = detail_master_types
        return output

    updated_at = data_source.db_detail.updated_at
    tables = get_list_tables_and_views(data_source, updated_at)
    partitions = None
    if data_source.master_type == MasterDBType.EFA.name:
        if table_prefix or data_source.is_direct_import:
            table_name, partitions, _ = get_efa_partitions(tables, table_prefix)
            tables = [table_name]
        else:
            partitions = []
            tables = EFA_TABLES
    elif data_source.master_type == MasterDBType.SOFTWARE_WORKSHOP.name:
        from bridge.services.etl_services.etl_software_workshop_services import quality_measurements_table

        expected_tables = [quality_measurements_table.name]
        tables = [t for t in expected_tables if t in tables]

    output['tables'] = tables
    output['partitions'] = partitions

    return output


@lru_cache(maxsize=20)
def get_list_tables_and_views(data_source: CfgDataSource, updated_at=None):
    # updated_at only for cache
    print('database config updated_at:', updated_at, ', so cache can not be used')
    with ReadOnlyDbProxy(data_source) as db_instance:
        tables = db_instance.list_tables_and_views()

    tables = sorted(tables, key=lambda v: v.lower())
    return tables


# def get_ds_tables(ds_id):
#     try:
#         tables = query_database_tables(ds_id)
#         return tables['tables'] or []
#     except Exception:
#         return []


def convert2serialize(obj):
    if isinstance(obj, dict):
        return {k: convert2serialize(v) for k, v in obj.items()}
    elif hasattr(obj, '_ast'):
        return convert2serialize(obj._ast())
    elif not isinstance(obj, str) and hasattr(obj, '__iter__'):
        return [convert2serialize(v) for v in obj]
    elif hasattr(obj, '__dict__'):
        return {k: convert2serialize(v) for k, v in obj.__dict__.items() if not callable(v) and not k.startswith('_')}
    else:
        return obj


@log_execution_time()
def get_efa_partitions(tables, table_prefix=None):
    """
    get table name and its partitions of EFA data source
    :param tables:
    :param table_prefix:
    :return:
    """
    if not table_prefix:
        table_prefix = EFA_TABLES[0]

    regex = re.compile(rf'^({table_prefix})_(\d{{6}})', re.IGNORECASE)
    partition_times = []
    table_name = None
    partition_tables = []
    for table in sorted(tables):
        matched_vals = regex.match(table)
        if matched_vals is None:
            continue

        partition_tables.append(matched_vals[0])
        table_name = matched_vals[1]
        partition_times.append(matched_vals[2])

    return table_name, partition_times, partition_tables


def get_random_partition_table_name(cfg_data_table: CfgDataTable):
    """
    get random partition table name
    :param cfg_data_table:
    :return:
    """
    if cfg_data_table.partition_tables:
        table_name = cfg_data_table.partition_tables[-1].table_name
    else:
        tables = get_list_tables_and_views(cfg_data_table.data_source)
        table_name, *_ = get_efa_partitions(tables)

    if not table_name:
        table_name = cfg_data_table.table_name

    return table_name


def gen_partition_table_name(table_name, partition_num):
    return f'{table_name}_{partition_num}'


def get_data_tables_by_proc_id(proc_id):
    r_factory_machines: list[RFactoryMachine] = RFactoryMachine.get_by_process_ids([proc_id])
    factory_machine_ids = list({_row.id for _row in r_factory_machines})
    mapping_factory_machines = MappingFactoryMachine.get_by_factory_machine_ids(factory_machine_ids)
    data_table_ids = list({_row.data_table_id for _row in mapping_factory_machines})
    cfg_data_tables = CfgDataTable.get_in_ids(data_table_ids)

    return cfg_data_tables


def get_all_processes_with_unit():
    m_data_df = MData.get_all_as_df()
    m_unit_df = MUnit.get_all_as_df()
    m_process_df = MProcess.get_all_as_df()
    m_data_group_df = MDataGroup.get_all_as_df()
    merged_df = m_data_df.merge(m_unit_df, on=MData.unit_id.name, how='left', suffixes=Suffixes.KEEP_LEFT)
    merged_df = merged_df.merge(m_process_df, on=MData.process_id.name, how='left', suffixes=Suffixes.KEEP_LEFT)
    merged_df = merged_df.merge(m_data_group_df, on=MData.data_group_id.name, how='left', suffixes=Suffixes.KEEP_LEFT)
    return_dict = {}
    for idx, process_df in merged_df.groupby(by=[MData.process_id.name]):
        process_id = str(process_df.iloc[0][MData.process_id.name])
        process_name_jp = str(process_df.iloc[0][MProcess.process_name_jp.name])
        return_dict[process_id] = {'process_name_jp': process_name_jp}
        columns_groups = process_df[
            [
                MappingProcessData.data_id.name,
                MDataGroup.data_name_jp.name,
                MData.data_group_id.name,
                MData.unit_id.name,
                MUnit.unit.name,
            ]
        ].values.tolist()
        for data_id, data_name_jp, data_group_id, unit_id, unit in columns_groups:
            return_dict[process_id][str(data_id)] = {
                'data_group_id': str(data_group_id),
                'data_name_jp': data_name_jp,
                'unit_id': str(unit_id),
                'unit': None if pd.isnull(unit) else unit,
            }

    return return_dict


def get_ct_range(proc_id, columns):
    is_using_dummy_datetime = True in [col['is_get_date'] and col['is_dummy_datetime'] for col in columns]

    if not is_using_dummy_datetime:
        return []

    try:
        with BridgeStationModel.get_db_proxy() as db_instance:
            trans_data = TransactionData(proc_id)
            ct_range = trans_data.get_ct_range(db_instance)
        # cycle_cls = find_cycle_class(proc_id)
        # ct_range = (
        #     db.session.query(cycle_cls.id, cycle_cls.time)
        #     .filter(cycle_cls.process_id == proc_id)
        #     .with_entities(
        #         func.min(cycle_cls.time).label('min_time'),
        #         func.max(cycle_cls.time).label('max_time'),
        #     )
        #     .first()
        # )
        return ct_range
    except Exception:
        return []


@BridgeStationModel.use_db_instance()
def gen_function_column_in_m_data(process_column, db_instance: PostgreSQL = None, cfg_func=None):
    from bridge.models.m_unit import MUnit as BSMUnit
    from bridge.services.master_data_import import gen_m_data_manual

    unit_id = BSMUnit.get_empty_unit_id(db_instance)
    return gen_m_data_manual(
        cfg_func.return_type,
        unit_id,
        process_column.name_en,
        process_column.name_jp,
        process_column.name_local,
        process_column.process_id,
        DataGroupType.GENERATED_EQUATION.value,
        db_instance=db_instance,
    )


def gen_function_column(process_columns, session: scoped_session | PostgreSQL):
    for process_column in process_columns:
        if process_column.function_config is None:
            continue

        dict_function_column = {
            'function_id': process_column.function_config.get('function_id'),
            'var_x': process_column.function_config.get('var_x'),
            'var_y': process_column.function_config.get('var_y'),
            'coe_a_n_s': process_column.function_config.get('coe_a_n_s'),
            'coe_b_k_t': process_column.function_config.get('coe_b_k_t'),
            'coe_c': process_column.function_config.get('coe_c'),
            'return_type': process_column.function_config.get('return_type'),
            'note': process_column.function_config.get('note'),
        }

        if process_column.function_config.get('function_column_id'):  # In case of exist record
            dict_function_column['id'] = process_column.function_config.get('function_column_id')

        select_cols = []
        rows = []
        for col, value in dict_function_column.items():
            select_cols.append(col)
            rows.append(value if value != '' else None)
        rows = [tuple(rows)]

        if isinstance(session, scoped_session):
            CfgProcessFunctionColumn.insert_records(select_cols, rows, session)
        else:
            session.bulk_insert(CfgProcessFunctionColumn.get_table_name(), select_cols, rows)
