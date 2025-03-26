from __future__ import annotations

import datetime

from ap.api.setting_module.services.show_latest_record import (
    get_exist_data_partition,
    get_latest_records,
    get_well_known_columns,
)
from ap.common.constants import (
    DATETIME_DUMMY,
    DataGroupType,
    DataType,
    DBType,
    MasterDBType,
    RawDataTypeDB,
)
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module.models import CfgDataSource, CfgDataTable, CfgDataTableColumn
from ap.setting_module.services.process_config import (
    gen_partition_table_name,
    get_efa_partitions,
    get_list_tables_and_views,
    query_database_tables_core,
)
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_data_source import EFA_MASTERS, EFA_TABLES
from bridge.models.cfg_data_source import CfgDataSource as BSCfgDataSource
from bridge.models.cfg_data_table import CfgDataTable as BSCfgDataTable
from bridge.models.cfg_data_table_column import CfgDataTableColumn as BSCfgDataTableColumn
from bridge.models.m_data_group import get_yoyakugo
from bridge.services.etl_services.etl_service import get_dummy_master_column_value
from bridge.services.master_data_import import BS_COMMON_PROCESS_COLUMNS


def gen_config_data(
    meta_session,
    data_source_id,
    serial_col,
    datetime_col,
    order_col,
    partition_from,
    partition_to,
    master_types,
):
    cfg_data_source: CfgDataSource = CfgDataSource.get_by_id(data_source_id, session=meta_session)

    # already exist
    if cfg_data_source.data_tables:
        # update partition from & partition to in data_tables if it were changed on UI
        is_changed = False
        for data_table in cfg_data_source.data_tables:  # type: CfgDataTable
            if data_table.partition_from != partition_from or data_table.partition_to != partition_to:
                data_table.partition_from = partition_from
                data_table.partition_to = partition_to
                is_changed = True
        if is_changed:
            meta_session.merge(cfg_data_source)

        return cfg_data_source, cfg_data_source.data_tables

    # gen data table info
    if cfg_data_source.master_type == MasterDBType.EFA.name:
        data_table_names = list(zip(EFA_TABLES, EFA_MASTERS))
    elif cfg_data_source.master_type == MasterDBType.OTHERS.name:
        data_table_names = [(None, cfg_data_source.master_type)]
    else:
        table_names = [f'{cfg_data_source.name}_{master_type}' for master_type in master_types]
        data_table_names = list(zip(table_names, master_types))

    # Save cfg data table column
    cfg_data_tables = []
    for name, detail_master_type in data_table_names:
        cfg_data_table, table_name = gen_data_table(
            cfg_data_source,
            partition_from,
            partition_to,
            name,
            detail_master_type,
        )
        # dic_well_know_cols = WELL_KNOWN_COLUMNS.get(master_type, {})
        gen_data_table_column(
            cfg_data_source,
            cfg_data_table,
            table_name,
            detail_master_type,
            serial_col,
            datetime_col,
            order_col,
        )
        cfg_data_tables.append(cfg_data_table)
        # in case missing master columns, dummy master columns
        if not MasterDBType.is_efa_group(detail_master_type):
            add_dummy_masters_for_data_table_column(cfg_data_table)

    return cfg_data_source, cfg_data_tables


@BridgeStationModel.use_db_instance()
def gen_config_data_db_instance(
    cfg_data_source: BSCfgDataSource,
    serial_col: str | None,
    datetime_col: str | None,
    order_col: str | None,
    partition_from: str | None,
    partition_to: str | None,
    master_types: list[str],
    db_instance: PostgreSQL = None,
    skip_merge: bool = False,
):
    """
    :param cfg_data_source:
    :param serial_col:
    :param datetime_col:
    :param order_col:
    :param partition_from:
    :param partition_to:
    :param master_types:
    :param db_instance:
    :param skip_merge: Whether to allow V2 processes to be merged (``skip_merge=True`` only in register by file)
    :return:
    """
    # already exist
    if cfg_data_source.data_tables:
        # update partition from & partition to in data_tables if it were changed on UI
        for data_table in cfg_data_source.data_tables:  # type: CfgDataTable
            if data_table.partition_from != partition_from or data_table.partition_to != partition_to:
                data_table.partition_from = partition_from
                data_table.partition_to = partition_to

                BSCfgDataTable.update_by_conditions(
                    db_instance,
                    {
                        BSCfgDataTable.Columns.partition_from.name: partition_from,
                        BSCfgDataTable.Columns.partition_to.name: partition_to,
                    },
                    {BSCfgDataTable.Columns.id.name: data_table.id},
                )

        return cfg_data_source, cfg_data_source.data_tables

    # gen data table info
    if cfg_data_source.master_type == MasterDBType.EFA.name:
        data_table_names = list(zip(EFA_TABLES, EFA_MASTERS))
    elif cfg_data_source.master_type == MasterDBType.OTHERS.name:
        data_table_names = [(None, cfg_data_source.master_type)]
    else:
        table_names = [f'{cfg_data_source.name}_{master_type}' for master_type in master_types]
        data_table_names = list(zip(table_names, master_types))

    # Save cfg data table column
    cfg_data_tables = []
    for name, detail_master_type in data_table_names:
        cfg_data_table, table_name = gen_data_table_db_instance(
            cfg_data_source,
            partition_from,
            partition_to,
            name,
            detail_master_type,
            db_instance=db_instance,
            skip_merge=skip_merge,
        )
        # dic_well_know_cols = WELL_KNOWN_COLUMNS.get(master_type, {})
        gen_data_table_column_db_instance(
            cfg_data_source,
            cfg_data_table,
            table_name,
            detail_master_type,
            serial_col,
            datetime_col,
            order_col,
            db_instance=db_instance,
        )
        cfg_data_tables.append(cfg_data_table)
        # in case missing master columns, dummy master columns
        if not MasterDBType.is_efa_group(detail_master_type):
            add_dummy_masters_for_data_table_column_db_instance(cfg_data_table, db_instance=db_instance)

    return cfg_data_source, cfg_data_tables


def gen_data_table_column(
    cfg_data_source,
    data_table: CfgDataTable,
    table_name,
    detail_master_type,
    serial_col,
    datetime_col,
    order_col,
):
    predefined_cols = get_yoyakugo()
    latest_rec = get_latest_records(cfg_data_source, table_name, None, 5, detail_master_type)

    cols_with_types, rows, cols_duplicated, previewed_files, master_type, _ = latest_rec
    is_csv = cfg_data_source.type == DBType.CSV.name
    if is_csv:
        file_name_info = {
            'name': DataGroupType.FileName.name,
            'column_name': DataGroupType.FileName.name,
            'data_type': DataType.TEXT.value,
            'name_en': DataGroupType.FileName.name,
            'romaji': DataGroupType.FileName.name,
            'is_get_date': False,
            'check_same_value': {'is_null': False, 'is_same': False, 'is_dupl': False},
            'is_big_int': False,
            'name_jp': DataGroupType.FileName.name,
            'name_local': DataGroupType.FileName.name,
            'column_raw_name': DataGroupType.FileName.name,
            'is_show': False,
        }
        cols_with_types.append(file_name_info)
    if len(data_table.columns) >= len(cols_with_types):
        return True

    # check dic_csv_col is EFA or OTHERS
    all_cols = [dict_col['column_name'] for dict_col in cols_with_types]
    dic_well_know_cols = get_well_known_columns(master_type, all_cols)
    if is_csv:  # add FileName for well knows columns
        dic_well_know_cols[DataGroupType.FileName.name] = DataGroupType.HORIZONTAL_DATA.value
    data_table_columns = []

    for idx, dic_csv_col in enumerate(cols_with_types):
        # Ignore non-candidate columns (only for datetime & serial columns)
        is_candidate_col = dic_csv_col.get('is_candidate_col', None)
        if is_candidate_col is not None and not is_candidate_col:
            continue

        data_table_column = CfgDataTableColumn()
        data_table_column.data_table_id = data_table.id
        data_table_column.column_name = dic_csv_col.get(CfgDataTableColumn.column_name.key)
        data_table_column.name = dic_csv_col.get(CfgDataTableColumn.name.key)
        data_group_type = dic_well_know_cols.get(
            dic_csv_col.get(CfgDataTableColumn.column_name.key),
            DataGroupType.HORIZONTAL_DATA.value,
        )

        # overwrite data group type to 34 for every column except datatime & serial columns
        other_cols = [DataGroupType.DATA_TIME.value, DataGroupType.DATA_SERIAL.value]
        if master_type == MasterDBType.OTHERS.name and data_group_type not in other_cols:
            if data_table_column.name == DATETIME_DUMMY:
                data_table_column.data_group_type = DataGroupType.DATA_TIME.value
            else:
                data_table_column.data_group_type = DataGroupType.HORIZONTAL_DATA.value
        else:
            data_table_column.data_group_type = data_group_type

        data_group_type_obj = DataGroupType(data_table_column.data_group_type)
        for col in predefined_cols:
            if col.get(CfgDataTableColumn.id.name) == data_table_column.data_group_type:
                if data_group_type_obj in [DataGroupType.DATA_TIME, DataGroupType.DATA_SERIAL]:
                    _, data_type = BS_COMMON_PROCESS_COLUMNS.get(data_group_type_obj)
                    data_table_column.data_type = data_type.name
                elif data_group_type_obj is DataGroupType.HORIZONTAL_DATA:
                    data_table_column.data_type = dic_csv_col.get('type')
                else:
                    data_table_column.data_type = col.get(CfgDataTableColumn.data_type.key)

                break

        if data_table_column.column_name == serial_col:
            data_table_column.data_group_type = DataGroupType.DATA_SERIAL.value

        if data_table_column.column_name == order_col:
            data_table_column.data_group_type = DataGroupType.AUTO_INCREMENTAL.value

        if data_table_column.column_name == datetime_col:
            data_table_column.data_group_type = DataGroupType.DATA_TIME.value

        # add order for data table column
        data_table_column.order = idx
        # insert_or_update_config(meta_session, data_table_column)
        data_table_columns.append(data_table_column)
    # overwrite data group type for serial and datetime when get well know column
    if cfg_data_source.is_direct_import and master_type == MasterDBType.OTHERS.name:
        data_table_columns = overwrite_data_group_type(data_table_columns, serial_col, datetime_col)
    data_table.columns = data_table_columns
    return True


@BridgeStationModel.use_db_instance()
def gen_data_table_column_db_instance(
    cfg_data_source: BSCfgDataSource,
    data_table: BSCfgDataTable,
    table_name,
    detail_master_type,
    serial_col,
    datetime_col,
    order_col,
    db_instance: PostgreSQL = None,
):
    predefined_cols = get_yoyakugo()
    latest_rec = get_latest_records(cfg_data_source, table_name, None, 5, detail_master_type)

    cols_with_types, rows, cols_duplicated, previewed_files, master_type, _ = latest_rec
    is_csv = cfg_data_source.type == DBType.CSV.name
    if is_csv:
        file_name_info = {
            'name': DataGroupType.FileName.name,
            'column_name': DataGroupType.FileName.name,
            'data_type': DataType.TEXT.value,
            'name_en': DataGroupType.FileName.name,
            'romaji': DataGroupType.FileName.name,
            'is_get_date': False,
            'check_same_value': {'is_null': False, 'is_same': False, 'is_dupl': False},
            'is_big_int': False,
            'name_jp': DataGroupType.FileName.name,
            'name_local': DataGroupType.FileName.name,
            'column_raw_name': DataGroupType.FileName.name,
            'is_show': False,
        }
        cols_with_types.append(file_name_info)
    if len(data_table.columns) >= len(cols_with_types):
        return True

    # check dic_csv_col is EFA or OTHERS
    all_cols = [dict_col['column_name'] for dict_col in cols_with_types]
    dic_well_know_cols = get_well_known_columns(master_type, all_cols)
    if is_csv:  # add FileName for well knows columns
        dic_well_know_cols[DataGroupType.FileName.name] = DataGroupType.HORIZONTAL_DATA.value
    data_table_columns = []

    for idx, dic_csv_col in enumerate(cols_with_types):
        # Ignore non-candidate columns (only for datetime & serial columns)
        is_candidate_col = dic_csv_col.get('is_candidate_col', None)
        if is_candidate_col is not None and not is_candidate_col:
            continue

        data_table_column = CfgDataTableColumn()
        data_table_column.data_table_id = data_table.id
        data_table_column.column_name = dic_csv_col.get(CfgDataTableColumn.column_name.key)
        data_table_column.name = dic_csv_col.get(CfgDataTableColumn.name.key)
        data_group_type = dic_well_know_cols.get(
            dic_csv_col.get(CfgDataTableColumn.name.key),
            DataGroupType.HORIZONTAL_DATA.value,
        )

        # overwrite data group type to 34 for every column except datatime & serial columns
        other_cols = [DataGroupType.DATA_TIME.value, DataGroupType.DATA_SERIAL.value]
        if master_type == MasterDBType.OTHERS.name and data_group_type not in other_cols:
            if data_table_column.name == DATETIME_DUMMY:
                data_table_column.data_group_type = DataGroupType.DATA_TIME.value
            else:
                data_table_column.data_group_type = DataGroupType.HORIZONTAL_DATA.value
        else:
            data_table_column.data_group_type = data_group_type

        data_group_type_obj = DataGroupType(data_table_column.data_group_type)
        for col in predefined_cols:
            if col.get(CfgDataTableColumn.id.name) == data_table_column.data_group_type:
                if data_group_type_obj in [DataGroupType.DATA_TIME, DataGroupType.DATA_SERIAL]:
                    _, data_type = BS_COMMON_PROCESS_COLUMNS.get(data_group_type_obj)
                    data_table_column.data_type = data_type.name
                elif data_group_type_obj is DataGroupType.HORIZONTAL_DATA:
                    data_table_column.data_type = dic_csv_col.get('type')
                else:
                    data_table_column.data_type = col.get(CfgDataTableColumn.data_type.key)

                break

        if data_table_column.column_name == serial_col:
            data_table_column.data_group_type = DataGroupType.DATA_SERIAL.value

        if data_table_column.column_name == order_col:
            data_table_column.data_group_type = DataGroupType.AUTO_INCREMENTAL.value

        if data_table_column.column_name == datetime_col:
            data_table_column.data_group_type = DataGroupType.DATA_TIME.value

        # add order for data table column
        data_table_column.order = idx
        data_table_column.created_at = datetime.datetime.utcnow()
        data_table_column.updated_at = data_table_column.created_at
        # insert_or_update_config(meta_session, data_table_column)
        data_table_columns.append(data_table_column)
    # overwrite data group type for serial and datetime when get well know column
    if cfg_data_source.is_direct_import and master_type == MasterDBType.OTHERS.name:
        data_table_columns = overwrite_data_group_type(data_table_columns, serial_col, datetime_col)

    for column in data_table_columns:
        column.id = BSCfgDataTableColumn.insert_record(db_instance, column, is_return_id=True, is_normalize=False)

    data_table.columns = data_table_columns

    return True


def overwrite_data_group_type(columns, serial_col, datetime_col):
    for col in columns:
        if col.data_group_type == DataGroupType.DATA_SERIAL.value and col.column_name != serial_col:
            col.data_group_type = DataGroupType.HORIZONTAL_DATA.value
        if col.data_group_type == DataGroupType.DATA_TIME.value and col.column_name != datetime_col:
            col.data_group_type = DataGroupType.HORIZONTAL_DATA.value

    return columns


def gen_data_table(
    cfg_data_source: CfgDataSource,
    partition_from,
    partition_to,
    data_table_name=None,
    detail_master_type=None,
):
    dict_tables = query_database_tables_core(cfg_data_source, data_table_name)
    cfg_data_table = CfgDataTable()
    cfg_data_table.data_source_id = cfg_data_source.id
    cfg_data_table.name = data_table_name if data_table_name else cfg_data_source.name
    cfg_data_table.detail_master_type = detail_master_type

    table_name = None
    table = None
    if dict_tables:
        if partition_from and partition_to:
            partitions = [partition_from, partition_to]
            # sort time
            partition_from = min(partitions)
            partition_to = max(partitions)
        cfg_data_table.partition_from = partition_from
        cfg_data_table.partition_to = partition_to
        table = dict_tables.get('tables')
        if len(table) > 0:
            exist_data_table = None
            partition_tables = None
            partition_to_table_name = None
            if partition_to:
                partition_to_table_name = gen_partition_table_name(table[0], partition_to)
                exist_data_table = get_exist_data_partition(cfg_data_source, [partition_to_table_name])

            if exist_data_table is None:
                tables = get_list_tables_and_views(cfg_data_source)
                *_, partition_tables = get_efa_partitions(tables, table[0])
                exist_data_table = get_exist_data_partition(cfg_data_source, partition_tables)

            table_name = (
                exist_data_table
                if exist_data_table
                else partition_to_table_name
                if partition_to_table_name
                else partition_tables[-1]
            )
    cfg_data_table.table_name = table[0] if table else None
    cfg_data_table.data_source = cfg_data_source

    # cfg_data_table = insert_or_update_config(meta_session, cfg_data_table)
    cfg_data_source.data_tables.append(cfg_data_table)

    return cfg_data_table, table_name


@BridgeStationModel.use_db_instance()
def gen_data_table_db_instance(
    cfg_data_source: CfgDataSource,
    partition_from,
    partition_to,
    data_table_name=None,
    detail_master_type=None,
    db_instance: PostgreSQL = None,
    skip_merge: bool = False,
):
    dict_tables = query_database_tables_core(cfg_data_source, data_table_name)
    cfg_data_table = BSCfgDataTable()
    cfg_data_table.data_source_id = cfg_data_source.id
    cfg_data_table.name = data_table_name if data_table_name else cfg_data_source.name
    cfg_data_table.detail_master_type = detail_master_type

    table_name = None
    table = None
    if dict_tables:
        if partition_from and partition_to:
            partitions = [partition_from, partition_to]
            # sort time
            partition_from = min(partitions)
            partition_to = max(partitions)
        cfg_data_table.partition_from = partition_from
        cfg_data_table.partition_to = partition_to
        table = dict_tables.get('tables')
        if len(table) > 0:
            exist_data_table = None
            partition_tables = None
            partition_to_table_name = None
            if partition_to:
                partition_to_table_name = gen_partition_table_name(table[0], partition_to)
                exist_data_table = get_exist_data_partition(cfg_data_source, [partition_to_table_name])

            if exist_data_table is None:
                tables = get_list_tables_and_views(cfg_data_source)
                *_, partition_tables = get_efa_partitions(tables, table[0])
                exist_data_table = get_exist_data_partition(cfg_data_source, partition_tables)

            table_name = (
                exist_data_table
                if exist_data_table
                else partition_to_table_name
                if partition_to_table_name
                else partition_tables[-1]
            )

    cfg_data_table.table_name = table[0] if table else None
    cfg_data_table.data_source = cfg_data_source
    cfg_data_table.created_at = datetime.datetime.utcnow()
    cfg_data_table.updated_at = cfg_data_table.created_at
    cfg_data_table.skip_merge = skip_merge
    cfg_data_table.id = BSCfgDataTable.insert_record(db_instance, cfg_data_table, is_return_id=True)
    cfg_data_source.data_tables.append(cfg_data_table)

    return cfg_data_table, table_name


def add_dummy_masters_for_data_table_column(cfg_data_table: CfgDataTable):
    dummy_column_name, _ = get_dummy_master_column_value(
        cfg_data_table.get_master_type(),
        cfg_data_table.data_source.is_direct_import,
    )
    dic_exist_col_group_types = [cfg_col.data_group_type for cfg_col in cfg_data_table.columns]

    for column_name, data_group_type in dummy_column_name.items():  # type: (str, DataGroupType)
        if data_group_type.value in dic_exist_col_group_types:
            continue

        _, data_type = BS_COMMON_PROCESS_COLUMNS.get(data_group_type)
        data_table_column = CfgDataTableColumn()
        data_table_column.data_table_id = cfg_data_table.id
        data_table_column.column_name = column_name
        data_table_column.english_name = column_name
        data_table_column.name = column_name
        data_table_column.data_group_type = data_group_type.value
        data_table_column.data_type = data_type.name
        data_table_column.is_get_date = False
        data_table_column.is_serial_no = False
        data_table_column.is_auto_increment = False

        # insert_or_update_config(meta_session, data_table_column)
        cfg_data_table.columns.append(data_table_column)

    return True


@BridgeStationModel.use_db_instance()
def add_dummy_masters_for_data_table_column_db_instance(cfg_data_table: BSCfgDataTable, db_instance: PostgreSQL = None):
    dummy_column_name, _ = get_dummy_master_column_value(
        cfg_data_table.get_master_type(),
        cfg_data_table.data_source.is_direct_import,
    )
    dic_exist_col_group_types = [cfg_col.data_group_type for cfg_col in cfg_data_table.columns]

    for column_name, data_group_type in dummy_column_name.items():  # type: (str, DataGroupType)
        if data_group_type.value in dic_exist_col_group_types:
            continue

        _, data_type = BS_COMMON_PROCESS_COLUMNS.get(data_group_type)
        data_table_column = BSCfgDataTableColumn()
        data_table_column.data_table_id = cfg_data_table.id
        data_table_column.column_name = column_name
        data_table_column.english_name = column_name
        data_table_column.name = column_name
        data_table_column.data_group_type = data_group_type.value
        data_table_column.data_type = data_type.name
        data_table_column.is_get_date = False
        data_table_column.is_serial_no = False
        data_table_column.is_auto_increment = False

        data_table_column.id = BSCfgDataTableColumn.insert_record(db_instance, data_table_column, is_return_id=True)
        cfg_data_table.columns.append(data_table_column)

    return True


def get_column_name_for_column_attribute(data_source):
    latest_rec = get_latest_records(data_source, None, None, 5)
    cols_with_types, rows, cols_duplicated, previewed_files, _, _ = latest_rec
    serial_cols = []
    datetime_cols = []
    order_cols = []
    for col in cols_with_types:
        data_type = col.get('data_type')
        name = col.get('name')
        if data_type == DataType.DATETIME.name:
            datetime_cols.append(name)
            order_cols.append(name)
        elif data_type in [
            DataType.INTEGER.name,
            DataType.TEXT.name,
            RawDataTypeDB.SMALL_INT.name,
            RawDataTypeDB.BIG_INT.name,
        ]:
            serial_cols.append(name)
            order_cols.append(name)

    return serial_cols, datetime_cols, order_cols
