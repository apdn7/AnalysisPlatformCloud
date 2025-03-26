from typing import Optional, Union

import pandas as pd
from pandas import DataFrame

from ap.common.common_utils import camel_to_snake
from ap.common.constants import (
    __NO_NAME__,
    DATETIME_DUMMY,
    EMPTY_STRING,
    LIMIT_LEN_BS_COL_NAME,
    MAX_NAME_LENGTH,
    UNDER_SCORE,
    DataGroupType,
    DataType,
    MasterDBType,
    RawDataTypeDB,
    Suffixes,
    dict_convert_raw_data_type,
)
from ap.common.pydn.dblib.db_common import OrderBy, SqlComparisonOperator
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.services.jp_to_romaji_utils import to_romaji
from ap.setting_module.models import CfgDataTable, CfgDataTableColumn, MProcess
from bridge.models.bridge_station import BridgeStationModel, ConfigModel
from bridge.models.cfg_data_table import CfgDataTable as BSCfgDataTable
from bridge.models.cfg_data_table_column import CfgDataTableColumn as BSCfgDataTableColumn
from bridge.models.cfg_filter import CfgFilter
from bridge.models.cfg_process_function_column import CfgProcessFunctionColumn
from bridge.models.m_data import MData
from bridge.models.m_data_group import MDataGroup
from bridge.models.m_unit import MUnit
from bridge.models.mapping_process_data import MappingProcessData
from bridge.models.model_utils import TableColumn


class CfgProcessColumn(ConfigModel):
    def __init__(self, dic_row=None, db_instance=None):
        dic_row = dic_row if dic_row else {}

        self.id = dic_row.get(CfgProcessColumn.Columns.id.name)
        if self.id is None:
            del self.id
        self.process_id = dic_row.get(CfgProcessColumn.Columns.process_id.name)
        self.column_name = dic_row.get(CfgProcessColumn.Columns.column_name.name)
        self.bridge_column_name = dic_row.get(CfgProcessColumn.Columns.bridge_column_name.name)
        self.column_raw_name = dic_row.get(CfgProcessColumn.Columns.column_raw_name.name)
        self.name_en = dic_row.get(CfgProcessColumn.Columns.name_en.name)
        self.name_jp = dic_row.get(CfgProcessColumn.Columns.name_jp.name)
        self.name_local = dic_row.get(CfgProcessColumn.Columns.name_local.name)
        self.data_type = dic_row.get(CfgProcessColumn.Columns.data_type.name)
        self.raw_data_type = dic_row.get(CfgProcessColumn.Columns.raw_data_type.name)
        self.operator = dic_row.get(CfgProcessColumn.Columns.operator.name)
        self.coef = dic_row.get(CfgProcessColumn.Columns.coef.name)
        self.column_type = dic_row.get(CfgProcessColumn.Columns.column_type.name)
        self.is_serial_no = dic_row.get(CfgProcessColumn.Columns.is_serial_no.name)
        self.is_get_date = dic_row.get(CfgProcessColumn.Columns.is_get_date.name)
        self.is_dummy_datetime = dic_row.get(CfgProcessColumn.Columns.is_dummy_datetime.name)
        self.is_auto_increment = dic_row.get(CfgProcessColumn.Columns.is_auto_increment.name)
        self.order = dic_row.get(CfgProcessColumn.Columns.order.name)
        self.format = dic_row.get(CfgProcessColumn.Columns.format.name)
        self.unit = dic_row.get(CfgProcessColumn.Columns.unit.name)

        self.created_at = dic_row.get(CfgProcessColumn.Columns.created_at.name)
        self.updated_at = dic_row.get(CfgProcessColumn.Columns.updated_at.name)

        self.m_data_group: Optional[MDataGroup] = None
        self.m_data: Optional[MData] = None

        self.function_details: list[CfgProcessFunctionColumn] = []
        if self.id:

            def _get_relation_data_(_self, _db_instance: PostgreSQL):
                dic_conditions = {CfgProcessFunctionColumn.Columns.process_column_id.name: self.id}
                dic_order = {f'"{CfgProcessFunctionColumn.Columns.order.name}"': OrderBy.ASC.name}
                _, cfg_process_function_columns = CfgProcessFunctionColumn.select_records(
                    _db_instance,
                    dic_conditions=dic_conditions,
                    dic_order_by=dic_order,
                    row_is_dict=True,
                )
                if cfg_process_function_columns:
                    self.function_details = [CfgProcessFunctionColumn(col) for col in cfg_process_function_columns]

            if db_instance:
                _get_relation_data_(self, db_instance)
            else:
                with self.get_db_proxy() as db_instance:
                    _get_relation_data_(self, db_instance)

    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        process_id = (2, DataType.INTEGER)
        column_name = (3, DataType.TEXT)
        format = (23, DataType.TEXT)
        name_en = (20, DataType.TEXT)
        name_jp = (21, DataType.TEXT)
        name_local = (22, DataType.TEXT)
        bridge_column_name = (18, DataType.TEXT)  # use in Bridge Station
        column_raw_name = (19, DataType.TEXT)  # column name in data source
        data_type = (6, DataType.TEXT)
        raw_data_type = (17, DataType.TEXT)
        operator = (7, DataType.TEXT)
        coef = (8, DataType.TEXT)
        column_type = (9, DataType.INTEGER)  # ref m_data_group.id
        is_serial_no = (10, DataType.BOOLEAN)
        is_get_date = (11, DataType.BOOLEAN)
        is_dummy_datetime = (12, DataType.BOOLEAN)
        is_auto_increment = (13, DataType.BOOLEAN)
        order = (15, DataType.INTEGER)
        unit = (16, DataType.TEXT)

        created_at = (97, DataType.DATETIME)
        updated_at = (98, DataType.DATETIME)

    _table_name = 'cfg_process_column'
    primary_keys = [Columns.id]

    # TODO trace key, cfg_filter: may not needed
    # visualizations = db.relationship('CfgVisualization', lazy='dynamic', backref="cfg_process_column", cascade="all")

    @classmethod
    def get_date_col(cls, db_instance: PostgreSQL, process_id: int, column_name_only: bool = True):
        """
        get date column
        :param column_name_only:
        :return:
        """
        dict_cond = {
            cls.Columns.process_id.name: process_id,
            cls.Columns.is_get_date.name: cls.parse_bool(True),
        }
        _col, rows = cls.select_records(db_instance, dic_conditions=dict_cond)
        if rows:
            if column_name_only:
                return rows[0][CfgProcessColumn.Columns.column_name.name]
            else:
                return rows[0]
        else:
            return None

    @classmethod
    def get_auto_increment_col(
        cls,
        db_instance: FloatingPointError,
        process_id: int,
        column_name_only: bool = True,
    ):
        """
        get auto increment column
        :param column_name_only:
        :return:
        """
        dict_cond = {
            cls.Columns.process_id.name: process_id,
            cls.Columns.is_auto_increment.name: True,
        }
        _col, rows = cls.select_records(db_instance, dic_conditions=dict_cond)
        if rows:
            if column_name_only:
                return rows[0][CfgProcessColumn.Columns.column_name.name]
            else:
                return rows[0]
        else:
            return None

    @classmethod
    def get_auto_increment_col_else_get_date(
        cls,
        db_instance: PostgreSQL,
        process_id: int,
        column_name_only: bool = True,
    ):
        return cls.get_auto_increment_col(db_instance, process_id, column_name_only) or cls.get_date_col(
            db_instance,
            process_id,
            column_name_only,
        )

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def get_by_proc_id(cls, db_instance: PostgreSQL, proc_id: int):
        _, rows = cls.select_records(
            db_instance,
            dic_conditions={cls.Columns.process_id.name: proc_id},
            dic_order_by={
                cls.Columns.is_serial_no.name: OrderBy.DESC.name,
                cls.Columns.is_get_date.name: OrderBy.DESC.name,
                cls.Columns.is_auto_increment.name: OrderBy.DESC.name,
            },
        )
        if not rows:
            return []
        return [CfgProcessColumn(row, db_instance=db_instance) for row in rows]

    @classmethod
    def get_by_names(cls, db_instance: PostgreSQL, column_names: list[str], proc_id: int = None):
        dic_conditions = {cls.Columns.english_name.name: [(SqlComparisonOperator.IN, tuple(column_names))]}
        if proc_id:
            dic_conditions[cls.Columns.process_id.name] = proc_id

        _, rows = cls.select_records(
            db_instance,
            dic_conditions=dic_conditions,
            dic_order_by={
                cls.Columns.is_serial_no.name: OrderBy.DESC.name,
                cls.Columns.is_get_date.name: OrderBy.DESC.name,
                cls.Columns.is_auto_increment.name: OrderBy.DESC.name,
            },
        )

        return rows

    @classmethod
    def get_by_col_ids(cls, db_instance: PostgreSQL, proc_id: int, column_ids: list[int]):
        conds = {
            cls.Columns.process_id.name: proc_id,
            cls.Columns.id.name: [(SqlComparisonOperator.IN, tuple(column_ids))],
        }
        _, rows = cls.select_records(
            db_instance,
            dic_conditions=conds,
            dic_order_by={
                cls.Columns.is_serial_no.name: OrderBy.DESC.name,
                cls.Columns.is_get_date.name: OrderBy.DESC.name,
                cls.Columns.is_auto_increment.name: OrderBy.DESC.name,
            },
        )

        return rows

    @classmethod
    def get_serials_by_proc_id(cls, db_instance: PostgreSQL, process_id: int):
        dict_cond = {
            cls.Columns.process_id.name: process_id,
            cls.Columns.is_serial_no.name: True,
        }
        _, rows = cls.select_records(db_instance, dic_conditions=dict_cond)
        return rows

    @classmethod
    def get_date_by_proc_id(cls, db_instance: PostgreSQL, proc_id: int):
        pass

    @classmethod
    def get_incremental_col_by_proc_id(cls, db_instance: PostgreSQL, proc_id: int):
        pass

    @classmethod
    def get_column_by_filter_id(cls, db_instance: PostgreSQL, filter_id: int):
        filter_basic = CfgFilter.get_by_id(db_instance, filter_id)
        column = cls.get_by_id(db_instance, filter_basic[CfgFilter.Columns.column_id.name])

        return column

    @classmethod
    def get_serials(cls, db_instance, proc_id):
        dic_conditions = {cls.Columns.process_id.name: proc_id, cls.Columns.is_serial_no.name: True}
        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions)
        return [CfgProcessColumn(row) for row in rows]

    @classmethod
    def get_data_time(cls, db_instance, proc_id):
        dic_conditions = {cls.Columns.process_id.name: proc_id, cls.Columns.is_get_date.name: True}
        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions)
        if rows:
            return CfgProcessColumn(rows[0])
        return None

    @classmethod
    def get_all_columns(cls, db_instance, proc_id):
        dic_conditions = {cls.Columns.process_id.name: proc_id}
        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions)
        return [CfgProcessColumn(row) for row in rows]

    @classmethod
    def get_by_column_types(cls, db_instance, column_types: list[int]) -> list:
        dic_conditions = {cls.Columns.column_type.name: [(SqlComparisonOperator.IN, tuple(column_types))]}
        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions)
        return [CfgProcessColumn(row) for row in rows]

    def is_master_data_column(self) -> bool:
        return DataGroupType.is_master_data_column(self.column_type) and self.function_detail is None

    def is_generate_equation_column(self) -> bool:
        return self.function_detail is not None

    @classmethod
    def get_columns_by_process_id(cls, db_instance, proc_id):
        dic_conditions = {cls.Columns.process_id.name: [(SqlComparisonOperator.IN, tuple(proc_id))]}
        _, rows = cls.select_records(db_instance, dic_conditions=dic_conditions)
        return [
            {
                cls.Columns.id.name: row.id,
                'name': row.shown_name,
                cls.Columns.data_type.name: row.data_type,
            }
            for row in rows
        ]

    def is_data_source_name_column(self):
        return DataGroupType.is_data_source_name(self.column_type)


def get_order_for_special_columns(
    df_columns: pd.DataFrame,
    cfg_data_table: Union[CfgDataTable, BSCfgDataTable],
) -> pd.Series:
    """
    :param df_columns:
    :param cfg_data_table:
    :return: A series mapped from data_group_type to order.
        Since master data, datetime and serial are unique for each data source, we can assure that
        this mapping to also unique
    """
    master_type_and_order_mapping = {
        c.data_group_type: c.order
        for c in cfg_data_table.columns
        if DataGroupType.is_master_data_column(c.data_group_type)
        or DataGroupType(c.data_group_type) is DataGroupType.DATA_SERIAL
        or DataGroupType(c.data_group_type) is DataGroupType.DATA_TIME
    }

    return df_columns[MDataGroup.Columns.data_group_type.name].map(master_type_and_order_mapping)


@BridgeStationModel.use_db_instance()
def get_order_for_normal_column(
    df_columns: pd.DataFrame,
    cfg_data_table: Union[CfgDataTable, BSCfgDataTable],
    db_instance: PostgreSQL = None,
) -> pd.DataFrame:
    """
    :param df_columns:
    :param cfg_data_table:
    :param db_instance:
    :return: A series mapped from data_id to order
    """
    sql_statement = f'''
SELECT
    {MappingProcessData.get_table_name()}.{MappingProcessData.Columns.data_id.name}
    , {CfgDataTableColumn.get_table_name()}."order"
FROM {CfgDataTableColumn.get_table_name()}
JOIN {MappingProcessData.get_table_name()} ON
    {MappingProcessData.get_table_name()}.{MappingProcessData.Columns.t_data_name.name}
        = {CfgDataTableColumn.get_table_name()}.{CfgDataTableColumn.column_name.key}
WHERE
    {CfgDataTableColumn.get_table_name()}.{CfgDataTableColumn.data_table_id.key}
        = {BridgeStationModel.get_parameter_marker()}
'''
    params = [str(cfg_data_table.id)]
    cols, rows = db_instance.run_sql(sql_statement, params=params, row_is_dict=False)
    df_mapping = pd.DataFrame(data=rows, columns=cols)
    data_name_and_order_mapping = df_mapping.set_index(MappingProcessData.Columns.data_id.name).to_dict()[
        BSCfgDataTableColumn.Columns.order.name
    ]

    return df_columns[MData.get_foreign_id_column_name()].map(data_name_and_order_mapping)


@BridgeStationModel.use_db_instance()
def get_order_for_all_columns(
    df_columns: pd.DataFrame,
    cfg_data_table: Union[CfgDataTable, BSCfgDataTable],
    db_instance: PostgreSQL = None,
) -> pd.DataFrame:
    df_columns[CfgProcessColumn.Columns.order.name] = None
    df_columns[CfgProcessColumn.Columns.order.name] = (
        df_columns[CfgProcessColumn.Columns.order.name]
        .fillna(get_order_for_special_columns(df_columns, cfg_data_table))
        .fillna(get_order_for_normal_column(df_columns, cfg_data_table, db_instance=db_instance))
    )
    return df_columns


def gen_cfg_process_column(
    db_instance: PostgreSQL,
    cfg_data_table: Union[CfgDataTable, BSCfgDataTable],
    df_m_process: pd.DataFrame,
    process_ids: list[int],
):
    from bridge.services.data_import import (
        insert_by_df_ignore_duplicate,
    )

    df_m_config_equation = CfgProcessFunctionColumn.get_all_as_df(
        db_instance,
        select_cols=[CfgProcessFunctionColumn.Columns.id.name, CfgProcessFunctionColumn.Columns.return_type.name],
    )
    df_m_data_group = MDataGroup.get_all_as_df(db_instance)
    df_m_unit = MUnit.get_all_as_df(db_instance)
    if cfg_data_table.data_source.master_type == MasterDBType.OTHERS.name:
        MData.hide_col_by_process_id(db_instance, process_ids[0], is_hide=False)

    df_m_data = MData.get_all_as_df(db_instance)
    df_m_data = df_m_data[~df_m_data[MData.Columns.is_hide.name].isin([True])]

    df_columns = df_m_data.merge(
        df_m_process,
        how='inner',
        on=MProcess.get_foreign_id_column_name(),
        suffixes=Suffixes.KEEP_LEFT,
    )
    df_columns = df_columns.merge(
        df_m_data_group,
        how='inner',
        on=MDataGroup.get_foreign_id_column_name(),
        suffixes=Suffixes.KEEP_LEFT,
    )
    df_columns = df_columns.merge(
        df_m_unit,
        how='inner',
        on=MUnit.get_foreign_id_column_name(),
        suffixes=Suffixes.KEEP_LEFT,
    )
    # TODO: Remove if delete column config_equation_id in m_data
    df_columns = df_columns.rename(
        columns={'config_equation_id': CfgProcessFunctionColumn.get_foreign_id_column_name()},
    )
    df_columns = df_columns.merge(
        df_m_config_equation,
        how='left',
        on=CfgProcessFunctionColumn.get_foreign_id_column_name(),
        suffixes=Suffixes.KEEP_LEFT,
    )

    df_columns[CfgProcessColumn.get_foreign_id_column_name()] = df_columns[MData.get_foreign_id_column_name()]
    df_columns[CfgProcessColumn.Columns.name_jp.name] = df_columns[MDataGroup.get_jp_name_column()]
    df_columns[CfgProcessColumn.Columns.name_en.name] = (
        df_columns[MDataGroup.get_en_name_column()]
        .replace({EMPTY_STRING: pd.NA})
        .fillna(
            df_columns[CfgProcessColumn.Columns.name_jp.name].apply(to_romaji, remote_underline=True),
        )
    )
    df_columns[CfgProcessColumn.Columns.name_local.name] = df_columns[MDataGroup.get_local_name_column()]
    df_columns[CfgProcessColumn.Columns.column_type.name] = df_columns[MDataGroup.Columns.data_group_type.name]
    df_columns[CfgProcessColumn.Columns.column_name.name] = df_columns[MDataGroup.get_sys_name_column()].fillna(
        __NO_NAME__.lower(),
    )
    if cfg_data_table.get_master_type() == MasterDBType.V2_MULTI_HISTORY.name:
        from bridge.services.etl_services.etl_v2_multi_history_service import V2MultiHistoryService

        df_columns[CfgProcessColumn.Columns.name_en.name] = df_columns[CfgProcessColumn.Columns.name_en.name].apply(
            V2MultiHistoryService.rename_column_name,
        )

    df_columns = get_order_for_all_columns(df_columns, cfg_data_table, db_instance=db_instance)

    # Set auto increment flag to process column
    if cfg_data_table:
        # find auto increment cols corresponding to data_table_id
        auto_increment_table_cols = list(
            filter(
                lambda x: x.data_group_type == DataGroupType.AUTO_INCREMENTAL.value,
                cfg_data_table.columns,
            ),
        )
        data_table_ids: list[int] = [x.data_table_id for x in auto_increment_table_cols]
        if data_table_ids:
            _, rows = MappingProcessData.get_by_data_table_ids(db_instance, data_table_ids, row_is_dict=True)
        auto_increment_cols = []
        for table_col in auto_increment_table_cols:
            _filter = filter(
                lambda x: x[MappingProcessData.Columns.data_table_id.name] == table_col.data_table_id
                and x[MappingProcessData.Columns.t_data_name.name] == table_col.column_name,
                rows,
            )
            auto_increment_cols.extend([x[MappingProcessData.Columns.data_id.name] for x in _filter])

        target_index = df_columns[df_columns[MappingProcessData.Columns.data_id.name].isin(auto_increment_cols)].index
        df_columns.loc[target_index, CfgProcessColumn.Columns.column_type.name] = DataGroupType.AUTO_INCREMENTAL.value
        df_columns.loc[target_index, CfgProcessColumn.Columns.is_auto_increment.name] = True

    if CfgProcessFunctionColumn.Columns.return_type.name in df_columns:
        df_columns[CfgProcessColumn.Columns.raw_data_type.name] = df_columns[
            CfgProcessColumn.Columns.data_type.name
        ].fillna(df_columns[CfgProcessFunctionColumn.Columns.return_type.name])

    df_columns = df_columns[df_columns[CfgProcessColumn.Columns.raw_data_type.name].notnull()]

    series = df_columns[MDataGroup.Columns.data_group_type.name] == DataGroupType.DATA_TIME.value
    df_columns[CfgProcessColumn.Columns.is_get_date.name] = series

    series = df_columns[MDataGroup.Columns.data_group_type.name] == DataGroupType.DATA_SERIAL.value
    df_columns[CfgProcessColumn.Columns.is_serial_no.name] = series

    df_columns[CfgProcessColumn.Columns.data_type.name] = df_columns[
        CfgProcessColumn.Columns.raw_data_type.name
    ].replace(dict_convert_raw_data_type)

    # set text-overflow
    df_columns[CfgProcessColumn.Columns.bridge_column_name.name] = (
        UNDER_SCORE
        + df_columns['process_column_id'].astype(str)
        + UNDER_SCORE
        + df_columns[CfgProcessColumn.Columns.column_name.name].apply(camel_to_snake, limit_len=LIMIT_LEN_BS_COL_NAME)
    ).str[:MAX_NAME_LENGTH:]

    # get source_column_name = origin_column_name
    # if master get origin_column_name in cfg_data_table_columns
    df_master = df_columns[
        ~df_columns[CfgProcessColumn.Columns.column_type.name].isin(
            [DataGroupType.GENERATED.value],
        )
    ]
    df_data_table_cols = BSCfgDataTableColumn.get_all_as_df(db_instance)
    df_data_table_col = df_data_table_cols[
        df_data_table_cols[CfgDataTableColumn.data_table_id.name] == cfg_data_table.id
    ][[CfgDataTableColumn.column_name.name, CfgDataTableColumn.data_group_type.name]]
    df_data_table_col = df_data_table_col.rename(
        columns={
            CfgDataTableColumn.column_name.name: CfgProcessColumn.Columns.column_raw_name.name,
            CfgDataTableColumn.data_group_type.name: CfgProcessColumn.Columns.column_type.name,
        },
    )

    df_merge_master = df_master.merge(
        df_data_table_col,
        how='inner',
        on=CfgProcessColumn.Columns.column_type.name,
        suffixes=Suffixes.KEEP_LEFT,
    )
    represent_column_types = DataGroupType.get_represent_column_values()
    represent_column_types.append(DataGroupType.FileName.value)
    df_represent_columns = df_columns[
        df_columns[CfgProcessColumn.Columns.column_type.name].isin(represent_column_types)
    ]
    df_represent_columns[CfgProcessColumn.Columns.column_raw_name.name] = df_represent_columns[
        CfgProcessColumn.Columns.column_name.name
    ]
    # if horizon get origin_column_name in mapping_process_data
    _, rows = MappingProcessData.get_by_data_table_ids(db_instance, [cfg_data_table.id], row_is_dict=True)
    df_mapping_process_data = pd.DataFrame(rows)
    df_mapping_process_data = df_mapping_process_data[
        [MappingProcessData.Columns.data_id.name, MappingProcessData.Columns.t_data_name.name]
    ].rename(columns={MappingProcessData.Columns.t_data_name.name: CfgProcessColumn.Columns.column_raw_name.name})

    df_generated = df_columns[df_columns[CfgProcessColumn.Columns.column_type.name] == DataGroupType.GENERATED.value]

    df_merge_generated = df_generated.merge(
        df_mapping_process_data,
        how='left',
        on=MappingProcessData.Columns.data_id.name,
        suffixes=Suffixes.KEEP_LEFT,
    )
    df_merge_generated.drop_duplicates(subset=MappingProcessData.Columns.data_id.name, inplace=True)
    df_merge_generated[CfgProcessColumn.Columns.column_raw_name.name] = df_merge_generated[
        CfgProcessColumn.Columns.column_raw_name.name
    ].fillna(df_merge_generated[CfgProcessColumn.Columns.column_name.name])
    df_columns = pd.concat([df_merge_master, df_merge_generated, df_represent_columns])
    # set Dummy Datetime is_get_date
    df_columns = df_columns.reset_index(drop=True)
    df_process_columns: DataFrame = pd.DataFrame()
    for process_id, df in df_columns.groupby('process_id'):
        if not df[CfgProcessColumn.Columns.is_get_date.name].any():
            df.reset_index(inplace=True)
            if not cfg_data_table.is_has_auto_increment_col():
                mask_is_get_date = df[CfgProcessColumn.Columns.column_raw_name.name] == DATETIME_DUMMY
                df[CfgProcessColumn.Columns.is_dummy_datetime.name] = mask_is_get_date

            mask_is_get_date = df[CfgProcessColumn.Columns.raw_data_type.name] == RawDataTypeDB.DATETIME.name
            # set first column has data time DATATIME is_get_date
            first_true_index = df[mask_is_get_date].iloc[0]['index']
            df.loc[first_true_index, CfgProcessColumn.Columns.is_get_date.name] = True

        df_process_columns = pd.concat([df_process_columns, df], ignore_index=True)

    insert_by_df_ignore_duplicate(db_instance, df_process_columns, CfgProcessColumn)
