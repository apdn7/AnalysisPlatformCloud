from typing import Iterator, Tuple, Type, Union

import pandas as pd
from pandas import DataFrame

from ap.common.common_utils import format_df, merge_list_in_list_to_one_list
from ap.common.constants import (
    DEFAULT_NONE_VALUE,
    SQL_LIMIT_SCAN_DATA_TYPE,
    DataGroupType,
    EFAMasterColumn,
    JobType,
    MasterDBType,
)
from ap.common.logger import log_execution_time
from ap.common.memoize import memoize
from ap.common.pydn.dblib.db_common import add_double_quote, gen_select_col_str
from ap.common.pydn.dblib.db_proxy_readonly import ReadOnlyDbProxy
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module.models import (
    CfgDataTable,
    CfgDataTableColumn,
)
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.mapping_factory_machine import MappingFactoryMachine
from bridge.models.mapping_part import MappingPart
from bridge.models.mapping_process_data import MappingProcessData
from bridge.services.etl_services.etl_db_service import (
    EtlDbService,
    build_distinct_sql_by_time_range,
    build_sql_by_time_range,
    get_dict_rename_column,
    get_factory_data_partitions,
    get_physical_tables_by_time,
    get_two_partitions_for_scan,
)
from bridge.services.extend_sessor_column_handler import add_extend_sensor_column
from bridge.services.utils import get_well_known_columns


class EFAService(EtlDbService):
    @log_execution_time()
    def get_transaction_data(self, factory_db_instance, start_dt, end_dt, is_only_pull_sample_data=False):
        """
        Gets raw data from data source
        :param factory_db_instance:
        :param is_only_pull_sample_data:
        :param start_dt:
        :param end_dt:
        :return:
        """
        self.check_db_connection()

        physical_tables = get_physical_tables_by_time(self.cfg_data_table.id, start_dt, end_dt)
        if not physical_tables:
            yield None
            return

        table_names = [table.table_name for table in physical_tables]

        select_columns = [col for col in self.source_column_names if col not in EFAMasterColumn.get_default_column()]
        data = get_factory_data_partitions(
            factory_db_instance,
            select_columns,
            self.auto_increment_col,
            start_dt,
            end_dt,
            table_names,
            is_only_pull_sample_data,
        )
        cols = next(data)
        if not cols:
            yield None
            return

        yield cols

        yield from data

    @log_execution_time(prefix='etl_efa_service')
    @BridgeStationModel.use_db_instance_generator()
    def get_master_data(
        self,
        db_instance: PostgreSQL = None,
    ) -> Iterator[
        Tuple[
            dict[Type[Union[MappingProcessData, MappingPart, MappingFactoryMachine, str]], DataFrame],
            Union[int, float],
        ]
    ]:
        self.check_db_connection()

        df = self.get_distinct_unique_id()
        if df is None:
            return

        convert_col, dict_config = get_factory_master_data(self.cfg_data_table)
        df = join_master_name_efa(df, convert_col, dict_config)

        df, *_ = add_extend_sensor_column(df)

        if CfgDataTableColumn.data_table_id.name not in df.columns:
            df[CfgDataTableColumn.data_table_id.name] = self.cfg_data_table.id

        yield from self.split_master_data([(df, None, 99)])

        self.set_done_status_for_scan_master_job(db_instance=db_instance)

    def gen_df_transaction(self, cols, rows, convert_col, dict_config):
        df = self.base_gen_df_transaction(cols, rows)

        # add col in df when user not select scan master
        master_type = self.master_type
        well_know_columns = get_well_known_columns(master_type, None)
        add_columns = set(well_know_columns.keys()) - set(df.columns)
        for col in add_columns:
            df[col] = DEFAULT_NONE_VALUE
        # TODO: why not join with master Oracle when getting transaction data
        df = join_master_name_efa(df, convert_col, dict_config, is_original_column_name=True)

        # no records
        if not len(df):
            return None

        data_name_col, data_id_col, data_value_col = 'DATA_NAME', 'CHECK_CODE', None
        for table_column in self.cfg_data_table.columns:
            if table_column.data_group_type == DataGroupType.DATA_NAME.value:
                data_name_col = table_column.column_name
            if table_column.data_group_type == DataGroupType.DATA_ID.value:
                data_id_col = table_column.column_name
            if table_column.data_group_type == DataGroupType.DATA_VALUE.value:
                data_value_col = table_column.column_name

        df, *_ = add_extend_sensor_column(df, data_name_col, data_id_col, data_value_col)

        return df

    @log_execution_time(prefix='etl_efa_service')
    @BridgeStationModel.use_db_instance_generator()
    def get_data_for_data_type(self, generator_df=None, db_instance: PostgreSQL = None):
        self.check_db_connection()

        datetime_column = self.cfg_data_table.get_date_col()

        dict_pair_bridge_source = dict(zip(self.bridge_column_names, self.source_column_names))

        default_select_columns = [
            DataGroupType.PROCESS_ID.name,
            DataGroupType.DATA_ID.name,
            DataGroupType.DATA_VALUE.name,
            DataGroupType.LINE_ID.name,
            DataGroupType.DATA_SERIAL.name,
            DataGroupType.DATA_TIME.name,
        ]
        # select_columns = [dict_pair_bridge_source[col.name] for col in select_columns]
        select_columns = []
        for key, value in dict_pair_bridge_source.items():
            if key in default_select_columns and value not in EFAMasterColumn.get_default_column():
                select_columns.append(value)

        sel_cols = ','.join([add_double_quote(col) for col in select_columns])

        dict_df_rename_columns = get_dict_rename_column(self.cfg_data_table)

        m_prod_df = None
        m_prod_family_df = None
        # ignore_cols = [DataGroupType.DATA_TIME.name, DataGroupType.DATA_SERIAL.name]
        ignore_cols = None
        if generator_df is not None:
            for _df, *_ in generator_df or []:
                res = self.gen_horizon_df_for_db_data(
                    _df,
                    dict_df_rename_columns,
                    default_select_columns,
                    ignore_cols,
                    m_prod_df=m_prod_df,
                    m_prod_family_df=m_prod_family_df,
                )
                df, dic_df_horizons, m_prod_df, m_prod_family_df = res

                yield df, dic_df_horizons, 99

            return

        # get partition that already scan master
        job_type = JobType.USER_APPROVED_MASTER if self.is_user_approved_master else JobType.SCAN_DATA_TYPE
        cfg_partitions = self.cfg_data_table.get_partition_for_job(job_type, many=True, **{'db_instance': db_instance})
        cfg_partitions = get_two_partitions_for_scan(cfg_partitions)
        if len(cfg_partitions):
            one_step_percent = 100 // len(cfg_partitions)
            with ReadOnlyDbProxy(self.cfg_data_table.data_source) as factory_db_instance:
                for idx, cfg_partition in enumerate(cfg_partitions, start=1):
                    if cfg_partition.is_no_min_max_date_time():
                        # Skip get data for empty partition tables
                        self.set_done_status_for_scan_data_type_job(cfg_partition)
                        continue

                    sqls = build_sql_by_time_range_data_type(
                        sel_cols,
                        cfg_partition.table_name,
                        datetime_column,
                        cfg_partition.min_time,
                        cfg_partition.max_time,
                        db_instance=factory_db_instance,
                    )
                    for sql, params in sqls:
                        cols, rows = factory_db_instance.run_sql(sql, row_is_dict=False, params=params)
                        if not rows:
                            continue

                        df = pd.DataFrame(rows, columns=cols, dtype='object')
                        df = format_df(df)
                        res = self.gen_horizon_df_for_db_data(
                            df,
                            dict_df_rename_columns,
                            default_select_columns,
                            ignore_cols,
                            m_prod_df,
                            m_prod_family_df,
                        )
                        df, dic_df_horizons, m_prod_df, m_prod_family_df = res
                        yield df, dic_df_horizons, one_step_percent * idx

                    self.set_done_status_for_scan_data_type_job(cfg_partition, db_instance=db_instance)

    @log_execution_time()
    def gen_horizon_df_for_db_data(
        self,
        df,
        dict_df_rename_columns,
        default_select_columns,
        ignore_cols,
        m_prod_df=None,
        m_prod_family_df=None,
    ):
        df.rename(columns=dict_df_rename_columns, inplace=True)
        convert_col, dict_config = get_factory_master_data(self.cfg_data_table)
        df = join_master_name_efa(df, convert_col, dict_config)
        add_columns = set(default_select_columns) - set(df.columns)
        for col in add_columns:
            if col == DataGroupType.PROCESS_ID.name:
                df[DataGroupType.PROCESS_NAME.name] = DEFAULT_NONE_VALUE

            if col == DataGroupType.DATA_ID.name:
                df[DataGroupType.DATA_NAME.name] = DEFAULT_NONE_VALUE

            df[col] = DEFAULT_NONE_VALUE
        df, m_prod_df, m_prod_family_df = add_extend_sensor_column(
            df,
            m_prod_df=m_prod_df,
            m_prod_family_df=m_prod_family_df,
        )
        df, dic_df_horizons = self.transform_horizon_columns_for_import(
            df,
            ignore_cols=ignore_cols,
        )
        return df, dic_df_horizons, m_prod_df, m_prod_family_df

    def get_distinct_unique_id(self):
        dict_pair_bridge_source = dict(zip(self.bridge_column_names, self.source_column_names))
        all_columns = set(merge_list_in_list_to_one_list(self.dic_mapping_group.values()))

        # select all
        select_columns = []
        for key, value in dict_pair_bridge_source.items():
            if key in all_columns and value not in EFAMasterColumn.get_default_column():
                select_columns.append(value)
        # select_columns = [dict_pair_bridge_source[col] for col in all_columns]

        # get distinct source data, all columns (relation of key columns)
        if len(select_columns) > 0:
            df_all_columns = self.get_distinct_data_from_factory_db(select_columns)

            if df_all_columns.empty:
                return None
        else:
            return None

        # rename to bridge station name
        df_all_columns.rename(columns=dict(zip(self.source_column_names, self.bridge_column_names)), inplace=True)
        # Add column __LINE_ID__
        df_columns = list(df_all_columns.columns)
        add_columns = set(self.bridge_column_names) - set(df_columns)
        for col in add_columns:
            df_all_columns[col] = DEFAULT_NONE_VALUE

        return df_all_columns

    def get_distinct_data_from_factory_db(self, select_columns):
        """
        Gets raw data from data source

        :param select_columns:
        :return:
        """
        data_source_db = self.cfg_data_table.data_source
        # get scan master
        cfg_partitions = self.cfg_data_table.get_partition_for_job(JobType.SCAN_MASTER, many=True)
        if not cfg_partitions:
            return pd.DataFrame()

        datetime_column = self.cfg_data_table.get_date_col()
        sel_cols = ','.join([add_double_quote(col) for col in select_columns])
        rows = []
        cols = []
        scan_master_partitions = get_two_partitions_for_scan(cfg_partitions)

        with ReadOnlyDbProxy(data_source_db) as factory_db_instance:
            for cfg_partition in scan_master_partitions:
                if cfg_partition.is_no_min_max_date_time():
                    # Skip get data for empty partition tables
                    continue

                _sql, _params = build_distinct_sql_by_time_range(
                    sel_cols,
                    cfg_partition.table_name,
                    datetime_column,
                    cfg_partition.min_time,
                    cfg_partition.max_time,
                    db_instance=factory_db_instance,
                )
                cols, _rows = factory_db_instance.run_sql(_sql, params=_params)
                rows.extend(_rows)

        df = pd.DataFrame(rows, columns=cols, dtype='object')
        if not df.empty:
            df = format_df(df)
            df = df.drop_duplicates()
        return df


@memoize(is_save_file=True, duration=5 * 60)
@log_execution_time()
def get_factory_master_data(cfg_data_table: CfgDataTable):
    _d = DataGroupType
    dict_config = {  # Rule for master name. TODO: how to common ?
        _d.LINE_ID: (
            'LINE_MASTER',
            f'LINE_NO as {_d.LINE_ID.name}',
            f'LINE_NAME as {_d.LINE_NAME.name}',
        ),
        _d.PROCESS_ID: (
            'PROCESS_MASTER',
            f'LINE_NO as {_d.LINE_ID.name}',
            f'PROCESS_NO as {_d.PROCESS_ID.name}',
            f'PROCESS_NAME as {_d.PROCESS_NAME.name}',
        ),
        _d.EQUIP_ID: (
            'EQUIP_MASTER',
            f'EQUIP_NO as {_d.EQUIP_ID.name}',
            f'LINE_NO as {_d.LINE_ID.name}',
            f'PROCESS_NO as {_d.PROCESS_ID.name}',
            f'EQUIP_NAME as {_d.EQUIP_NAME.name}',
        ),
        _d.DATA_ID: (
            'QUALITY_ID_MASTER',
            f'QUALITY_ID as {_d.DATA_ID.name}',
            f'QUALITY_NAME as {_d.DATA_NAME.name}',
        ),
    }

    convert_col = {
        _d.LINE_ID.name: 'LINE_NO',
        _d.PROCESS_ID.name: 'PROCESS_NO',
        _d.EQUIP_ID.name: 'EQUIP_NO',
        _d.DATA_ID.name: 'CHECK_CODE',
    }

    master_type = cfg_data_table.get_master_type()

    if master_type == MasterDBType.EFA_HISTORY.name:
        dict_config.update(
            {
                _d.DATA_ID: (
                    'PARTS_MASTER',
                    f'PART_TYPE_CODE as {_d.DATA_ID.name}',
                    f"'part_' || PART_TYPE_NAME as {_d.DATA_NAME.name}",
                ),
            },
        )

        convert_col.update({_d.DATA_ID.name: 'PART_TYPE_CODE'})

    return_dict = {}
    with ReadOnlyDbProxy(cfg_data_table.data_source) as factory_db_instance:
        for data_group_id, table_and_name_column in dict_config.items():
            key_cols, df_temp = get_external_master_data(factory_db_instance, table_and_name_column)
            return_dict[data_group_id] = (key_cols, df_temp)

    return convert_col, return_dict


@log_execution_time()
def join_master_name_efa(
    df: DataFrame,
    dic_convert_cols: dict,
    dict_config: dict,
    is_original_column_name: bool = False,
):
    """
    Join with master tables of Factory DB (short term) to get more information
     -> change to Bridge DB (long term) in the future
    :param dict_config:
    :param dic_convert_cols:
    :param is_original_column_name:
    :param df:
    :return: df
    """

    # primary_groups: PrimaryGroup = get_primary_group()
    temp_cols = set()
    for data_group_id, table_data in dict_config.items():
        master_key_cols, df_master = table_data

        common_cols = []
        for bridge_column_name in master_key_cols:
            if bridge_column_name in df.columns:
                common_cols.append(bridge_column_name)
            else:
                convert_col = dic_convert_cols.get(bridge_column_name)
                if is_original_column_name and convert_col in df.columns:
                    temp_cols.add(bridge_column_name)
                    df[bridge_column_name] = df[convert_col]
                    common_cols.append(bridge_column_name)

        if len(common_cols) == len(master_key_cols):
            df = df.merge(df_master, how='inner', on=common_cols, suffixes=(None, '_y'))

    if temp_cols:
        df.drop(columns=list(temp_cols), inplace=True)

    return df


def get_external_master_data(factory_db_instance, table_and_name_column):
    table_name, *col_names = table_and_name_column
    col_names_str = gen_select_col_str(column_names=col_names, is_add_double_quote=False)
    sql = f'''SELECT DISTINCT {col_names_str} FROM {table_name}'''
    cols, rows = factory_db_instance.run_sql(sql, row_is_dict=False)
    df_temp = pd.DataFrame(rows, columns=cols, dtype='object')
    df_temp = format_df(df_temp)
    # ignore last column ( ex : name )
    return cols[:-1], df_temp


def build_sql_by_time_range_data_type(
    sel_cols,
    table_name,
    date_column,
    min_time,
    max_time,
    limit=SQL_LIMIT_SCAN_DATA_TYPE,
    db_instance=None,
):
    all_params = ()
    all_sqls = []
    sqls_generators = build_sql_by_time_range(
        sel_cols,
        table_name,
        date_column,
        min_time,
        max_time,
        limit,
        is_distinct=False,
        db_instance=db_instance,
    )
    for _sql, _param in sqls_generators:
        all_sqls.append(_sql)
        all_params += tuple(_param)

    sql = f'SELECT {sel_cols} FROM ({" UNION ALL ".join(all_sqls)}) t'
    return [[sql, all_params]]
