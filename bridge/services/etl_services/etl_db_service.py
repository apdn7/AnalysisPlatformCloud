from __future__ import annotations

import contextlib
from datetime import datetime
from typing import Any, Iterator, Tuple, Type, Union

import numpy as np
import pandas as pd
import sqlalchemy as sa
from dateutil.relativedelta import relativedelta
from pandas import DataFrame
from sqlalchemy import func, select
from sqlalchemy.orm import scoped_session

from ap.api.setting_module.services.data_import import (
    check_update_time_by_changed_tz,
    convert_df_col_to_utc,
    get_tzoffset_of_random_record,
    validate_datetime,
)
from ap.common.common_utils import (
    DATE_FORMAT_STR_YYYYMM,
    add_delta_to_datetime,
    add_months,
    add_seconds,
    add_years,
    calculator_month_ago,
    convert_time,
    format_df,
    get_recent_elements_in_list,
)
from ap.common.constants import (
    DAY_ANCHORS,
    DB_LIMIT_SCAN_MASTER,
    DEFAULT_NONE_VALUE,
    EFA_LIMIT_SCAN_MASTER,
    FETCH_MANY_SIZE,
    IMPORT_FUTURE_MONTH_AGO,
    LATEST_RECORDS_SQL_LIMIT,
    MSG_DB_CON_FAILED,
    SQL_FACTORY_LIMIT,
    SQL_LIMIT_SCAN_DATA_TYPE,
    TIME_ANCHORS,
    BaseMasterColumn,
    DataGroupType,
    JobType,
    MasterDBType,
    OtherMasterColumn,
)
from ap.common.logger import log_execution_time, logger
from ap.common.memoize import memoize
from ap.common.pydn.dblib import mssqlserver
from ap.common.pydn.dblib.db_common import add_double_quote, db_instance_exec
from ap.common.pydn.dblib.db_proxy_readonly import ReadOnlyDbProxy
from ap.common.pydn.dblib.mssqlserver import MSSQLServer
from ap.common.pydn.dblib.mysql import MySQL
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.timezone_utils import detect_timezone, get_db_timezone, get_time_info
from ap.setting_module.models import (
    CfgDataSource,
    CfgDataTable,
    CfgPartitionTable,
    FactoryImport,
    MappingFactoryMachine,
    MappingPart,
    MappingProcessData,
    MDataGroup,
    make_session,
)
from ap.setting_module.services.process_config import (
    get_efa_partitions,
    get_random_partition_table_name,
)
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_data_source import CfgDataSource as BSCfgDataSource
from bridge.models.cfg_data_table import CfgDataTable as BSCfgDataTable
from bridge.models.cfg_partition_table import CfgPartitionTable as BSCfgPartitionTable
from bridge.services.etl_services.etl_service import ETLService


class EtlDbService(ETLService):
    limit_record = SQL_FACTORY_LIMIT
    limit_max_second = 16 * 24 * 60 * 60  # day x hour x minute x second = 32 days
    limit_min_second = 60 * 60  # 1 hour

    ng_infos = []
    ok_infos = []
    factory_next_sql_range_seconds = 12 * 60 * 60

    def is_efa_db(self):
        master_db_type = self.cfg_data_table.get_master_type()
        return master_db_type == MasterDBType.EFA.name

    def calc_sql_range_days(self, cur_record_cnt, start_dt, end_dt):
        start_datetime = convert_time(start_dt, return_string=False)
        diff_dt = convert_time(end_dt, return_string=False) - start_datetime
        cur_second_cnt = diff_dt.seconds + (diff_dt.days * 24 * 60 * 60)

        logger.info(f'PULL DATA INFO: {start_dt} - {end_dt}, {cur_second_cnt / 3600} hours, {cur_record_cnt} records')

        if cur_record_cnt >= self.limit_record:
            self.ng_infos.append((cur_second_cnt, start_datetime))
            second_cnt = cur_second_cnt / 2
        else:
            self.ok_infos.append((cur_second_cnt, start_datetime))
            second_cnt = cur_second_cnt * 2 if cur_record_cnt * 2 < self.limit_record else cur_second_cnt

            prev_second = self.limit_min_second
            if self.ok_infos:
                self.ok_infos = get_recent_elements_in_list(self.ok_infos, recent_day=14)
                prev_second, _ = max(self.ok_infos)

            prev_ng_second = self.limit_max_second
            if self.ng_infos:
                self.ng_infos = get_recent_elements_in_list(self.ng_infos, recent_day=14)
                prev_ng_second, _ = min(self.ng_infos)

            # use last good candidate
            if second_cnt < prev_second or second_cnt >= prev_ng_second:
                second_cnt = prev_second

        # make sure range is 1 ~ 256 days
        second_cnt = min(second_cnt, self.limit_max_second)
        second_cnt = max(second_cnt, self.limit_min_second)

        self.factory_next_sql_range_seconds = second_cnt
        logger.info(f'PULL DATA INFO: next time range will be {second_cnt / 3600} hours')

        return second_cnt

    def check_db_connection(self):
        """
        Check database connection is available or not

        If it cannot connect to Database, raise error to stop doing anything
        """
        data_source_db: CfgDataSource = self.cfg_data_table.data_source
        with ReadOnlyDbProxy(data_source_db) as db_instance:
            if not db_instance.is_connected:
                raise Exception(MSG_DB_CON_FAILED)

    @log_execution_time(prefix='etl_db_service')
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

        yield from self.split_master_data([(df, None, 99)])

        self.set_done_status_for_scan_master_job(db_instance=db_instance)

    @BridgeStationModel.use_db_instance()
    def set_done_status_for_scan_master_job(self, db_instance: PostgreSQL = None):
        rows = BSCfgPartitionTable.get_by_data_table_id(db_instance, self.cfg_data_table.id)
        cfg_partition_ids = [
            x.get(BSCfgPartitionTable.Columns.id.name)
            for x in filter(lambda x: x.get(BSCfgPartitionTable.Columns.job_done.name) is None, rows)
        ]
        BSCfgPartitionTable.bulk_update_by_ids(
            db_instance,
            ids=cfg_partition_ids,
            dic_update_values={BSCfgPartitionTable.Columns.job_done.name: JobType.SCAN_MASTER.name},
        )

    @BridgeStationModel.use_db_instance()
    def set_done_status_for_scan_data_type_job(
        self,
        cfg_partition: CfgPartitionTable,
        db_instance: PostgreSQL = None,
    ):
        if cfg_partition.is_no_min_max_date_time():
            # Skip get data for empty partition tables
            return

        BSCfgPartitionTable.bulk_update_by_ids(
            db_instance,
            ids=[cfg_partition.id],
            dic_update_values={BSCfgPartitionTable.Columns.job_done.name: JobType.SCAN_DATA_TYPE.name},
        )

    @BridgeStationModel.use_db_instance()
    def set_all_scan_data_type_status_done(self, db_instance: PostgreSQL = None):
        cfg_partitions = BSCfgPartitionTable.get_by_data_table_id(db_instance, self.cfg_data_table.id)
        BSCfgPartitionTable.bulk_update_by_ids(
            db_instance,
            ids=[cfg_partition[BSCfgPartitionTable.Columns.id.name] for cfg_partition in cfg_partitions],
            dic_update_values={BSCfgPartitionTable.Columns.job_done.name: JobType.SCAN_DATA_TYPE.name},
        )

    @log_execution_time(prefix='etl_db_service')
    @BridgeStationModel.use_db_instance_generator()
    def get_data_for_data_type(self, generator_df=None, db_instance: PostgreSQL = None):
        self.check_db_connection()

        limit = LATEST_RECORDS_SQL_LIMIT if self.is_horizon_data() else SQL_LIMIT_SCAN_DATA_TYPE

        self.cfg_data_table.get_auto_increment_col_else_get_date()
        data_table_columns = self.cfg_data_table_columns
        dummy_column_names = BaseMasterColumn.get_dummy_column_name()
        select_columns = [
            column.column_name for column in data_table_columns if column.column_name not in dummy_column_names
        ]

        sel_cols = ','.join([add_double_quote(col) for col in select_columns])

        dict_df_rename_columns = get_dict_rename_column(self.cfg_data_table)

        m_prod_df = None
        m_prod_family_df = None
        ignore_cols = None
        if generator_df is not None:
            for df_original, *_ in generator_df or []:
                res = self.gen_horizon_df_for_db_data(
                    df_original,
                    dict_df_rename_columns,
                    ignore_cols,
                    m_prod_df=m_prod_df,
                    m_prod_family_df=m_prod_family_df,
                )
                df, dic_df_horizons, m_prod_df, m_prod_family_df = res

                yield df, dic_df_horizons, 99

            return

        # get partition that already scan master
        job_type = JobType.USER_APPROVED_MASTER if self.is_user_approved_master else JobType.SCAN_DATA_TYPE
        if isinstance(self.cfg_data_table, BSCfgDataTable):
            # TODO: implement later
            cfg_partitions = BSCfgPartitionTable.get_partition_for_job(
                db_instance,
                self.cfg_data_table.id,
                job_type,
            )
        else:
            cfg_partitions = self.cfg_data_table.get_partition_for_job(job_type, many=True)
        cfg_partitions = get_two_partitions_for_scan(cfg_partitions)
        if len(cfg_partitions):
            dic_use_cols = {col.column_name: col.data_type for col in self.cfg_data_table_columns}
            master_columns, master_values = self.get_dummy_master_column_value()
            one_step_percent = 100 // len(cfg_partitions)
            sent_count = 0
            with ReadOnlyDbProxy(self.cfg_data_table.data_source) as factory_db_instance:
                for idx, cfg_partition in enumerate(cfg_partitions, start=1):
                    if cfg_partition.is_no_min_max_date_time():
                        # Skip get data for empty partition tables
                        self.set_done_status_for_scan_data_type_job(cfg_partition, db_instance=db_instance)
                        continue

                    sql, params = db_instance_exec(
                        factory_db_instance,
                        select=sel_cols,
                        from_table=cfg_partition.table_name,
                        limit=limit,
                        with_run=False,
                    )

                    cols, rows = factory_db_instance.run_sql(sql, row_is_dict=False, params=params)
                    if not rows:
                        self.set_done_status_for_scan_data_type_job(cfg_partition, db_instance=db_instance)
                        continue

                    sent_count += len(rows)
                    df = pd.DataFrame(rows, columns=cols, dtype='object')
                    df = format_df(df)

                    # Add NULL for master column not select
                    self.add_dummy_master_columns(df, master_columns, master_values, dic_use_cols)
                    # check and add columns to dataframe if not present in list
                    self.add_dummy_horizon_columns(df)

                    res = self.gen_horizon_df_for_db_data(
                        df,
                        dict_df_rename_columns,
                        ignore_cols,
                        m_prod_df,
                        m_prod_family_df,
                    )
                    df, dic_df_horizons, m_prod_df, m_prod_family_df = res
                    yield df, dic_df_horizons, one_step_percent * idx

                    self.set_done_status_for_scan_data_type_job(cfg_partition, db_instance=db_instance)
                    if limit and sent_count >= limit:
                        return

    @log_execution_time()
    def gen_horizon_df_for_db_data(
        self,
        df,
        dict_df_rename_columns,
        ignore_cols,
        m_prod_df=None,
        m_prod_family_df=None,
    ):
        df, dic_df_horizons = self.transform_horizon_columns_for_import(
            df,
            ignore_cols=ignore_cols,
        )
        return df, dic_df_horizons, m_prod_df, m_prod_family_df

    def get_all_dt_cols(self):
        cols = self.datetime_cols + [self.auto_increment_col]
        if self.get_date_col:
            cols.append(self.get_date_col)
        return set(cols)

    def get_time_zone_info(self):
        cols = self.get_all_dt_cols()
        return {col: handle_time_zone(self.cfg_data_table, col) for col in cols}

    def convert_timezone(self, df, dic_tz_info):
        cols = self.get_all_dt_cols()
        for col in cols:
            # TODO : output error and duplicate info
            validate_datetime(df, col, add_is_error_col=False)
            is_tz_inside, db_time_zone, time_offset = dic_tz_info[col]
            df[col] = convert_df_col_to_utc(df, col, is_tz_inside, db_time_zone, time_offset)
            if is_tz_inside:
                with contextlib.suppress(Exception):
                    df[col] = df[col].dt.tz_convert(None)

    @log_execution_time()
    def count_transaction_data(self, factory_db_instance, start_dt: str, end_dt: str):
        """
        Gets raw data from data source
        :param factory_db_instance:
        :param start_dt: string start datetime
        :param end_dt: string end datetime
        :return:
        """
        self.check_db_connection()

        physical_tables = get_physical_tables_by_time(self.cfg_data_table.id, start_dt, end_dt)
        if not physical_tables:
            return None

        table_names = [table.table_name for table in physical_tables]

        select_columns = []
        data = get_factory_data_partitions(
            factory_db_instance,
            select_columns,
            self.auto_increment_col,
            start_dt,
            end_dt,
            table_names,
            is_count=True,
        )
        if data is None:
            return None

        _ = next(data)

        cnt = max(cnt for row in list(data) for cnt, *_ in row)
        return cnt

    @log_execution_time()
    def get_transaction_data(self, factory_db_instance, start_dt: str, end_dt: str):
        """
        Gets raw data from data source
        :param factory_db_instance:
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

        select_columns = [col for col in self.source_column_names if col not in OtherMasterColumn.get_default_value()]
        data = get_factory_data_partitions(
            factory_db_instance,
            select_columns,
            self.auto_increment_col,
            start_dt,
            end_dt,
            table_names,
        )
        cols = next(data)
        if not cols:
            yield None
            return

        yield cols

        yield from data

    def base_gen_df_transaction(self, cols, rows):
        df = pd.DataFrame(rows, columns=cols, dtype='object')
        df = format_df(df)
        df_origin = df.copy()
        df.drop_duplicates(inplace=True)
        df_duplicate: DataFrame = df_origin[~df_origin.index.isin(df.index)]
        self.export_duplicate_data_to_file(df_duplicate, self.cfg_data_table.name)
        return df

    def gen_df_transaction(self, cols, rows, convert_col, dict_config):
        df = self.base_gen_df_transaction(cols, rows)

        # no records
        if not len(df):
            return None

        # Add NULL for master column not select
        dic_use_cols = {col.column_name: col.data_type for col in self.cfg_data_table_columns}
        master_columns, master_values = self.get_dummy_master_column_value()
        self.add_dummy_master_columns(df, master_columns, master_values, dic_use_cols)

        if MasterDBType.is_long_db(self.cfg_data_table.get_master_type()):
            df = self.convert_to_standard_data(df)

        return df

    @log_execution_time()
    def convert_df_horizontal_to_vertical(self, df_horizontal_data: DataFrame):
        # Convert other horizontal columns such as "lot_not", "tray_no", etc.  to vertical columns
        data_name_col = None
        data_id_col = None
        data_value_col = None
        unique_cols: list[str] = []
        horizontal_cols = []

        for data_table_column in self.cfg_data_table_columns:
            if data_table_column.data_group_type == DataGroupType.DATA_NAME.value:
                data_name_col = data_table_column.column_name
            elif data_table_column.data_group_type == DataGroupType.DATA_ID.value:
                data_id_col = data_table_column.column_name
            elif data_table_column.data_group_type == DataGroupType.DATA_VALUE.value:
                data_value_col = data_table_column.column_name
            elif data_table_column.data_group_type == DataGroupType.UNIT.value:
                continue
            elif data_table_column.data_group_type == DataGroupType.HORIZONTAL_DATA.value:
                horizontal_cols.append(data_table_column.column_name)
            else:
                unique_cols.append(data_table_column.column_name)

        if not horizontal_cols:
            return pd.DataFrame()

        if data_name_col is None:
            data_name_col = DataGroupType.DATA_NAME.name
        if data_id_col is None:
            data_id_col = DataGroupType.DATA_ID.name
        if data_value_col is None:
            data_value_col = DataGroupType.DATA_VALUE.name

        dfs: list[DataFrame] = []
        for horizontal_col in horizontal_cols:
            df_vertical_data = (
                df_horizontal_data[[*unique_cols, horizontal_col]]
                .rename(
                    columns={horizontal_col: data_value_col},
                )
                .drop_duplicates()
                .assign(
                    **{
                        data_name_col: horizontal_col,
                        # set None because horizontal column has not unit & data id
                        self.unit_column: DEFAULT_NONE_VALUE,
                        data_id_col: DEFAULT_NONE_VALUE,
                    },
                )
            )

            dfs.append(df_vertical_data)

        return pd.concat(dfs).reset_index(drop=True)

    def get_distinct_unique_id(self):
        is_horizon_data_no_master = self.is_horizon_data() and not self.have_real_master_columns()
        if is_horizon_data_no_master:
            df_master_columns = self.generate_input_scan_master_for_horizontal_columns()
            return None if df_master_columns.empty else df_master_columns

        horizon_columns = [
            column.column_name
            for column in self.cfg_data_table_columns
            if column.data_group_type in [DataGroupType.DATA_SERIAL.value, DataGroupType.HORIZONTAL_DATA.value]
        ]
        # select column selected master in cfg_data_table
        select_columns = [
            column.column_name
            for column in self.cfg_data_table_columns
            if (
                column.column_name not in OtherMasterColumn.get_default_value()
                and column.data_group_type
                not in [
                    DataGroupType.HORIZONTAL_DATA.value,
                    DataGroupType.DATA_VALUE.value,
                    DataGroupType.DATA_TIME.value,
                    DataGroupType.DATA_SERIAL.value,
                ]
            )
        ]

        # get distinct source data, all columns (relation of key columns)
        if not select_columns:
            df = pd.DataFrame()
            return df

        df_master_columns = self.get_distinct_data_from_factory_db(select_columns)
        df_master_columns = df_master_columns.drop_duplicates()

        # add column horizon
        if len(horizon_columns):
            df_master_columns[horizon_columns] = DEFAULT_NONE_VALUE

        master_columns, master_values = self.get_dummy_master_column_value()
        dic_use_cols = {col.column_name: col.data_type for col in self.cfg_data_table_columns}
        self.add_dummy_master_columns(df_master_columns, master_columns, master_values, dic_use_cols)

        return None if df_master_columns.empty else df_master_columns

    def get_distinct_data_from_factory_db(self, select_columns):
        """
        Gets raw data from data source to use for scan master
        Currently, we only take at most 50_000 records for each partition (at most 2 partitions are used)
        :param select_columns:
        :return: concatenated dataframe contains 50_000 records for each partition
        """
        data_source_db = self.cfg_data_table.data_source
        # get scan master
        cfg_partitions = self.cfg_data_table.get_partition_for_job(JobType.SCAN_MASTER, many=True)
        if not cfg_partitions:
            return pd.DataFrame()

        sel_cols = ','.join([add_double_quote(col) for col in select_columns])
        rows = []
        scan_master_partitions = get_two_partitions_for_scan(cfg_partitions)
        with ReadOnlyDbProxy(data_source_db) as factory_db_instance:
            for cfg_partition in scan_master_partitions:
                # khanhdq: `cfg_partitions` is always have min or max date time, since we checked it
                # inside `get_two_partitions_for_scan` already

                cols, _rows = db_instance_exec(
                    factory_db_instance,
                    select=sel_cols,
                    from_table=cfg_partition.table_name,
                    limit=DB_LIMIT_SCAN_MASTER,
                    with_run=True,
                )

                rows.extend(_rows)

                if cols != select_columns:
                    raise RuntimeError(
                        f'''
SCAN_MASTER: partitions have different columns with `select_columns`:
Expected: {select_columns}
Actual: {cols}
                ''',
                    )

        df = pd.DataFrame(rows, columns=select_columns, dtype=np.object.__name__)
        df = format_df(df)
        return df


@log_execution_time()
def build_distinct_sql_by_time_range(
    sel_cols,
    table_name,
    date_column,
    min_time,
    max_time,
    limit=EFA_LIMIT_SCAN_MASTER,
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
        all_params += (*_param,)

    sql = f'SELECT DISTINCT {sel_cols} FROM ({" UNION ALL ".join(all_sqls)}) t'
    return sql, all_params


def build_sql_by_time_range(
    sel_cols,
    table_name,
    date_column,
    min_time,
    max_time,
    limit,
    is_distinct=True,
    db_instance=None,
    desc=None,
):
    _pairs = get_sql_time_range(min_time, max_time, is_mssql_datetime=isinstance(db_instance, mssqlserver.MSSQLServer))
    if not _pairs:
        _pairs = [[min_time, max_time]]

    if desc:
        _pairs.reverse()

    # in case oracle
    for from_tm, to_tm in _pairs:
        select_sql = f'{"DISTINCT" if is_distinct else ""} {sel_cols}'
        sql, params = db_instance_exec(
            db_instance,
            select=select_sql,
            from_table=table_name,
            filter=[(date_column, 'BETWEEN', [from_tm, to_tm])],
            limit=limit,
            with_run=False,
        )
        yield sql, params


def get_sql_time_range(min_time: Union[datetime, str], max_time: Union[datetime, str], is_mssql_datetime: bool = False):
    date_ranges = []
    year_month = min_time
    datetime_max = convert_time(max_time, return_string=False, without_timezone=True)
    datetime_min = convert_time(min_time, return_string=False, without_timezone=True)

    while True:
        year_month = convert_time(year_month, format_str=DATE_FORMAT_STR_YYYYMM)
        for day in DAY_ANCHORS:
            for from_hour, to_hour in zip(TIME_ANCHORS, TIME_ANCHORS[1:]):
                from_time_str = f'{year_month}{day}{from_hour}'
                to_time_str = f'{year_month}{day}{to_hour}'
                from_time = convert_time(from_time_str, return_string=False, without_timezone=True)
                to_time = convert_time(to_time_str, return_string=False, without_timezone=True)

                if from_time <= datetime_max and to_time >= datetime_min:
                    from_time = add_delta_to_datetime(
                        min_time,
                        time_delta=from_time - datetime_min,
                        is_mssql_datetime=is_mssql_datetime,
                    )
                    to_time = add_delta_to_datetime(
                        max_time,
                        time_delta=to_time - datetime_max,
                        is_mssql_datetime=is_mssql_datetime,
                    )
                    date_ranges.append([from_time, to_time])

                elif from_time > datetime_max:
                    return date_ranges

        year_month = convert_time(year_month, return_string=False)
        year_month = add_months(year_month, months=1)


def get_sql_day_range(month: str):
    from_day, to_day = None, None
    time = '000000'
    for day in DAY_ANCHORS:
        to_day = f'{month}{day}{time}'
        yield from_day, to_day
        from_day = to_day
    yield from_day, None


def next_partition(partition_number):
    """
    gen next partition
    :param partition_number:
    :return:
    """
    partition_str = str(partition_number)
    year = int(partition_str)[:4]
    month = int(partition_str)[4:]
    if month == 12:
        year += 1
        month = 1
    else:
        month += 1

    return str(year).zfill(4) + str(month).zfill(2)


def get_physical_tables_by_time(cfg_data_table_id, start_time: str, end_time: str):
    cgf_partition_tables: list[CfgPartitionTable] = CfgPartitionTable.get_by_data_table_id(cfg_data_table_id)
    tables = []

    start_time_obj = convert_time(start_time, return_string=False, without_timezone=True)
    end_time_obj = convert_time(end_time, return_string=False, without_timezone=True)

    for table in cgf_partition_tables:
        if table.is_no_min_max_date_time():
            # Skip get data for empty partition tables
            continue

        max_time = convert_time(table.max_time, return_string=False, without_timezone=True)
        min_time = convert_time(table.min_time, return_string=False, without_timezone=True)

        if start_time_obj <= max_time and end_time_obj >= min_time:
            tables.append(table)

    return tables


@memoize(duration=60)
def get_n_save_partition_range_time_from_factory_db(
    cfg_data_table: CfgDataTable,
    is_scan=False,
    meta_session: scoped_session = None,
):
    return get_n_save_partition_range_time_from_factory_db_db_instance(
        cfg_data_table,
        is_scan=is_scan,
        db_instance=meta_session,
    )


def get_n_save_partition_range_time_from_factory_db_db_instance(
    cfg_data_table: CfgDataTable,
    is_scan=False,
    db_instance: Union[scoped_session, PostgreSQL] = None,
):
    data_source = cfg_data_table.data_source
    is_csv = bool(cfg_data_table.data_source.csv_detail)
    if is_csv:
        return

    master_db_type = cfg_data_table.get_master_type()
    if MasterDBType.is_efa_group(master_db_type):
        tables = get_all_tables_from_factory_db(data_source)
        if not tables:
            logger.error('Can not connect to database.')
        table_name, partition_times, partition_tables = get_efa_partitions(tables, cfg_data_table.table_name)
        if table_name is None:
            return

        # get valid range partitions
        partition_times, partition_tables = get_valid_range_efa_partitions(
            cfg_data_table,
            partition_times,
            partition_tables,
        )  # last 6 months
        recent_times, recent_tables = get_recent_partition_times(partition_times, partition_tables)
    else:
        table_name = cfg_data_table.table_name
        partition_times = [None]
        partition_tables = [table_name]
        recent_times = None

    # get history checked partitions
    dic_done_partitions = {_cfg.table_name: _cfg for _cfg in cfg_data_table.partition_tables}
    query_results = get_min_max_time_from_factory_db(
        cfg_data_table,
        data_source,
        dic_done_partitions,
        partition_tables,
        partition_times,
        recent_times,
        is_scan,
        db_instance=db_instance if isinstance(db_instance, PostgreSQL) else None,
    )

    # save to app database
    def update_partition_times(session: Union[scoped_session, PostgreSQL]):
        for partition_table, partition_time, _min, _max in query_results:
            cfg_partition_table = dic_done_partitions.get(partition_table)
            if cfg_partition_table is None:
                cfg_partition_table = CfgPartitionTable()
                cfg_partition_table.data_table_id = cfg_data_table.id
                cfg_partition_table.table_name = partition_table
                cfg_partition_table.partition_time = partition_time
            cfg_partition_table.min_time = _min if _min is None else str(_min)
            cfg_partition_table.max_time = _max if _max is None else str(_max)

            if isinstance(db_instance, scoped_session):
                session.merge(cfg_partition_table)
            elif cfg_partition_table.id is None:
                BSCfgPartitionTable.insert_record(session, cfg_partition_table)
            else:
                BSCfgPartitionTable.update_by_conditions(
                    session,
                    {
                        BSCfgPartitionTable.Columns.min_time.name: cfg_partition_table.min_time,
                        BSCfgPartitionTable.Columns.max_time.name: cfg_partition_table.max_time,
                    },
                    {
                        BSCfgPartitionTable.Columns.id.name: cfg_partition_table.id,
                    },
                )

    if db_instance is None:
        with make_session() as db_instance:
            update_partition_times(db_instance)
    else:
        update_partition_times(db_instance)

    return True


@log_execution_time()
def get_min_max_time_from_factory_db(
    cfg_data_table: Union[CfgDataTable, BSCfgDataTable],
    data_source: Union[CfgDataSource, BSCfgDataSource],
    dic_done_partitions,
    partition_tables,
    partition_times,
    recent_times,
    is_scan: bool,
    db_instance: PostgreSQL = None,
):
    # get auto incremental column
    # get_date_col = cfg_data_table.get_date_col()
    auto_incremental_col = cfg_data_table.get_auto_increment_col_else_get_date()
    query_results = []
    # query from factory database
    with ReadOnlyDbProxy(data_source) as factory_db_instance:
        db_timezone = get_db_timezone(factory_db_instance)
        for partition_table, partition_time in zip(partition_tables, partition_times):
            # done and not recent => continue
            if (
                partition_time
                and recent_times
                and partition_table in dic_done_partitions
                and partition_time not in recent_times
            ):
                continue

            agg_results = []
            datetime_col = sa.Column(auto_incremental_col)
            sa.Table(partition_table, sa.MetaData(), datetime_col)

            if isinstance(factory_db_instance, mssqlserver.MSSQLServer):
                converted_datetime_col = mssqlserver.MSSQLServer.datetime_converter(datetime_col)
            else:
                converted_datetime_col = datetime_col

            # Execute the query
            for agg_func in ['MIN', 'MAX']:
                if agg_func == 'MAX':
                    # convert to database timezone then remove +00:00 information
                    now = datetime.now().astimezone(db_timezone).replace(tzinfo=None)
                    if isinstance(agg_results[0], str):
                        now = convert_time(now, return_string=True, without_timezone=False)
                    else:
                        now = convert_time(now, return_string=False, without_timezone=False)
                    stmt = select([func.max(converted_datetime_col)]).where(
                        datetime_col <= now,
                    )
                else:
                    stmt = select([func.min(converted_datetime_col)])

                sql, params = factory_db_instance.gen_sql_and_params(stmt)
                _, rows = factory_db_instance.run_sql(sql, row_is_dict=False, params=params)
                # there is no record
                if not rows:
                    break

                agg_results.append(rows[0][0])

            # there is no record
            if not agg_results:
                continue

            _min, _max = agg_results
            if _min is None and _max is None:  # Fix bug that new partition table no any records
                query_results.append((partition_table, partition_time, None, None))
                continue

            query_results.append((partition_table, partition_time, _min, _max))

        if is_scan:
            if db_instance is not None:
                BSCfgPartitionTable.delete_not_in_partition_times(db_instance, cfg_data_table.id, partition_times)
            else:
                CfgPartitionTable.delete_not_in_partition_times(cfg_data_table.id, partition_times)

        return query_results


@log_execution_time()
def get_all_tables_from_factory_db(cfg_data_source):
    """
    get all partitions from factory db
    :param cfg_data_source:
    :return:
    """
    with ReadOnlyDbProxy(cfg_data_source) as factory_db_instance:
        tables = factory_db_instance.list_tables_and_views()

    return tables


@log_execution_time()
def get_valid_range_efa_partitions(cfg_data_table: CfgDataTable, partition_times, partition_tables):
    """
    get target partitions base on GUI setting.
    :param cfg_data_table:
    :param partition_times:
    :param partition_tables:
    :return:
    """
    if not cfg_data_table.partition_from and not cfg_data_table.partition_to:
        return partition_times, partition_tables

    valid_times = []
    valid_tables = []
    for partition_table, partition_time in zip(partition_tables, partition_times):
        if cfg_data_table.partition_from and partition_time < cfg_data_table.partition_from:
            continue

        if cfg_data_table.partition_to and partition_time > cfg_data_table.partition_to:
            continue

        valid_tables.append(partition_table)
        valid_times.append(partition_time)

    return valid_times, valid_tables


@log_execution_time()
def get_recent_partition_times(partition_times, partition_tables, recent_month=2):
    """
    get recent 6 months partition ( purpose : get min , max datetime again)
    :param partition_times:
    :param partition_tables:
    :param recent_month:
    :return:
    """
    recent_partition = datetime.utcnow() - relativedelta(months=recent_month)
    recent_partition = str(recent_partition.year).zfill(4) + str(recent_partition.month).zfill(2)
    recent_times = []
    recent_tables = []
    for partition_table, partition_time in zip(reversed(partition_tables), reversed(partition_times)):
        if partition_time < recent_partition:
            break

        recent_tables.append(partition_table)
        recent_times.append(partition_time)

    return recent_times, recent_tables


def get_sql_range_time(filter_time, range_second):
    # start time
    start_time = convert_time(filter_time, return_string=False, without_timezone=True)

    # 8 days after
    end_time = add_seconds(start_time, seconds=range_second)

    return start_time, end_time


def get_auto_link_data_range(data_table_id, max_dt, seconds=None, filter_time=None):
    """
    Get the auto link data time range to be pulled
    if we did past pulled before:
        - take that
    if we have never pulled before:
        - get the first pull
    if no have any pull yet:
        - get max time from db to pull
    """
    if not seconds:
        seconds = EtlDbService.factory_next_sql_range_seconds

    # last import date
    if not filter_time:
        last_import = FactoryImport.get_last_pull_by_data_table(data_table_id, JobType.PULL_PAST_DB_DATA.name)
        if not last_import:
            # check if first time factory import was DONE !
            last_import = FactoryImport.get_first_pull_by_data_table(data_table_id, JobType.PULL_DB_DATA.name)

        filter_time = max_dt if last_import is None or last_import.import_to is None else last_import.import_from

    end_time, start_time = get_sql_range_time(filter_time, range_second=-seconds)

    return start_time, end_time, seconds


def get_past_range(data_table_id, seconds=None, filter_time=None):
    """
    Get the past data time range to be pulled
    if we did past pulled before:
        - take that
    if we have never pulled before:
        - get the first pull
    """
    start_time = False
    end_time = False
    seconds = 0
    is_break = False
    if not seconds:
        seconds = EtlDbService.factory_next_sql_range_seconds

    # last import date
    if not filter_time:
        last_import = FactoryImport.get_last_pull_by_data_table(data_table_id, JobType.PULL_PAST_DB_DATA.name)
        if not last_import:
            # check if first time factory import was DONE !
            last_import = FactoryImport.get_first_pull_by_data_table(data_table_id, JobType.PULL_DB_DATA.name)

        if last_import is None or last_import.import_from is None:
            is_break = True
            return start_time, end_time, seconds, is_break

        filter_time = last_import.import_from

    # do not import past data that is older than `now - 1 year`
    if convert_time(filter_time) < convert_time(add_years(years=-1)):
        is_break = True
        return start_time, end_time, seconds, is_break

    end_time, start_time = get_sql_range_time(filter_time, range_second=-seconds)

    return start_time, end_time, seconds, is_break


def get_future_range(data_table_id, min_dt, max_dt, seconds=None, filter_time=None):
    """
    Get future range to be imported
    If we haven't pulled before:
                     (IMPORT_FUTURE_MONTH_AGO)            now
             -------------------|--------------------------|------------>
        - case 1:  min --- max => start_time = max_time-IMPORT_FUTURE_MONTH_AGO
        - case 2:         min ------ max => start_time = IMPORT_FUTURE_MONTH_AGO
        - case 3:                    min --------- max => start_time = min
        - case 4:                                min ------------ max => start_time = min
        - case 5:                                               min ------------ max => start_time = min
    If we pulled at least once:
        We take the last imported time
    """
    if not seconds:
        seconds = EtlDbService.factory_next_sql_range_seconds

    # last import date
    if not filter_time:
        last_import = FactoryImport.get_last_pull_by_data_table(data_table_id, JobType.PULL_DB_DATA.name)
        filter_time = last_import.import_to if last_import else get_future_import_first_time(min_dt, max_dt)

    start_time, end_time = get_sql_range_time(filter_time, range_second=seconds)

    return start_time, end_time, seconds


def get_future_import_first_time(min_time, max_time):
    # if max time < IMPORT_FUTURE_MONTH_AGO, first fime = max_time - IMPORT_FUTURE_MONTH_AGO
    if calculator_month_ago(max_time) >= IMPORT_FUTURE_MONTH_AGO:
        first_time = add_months(time=max_time, months=-IMPORT_FUTURE_MONTH_AGO)
    else:
        first_time = add_months(months=-IMPORT_FUTURE_MONTH_AGO)

    # if first_time < min_time:
    #     return min_time
    # else:
    return first_time


def get_import_feature_month_old(data_table_id):
    month_ago = IMPORT_FUTURE_MONTH_AGO
    last_transaction_import = FactoryImport.get_last_import_transaction(data_table_id, JobType.PULL_DB_DATA.name)
    if last_transaction_import:
        return month_ago

    max_time = CfgPartitionTable.get_max_time_by_data_table(data_table_id)
    if not max_time:
        return month_ago

    if calculator_month_ago(max_time) < IMPORT_FUTURE_MONTH_AGO:
        return month_ago

    min_time = CfgPartitionTable.get_min_time_by_data_table(data_table_id)
    if not min_time:
        return month_ago

    month_ago = calculator_month_ago(min_time)

    return month_ago


@log_execution_time()
def get_factory_data_partitions(
    factory_db_instance,
    column_names,
    auto_increment_col,
    start_time: str,
    end_time: str,
    partition_table_names,
    is_count=False,
):
    """generate select statement and get data from factory db

    Arguments:
        proc_id {[type]} -- [description]
        db_config_yaml {DBConfigYaml} -- [description]
        proc_config_yaml {ProcConfigYaml} -- [description]
    """

    # exe sql
    cols = None
    for table_name in partition_table_names:
        sql_limit = SQL_FACTORY_LIMIT
        data = get_data_by_range_time_partitions(
            factory_db_instance,
            auto_increment_col,
            column_names,
            table_name,
            start_time,
            end_time,
            sql_limit,
            is_count=is_count,
        )
        if not data:
            return None

        if cols:
            next(data)
        else:
            cols = next(data)
            yield cols

        yield from data


@log_execution_time('[FACTORY DATA IMPORT SELECT SQL]')
def get_data_by_range_time_partitions(
    db_instance,
    get_date_col,
    column_names: list,
    table_name: str,
    start_time: str,
    end_time: str,
    sql_limit: int,
    is_count: bool = False,
):
    # remove datetime column here since we will insert it back later
    cols = [sa.Column(col) for col in column_names if col != get_date_col]
    datetime_col = sa.Column(get_date_col)

    # construct table with columns and datetime column
    table = sa.Table(table_name, sa.MetaData(), *cols, datetime_col)

    start_time = adapter_datetime_for_database(db_instance, start_time)
    end_time = adapter_datetime_for_database(db_instance, end_time)

    # workaround for mssql to avoid binary datetime data
    if isinstance(db_instance, mssqlserver.MSSQLServer):
        datetime_col = mssqlserver.MSSQLServer.datetime_converter(datetime_col).label(get_date_col)

    condition = sa.and_(datetime_col > start_time, datetime_col <= end_time)
    stmt = select([sa.func.count('*')]) if is_count else select([*cols, datetime_col])
    stmt = stmt.select_from(table).where(condition).limit(sql_limit)

    sql, params = db_instance.gen_sql_and_params(stmt)
    data = db_instance.fetch_many(sql, FETCH_MANY_SIZE, params=params)

    if not data:
        return None

    yield from data


@log_execution_time()
def handle_time_zone(cfg_data_table: CfgDataTable, get_date_col):
    table_name = get_random_partition_table_name(cfg_data_table)

    # convert utc time func
    get_date, tzoffset_str, db_timezone = get_tzoffset_of_random_record(
        cfg_data_table.data_source,
        table_name,
        get_date_col,
    )

    if tzoffset_str:
        # use os time zone
        db_timezone = None
    else:
        detected_timezone = detect_timezone(get_date)
        # if there is time offset in datetime value, do not force time.
        if detected_timezone is None:
            # check and update if you use os time zone flag changed
            # if tz offset in val date, do not need to force
            check_update_time_by_changed_tz(cfg_data_table)

    if cfg_data_table.data_source.db_detail.use_os_timezone:
        # use os time zone
        db_timezone = None

    is_tz_inside, db_time_zone, time_offset = get_time_info(get_date, db_timezone)
    print('TIME ZONE INFO:', cfg_data_table, get_date_col, is_tz_inside, time_offset)

    return is_tz_inside, db_time_zone, time_offset


def get_dict_rename_column(cfg_data_table):
    source_column_names = [col.column_name for col in cfg_data_table.columns if col.data_group_type]
    m_data_groups = MDataGroup.get_data_group_in_group_types([col.data_group_type for col in cfg_data_table.columns])
    m_data_groups = {group.data_group_type: group.get_sys_name() for group in m_data_groups}
    bridge_column_names = [
        m_data_groups.get(col.data_group_type) for col in cfg_data_table.columns if col.data_group_type
    ]
    return dict(zip(source_column_names, bridge_column_names))


def get_two_partitions_for_scan(cfg_partitions: list[CfgPartitionTable]):
    """
    get partitions for scan_master, we only take at most 2 partitions
    In case we have more than 2 partitions, we take the first one, and the second last one.
    Because the last one might not have enough data
    :param cfg_partitions:
    :return:
    """
    scan_master_partitions = [target for target in cfg_partitions if not target.is_no_min_max_date_time()]
    # TODO(khanhdq): x.partition_time should all None or all not None
    scan_master_partitions = sorted(scan_master_partitions, key=lambda x: x.partition_time)
    if len(scan_master_partitions) > 2:
        scan_master_partitions = [scan_master_partitions[0], scan_master_partitions[-2]]
    logger.info(f'SCAN PARTITIONS: {scan_master_partitions}')
    return scan_master_partitions


def adapter_datetime_for_database(db_instance, datetime_data: str) -> Any:
    if isinstance(db_instance, PostgreSQL):
        return datetime_data

    # convert to pandas datetime first, to get rid of .000 microseconds
    pd_datetime = pd.to_datetime(datetime_data)
    if pd_datetime.tzinfo is not None:
        pd_datetime = pd_datetime.tz_localize(None)

    if isinstance(db_instance, MySQL):
        return pd_datetime

    if isinstance(db_instance, MSSQLServer):
        return sa.func.convert(sa.sql.literal_column('DATETIME'), pd_datetime)

    return datetime_data
