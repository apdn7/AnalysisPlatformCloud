from __future__ import annotations

import calendar
import os
from datetime import datetime, time
from enum import auto
from functools import cached_property
from typing import List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
import sqlalchemy as sa
from pandas import DataFrame

from ap import dic_config, log_execution_time, logger
from ap.common.common_utils import (
    DATE_FORMAT_STR_FACTORY_DB,
    chunks,
    convert_nan_to_none,
    convert_nullable_int64_to_numpy_int64,
    convert_type_base_df,
    gen_sql_cast_text,
    get_error_path,
    get_nullable_int64_columns,
    get_type_all_columns,
    make_dir_from_file_path,
)
from ap.common.constants import (
    CATEGORY_TYPES,
    DATA_SOURCE_ID_COL,
    LOCK,
    PROCESS_QUEUE,
    SQL_LIMIT,
    THIN_DATA_CHUNK,
    BaseEnum,
    DataGroupType,
    DataType,
    MasterDBType,
    ProcessCfgConst,
    RawDataTypeDB,
    dict_data_type_db,
    dict_invalid_data_type_regex,
    dict_numeric_type_ranges,
)
from ap.common.pydn.dblib.db_common import SqlComparisonOperator
from ap.common.pydn.dblib.mssqlserver import MSSQLServer
from ap.common.pydn.dblib.mysql import MySQL
from ap.common.pydn.dblib.oracle import Oracle
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.setting_module.models import (
    CfgDataTable,
    CfgProcess,
    CfgProcessColumn,
    CfgTrace,
    CfgTraceKey,
    MColumnGroup,
    MData,
)
from bridge.common.database_index import (
    ColumnInfo,
    MultipleIndexes,
    SingleIndex,
    add_multiple_indexes_to_set,
)
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.cfg_process import CfgProcess as BSCfgProcess
from bridge.models.mapping_process_data import MappingProcessData
from bridge.models.semi_master import SemiMaster
from bridge.services.partition_gen import check_exist_partition

SEQUENCE_CACHE = 1000


class DuplicateMode(BaseEnum):
    FIRST = auto()
    LAST = auto()
    BOTH = auto()


class MergeFlag(BaseEnum):
    GENERAL = 0b0000
    V2_MEASUREMENT = 0b0001
    V2_HISTORY = 0b0010
    EFA_MEASUREMENT = 0b0100
    EFA_HISTORY = 0b1000

    @classmethod
    def current_flag(cls, master_type: str) -> int:
        master_type = MasterDBType[master_type]
        if master_type is MasterDBType.OTHERS:
            return cls.GENERAL.value
        if master_type is MasterDBType.SOFTWARE_WORKSHOP:
            # TODO: implement merge for history and measurement
            return cls.GENERAL.value
        if master_type in (MasterDBType.V2, MasterDBType.V2_MULTI):
            return cls.V2_MEASUREMENT.value
        if master_type in (MasterDBType.V2_HISTORY, MasterDBType.V2_MULTI_HISTORY):
            return cls.V2_HISTORY.value
        if master_type is MasterDBType.EFA:
            return cls.EFA_MEASUREMENT.value
        if master_type is MasterDBType.EFA_HISTORY:
            return cls.EFA_HISTORY.value
        raise ValueError(f'Invalid MasterDBType {master_type}')

    @classmethod
    def missing_flag(cls, master_type: str) -> int | None:
        current_flag = cls.current_flag(master_type)
        if current_flag == cls.GENERAL.value:
            return None
        if current_flag == cls.V2_MEASUREMENT.value:
            return cls.V2_HISTORY.value
        if current_flag == cls.V2_HISTORY.value:
            return cls.V2_MEASUREMENT.value
        if current_flag == cls.EFA_MEASUREMENT.value:
            return cls.EFA_HISTORY.value
        if current_flag == cls.EFA_HISTORY.value:
            return cls.EFA_MEASUREMENT.value
        raise ValueError(f'Invalid MasterDBType {master_type}')

    @classmethod
    def done_flag(cls, master_type: str) -> int:
        current_flag = cls.current_flag(master_type)
        missing_flag = cls.missing_flag(master_type)
        done_flag = current_flag | missing_flag if missing_flag is not None else current_flag
        return done_flag

    @classmethod
    def check_duplicated_flags(cls, master_type: str) -> list[int]:
        current_flag = cls.current_flag(master_type)
        done_flag = cls.done_flag(master_type)
        return list({current_flag, done_flag})


def update_master_config_of_category(db_instance, data_id, new_raw_data_type):
    if new_raw_data_type in [RawDataTypeDB.CATEGORY.value, RawDataTypeDB.BIG_INT.value]:
        new_data_type = RawDataTypeDB.TEXT.value
    elif new_raw_data_type == RawDataTypeDB.SMALL_INT.value:
        new_data_type = RawDataTypeDB.INTEGER.value
    else:
        new_data_type = new_raw_data_type

    param_marker = BridgeStationModel.get_parameter_marker()
    update_master_sql = f'''
        UPDATE {MData.get_table_name()}
        SET {MData.data_type.key} = {param_marker}
        WHERE {MData.id.key} = {param_marker}
        '''
    update_config_sql = f'''
        UPDATE {CfgProcessColumn.get_table_name()}
        SET {CfgProcessColumn.raw_data_type.key} = {param_marker}
        , {CfgProcessColumn.data_type.key} = {param_marker}
        WHERE {CfgProcessColumn.id.key} = {param_marker}
        '''
    master_params = [new_raw_data_type, data_id]
    config_params = [new_raw_data_type, new_data_type, data_id]
    db_instance.execute_sql(update_master_sql, params=master_params)
    db_instance.execute_sql(update_config_sql, params=config_params)


class TransactionData:
    id_col_name: str = 'id'
    factory_machine_id_col_name: str = 'factory_machine_id'
    prod_part_id_col_name: str = 'prod_part_id'
    data_source_id_col_name: str = 'data_source_id'
    merge_flag_col_name: str = 'merge_flag'
    factor_col_name: str = None
    process_id: int = None
    table_name: str = None
    cfg_process: CfgProcess = None
    serial_column: CfgProcessColumn = None
    getdate_column: CfgProcessColumn = None
    main_date_column: CfgProcessColumn = None
    main_time_column: CfgProcessColumn = None
    file_name_column: CfgProcessColumn = None
    auto_incremental_column: CfgProcessColumn = None
    cfg_process_columns: list[CfgProcessColumn] = None
    category_text_columns: list[CfgProcessColumn] = None
    boolean_columns: list[CfgProcessColumn] = None
    select_column_names: list[str] = None
    cfg_filters = None
    show_duplicate: DuplicateMode = None
    actual_record_number: int = None
    unique_serial_number: int = None
    duplicate_serial_number: int = None
    df: DataFrame = None

    def __init__(self, process_id: int, db_instance: PostgreSQL = None):
        if not db_instance:
            self.cfg_process: CfgProcess = CfgProcess.get_by_id(process_id)
        else:
            self.cfg_process: BSCfgProcess = BSCfgProcess.get_by_id(db_instance, process_id, is_cascade=True)

        if not self.cfg_process:
            raise Exception('Not exist process id')

        self.process_id = process_id
        self.table_name = self.cfg_process.table_name

        self.cfg_process_columns = self.cfg_process.columns
        self.category_text_columns, self.boolean_columns = [], []
        for cfg_process_column in self.cfg_process_columns:
            if RawDataTypeDB.is_category_data_type(cfg_process_column.raw_data_type):
                self.category_text_columns.append(cfg_process_column)
            elif cfg_process_column.raw_data_type == RawDataTypeDB.BOOLEAN.value:
                self.boolean_columns.append(cfg_process_column)

            if cfg_process_column.column_raw_name == DataGroupType.FileName.name:
                self.file_name_column = cfg_process_column

            if cfg_process_column.is_serial_no:
                self.serial_column = cfg_process_column
                continue
            if cfg_process_column.is_get_date:
                self.getdate_column = cfg_process_column
                continue
            if cfg_process_column.is_auto_increment:
                self.auto_incremental_column = cfg_process_column

            if cfg_process_column.column_type == DataGroupType.MAIN_DATE.value:
                self.main_date_column = cfg_process_column

            if cfg_process_column.column_type == DataGroupType.MAIN_TIME.value:
                self.main_time_column = cfg_process_column

        self.master_columns = [self.factory_machine_id_col_name, self.prod_part_id_col_name]
        if self.serial_column:
            self.master_columns.append(self.serial_column.bridge_column_name)
        if self.getdate_column:
            self.master_columns.append(self.getdate_column.bridge_column_name)

    def get_new_columns(self, db_instance: Union[PostgreSQL, Oracle, MySQL, MSSQLServer]) -> list[CfgProcessColumn]:
        sql = f'SELECT * FROM {self.table_name} WHERE FALSE;'
        exist_columns, _ = db_instance.run_sql(sql)
        new_columns = []
        for cfg_process_column in self.cfg_process_columns:
            if cfg_process_column.is_master_data_column() or (
                cfg_process_column.column_type
                in [
                    DataGroupType.DATA_SOURCE_NAME.value,
                ]
                or len(cfg_process_column.function_details)
            ):
                continue

            if cfg_process_column.bridge_column_name not in exist_columns:
                new_columns.append(cfg_process_column)
        return new_columns

    def create_table(self, db_instance):
        dict_col_with_type = {
            column.bridge_column_name: column.raw_data_type
            for column in self.cfg_process_columns
            if column.column_type in DataGroupType.get_physical_column_types()
        }
        table_name = self.table_name
        if table_name in db_instance.list_tables():
            new_columns = self.get_new_columns(db_instance)
            if new_columns:
                dict_new_col_with_type = {column.bridge_column_name: column.raw_data_type for column in new_columns}
                self.add_columns(db_instance, dict_new_col_with_type)
            self.update_data_types(db_instance, dict_col_with_type)
            return table_name

        self.__create_sequence_table(db_instance)
        sql = f'CREATE TABLE IF NOT EXISTS {self.table_name}'
        sql_col = f'''
                {self.id_col_name} INT DEFAULT nextval('{self.table_name}_id_seq'),
                {self.factory_machine_id_col_name} INT,
                {self.prod_part_id_col_name} INT,
                {self.merge_flag_col_name} SMALLINT,
                {self.data_source_id_col_name} INT,
        '''
        for col_name, data_type in dict_col_with_type.items():
            data_type_db = dict_data_type_db.get(data_type)
            sql_col += f'{col_name} {data_type_db}, '
        sql_col = sql_col.rstrip(', ')  # Remove trailing comma and whitespace
        sql = f'{sql} ({sql_col})'
        # make table with partition
        sql += f' PARTITION BY RANGE ({self.getdate_column.bridge_column_name})'
        db_instance.execute_sql(sql)
        return table_name

    def __create_sequence_table(self, db_instance):
        sql = f'''
            CREATE SEQUENCE IF NOT EXISTS {self.table_name}_id_seq
            START WITH 1
            INCREMENT BY 1
            CACHE {SEQUENCE_CACHE};
        '''
        db_instance.execute_sql(sql)

    def __delete_sequence_table(self, db_instance):
        sql = f'DROP SEQUENCE {self.table_name}_id_seq CASCADE;'
        db_instance.execute_sql(sql)
        db_instance.connection.commit()

    def rename_column(self, df: DataFrame):
        df_columns = list(df.columns)
        rename_columns = self.cfg_process_columns  # column of current version
        dict_rename_col = dict(zip(df_columns, rename_columns))
        df = df.rename(columns=dict_rename_col)
        return df

    def create_partition_by_time(self, db_instance, year_month=None):
        """
        create partition for transaction-data base on YYYYMM

        Keyword arguments:
        db_instane -- BridgeStationModel
        year_month -- string/int YYYYMM
        """
        if not year_month:
            pass

        # extract year and month to determine time range
        year_month = str(year_month)
        year = int(year_month[:4])
        month = int(year_month[4:])

        # create partition name with datetime suffix
        partition_name = f'{self.table_name}_{year_month}'

        if not check_exist_partition(db_instance, partition_name):
            # make time range of data in partition
            from_date = datetime.combine(datetime(year=year, month=month, day=1), time.min)
            to_date = datetime.combine(
                datetime(year=year, month=month, day=calendar.monthrange(year, month)[1]),
                time.max,
            )
            from_date = datetime.strftime(from_date, DATE_FORMAT_STR_FACTORY_DB)
            to_date = datetime.strftime(to_date, DATE_FORMAT_STR_FACTORY_DB)

            # generate sql to create partition
            sql = f'''
CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF {self.table_name} FOR
VALUES
FROM
    ('{from_date}') TO ('{to_date}')
            '''

            try:
                # create partition
                db_instance.execute_sql(sql)
                db_instance.connection.commit()
            except Exception:
                db_instance.connection.rollback()
        return partition_name

    def add_default_indexes_column(
        self,
        set_multiple_indexes: Optional[Set[MultipleIndexes]] = None,
    ) -> Set[MultipleIndexes]:
        set_indexes = set_multiple_indexes or set()
        # add default index columns
        data_time = self.getdate_column.bridge_column_name if self.getdate_column else None
        default_indexes = []
        if data_time is not None:
            default_indexes.append(SingleIndex(data_time))
        add_multiple_indexes_to_set(set_indexes, MultipleIndexes(default_indexes))
        return set_indexes

    def __get_table_indexes(self, db_instance) -> Set[MultipleIndexes]:
        """
        get all index columns of process
        """
        sql = f'''
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = '{db_instance.schema}' and tablename = '{self.table_name}'
            ORDER BY indexdef;
            '''
        _, rows = db_instance.run_sql(sql, row_is_dict=False)
        table_indexes: Set[MultipleIndexes] = set()
        for col_dat in rows:
            multiple_indexes = MultipleIndexes.from_str(col_dat[0])
            table_indexes.add(multiple_indexes)
        return table_indexes

    def __get_missing_indexes(self, db_instance, new_link_key_indexes: Set[MultipleIndexes]):
        already_indexes = self.__get_table_indexes(db_instance)
        return new_link_key_indexes - already_indexes

    def __get_expired_indexes(self, db_instance, link_key_indexes: Set[MultipleIndexes]):
        """
        get expired indexes to remove
        """
        already_indexes = self.__get_table_indexes(db_instance)
        return already_indexes - link_key_indexes

    def __gen_index_col_name(self, index: MultipleIndexes) -> str:
        """
        generate indexes alias name for column
        """
        return index.to_idx(prefix=self.table_name)

    def __default_indexes(self) -> Set[MultipleIndexes]:
        return {
            MultipleIndexes([SingleIndex(self.factory_machine_id_col_name)]),
            MultipleIndexes([SingleIndex(self.prod_part_id_col_name)]),
        }

    def __create_index(self, db_instance: PostgreSQL, set_multiple_indexes: set[MultipleIndexes]) -> None:
        for multiple_indexes in set_multiple_indexes:
            index_alias = self.__gen_index_col_name(multiple_indexes)
            sql = f'CREATE INDEX IF NOT EXISTS {index_alias} ON {self.table_name} {multiple_indexes}'
            db_instance.execute_sql(sql)

    def create_index(self, db_instance, new_link_key_indexes: Set[MultipleIndexes]):
        """
        create composite indexes
        """
        # add default indexes
        self.__create_index(db_instance, self.__default_indexes())

        # add missing indexes
        missing_indexes = self.__get_missing_indexes(db_instance, new_link_key_indexes=new_link_key_indexes)
        self.__create_index(db_instance, missing_indexes)
        return missing_indexes

    def remove_index(self, db_instance, new_link_key_indexes: Set[MultipleIndexes], auto_commit=False):
        """
        remove unused indexes
        in case of import data, remove before import
        after that, import data then create indexes again
        """
        # retrieve unused indexes
        expired_indexes = self.__get_expired_indexes(db_instance, link_key_indexes=new_link_key_indexes)

        # ignore default indexes
        expired_indexes = expired_indexes - self.__default_indexes()

        # remove unused indexes
        try:
            for multiple_indexes in expired_indexes:
                # drop index, sub index on partition will be removed too
                # column is index_alias
                sql = f'DROP INDEX IF EXISTS {self.__gen_index_col_name(multiple_indexes)};'
                db_instance.execute_sql(sql)
            if auto_commit:
                db_instance.connection.commit()
        except Exception as e:
            db_instance.connection.rollback()
            raise e

        return expired_indexes

    def __get_link_key_indexes(self) -> Set[MultipleIndexes]:
        """
        get all link_keys of process from CfgTrace
        """
        edges: List[CfgTrace] = CfgTrace.get_traces_of_proc([self.process_id])
        link_key_indexes: Set[MultipleIndexes] = set()

        for trace in edges:
            # indexes = [SingleIndex(self.getdate_column.bridge_column_name)]
            indexes = []

            is_swap = trace.self_process_id != self.process_id
            trace_key: CfgTraceKey
            for trace_key in trace.trace_keys:
                self_info = ColumnInfo(
                    bridge_column_name=trace_key.self_column.bridge_column_name,
                    column_type=trace_key.self_column.column_type,
                    raw_data_type=trace_key.self_column.raw_data_type,
                    substr_from=trace_key.self_column_substr_from,
                    substr_to=trace_key.self_column_substr_to,
                )

                target_info = ColumnInfo(
                    bridge_column_name=trace_key.target_column.bridge_column_name,
                    column_type=trace_key.target_column.column_type,
                    raw_data_type=trace_key.target_column.raw_data_type,
                    substr_from=trace_key.target_column_substr_from,
                    substr_to=trace_key.target_column_substr_to,
                )

                if is_swap:
                    self_info, target_info = target_info, self_info

                # we don't set index for master column
                if DataGroupType.is_master_data_column(self_info.column_type) and not len(
                    trace_key.self_column.function_details,
                ):
                    continue

                # we cast it to substr any way if it's need to
                if self_info.is_substr_key():
                    index = self_info.to_substr_index()
                else:
                    index_should_be_text = target_info.is_substr_key() or target_info.is_text()
                    if index_should_be_text and not self_info.is_text():
                        index = self_info.to_cast_text_index()
                    else:
                        index = self_info.to_single_index()
                indexes.append(index)

            if not indexes:
                continue

            multiple_indexes = MultipleIndexes(indexes)

            # don't short date_time index, datetime index should always be the first one
            multiple_indexes.sort_from(start_index=0)
            add_multiple_indexes_to_set(link_key_indexes, multiple_indexes)

        # add default index columns
        link_key_indexes = self.add_default_indexes_column(link_key_indexes)

        return link_key_indexes

    def is_table_exist(self, db_instance):
        sql = f'''
        SELECT COUNT(1) FROM pg_tables WHERE schemaname = '{db_instance.schema}' AND tablename = '{self.table_name}'
        '''
        _, row = db_instance.run_sql(sql, row_is_dict=False)
        return bool(row[0][0])

    def is_exist_data(self, db_instance):
        sql = f'SELECT COUNT(1) FROM {self.table_name} LIMIT 1'
        _, row = db_instance.run_sql(sql, row_is_dict=False)
        return bool(row[0][0])

    def re_structure_index(self, db_instance):
        """
        restructure index: add new or remove unused index base on cfg_trace
        """
        # check if process table is already existing
        table_existing = self.is_table_exist(db_instance)
        if table_existing:
            new_link_key_indexes = self.__get_link_key_indexes()

            # remove unused indexes
            self.remove_index(db_instance, new_link_key_indexes=new_link_key_indexes)

            # create (composite) indexes
            self.create_index(db_instance, new_link_key_indexes=new_link_key_indexes)

    def add_columns(self, db_instance, dict_new_col_with_type: dict, auto_commit: bool = True):
        table = self.table_name
        sql = f'ALTER TABLE {table} '
        for column_name, data_type in dict_new_col_with_type.items():
            data_type_db = dict_data_type_db.get(data_type)
            sql += f'ADD COLUMN IF NOT EXISTS {column_name} {data_type_db}, '
        sql = sql.rstrip(', ')  # Remove trailing comma and whitespace

        db_instance.execute_sql(sql)
        if not auto_commit:
            return

        db_instance.connection.commit()

    def get_column_name(self, column_id, brs_column_name=True):
        for cfg_process_column in self.cfg_process_columns:
            if cfg_process_column.id == column_id:
                if brs_column_name:
                    return cfg_process_column.bridge_column_name
                return cfg_process_column.column_name

    def get_column_id(self, column_name, is_compare_bridge_column_name=True):
        for cfg_process_column in self.cfg_process_columns:
            compare_name = (
                cfg_process_column.bridge_column_name
                if is_compare_bridge_column_name
                else cfg_process_column.column_name
            )
            if compare_name == column_name:
                return cfg_process_column.id

    def get_cfg_column_by_name(self, column_name, is_compare_bridge_column_name=True):
        for cfg_process_column in self.cfg_process_columns:
            compare_name = (
                cfg_process_column.bridge_column_name
                if is_compare_bridge_column_name
                else cfg_process_column.column_name
            )
            if compare_name == column_name:
                return cfg_process_column

    def get_bs_col_name_by_column_name(self, column_name):
        for cfg_process_column in self.cfg_process_columns:
            if cfg_process_column.column_name == column_name:
                return cfg_process_column.bridge_column_name

    def get_cfg_column_by_id(self, column_id) -> Optional[CfgProcessColumn]:
        for cfg_process_column in self.cfg_process_columns:
            if cfg_process_column.id == column_id:
                return cfg_process_column
        return None

    def delete_columns(self, db_instance, column_names: List):
        table = self.table_name
        sql = f'ALTER TABLE {table} '
        for column_name in column_names:
            sql += f'DROP COLUMN IF EXISTS {column_name}, '

        sql = sql.rstrip(', ')
        db_instance.execute_sql(sql)

    def update_data_types(self, db_instance, dict_col_with_type):  # TODO: check can update float to int???
        table_name = self.table_name
        sql = f'ALTER TABLE {table_name} '
        sql_col = ''
        for column_name, data_type in dict_col_with_type.items():
            if column_name != self.getdate_column.bridge_column_name:  # can not update for column get_date
                data_type_db = dict_data_type_db.get(data_type)
                using_clause = (
                    f'::TEXT::{data_type_db}' if data_type == RawDataTypeDB.BOOLEAN.value else f'::{data_type_db}'
                )
                sql_col += f'ALTER COLUMN {column_name} TYPE {data_type_db} USING({column_name}{using_clause}), '

        sql_col = sql_col.rstrip(', ')  # Remove trailing comma and whitespace
        sql = f'{sql} {sql_col}'
        db_instance.execute_sql(sql)

    @log_execution_time(prefix='CAST_DATA_TYPE')
    def cast_data_type_for_columns(
        self,
        db_instance: PostgreSQL,
        process: CfgProcess,
        proc_data: dict,
    ) -> Tuple[bool, Union[None, list[CfgProcessColumn]]]:
        """
        Do change data type for request columns
        :param db_instance: a database instance of PostgreSQL instance
        :param process: a process object
        :param proc_data: a dictionary with process columns data
        :return: True, None if all columns changed successfully otherwise return False and list of failed change columns
        """
        db_columns: list[CfgProcessColumn] = process.columns
        request_columns: list[CfgProcessColumn] = proc_data[ProcessCfgConst.PROC_COLUMNS.value]
        failed_change_columns: list[CfgProcessColumn] = []
        if self.table_name not in db_instance.list_tables():
            # In case of non-exist table, do nothing
            return True, None

        transaction_data_obj = TransactionData(process.id, db_instance=db_instance)
        if not transaction_data_obj.is_exist_data(db_instance):
            return True, None

        cat_counts = transaction_data_obj.get_count_by_category(db_instance)
        dic_cat_count = {_dic_cat['data_id']: _dic_cat for _dic_cat in cat_counts}
        dic_db_cols = {col.id: col for col in db_columns}
        lock = dic_config[PROCESS_QUEUE][LOCK]
        for request_column in request_columns:
            column = dic_db_cols.get(request_column.id)
            if column is None:
                continue

            # Filter out un-change columns
            if request_column.raw_data_type == column.raw_data_type:
                continue

            if column.raw_data_type in CATEGORY_TYPES and request_column.raw_data_type not in CATEGORY_TYPES:
                if failed_change_columns:
                    # break if there are some error ( reduce running time )
                    continue

                dic_cat_detail = dic_cat_count.get(request_column.id)
                if dic_cat_detail is None or not dic_cat_detail['unique_count']:
                    with lock:
                        transaction_data_obj.convert_category_to_normal_transaction_data_first_time(
                            db_instance,
                            dic_cat_detail['data_id'],
                            dic_cat_detail['col_name'],
                            to_raw_data_type=request_column.raw_data_type,
                        )
                else:
                    with lock:
                        transaction_data_obj.convert_category_to_normal_transaction_data(
                            db_instance,
                            dic_cat_detail['data_id'],
                            dic_cat_detail['col_name'],
                            dic_cat_detail['group_id'],
                            to_raw_data_type=request_column.raw_data_type,
                        )
            else:
                is_success = self.cast_data_type(
                    db_instance,
                    column.bridge_column_name,
                    request_column.raw_data_type,
                )

                if not is_success:
                    request_column.origin_raw_data_type = column.raw_data_type
                    failed_change_columns.append(request_column)

            # Must roll back and commit every loop to keep session alive for next loop or process later
            if failed_change_columns:
                db_instance.connection.rollback()

        if failed_change_columns:
            return False, failed_change_columns
        else:
            return True, None

    @log_execution_time(prefix='CAST_DATA_TYPE')
    def get_failed_cast_data(
        self,
        db_instance: PostgreSQL,
        failed_change_columns: list[CfgProcessColumn],
    ) -> dict[CfgProcessColumn, list[object]]:
        """
        Collect exception/special data that cannot convert to new data type
        :param db_instance: a database instance of PostgreSQL connection
        :param table_name: a table name of process
        :param failed_change_columns: a list of columns that failed convert data type
        :return: a dictionary of column with exception data
        """
        result: dict[CfgProcessColumn, list[object]] = {}
        for column in failed_change_columns:
            condition = ''
            if column.raw_data_type in dict_invalid_data_type_regex:
                condition += (
                    f" CAST({column.bridge_column_name} AS TEXT) ~ "
                    f"'{dict_invalid_data_type_regex[column.raw_data_type]}'"
                )
            if column.raw_data_type in dict_numeric_type_ranges:
                min_max_dict = dict_numeric_type_ranges[column.raw_data_type]
                min_value = min_max_dict.get('min')
                max_value = min_max_dict.get('max')
                max_numeric_data_type_db = dict_data_type_db.get(RawDataTypeDB.BIG_INT.value)
                condition += ' OR ' if condition else ''
                condition += (
                    f'('
                    f'CAST({column.bridge_column_name} AS {max_numeric_data_type_db}) < {min_value}'
                    f' OR '
                    f'CAST({column.bridge_column_name} AS {max_numeric_data_type_db}) > {max_value}'
                    f')'
                )

            if not condition:
                raise Exception('There are no condition to query specify data')

            sql = f'SELECT DISTINCT {column.bridge_column_name} FROM {self.table_name} WHERE {condition}'
            _, rows = db_instance.run_sql(sql, row_is_dict=False)
            data = [row[0] for row in rows]

            result[column] = data

        return result

    def cast_data_type(
        self,
        db_instance: PostgreSQL,
        column_name: str,
        new_data_type: str,
    ) -> bool:  # TODO: check can update float to int???
        """
        Change data type of column in table t_process_...
        :param db_instance: a database instance
        :param column_name: a column name
        :param new_data_type: new data type dict_data_type_db
        :return: True if success, False otherwise
        """
        if column_name == self.getdate_column.bridge_column_name:  # can not update for column get_date
            return True

        table_name = self.table_name
        data_type_db = dict_data_type_db.get(new_data_type)
        sql = f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {data_type_db} '

        if new_data_type == RawDataTypeDB.BOOLEAN.value:
            sql += (
                f'USING CAST'
                f'('
                f'CAST({column_name} AS {dict_data_type_db.get(RawDataTypeDB.INTEGER.value)})'
                f' AS '
                f'{data_type_db}'
                f');'
            )
        else:
            sql += f'USING CAST({column_name} AS {data_type_db});'

        try:
            db_instance.execute_sql(sql)
        except Exception as e:
            logger.warning(e)
            return False

        return True

    def data_count(self, db_instance):
        table_name = self.table_name
        count = 0
        if table_name in db_instance.list_tables():
            sql = f'SELECT COUNT(*) FROM {table_name}'
            _, rows = db_instance.run_sql(sql, row_is_dict=False)
            count = rows[0][0]

        return count

    def delete_process(self, db_instance):
        table_name = self.table_name
        if table_name in db_instance.list_tables():
            self.__delete_sequence_table(db_instance)
            sql = f'DROP TABLE {table_name} '
            db_instance.execute_sql(sql)

    def rename_columns_for_import(self, df: DataFrame) -> DataFrame:
        rename_column_dic = {}
        for cfg_process_column in self.cfg_process_columns:
            if str(cfg_process_column.id) in df:
                rename_column_dic[str(cfg_process_column.id)] = cfg_process_column.bridge_column_name

        df = df.rename(columns=rename_column_dic)
        df = df.loc[:, ~df.columns.duplicated()]  # drop duplicate columns (duplicate data_serial)
        return df

    def import_data(
        self,
        db_instance: Union[PostgreSQL, Oracle, MySQL, MSSQLServer],
        insert_df: DataFrame,
        cfg_data_table: CfgDataTable,
    ) -> [int, int]:
        # ↓====== Correct name of df corresponding to columns in tables ======↓
        insert_df = self.rename_columns_for_import(insert_df)
        # ↑====== Correct name of df corresponding to columns in tables ======↑

        # since sqlite keep 0 and 1 as boolean,
        # we need to convert it to true and false before inserting to db
        for cfg_process_column in self.cfg_process_columns:
            if (
                cfg_process_column in self.boolean_columns
                and cfg_process_column.bridge_column_name in insert_df.columns
            ):
                column_name = cfg_process_column.bridge_column_name
                insert_df[column_name] = pd.to_numeric(insert_df[column_name]).astype('boolean')

        insert_df, merged_ids = self.__get_merge_ids(db_instance, insert_df, cfg_data_table)
        master_type = cfg_data_table.get_master_type()
        insert_df = self.__merge_data_from_df(
            db_instance,
            insert_df,
            merged_ids,
            master_type,
            merge_flag_done=MergeFlag.done_flag(master_type),
        )
        self.remove_by_ids(db_instance, merged_ids)

        # mark all non-merge flag rows to `current_flag`
        insert_df = self.__set_merge_flag(
            insert_df,
            merge_flag=MergeFlag.current_flag(cfg_data_table.get_master_type()),
        )

        # Add data_source_id in insert_df
        insert_df[DATA_SOURCE_ID_COL] = cfg_data_table.data_source_id

        # ↓====== Insert new data into DB ======↓
        columns = [
            self.factory_machine_id_col_name,
            self.prod_part_id_col_name,
            self.data_source_id_col_name,
            self.merge_flag_col_name,
        ]
        [
            columns.append(col.bridge_column_name)
            for col in self.cfg_process_columns
            if col.bridge_column_name in insert_df
        ]
        rows = convert_nan_to_none(insert_df[columns], convert_to_list=True)
        param_marker = BridgeStationModel.get_parameter_marker()
        db_instance.bulk_insert(self.table_name, columns, rows, parameter_marker=param_marker)
        # ↑====== Insert new data into DB ======↑

        _, max_id = db_instance.run_sql(f'SELECT MAX(id) FROM {self.table_name}', row_is_dict=False)

        # remove merge records from `insert_df` to avoid duplication in `t_proc_data_count`
        insert_df = insert_df[insert_df.index.isna()]
        return len(rows), max_id[0][0], insert_df

    def __write_duplicated_ids(self, duplicated_df: pd.DataFrame) -> None:
        now = datetime.now().strftime('%Y%m%d%H%M%S')
        duplicate_file_name = os.path.join(
            get_error_path(),
            self.cfg_process.name,
            f'{self.cfg_process.name}_{now}.csv',
        )
        make_dir_from_file_path(duplicate_file_name)
        duplicated_df.to_csv(duplicate_file_name, index=False)
        logger.info(f'[CHECK_LOST_IMPORTED_DATA][{self.process_id}][InFILE] Export duplicate data in DB')

    def __get_intersected_columns_with_data_table(
        self,
        db_instance: Union[PostgreSQL, Oracle, MySQL, MSSQLServer],
        cfg_data_table: CfgDataTable,
    ) -> set[str]:
        """
        @param cfg_data_table:
        @return:
        """
        cols, rows = MappingProcessData.get_data_ids_by_data_table_id(db_instance, data_table_id=cfg_data_table.id)
        data_ids_df = pd.DataFrame(rows, columns=cols)
        data_ids = set(data_ids_df[MappingProcessData.Columns.data_id.name])
        return {
            str(col.bridge_column_name)  # we get bridge column name here
            for col in self.cfg_process_columns
            if col.id in data_ids and self.file_name_column and self.file_name_column.id != col.id
        }

    def __get_merge_ids_from_df(self, insert_df: pd.DataFrame, existed_df: pd.DataFrame) -> list[int]:
        """
        Get all ids require for merging
        - General: DO NOT MERGE
        - V2 and EFA: MERGE if and only if they have same master and horizontal columns are all `null`
        """
        duplicated_master_df = self.__get_duplicated_df(
            existed_df,
            insert_df[self.master_columns],
            columns=self.master_columns,
        )
        return duplicated_master_df[self.id_col_name].astype(int).tolist()

    def __set_merge_flag(self, insert_df: pd.DataFrame, merge_flag: int) -> pd.DataFrame:
        if self.merge_flag_col_name in insert_df:
            insert_df[self.merge_flag_col_name] = insert_df[self.merge_flag_col_name].fillna(merge_flag)
        else:
            insert_df[self.merge_flag_col_name] = merge_flag
        return insert_df

    def __get_duplicated_df(
        self,
        insert_df: pd.DataFrame,
        existed_df: pd.DataFrame,
        columns: list[str] | pd.Index,
    ) -> pd.DataFrame:
        set_columns = set(columns)
        if set_columns > set(insert_df.columns) or set_columns > set(existed_df.columns):
            raise NotImplementedError('Handle redundant columns in `columns`')

        convert_type_base_df(insert_df, existed_df, columns)
        return insert_df.merge(existed_df, on=columns, how='left')

    def __get_merge_ids(
        self,
        db_instance: Union[PostgreSQL, Oracle, MySQL, MSSQLServer],
        insert_df: DataFrame,
        cfg_data_table: CfgDataTable,
    ) -> tuple[DataFrame, list[int]]:
        master_type = cfg_data_table.get_master_type()
        required_columns = self.__get_intersected_columns_with_data_table(db_instance, cfg_data_table)
        # add None if file missing column
        for column in required_columns:
            if column not in insert_df.columns:
                insert_df[column] = None

        select_columns = list(
            {
                *self.master_columns,
                *(col for col in required_columns),
            },
        )

        factory_machine_ids = insert_df[self.factory_machine_id_col_name].unique().tolist()
        prod_part_ids = insert_df[self.prod_part_id_col_name].unique().tolist()
        start_time = insert_df[self.getdate_column.bridge_column_name].min()
        end_time = insert_df[self.getdate_column.bridge_column_name].max()

        # if time is string, convert to datetime and re-collect it
        if isinstance(start_time, str):
            start_time = insert_df[self.getdate_column.bridge_column_name].astype(np.datetime64).min()
        if isinstance(end_time, str):
            end_time = insert_df[self.getdate_column.bridge_column_name].astype(np.datetime64).max()

        default_params = (
            tuple(factory_machine_ids),
            tuple(prod_part_ids),
            cfg_data_table.data_source_id,
            start_time.replace(tzinfo=None),
            end_time.replace(tzinfo=None),
        )
        duplicate_params = default_params + (tuple(MergeFlag.check_duplicated_flags(master_type)),)
        exist_df = self.get_transaction_data(
            db_instance,
            duplicate_params,
            select_columns,
        )

        df = self.__get_duplicated_df(insert_df, exist_df, columns=select_columns)
        duplicated_ids = df[self.id_col_name].dropna().astype(int).tolist()
        if duplicated_ids:
            df_duplicate = df[df[self.id_col_name].isin(duplicated_ids)]
            self.__write_duplicated_ids(df_duplicate)

        df = df[~df[self.id_col_name].isin(duplicated_ids)]

        merged_ids = []

        # TODO: refactor and add comment to this
        if master_type != MasterDBType.OTHERS.name:
            merge_params = default_params + ((MergeFlag.missing_flag(master_type),),)
            missing_df = self.get_transaction_data(
                db_instance,
                merge_params,
                self.master_columns,
            )
            merged_ids = self.__get_merge_ids_from_df(df, missing_df)
            total_merge_records = min(len(merged_ids), len(df))
            merged_ids = merged_ids[:total_merge_records]

        df.set_index(self.id_col_name, inplace=True)
        return df, merged_ids

    def __merge_data_from_df(
        self,
        db_instance,
        insert_df: DataFrame,
        duplicate_ids: list[int],
        master_type,
        merge_flag_done: int,
    ):
        """This function intends to work with EFA and V2 only. We do not merge for GENERAL data"""
        if not duplicate_ids:
            return insert_df

        # TODO: int64 must cast to text
        params = tuple(duplicate_ids)
        param_marker = BridgeStationModel.get_parameter_marker()
        list_param_maker = [param_marker] * len(duplicate_ids)
        param_masker = ', '.join(list_param_maker)
        sql_statement = f'SELECT * FROM {self.table_name} WHERE id IN ({param_masker});'
        cols, rows = db_instance.run_sql(sql_statement, row_is_dict=False, params=params)
        db_df = pd.DataFrame(rows, columns=cols)

        db_df[self.merge_flag_col_name] = merge_flag_done

        # overwrite with old columns in database and `merge_flag` columns
        overwrite_cols = set(db_df.columns) - set(insert_df.columns)
        overwrite_cols.add(self.merge_flag_col_name)
        overwrite_cols = list(overwrite_cols)
        if (
            master_type == MasterDBType.V2_HISTORY.name
            and self.file_name_column
            and self.file_name_column.bridge_column_name in db_df.columns
        ):
            overwrite_cols.append(self.file_name_column.bridge_column_name)

        insert_df = insert_df.reset_index()
        insert_df = merge_rows_one_by_one(insert_df, db_df, on=self.master_columns, right_columns=overwrite_cols)
        insert_df = insert_df.set_index(self.id_col_name)

        return insert_df

    def remove_by_ids(
        self,
        db_instance: Union[PostgreSQL, Oracle, MySQL, MSSQLServer],
        duplicated_ids: list[int],
    ) -> bool:
        if not duplicated_ids:
            return False

        params = tuple(duplicated_ids)
        param_marker = BridgeStationModel.get_parameter_marker()
        list_param_maker = [param_marker] * len(duplicated_ids)
        param_masker = ', '.join(list_param_maker)
        sql_statement = f'DELETE FROM {self.table_name} WHERE id IN ({param_masker});'
        res = db_instance.execute_sql(sql_statement, params=params)
        return res

    def get_transaction_data(
        self,
        db_instance: Union[PostgreSQL, Oracle, MySQL, MSSQLServer],
        params,
        select_columns: list[str],
        sql_limit: int = SQL_LIMIT,
    ) -> Union[tuple[list, list], DataFrame]:
        column_type_dicts = get_type_all_columns(db_instance, self.table_name)
        nullable_int64_columns = get_nullable_int64_columns(
            db_instance,
            self.table_name,
            list_dict_rows=column_type_dicts,
        )

        _select_columns = [self.id_col_name] + select_columns
        _select_columns = [
            gen_sql_cast_text(column) if column in nullable_int64_columns else f'"{column}"'
            for column in _select_columns
        ]
        select_columns_sql = ', '.join(_select_columns)
        # ↑====== Prepare columns to collect ======↑

        # ↓====== Collect data ======↓
        param_marker = BridgeStationModel.get_parameter_marker()  # %s
        sql = f'''
            SELECT {select_columns_sql}
            FROM {self.table_name}
            WHERE {self.factory_machine_id_col_name} IN {param_marker}
                AND {self.prod_part_id_col_name} IN {param_marker}
                AND {self.data_source_id_col_name} = {param_marker}
                AND {self.getdate_column.bridge_column_name} >= {param_marker}
                AND {self.getdate_column.bridge_column_name} <= {param_marker}
                AND {self.merge_flag_col_name} IN {param_marker}
            ORDER BY {self.id_col_name}
            LIMIT {sql_limit};
        '''
        # params = (start_dt, end_dt)
        cols, rows = db_instance.run_sql(sql, row_is_dict=False, params=params)

        df = pd.DataFrame(rows, columns=cols, dtype='object')
        # df = format_df(df)
        # ↑====== Collect data ======↑

        # ↓====== Correct data type in dataFrame ======↓
        for column_type_dict in column_type_dicts:
            column_name = column_type_dict.get('column_name')
            if column_name not in cols:
                continue

            data_type = column_type_dict.get('data_type')
            if data_type == 'bigint':
                convert_nullable_int64_to_numpy_int64(df, [column_name])
                continue
            if data_type == 'integer':
                if column_name in [self.factory_machine_id_col_name, self.prod_part_id_col_name]:
                    df[column_name] = df[column_name].astype('int32')
                else:
                    df[column_name] = df[column_name].astype(pd.Int32Dtype.name)
                continue
            if data_type == 'smallint':
                df[column_name] = df[column_name].astype(pd.Int16Dtype.name)
                continue
            if data_type == 'real':
                df[column_name] = df[column_name].astype(pd.Float32Dtype.name)
                continue
            if data_type == 'text':
                df[column_name] = df[column_name].astype(pd.StringDtype.name)
                continue
            if 'timestamp' in data_type:
                df[column_name] = df[column_name].astype(np.datetime64.__name__)
                continue
            if data_type == 'boolean':
                df[column_name] = df[column_name].astype('boolean')
                continue
        # ↑====== Correct data type in dataFrame ======↑

        return df

    @BridgeStationModel.use_db_instance()
    def get_max_date_time_by_process_id(
        self,
        db_instance: Union[PostgreSQL, Oracle, MySQL, MSSQLServer] = None,
    ):
        max_time = 'max_time'
        sql = f'SELECT max({self.getdate_column.bridge_column_name}) as {max_time} FROM {self.table_name}'
        _, rows = db_instance.run_sql(sql, row_is_dict=True)
        return rows[0].get(max_time)

    @BridgeStationModel.use_db_instance()
    def get_min_date_time_by_process_id(
        self,
        db_instance: Union[PostgreSQL, Oracle, MySQL, MSSQLServer] = None,
    ):
        min_time = 'min_time'
        sql = f'SELECT min({self.getdate_column.bridge_column_name}) as {min_time} FROM {self.table_name}'
        _, rows = db_instance.run_sql(sql, row_is_dict=True)
        return rows[0].get(min_time)

    def get_transaction_distinct_values(
        self,
        db_instance: Union[PostgreSQL, Oracle, MySQL, MSSQLServer],
        cfg_process_col: CfgProcessColumn,
        sql_limit: int = 10000,
    ):
        sql = f'''
            SELECT DISTINCT SUB.{cfg_process_col.bridge_column_name}
            FROM (SELECT {cfg_process_col.bridge_column_name}
                  FROM {self.table_name}
                  LIMIT {sql_limit}) SUB;
        '''
        _, rows = db_instance.run_sql(sql, row_is_dict=False)
        rows = [x[0] for x in rows]
        if RawDataTypeDB.is_category_data_type(cfg_process_col.raw_data_type):
            m_column_group = MColumnGroup.get_by_data_ids([cfg_process_col.id])
            if not m_column_group:
                return []
            group_id = m_column_group[0].group_id
            values = []
            for chunk_rows in chunks(rows, THIN_DATA_CHUNK):
                dic_conditions = {
                    SemiMaster.Columns.factor.name: [(SqlComparisonOperator.IN, tuple(chunk_rows))],
                    SemiMaster.Columns.group_id.name: group_id,
                }
                _, _rows = SemiMaster.select_records(
                    db_instance,
                    dic_conditions=dic_conditions,
                    row_is_dict=False,
                    select_cols=[SemiMaster.Columns.value.name],
                )
                values.extend([x[0] for x in _rows])
            rows = values

        return rows

    @cached_property
    def table_model(self) -> sa.Table:
        columns = [
            sa.Column(self.id_col_name, sa.Integer),
            sa.Column(self.getdate_column.bridge_column_name, sa.DateTime),
            sa.Column(self.factory_machine_id_col_name, sa.Integer),
            sa.Column(self.prod_part_id_col_name, sa.Integer),
            sa.Column(self.data_source_id_col_name, sa.Integer),
        ]

        existed_columns_name = {
            self.id_col_name,
            self.getdate_column,
            self.factory_machine_id_col_name,
            self.prod_part_id_col_name,
            self.data_source_id_col_name,
        }

        for column in self.cfg_process_columns:
            column_name = column.bridge_column_name
            if column_name in existed_columns_name:
                continue

            data_type = DataType(column.data_type)
            sql_type = sa.Integer if data_type in [data_type.INTEGER, data_type.REAL] else sa.String
            columns.append(sa.Column(column_name, sql_type))

        return sa.Table(self.table_name, sa.MetaData(), *columns)

    def get_all(self, db_instance: Union[PostgreSQL], order_by_time=False):
        sql = f'SELECT * FROM {self.table_name}'
        if order_by_time:
            sql = f'{sql} ORDER BY {self.getdate_column.bridge_column_name}'

        cols, rows = db_instance.run_sql(sql, row_is_dict=False)
        return cols, rows

    def get_transaction_by_time_range(self, db_instance: Union[PostgreSQL], start_time, end_time, limit=1_000_000):
        param_marker = BridgeStationModel.get_parameter_marker()  # %s
        sql = f'''
            SELECT *
            FROM {self.table_name}
            WHERE  {self.getdate_column.bridge_column_name} >= {param_marker}
                AND {self.getdate_column.bridge_column_name} < {param_marker}
            LIMIT {limit};
        '''
        params = (start_time, end_time)
        cols, rows = db_instance.run_sql(sql, params=params, row_is_dict=False)
        df = pd.DataFrame(rows, columns=cols, dtype='object')
        return df

    def get_total_imported_row(self, db_instance, import_type):
        table_name = 't_factory_import'
        param_marker = BridgeStationModel.get_parameter_marker()  # %s
        sql = f'''
            SELECT SUM(imported_row) AS total FROM {table_name}
            WHERE process_id = {param_marker} AND import_type = {param_marker}
        '''
        params = [self.process_id, import_type]
        _, data = db_instance.run_sql(sql, params=params)
        return data[0]['total']

    def get_count_by_category(self, db_instance):
        param_marker = BridgeStationModel.get_parameter_marker()
        data_type_markers = [param_marker] * len(CATEGORY_TYPES)
        data_type_markers_str = ', '.join(data_type_markers)
        sql = f'''
            SELECT m_data.id as data_id, bridge_column_name as col_name,
                   semi_master.group_id, count(semi_master.group_id) as unique_count
            FROM cfg_process_column pc
                     JOIN m_data ON m_data.process_id = pc.process_id AND m_data.id = pc.id
                     LEFT JOIN m_group ON m_group.data_group_id = m_data.data_group_id
                     LEFT JOIN semi_master ON semi_master.group_id = m_group.id
            WHERE pc.process_id = {param_marker}
              AND pc.raw_data_type IN ({data_type_markers_str})
            GROUP BY m_data.id, pc.bridge_column_name, semi_master.group_id
            '''

        _, cat_rows = db_instance.run_sql(sql, row_is_dict=True, params=[self.process_id, *CATEGORY_TYPES])

        return cat_rows

    def convert_category_to_normal_transaction_data(
        self,
        db_instance,
        data_id,
        col_name,
        group_id,
        to_raw_data_type: str = None,
    ):
        param_marker = BridgeStationModel.get_parameter_marker()
        db_data_type, to_raw_data_type = determine_data_type_for_update(to_raw_data_type=to_raw_data_type)
        semi_val_col_name = SemiMaster.Columns.value.name

        old_col_name = f'_{col_name}'
        rename_col_sql = f'ALTER TABLE {self.table_name} RENAME COLUMN {col_name} TO {old_col_name}'
        add_col_sql = f'ALTER TABLE {self.table_name} ADD COLUMN {col_name} {db_data_type}'
        update_sql = f'''
            UPDATE {self.table_name}
            SET {col_name} = semi_master.{semi_val_col_name}::{db_data_type}
            FROM semi_master
            WHERE semi_master.group_id = {param_marker}
              AND semi_master.factor = {self.table_name}.{old_col_name};
            '''
        remove_col_sql = f'ALTER TABLE {self.table_name} DROP COLUMN {old_col_name}'

        # convert transaction data
        db_instance.execute_sql(rename_col_sql)
        db_instance.execute_sql(add_col_sql)
        db_instance.execute_sql(update_sql, params=[group_id])
        db_instance.execute_sql(remove_col_sql)

        # convert datatype in master and config
        update_master_config_of_category(db_instance, data_id, to_raw_data_type)

        return True

    def convert_category_to_normal_transaction_data_first_time(
        self,
        db_instance,
        data_id,
        col_name,
        to_raw_data_type: str = None,
    ):
        db_data_type, to_raw_data_type = determine_data_type_for_update(to_raw_data_type=to_raw_data_type)

        remove_col_sql = f'ALTER TABLE {self.table_name} DROP COLUMN {col_name}'
        add_col_sql = f'ALTER TABLE {self.table_name} ADD COLUMN {col_name} {db_data_type}'

        # convert transaction data
        db_instance.execute_sql(remove_col_sql)
        db_instance.execute_sql(add_col_sql)

        # convert datatype in master and config
        update_master_config_of_category(db_instance, data_id, to_raw_data_type)

        return True


def determine_data_type_for_update(to_raw_data_type: str = None):
    if to_raw_data_type is None:
        to_raw_data_type = RawDataTypeDB.TEXT.value

    db_data_type = dict_data_type_db.get(to_raw_data_type)

    return db_data_type, to_raw_data_type


def merge_rows_one_by_one(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    on: list[str],
    right_columns: list[str],
) -> pd.DataFrame:
    """
    Perform merge one by one rows
    For example:
    >>> df1 = pd.DataFrame({'a': [2, 2, 3, 3], 'b': [2, 2, 3, 3]})
    >>> df2 = pd.DataFrame({'a': [2, 2, 3], 'b': [2, 2, 3], 'c': ['x', 'y', 'z']})
    >>> merge_rows_one_by_one(df1, df2, on=['a', 'b'], right_columns=['c'])
           a  b  c
        0  2  2  x
        1  2  2  y
        2  3  3  z
        3  3  3  None
    """

    # set new columns to None
    left_df[right_columns] = None

    for keys in right_df[on].drop_duplicates().values:
        left_mask = np.all([left_df[col] == value for col, value in zip(on, keys)], axis=0)
        right_mask = np.all([right_df[col] == value for col, value in zip(on, keys)], axis=0)
        left_df.loc[left_mask, right_columns] = right_df.loc[right_mask, right_columns]

    return left_df


def combine_rows_one_by_one(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    on: list[str],
) -> tuple[pd.DataFrame, np.array, np.array]:
    """
    Perform combine one by one rows, both of dataframe must have the same column
    Each matching row in `right_df` will overwrite row in `left_df` if they are not `None`
    For example:
    >>> df1 = pd.DataFrame({'id': [1, 2, 3, 4, 5], 'a': [2, 2, 3, 4, 5], 'b': [None, None, 'c', 'd', 'e']})
          id a b
        0 1  2 a
        1 2  2 b
        2 3  3
        3 4  4
        4 5  4 e
    >>> df2 = pd.DataFrame({'id': [6, 7, 8, 9], 'a': [2, 2, 3, 3], 'b': ['x', None, None, 'w']})
          id a b
        0 6  2 x
        1 7  2
        2 8  3 z
        3 9  4
        3 10 5 w
        3 11 6 u
    >>> combine_rows_one_by_one(df1, df2, on=['a', 'b'], right_columns=['c'])
          id a b
        0 6  2 x
        1 7  2 b
        2 8  3 z
        3 9  4
    Returns:
        matched dataframe
        boolean array indicate which rows are used from `left_df`
        boolean array indicate which rows are used from `right_df`
    """

    left_used = np.full(len(left_df), fill_value=False, dtype=bool)
    right_used = np.full(len(right_df), fill_value=False, dtype=bool)

    if left_df.empty or right_df.empty:
        return left_df[left_used], left_used, right_used

    combined_dfs = []

    for keys in left_df[on].drop_duplicates().values:
        left_mask = np.all([left_df[col] == value for col, value in zip(on, keys)], axis=0)
        right_mask = np.all([right_df[col] == value for col, value in zip(on, keys)], axis=0)

        # reduce the mask
        # for example: if left mask = [False, False, True, False], and right mask = [False, False, True, True]
        # we want to reduce the right mask to [False, False, True, False] (Because there is only 1 `True` on left mask)
        left_mask_cum_sum = left_mask.cumsum()
        right_mask_cum_sum = right_mask.cumsum()
        # because we checked empty above, hence these will not raise exception
        total_true = min(left_mask_cum_sum[-1], right_mask_cum_sum[-1])
        # mark all values exceeded `total_true` to False
        left_mask[left_mask_cum_sum.searchsorted(total_true) + 1 :] = False
        right_mask[right_mask_cum_sum.searchsorted(total_true) + 1 :] = False

        combined_dfs.append(right_df.loc[right_mask].combine_first(left_df.loc[left_mask]))

        left_used |= left_mask
        right_used |= right_mask

    return pd.concat(combined_dfs), left_used, right_used
