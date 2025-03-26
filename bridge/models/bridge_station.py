from datetime import datetime
from functools import wraps
from typing import Any, Dict, Iterator, List, Text, Tuple, Union

import numpy as np
import pandas as pd
from pandas import DataFrame
from sqlalchemy.orm import scoped_session

from ap.common.common_utils import (
    convert_nullable_int64_to_numpy_int64,
    format_df,
    gen_sql_cast_text,
    get_nullable_int64_columns,
)
from ap.common.constants import (
    DEFAULT_NONE_VALUE,
    HALF_WIDTH_SPACE,
    NULL_DEFAULT_STRING,
    DataType,
    ServerType,
)
from ap.common.logger import logger
from ap.common.pydn.dblib.db_common import (
    AggregateFunction,
    OrderBy,
    SqlComparisonOperator,
    _gen_condition_str,
    gen_check_exist_sql,
    gen_delete_sql,
    gen_insert_sql,
    gen_select_all_sql,
    gen_select_by_condition_sql,
    gen_update_sql,
)
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.services.normalization import dict_normalize
from ap.setting_module.models import MasterDBModel as APMasterDBModel
from bridge.common.server_config import ServerConfig
from bridge.models.model_utils import TableColumn

DEFAULT_COLUMNS = ['parent_id', 'parent_detail_id', 'id', 'filter_from_pos', 'order']
DEFAULT_VALUES = [0, '']


class AbstractProcess:  # unused todo remove
    def get_name(self):
        raise NotImplementedError

    def get_id(self):
        raise NotImplementedError


class BridgeStationModel:
    primary_keys: List[TableColumn]
    _table_name: Text
    children_model: Dict = {}
    _nullable_int64_cols: List[str] = None

    server_type_parameter_marker = {
        ServerType.BridgeStationGrpc: '%s',
        ServerType.BridgeStationWeb: '%s',
        ServerType.EdgeServer: '%s',
        ServerType.StandAlone: '%s',
        # ServerType.EdgeServer: '?',
        # ServerType.StandAlone: '?',
    }

    _get_db_proxy = None
    partition_columns = []
    dic_auto_incremental = {}
    dic_config = {}  # shadow of ap.dic_config

    class Columns(TableColumn):
        """
        common columns
        """

        id = (1, DataType.INTEGER)
        created_at = (2, DataType.DATETIME)
        updated_at = (3, DataType.DATETIME)
        edit_userid = (30, DataType.TEXT)
        edit_date = (31, DataType.DATETIME)
        master_type = (99, DataType.DATETIME)

    def convert_to_list_of_values(self):
        return [getattr(self, col) for col in self.Columns.get_column_names()]

    @classmethod
    def get_by_id(cls, db_instance, id):
        id_col = cls.get_pk_column_names()[0]
        _, row = cls.select_records(db_instance, {id_col: int(id)}, limit=1)
        if not row:
            return None
        return row

    @classmethod
    def get_in_ids(cls, db_instance, ids: [List, Tuple], is_return_dict=False):
        if not ids:
            return {} if is_return_dict else []
        id_col = cls.get_pk_column_names()[0]
        _, rows = cls.select_records(
            db_instance,
            {id_col: [(SqlComparisonOperator.IN, tuple(ids))]},
            filter_deleted=False,
        )
        if is_return_dict:
            return {row[id_col]: row for row in rows}
        return rows

    @classmethod
    def get_all_tables(cls):
        """
        Before call this method, make sure you call 'import_all_sub_modules(models)'
        """
        all_sub_classes = cls.get_all_subclasses()
        return tuple([_class.get_original_table_name() for _class in all_sub_classes if hasattr(_class, '_table_name')])

    @classmethod
    def get_all_subclasses(cls):
        """
        Before call this method, make sure you call 'import_all_sub_modules(models)'
        """
        subclasses: set[cls] = set(cls.__subclasses__()).union(
            [s for c in cls.__subclasses__() for s in c.get_all_subclasses()],
        )
        return {subclass for subclass in subclasses if getattr(subclass, '_table_name', None) is not None}

    @classmethod
    def get_model_cls_by_table(cls, table_name):
        dict_table = dict(zip(cls.get_all_tables(), cls.get_all_subclasses()))
        return dict_table.get(table_name, None)

    @classmethod
    def get_pk_column_names(cls):
        return [column.name for column in cls.primary_keys]

    @classmethod
    def get_deleted_logical_column(cls):
        base_deleted_at = 'deleted_at'
        if base_deleted_at in cls.Columns.get_column_names():
            return base_deleted_at
        return None

    @classmethod
    def gen_pk_condition(cls, data):
        """
        generate sql where statement
        :param data:
        :return: dict
        """
        if not data:
            return data

        conditions = {}
        primary_keys = cls.get_pk_column_names()
        for col, val in data.items():
            if col not in primary_keys:
                continue

            conditions[col] = val

        return conditions

    @classmethod
    def select_records(
        cls,
        db_instance: Union[PostgreSQL, scoped_session],
        dic_conditions=None,
        select_cols=None,
        dict_aggregate_function=None,
        dic_order_by=None,
        limit=None,
        is_or_operation=False,
        row_is_dict=True,
        filter_deleted: bool = True,
    ):
        dic_conds = cls.convert_data_to_dict(dic_conditions)
        sql, params = gen_select_by_condition_sql(
            cls,
            dic_conds,
            select_cols=select_cols,
            dict_aggregate_function=dict_aggregate_function,
            dic_order_by=dic_order_by,
            limit=limit,
            is_or_operation=is_or_operation,
            filter_deleted=filter_deleted,
        )

        def _select():
            _cols, _rows = db_instance.run_sql(sql, row_is_dict=row_is_dict, params=params)
            return _cols, _rows

        cols, rows = cls._handle_for_sqlalchemy(
            db_instance,
            sql,
            params,
            select_cols,
            is_return=True,
            row_is_dict=row_is_dict,
            postgres_execute_func=_select,
        )

        # return 1 record when limit is 1 (easy to use)
        if limit == 1 and rows:
            rows = rows[0]

        return cols, rows

    @classmethod
    def get_all_records(cls, db_instance: PostgreSQL, row_is_dict=False, is_return_object=False):
        """
        Gets all records without any params.

        :param db_instance:
        :param row_is_dict:
        :param is_return_object:
        :return:
        """
        sql = gen_select_all_sql(cls)
        cols, rows = db_instance.run_sql(sql, row_is_dict=row_is_dict)  # type: list[str], list[Union[dict, Tuple]]
        if is_return_object:
            return [cls(row) for row in rows]
        return cols, rows

    @classmethod
    def convert_data_to_dict(cls, data, is_normalize: bool = True):
        """
        Return dict of data, even data in any type.
        Also normalize data before return dict.
        Usage:
            delete_by_pk
            insert_or_update
            insert_record
            select_records
            update_by_pk

        :param data: anything
        :param is_normalize: bool
        :return Dict: dict of normalized data
        """
        if not data:
            return data

        if isinstance(data, dict):
            dic_record = data
        elif isinstance(data, cls):
            dic_record = cls.to_dict(data)
        else:
            dic_record = data.__dict__

        if is_normalize:
            dict_normalize(dic_record)

        return dic_record

    @classmethod
    def update_by_conditions(cls, db_instance: PostgreSQL, dic_update_values, dic_conditions=None):
        """
        update one record
        :param db_instance:
        :param dic_update_values:
        :param dic_conditions:
        :return:
        """
        # get values that not in primary keys to set update
        dic_values = dict(dic_update_values.items())

        # update "updated_at"
        cls.set_updated_at(dic_values)

        # update data
        sql, params = gen_update_sql(cls, dic_values, dic_conditions)
        db_instance.execute_sql(sql, params=params)

        return True

    @classmethod
    def bulk_update_by_ids(cls, db_instance: Union[PostgreSQL, scoped_session], ids, dic_update_values):
        """
        update one record
        :param db_instance:
        :param ids:
        :param dic_update_values:
        :return:
        """
        if not ids:
            return

        # primary key conditions
        dic_conditions = {cls.Columns.id.name: [(SqlComparisonOperator.IN, tuple(ids))]}
        # get values that not in primary keys to set update
        dic_values = dict(dic_update_values.items())

        # update "updated_at"
        cls.set_updated_at(dic_values)

        # update data
        sql, params = gen_update_sql(cls, dic_values, dic_conditions)

        def _update():
            db_instance.execute_sql(sql, params=params)

        cls._handle_for_sqlalchemy(
            db_instance,
            sql,
            params,
            [],
            is_return=False,
            row_is_dict=False,
            postgres_execute_func=_update,
        )

        return True

    @classmethod
    def insert_record(cls, db_instance: Union[PostgreSQL, scoped_session], data, is_return_id=False, is_normalize=True):
        """
        insert one record
        :param db_instance:
        :param data: data to be insert or update
        :param is_return_id:
        :param is_normalize: normalize data before inserting
        :return:
        """
        dic_record = cls.convert_data_to_dict(data, is_normalize=is_normalize)
        dic_record = cls.convert_default_value_to_null(dic_record)

        # if ServerConfig.get_server_type() in (ServerType.BridgeStationGrpc, ServerType.BridgeStationWeb):
        return_cols = []
        if is_return_id:
            return_cols.append(BridgeStationModel.Columns.id.name)

        sql, params = gen_insert_sql(cls, dic_record, return_cols=return_cols)

        def _insert():
            _cols, _rows = db_instance.run_sql(sql, params=params, row_is_dict=False)
            return _cols, _rows

        cols, rows = cls._handle_for_sqlalchemy(
            db_instance,
            sql,
            params,
            [],
            is_return=return_cols,
            row_is_dict=False,
            postgres_execute_func=_insert,
        )

        if not rows:
            return None
        return rows[0][0]
        # else:
        #     sql, params = gen_insert_sql(cls, dic_record)
        #     id = db_instance.execute_sql(sql, params=params, return_value='lastrowid')  # support sqlite
        #     return id

    @classmethod
    def delete_by_pk(cls, db_instance: PostgreSQL, data, mode: int = 0, return_cols=None):
        """Delete one record of 'cls'

        :param db_instance:
        :param data: data to be delete
        :param mode: 0: delete physically, 1: delete logically
        :param return_cols:
        :return:
        """
        dic_record = cls.convert_data_to_dict(data)

        # primary key conditions
        dic_conditions = cls.gen_pk_condition(dic_record)

        recs = cls.delete_by_condition(db_instance, dic_conditions, mode, return_cols=return_cols)

        if not recs:
            return None

        return recs[0]

    @classmethod
    def delete_by_condition(
        cls,
        db_instance: Union[PostgreSQL, scoped_session],
        dic_conditions,
        mode: int = 1,
        return_cols=None,
    ):
        """Delete one record of 'cls'

        :param db_instance:
        :param dic_conditions:
        :param mode: 0: delete physically, 1: delete logically
        :param return_cols
        :return:
        """
        # primary key conditions
        effected_repo_ids = []
        rows = None
        if mode == 0:
            sql, params = gen_delete_sql(cls, dic_conditions, return_cols=return_cols)

            def _remove():
                # TODO: how to delete cascade when delete physically?
                if return_cols:
                    _, _rows = db_instance.run_sql(sql, params=params)
                else:
                    db_instance.execute_sql(sql, params)

            cls._handle_for_sqlalchemy(
                db_instance,
                sql,
                params,
                [],
                is_return=return_cols,
                row_is_dict=False,
                postgres_execute_func=_remove,
            )
        else:
            # get values that not in primary keys to set update
            bundle_sql = cls.gen_delete_logically_sql(db_instance, dic_conditions)
            for sql, params in bundle_sql:

                def _remove():
                    _, _rows = db_instance.run_sql(sql, params=params)
                    return _, rows

                _, rows = cls._handle_for_sqlalchemy(
                    db_instance,
                    sql,
                    params,
                    [],
                    is_return=True,
                    row_is_dict=False,
                    postgres_execute_func=_remove,
                )

        return effected_repo_ids

    @classmethod
    def _handle_for_sqlalchemy(
        cls,
        db_instance,
        sql,
        params,
        select_cols,
        is_return=False,
        row_is_dict=True,
        postgres_execute_func=None,
    ):
        if isinstance(db_instance, scoped_session):
            sqlalchemy_param = {}
            sqlalchemy_sql = sql
            for index, param in enumerate(params, start=1):
                sqlalchemy_param[str(index)] = param
                sqlalchemy_sql = sqlalchemy_sql.replace(cls.get_parameter_marker(), f':{index}', 1)
            cols = select_cols
            rows = db_instance.execute(sqlalchemy_sql, sqlalchemy_param)

            if is_return and postgres_execute_func.__name__ != '_insert':
                rows = [dict(zip(select_cols, row)) for row in rows] if row_is_dict else list(rows)

                return cols, rows
            else:
                return None, None
        else:
            cols, rows = None, None
            if is_return:
                cols, rows = postgres_execute_func()
            else:
                postgres_execute_func()

            return cols, rows

    @classmethod
    def gen_delete_logically_sql(cls, db_instance, dic_conditions):
        raise Exception('This model can not delete logically')

    @classmethod
    def check_exists(cls, db_instance: PostgreSQL, dic_record: Dict):
        dic_conditions = cls.gen_pk_condition(dic_record)
        print(dic_conditions)
        # check data already exist
        sql, params = gen_check_exist_sql(cls, dic_conditions)
        _, rows = db_instance.run_sql(sql, params=params)
        return bool(rows)

    @classmethod
    def to_dict(cls, data, exclude: Tuple = None, extend: Dict = None):
        dict_ret = {}
        for col in cls.Columns.get_column_names():
            if not hasattr(data, col):
                continue
            if exclude is None or col not in exclude:
                dict_ret[col] = getattr(data, col)
        if extend is not None:
            dict_ret.update(extend)
        return dict_ret

    @classmethod
    def get_parameter_marker(cls):
        return cls.server_type_parameter_marker[ServerConfig.get_server_type()]

    @classmethod
    def convert_default_value_to_null(cls, dic_data):
        for key, val in dic_data.items():
            if key in DEFAULT_COLUMNS and val in DEFAULT_VALUES:
                dic_data[key] = None

        return dic_data

    @classmethod
    def get_table_name(cls, partition_value=None):
        """
        Supports Edge Server gets partition table name by process id
        :param partition_value: can be process id or sum (self process id, target process id)
                                or can be a dictionary. this method will extract partition value from the dict
        :return: cls._table_name or cls._table_name with partition suffix
        """
        return cls._table_name
        # if ServerConfig.get_server_type() in (
        #         ServerType.BridgeStationGrpc, ServerType.BridgeStationWeb) or not cls.partition_columns:
        #     return cls._table_name
        # if isinstance(partition_value, dict):
        #     partition_value = cls.get_partition_value_from_dict(partition_value)
        # if not partition_value:
        #     return cls._table_name
        #
        # partition_idx = int(partition_value) % ServerConfig.get_partition_number()
        # partition_idx = str(partition_idx).zfill(len(str(ServerConfig.get_partition_number())))
        # return f'{cls._table_name}_{partition_idx}'

    @classmethod
    def get_original_table_name(cls):
        """
        Gets original table name
        Use this rather than get_table_name(cls, partition_value=None) if you sure the model cls is not partitioned
        Because get_table_name is slower a bit
        :return:
        """
        return cls._table_name

    @classmethod
    def get_partition_value_from_dict(cls, dic_condition):
        if not dic_condition:
            return None
        col_name = [col.name for col in cls.partition_columns]
        val_s = [value for key, value in dic_condition.items() if key in col_name]
        if not val_s:
            return None
        return sum(val_s)

    @classmethod
    def parse_bool(cls, bool_value):
        """
        Converts to bool if Bridge Server (Postgres)
        Converts to int if Edge Server (Sqlite)
        :param bool_value:
        :return:
        """
        return bool(bool_value)
        # if ServerConfig.get_server_type() in (ServerType.BridgeStationGrpc, ServerType.BridgeStationWeb):
        #     return bool(bool_value)
        # elif ServerConfig.get_server_type() == ServerType.EdgeServer:
        #     return int(bool_value)
        # return bool_value

    # unused
    # use ServerConfig.set_server_config(dic_config={ServerConfig.DB_PROXY: DbProxy)
    @classmethod
    def set_db_proxy_instance_method(cls, func):
        cls._get_db_proxy = func

    @classmethod
    def get_db_proxy(cls, db_instance=None) -> PostgreSQL:
        if db_instance is not None:
            return db_instance
        return ServerConfig.get_db_proxy()

    @classmethod
    def set_col_value(cls, dict_data: Dict, col, value):
        if col in cls.Columns.get_column_names() and not dict_data.get(col, None):
            dict_data[col] = value

    @classmethod
    def set_updated_at(cls, dict_data: Dict):
        updated_at_col = BridgeStationModel.Columns.updated_at.name
        cls.set_col_value(dict_data, updated_at_col, datetime.utcnow())

    @classmethod
    def truncate_table_with_cascade(cls, db_instance):
        sql = f'TRUNCATE TABLE "{cls._table_name}" CASCADE;'
        db_instance.run_sql(sql)

    @classmethod
    def truncate_table(cls, db_instance):
        sql = f'TRUNCATE TABLE "{cls._table_name}"'
        db_instance.run_sql(sql)

    @classmethod
    def get_nullable_int64_cols(cls, db_instance):
        if cls._nullable_int64_cols is None:
            cls._nullable_int64_cols = get_nullable_int64_columns(db_instance, cls.get_table_name())
        return cls._nullable_int64_cols

    @classmethod
    def get_all_as_df(
        cls,
        db_instance,
        process_id=None,
        select_cols=None,
        is_convert_null_string_to_na: bool = True,
    ) -> DataFrame:
        nullable_int64_cols = cls.get_nullable_int64_cols(db_instance)
        if select_cols:
            nullable_int64_cols = [col for col in nullable_int64_cols if col in select_cols]

        normal_cols = [col for col in cls.Columns.get_column_names() if col not in nullable_int64_cols]
        if select_cols:
            normal_cols = [col for col in normal_cols if col in select_cols]

        normal_cols = [f'"{col}"' for col in normal_cols]
        nullable_int64_cols = [gen_sql_cast_text(col) for col in nullable_int64_cols]
        all_cols = [*nullable_int64_cols, *normal_cols]
        select_cols = ', '.join(all_cols)

        parameter_marker = cls.get_parameter_marker()  # %s
        sql = f'''SELECT {select_cols} FROM {cls.get_table_name()}'''
        params = None
        if process_id:
            sql = f'{sql} WHERE process_id = {parameter_marker}'
            params = (process_id,)

        cols, rows = db_instance.run_sql(sql, row_is_dict=False, params=params)
        df = pd.DataFrame(rows, columns=cols, dtype='object')
        if is_convert_null_string_to_na:
            df.replace({NULL_DEFAULT_STRING: DEFAULT_NONE_VALUE}, inplace=True)
        df = format_df(df)

        for col in cls.Columns.get_values():
            if col.name not in cols:
                continue

            if col.value[1] == DataType.INTEGER:
                convert_nullable_int64_to_numpy_int64(df, [col.name])
            elif col.value[1] == DataType.REAL:
                df[col.name] = df[col.name].astype(pd.Float64Dtype.name)
            elif col.value[1] == DataType.TEXT:
                df[col.name] = df[col.name].astype(pd.StringDtype.name)
            elif col.value[1] == DataType.DATETIME:
                df[col.name] = df[col.name].astype(np.datetime64.__name__)
            elif col.value[1] == DataType.BOOLEAN:
                df[col.name] = df[col.name].astype(pd.BooleanDtype.name)

        if 'id' in df.columns and not getattr(cls, '__is_mapping_table__', None):
            df.rename(columns={'id': cls.get_foreign_id_column_name()}, inplace=True)

        return df

    @classmethod
    def get_foreign_id_column_name(cls) -> str:  # only use for cfg_ and m_
        """
        m_line  ->  line_id

        :return:
        """
        elems = cls.get_original_table_name().split('_')
        return f"{'_'.join(elems[1:])}_id"

    @staticmethod
    def use_db_instance(db_instance_argument_name: str = 'db_instance'):
        """Decorator to auto create db instance when no pass it in argument"""

        def decorator(fn):
            @wraps(fn)
            def inner(*args, **kwargs):
                db_instance: PostgreSQL = kwargs.get(db_instance_argument_name)
                if db_instance is None:
                    with BridgeStationModel.get_db_proxy() as new_db_instance:
                        kwargs[db_instance_argument_name] = new_db_instance
                        return fn(*args, **kwargs)
                else:
                    return fn(*args, **kwargs)

            return inner

        return decorator

    @staticmethod
    def use_db_instance_generator(db_instance_argument_name: str = 'db_instance'):
        """Decorator to auto create db instance when no pass it in argument"""

        def decorator(fn):
            @wraps(fn)
            def inner(*args, **kwargs):
                db_instance: PostgreSQL = kwargs.get(db_instance_argument_name)
                if db_instance is None:
                    with BridgeStationModel.get_db_proxy() as new_db_instance:
                        kwargs[db_instance_argument_name] = new_db_instance
                        return (yield from fn(*args, **kwargs))
                else:
                    return (yield from fn(*args, **kwargs))

            return inner

        return decorator


class ConfigModel(BridgeStationModel):
    @classmethod
    def _get_dict_delete_logically_values(cls, deleted_time):
        return {'deleted_at': deleted_time}

    @classmethod
    def get_all_records(cls, db_instance: PostgreSQL, row_is_dict=False, is_return_object=False):
        sql = gen_select_all_sql(cls)
        cols, rows = db_instance.run_sql(
            sql,
            row_is_dict=row_is_dict,
        )  # type: list[str], list[Union[dict, Tuple]]

        if is_return_object:
            return [cls(row) for row in rows]
        return cols, rows


class TransactionModel(BridgeStationModel):
    CYCLE_ID_START = 1
    SQLITE_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%f000Z'

    # Only for quick typing
    class Columns(TableColumn):
        process_id = (1, DataType.INTEGER)
        cycle_id = (2, DataType.INTEGER)
        col_group_id = (3, DataType.INTEGER)
        time = (3, DataType.DATETIME)
        val_01 = (4, DataType.INTEGER)
        val_02 = (5, DataType.INTEGER)
        val_03 = (6, DataType.INTEGER)
        val_04 = (7, DataType.INTEGER)
        val_05 = (8, DataType.INTEGER)
        val_06 = (9, DataType.INTEGER)
        val_07 = (10, DataType.INTEGER)
        val_08 = (11, DataType.INTEGER)
        val_09 = (12, DataType.INTEGER)
        val_10 = (13, DataType.INTEGER)

    @classmethod
    def get_all_records(cls, db_instance: PostgreSQL):  # override BridgeStationModel.get_all_records
        raise Exception('Not to use this method for transaction')

    @classmethod
    def get_column_names_startswith_val(cls):
        return [
            cls.Columns.val_01.name,
            cls.Columns.val_02.name,
            cls.Columns.val_03.name,
            cls.Columns.val_04.name,
            cls.Columns.val_05.name,
            cls.Columns.val_06.name,
            cls.Columns.val_07.name,
            cls.Columns.val_08.name,
            cls.Columns.val_09.name,
            cls.Columns.val_10.name,
        ]

    # see: data_import.get_latest_records
    @classmethod
    def get_records_by_process_id(cls, db_instance, process_id, dic_order=None, limit=None):
        if not dic_order:
            dic_order = {cls.Columns.time.name: OrderBy.ASC.name}
        dic_conditions = {cls.Columns.process_id.name: process_id}
        col, rows = cls.select_records(
            db_instance,
            dic_conditions=dic_conditions,
            dic_order_by=dic_order,
            row_is_dict=False,
            limit=limit,
        )
        return col, rows

    # todo: check if bad performance ?
    @classmethod
    def get_records_by_cycle_id(cls, db_instance, process_id, cycle_ids, dic_order=None):
        if not dic_order:
            dic_order = {cls.Columns.time.name: OrderBy.ASC.name}
        dic_conditions = {
            cls.Columns.process_id.name: process_id,
            cls.Columns.cycle_id.name: [(SqlComparisonOperator.IN, tuple(cycle_ids))],
        }

        col, rows = cls.select_records(
            db_instance,
            dic_conditions=dic_conditions,
            dic_order_by=dic_order,
            row_is_dict=False,
        )
        return col, rows

    # see: data_import.get_records_by_range_time
    @classmethod
    def get_records_by_range_time(cls, db_instance, process_id, start_tm, end_time, dic_order=None):
        if not dic_order:
            dic_order = {cls.Columns.time.name: OrderBy.ASC.name}
        dic_conditions = {
            cls.Columns.process_id.name: process_id,
            cls.Columns.time.name: [
                (SqlComparisonOperator.GREATER_THAN_OR_EQ, start_tm),
                (SqlComparisonOperator.LESS_THAN_OR_EQ, end_time),
            ],
        }
        col, rows = cls.select_records(
            db_instance,
            dic_conditions=dic_conditions,
            dic_order_by=dic_order,
            row_is_dict=False,
        )
        return col, rows

    @classmethod
    def get_records_by_pk(cls, db_instance, process_id, cycle_id, col_group_id, start_tm, end_time, dic_order=None):
        # Get by process_id cycle_id col_group_id time
        if not dic_order:
            dic_order = {cls.Columns.time.name: OrderBy.ASC.name}
        dic_conditions = {
            cls.Columns.process_id.name: process_id,
            cls.Columns.cycle_id.name: cycle_id,
            cls.Columns.col_group_id.name: col_group_id,
            cls.Columns.time.name: [
                (SqlComparisonOperator.GREATER_THAN_OR_EQ, start_tm),
                (SqlComparisonOperator.LESS_THAN_OR_EQ, end_time),
            ],
        }
        col, rows = cls.select_records(
            db_instance,
            dic_conditions=dic_conditions,
            dic_order_by=dic_order,
            row_is_dict=False,
        )
        return col, rows

    @classmethod
    def get_max_cycle_id(cls, db_instance, process_id):
        dic_conditions = {cls.Columns.process_id.name: process_id}
        dict_aggregate_function = {cls.Columns.cycle_id.name: (AggregateFunction.MAX.name, cls.Columns.cycle_id.name)}
        _, max_cycle_id = cls.select_records(
            db_instance,
            dic_conditions=dic_conditions,
            dict_aggregate_function=dict_aggregate_function,
            limit=1,
            row_is_dict=False,
        )
        if not max_cycle_id or not max_cycle_id[0]:
            return 0
        return max_cycle_id[0]

    @classmethod
    def get_count_cycle_id_by_proc_id(cls, db_instance, process_id, max_past_cycle_id=None):
        """
        Count number of cycle by process id. Use max_past_cycle_id to limit counting
        :param db_instance:
        :param process_id:
        :param max_past_cycle_id:
        :return:
        """
        pm = cls.get_parameter_marker()
        limit_sql = f'LIMIT {pm}' if max_past_cycle_id else ''
        t_data_int_tb_name = cls.get_table_name(process_id)

        # Limit supports to stop counting if reach expect limitation.
        sql = f'''
        SELECT COUNT(1) FROM (
            SELECT 1 FROM (
                (select DISTINCT cycle_id from {t_data_int_tb_name} where {cls.Columns.process_id.name} = {pm})
            ) as _cycle {limit_sql}
        ) as _count
        '''

        params = (process_id, max_past_cycle_id) if max_past_cycle_id else (process_id,)
        _, rows = db_instance.run_sql(sql, params=params, row_is_dict=False)
        if not rows or not rows[0]:
            return 0
        return rows[0][0]

    @classmethod
    def update_time_by_tzoffset(cls, db_instance, proc_id, tz_offset):
        # Update column 'time', add or substract for tz_offset.
        # If Rainbow7 only update sensor of 'get_date_col'. Bridge7 need to update all records of this proc_id
        # Because Bridge7 save 'get_date_col' as 'time' in all tables [t_data_....]
        pm = cls.get_parameter_marker()
        table_name = cls.get_table_name(proc_id)
        col_time = cls.Columns.time.name
        col_proc_id = cls.Columns.process_id.name
        sql_delta_time = f"'{tz_offset}'"
        sql_where = f'WHERE {col_proc_id}= {pm}'
        # time_fmt = add_single_quote(cls.SQLITE_DATETIME_FORMAT)

        sql_update = f'UPDATE {table_name} SET {col_time}={col_time} + interval {sql_delta_time}'
        sql = f'{sql_update} {sql_where}'
        db_instance.execute_sql(sql, params=(proc_id,))

    @classmethod
    def get_delete_sql(cls, process_id):
        """
        Gets delete sql for delete duplicates cycle ids

        :return:
        """
        pm = cls.get_parameter_marker()
        return (
            f'DELETE FROM {cls.get_table_name(process_id)}'
            f' WHERE "process_id" = {pm} AND "col_group_id" = {pm} AND cycle_id IN {pm}'
        )

    @classmethod
    def get_delete_cycles_sql(cls, process_id, cycle_ids, start_tm=None, end_tm=None, is_select=False):
        """
        Gets delete sql for delete duplicates cycle ids

        :return:
        """
        table = cls.get_table_name(process_id)  # t_data_int_0
        dic_conditions = {
            cls.Columns.process_id.name: process_id,
            cls.Columns.cycle_id.name: [(SqlComparisonOperator.IN, tuple(cycle_ids))],
        }

        if start_tm and end_tm:
            dic_conditions.update(
                {
                    cls.Columns.time.name: [
                        (SqlComparisonOperator.GREATER_THAN_OR_EQ, start_tm),
                        (SqlComparisonOperator.LESS_THAN_OR_EQ, end_tm),
                    ],
                },
            )

        condition_str, params_condition = _gen_condition_str(cls, dic_conditions)
        if is_select:
            sql = f'SELECT * FROM {table} WHERE {condition_str}'
        else:
            sql = f'DELETE FROM {table} WHERE {condition_str}'
        return sql, params_condition

    @classmethod
    def get_foreign_id_column_name(cls) -> str:
        raise Exception('DO NOT use this method for transaction')


class MasterModel(BridgeStationModel):
    @classmethod  # unused # todo remove
    def from_dict_to_name(cls, dict_data: Dict) -> str:
        return dict_data[str(cls.get_name_column())]

    @classmethod
    def get_name_column(cls):
        """
        Human-friendly column

        :return:
        """
        raise Exception('Cannot call abstract method')

    @classmethod
    def get_default_name_column(cls) -> str:
        """
        m_line  ->  line_name

        :return:
        """
        elems = cls.get_original_table_name().split('_')
        return f"{'_'.join(elems[1:])}_name"

    @classmethod
    def get_default_abbr_column(cls) -> str:
        """
        m_line  ->  line_abbr

        :return:
        """
        elems = cls.get_original_table_name().split('_')
        return f"{'_'.join(elems[1:])}_abbr"

    @classmethod
    def get_jp_name_column(cls) -> str:
        columns = cls.Columns.get_column_by_name_like('_name_jp')
        return columns[0].name if columns else None

    @classmethod
    def get_jp_abbr_column(cls) -> str:
        columns = cls.Columns.get_column_by_name_like('_abbr_jp')
        return columns[0].name if columns else None

    def get_jp_name(self) -> str:
        jp_name_column = self.get_jp_name_column()
        return getattr(self, jp_name_column) if hasattr(self, jp_name_column) else None

    @classmethod
    def get_en_name_column(cls) -> str:
        columns = cls.Columns.get_column_by_name_like('_name_en')
        return columns[0].name if columns else None

    @classmethod
    def get_en_abbr_column(cls) -> str:
        columns = cls.Columns.get_column_by_name_like('_abbr_en')
        return columns[0].name if columns else None

    def get_en_name(self) -> str:
        en_name_column = self.get_en_name_column()
        return getattr(self, en_name_column) if hasattr(self, en_name_column) else None

    @classmethod
    def get_sys_name_column(cls) -> str:
        columns = cls.Columns.get_column_by_name_like('_name_sys')
        return columns[0].name if columns else None

    @classmethod
    def get_sys_abbr_column(cls) -> str:
        columns = cls.Columns.get_column_by_name_like('_abbr_sys')
        return columns[0].name if columns else None

    def get_sys_name(self) -> str:
        """
        Return name that contains ascii character only.
        If model class has no name_sys column, return None

        :return:
        """
        sys_name_column = self.get_sys_name_column()
        return getattr(self, sys_name_column) if hasattr(self, sys_name_column) else None

    @classmethod
    def get_abbr_name_column(cls) -> str:
        columns = cls.Columns.get_column_by_name_like('_abbr')
        return columns[0].name if columns else None

    @classmethod
    def get_sign_n_no_column(cls) -> list[Any]:
        short_table_name = cls.get_table_name()[2:]
        columns = cls.Columns.get_column_by_name_like(f'{short_table_name}_sign') + cls.Columns.get_column_by_name_like(
            f'{short_table_name}_no',
        )
        if not columns:
            columns = cls.Columns.get_column_by_name_like(f'{short_table_name}_abbr')

        return [col.name for col in columns]

    @classmethod
    def get_local_name_column(cls) -> str:
        columns = cls.Columns.get_column_by_name_like('_name_local')
        return columns[0].name if columns else None

    @classmethod
    def get_local_abbr_column(cls) -> str:
        columns = cls.Columns.get_column_by_name_like('_abbr_local')
        return columns[0].name if columns else None

    @classmethod
    def get_group_id_column(cls) -> str:
        columns = cls.Columns.get_column_by_name_like('_group_id')
        return columns[0].name if columns else None

    def get_local_name(self) -> str:
        loca_name_column = self.get_local_name_column()
        return getattr(self, loca_name_column) if hasattr(self, loca_name_column) else None

    @classmethod
    def get_all_name_columns(cls):
        columns = [
            cls.get_jp_name_column(),
            cls.get_en_name_column(),
            cls.get_local_name_column(),
            cls.get_sys_name_column(),
        ]  # order is daiji

        # don't use intersection method. This will shuffle order
        # intersection(columns, cls.Columns.get_column_names())
        origin_col = cls.Columns.get_column_names()
        return [col for col in columns if col and col in origin_col]

    @classmethod
    def get_all_abbr_columns(cls):
        columns = [
            cls.get_jp_abbr_column(),
            cls.get_en_abbr_column(),
            cls.get_local_abbr_column(),
            cls.get_sys_abbr_column(),
        ]  # order is daiji

        # don't use intersection method. This will shuffle order
        # intersection(columns, cls.Columns.get_column_names())
        origin_col = cls.Columns.get_column_names()
        return [col for col in columns if col and col in origin_col]

    @classmethod
    def pick_column_by_language(cls, lang, mode='I', is_show_graph=False):
        """

        :param is_show_graph: column to show graph
        :param lang: 'en' 'ja' 'vi' ...
        :param mode: 'I' if input (insert to db), 'O' if output (get from db)
        :return:
        """
        if is_show_graph:
            dict_lang_and_column = {
                'ja': cls.get_jp_abbr_column(),
                'jp': cls.get_jp_abbr_column(),
                'en': cls.get_en_abbr_column(),
            }
        else:
            dict_lang_and_column = {
                'ja': cls.get_jp_name_column(),
                'jp': cls.get_jp_name_column(),
                'en': cls.get_en_name_column(),
            }
        if mode not in ('I', 'O'):  # todo bỏ biến mode. không dùng
            logger.warning(f'Parameter incorrect. Expect "I" or "O". Get {mode}')
        not_found_case_column = cls.get_local_name_column() if mode == 'I' else cls.get_sys_name_column()
        col = dict_lang_and_column.get(lang)
        return col if col else not_found_case_column

    @classmethod
    def get_in_ids(cls, db_instance, ids: [List, Tuple], is_return_dict=False):
        if not ids:
            return {} if is_return_dict else []
        id_col = cls.get_pk_column_names()[0]
        _, rows = cls.select_records(
            db_instance,
            {id_col: [(SqlComparisonOperator.IN, tuple(ids))]},
            filter_deleted=False,
        )
        if not rows:
            return {} if is_return_dict else []
        data_objects = [cls(row) for row in rows]
        if is_return_dict:
            return {obj.id: obj for obj in data_objects}
        return data_objects

    def get_name(self):  # NOT A CLASS METHOD, support object created by constructor
        all_name_columns = self.get_all_name_columns()
        dict_val = self.__dict__
        for col in all_name_columns:
            val = dict_val.get(col)
            if val:
                return val
        return None

    @classmethod
    def is_row_exist(cls, db_instance, row: Dict):
        _, rows = cls.select_records(db_instance, row)
        return bool(rows), rows

    @classmethod
    def get_unique_by_data_table_ids(
        cls,
        *,
        db_instance: PostgreSQL,
        data_table_ids: list[int],
    ) -> Iterator[APMasterDBModel]:
        raise NotImplementedError('This method should be implemented in child classes')


class SemiMasterModel(BridgeStationModel):
    pass


class OthersDBModel(BridgeStationModel):
    pass


class MasterModelMapping(BridgeStationModel):  # Added Sprint 118
    # TODO: add index for table columns : data_source_id, master_data_id

    master_model: MasterModel = None

    # Only for quick typing.
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)
        data_source_id = (2, DataType.INTEGER)
        master_data_id = (99, DataType.INTEGER)

    @classmethod
    def get_by_data_source_id(cls, db_instance: PostgreSQL, data_source_id):
        dic_conds = {cls.Columns.data_source_id.name: data_source_id}
        dic_order = {cls.Columns.master_data_id.name: OrderBy.ASC.name}
        cols, rows = cls.select_records(
            db_instance,
            dic_conditions=dic_conds,
            dic_order_by=dic_order,
            row_is_dict=False,
        )
        return cols, rows

    @classmethod
    def update_parent_by_master_table(cls, db_instance):
        # --- sample ---
        # update m_line_mapping
        # set master_data_id = m_line.id
        # from m_line
        # where m_line.plant_no = m_line_mapping.plant_no and m_line.line_no = m_line_mapping.line_no;
        # TODO: where increasement column between ..... (for performance)
        # --------------
        table_name = cls.get_table_name()
        pk_column_names = cls.master_model.get_pk_column_names()
        master_data_id_col = cls.Columns.master_data_id.name
        sql = f'UPDATE {table_name}'
        set_sql = f'SET {master_data_id_col} = {cls.master_model.get_table_name()}.{cls.master_model.Columns.id.name} '
        from_sql = f'FROM {cls.master_model.get_table_name()}'
        join_sql = [f'{table_name}.{col} = {cls.master_model.get_table_name()}.{col}' for col in pk_column_names]
        where_sql = 'WHERE ' + ' AND '.join(join_sql)
        sql = HALF_WIDTH_SPACE.join([sql, set_sql, from_sql, where_sql])
        db_instance.execute_sql(sql)

    @classmethod
    def update_child_by_parent(cls, db_instance):
        # --- sample ---
        # update m_plant_mapping as child
        # set master_data_id = parent.master_data_id
        # from m_plant_mapping as parent
        # where(parent.parent_id = 0 and child.parent_id = parent.id and parent.master_data_id is not NULL);
        # TODO: where increasement column between ..... (for performance)
        # --------------
        table_name = cls.get_table_name()
        master_data_id_col = cls.Columns.master_data_id.name
        parent_id_col = cls.Columns.parent_id.name
        sql = f'UPDATE {table_name} as child'
        set_sql = f'SET {master_data_id_col} = parent.{master_data_id_col} '
        from_sql = f'FROM {table_name} as parent'
        conds = [
            f'parent.{parent_id_col} = 0',
            f'child.{parent_id_col} = parent.id',
            f'parent.{master_data_id_col} IS NOT NULL',
        ]
        where_sql = 'WHERE ' + ' AND '.join(conds)
        sql = HALF_WIDTH_SPACE.join([sql, set_sql, from_sql, where_sql])
        db_instance.execute_sql(sql)
