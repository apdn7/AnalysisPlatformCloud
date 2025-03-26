from enum import Enum, auto
from typing import Dict, List, Tuple, Union

from ap.common.common_utils import DATE_FORMAT_STR_SQLITE, convert_time
from ap.common.pydn.dblib import mssqlserver, mysql, oracle, postgresql, sqlite

PARAM_SYMBOL = '%s'


class OrderBy(Enum):
    DESC = auto()
    ASC = auto()


class SqlComparisonOperator(Enum):
    EQUAL = '='
    NOT_EQUAL = '!='
    LESS_THAN = '<'
    GREATER_THAN = '>'
    LESS_THAN_OR_EQ = '<='
    GREATER_THAN_OR_EQ = '>='
    IS_NULL = 'IS NULL'  # TODO: is null, like, between have separate sql syntax, have not test yet. be careful
    LIKE = 'LIKE'
    BETWEEN = 'BETWEEN'
    IN = 'IN'  # Ex: cls.Columns.status.name: [(SqlComparisonOperator.IN, tuple(job_statuses))]
    NOT_IN = 'NOT IN'  # Ex: cls.Columns.status.name: [(SqlComparisonOperator.NOT_IN, tuple(job_statuses))]


class AggregateFunction(Enum):
    # Example
    # dict_aggregate_function = {'cycle_id': (AggregateFunction.MAX.value, 'cycle_id')}
    # ==> select max(cycle_id) as "cycle_id"
    # dict_aggregate_function = {'time_1': (AggregateFunction.TO_CHAR.value, 'time', 'yyyymmdd')}
    # ==> select TO_CHAR(time, 'yyyymmdd') as "time_1"
    MAX = 'MAX'
    TO_CHAR = 'TO_CHAR'
    DISTINCT = 'DISTINCT'


def strip_all_quote(instr):
    return str(instr).strip("'").strip('"')


def _gen_condition_str(model_cls, dic_conditions: Dict[str, Tuple], is_or_operation=False):
    """

    :param model_cls:
    :param dic_conditions: should follow the following format
             {'column name' : ((SqlComparisonOperator.EQUAL, value), (SqlComparisonOperator.LESS_THAN, value))}
    :param is_or_operation:
    :return:
    """
    # draft, replace for gen_condition_str
    model_columns = model_cls.Columns.get_column_names()
    dic_model_cols = {col: val for col, val in dic_conditions.items() if col in model_columns}

    parameter_marker = model_cls.get_parameter_marker()  # %s
    sql_condition = []
    params = []
    for col, conditions in dic_model_cols.items():
        if not isinstance(conditions, (tuple, list)):
            if conditions is None:
                sql_condition.append(f'"{col}" IS NULL')
            else:
                sql_condition.append(f'"{col}" {SqlComparisonOperator.EQUAL.value} {parameter_marker}')
                params.append(check_none_value(conditions))
        else:
            for condition in conditions:
                operator, value = condition
                sql_condition.append(f'"{col}" {operator.value} {parameter_marker}')
                params.append(check_none_value(value))

    or_and_ope = 'AND'
    if is_or_operation:
        or_and_ope = 'OR'

    sql = f' {or_and_ope} '.join(sql_condition)

    # params = [vals[1] if isinstance(vals, (tuple, list)) else vals for col, vals in dic_model_cols.items()]
    return sql, params


def gen_update_value_str(model_cls, dic_values: Dict):
    model_columns = model_cls.Columns.get_column_names()
    parameter_marker = model_cls.get_parameter_marker()  # %s
    sql = ','.join([f'"{col}" = {parameter_marker}' for col in dic_values.keys() if col in model_columns])
    param = [value for col, value in dic_values.items() if col in model_columns]
    return sql, param


def check_none_value(value):
    """
    return 'NULL' if value is None
    """
    if value is None or value == '':
        return 'NULL'
    return value


def gen_insert_col_str(model_cls=None, column_names: Union[Dict, List] = None):
    """
    Add double quotes to each name of column existing in model columns and combine to string
    :param model_cls:
    :param column_names:
    """
    if not column_names:
        return ''

    model_columns = model_cls.Columns.get_column_names() if model_cls else column_names

    return ','.join([add_double_quote(col) for col in column_names if col in model_columns])


def gen_insert_val_str(model_cls, dic_values: Dict):
    """
    Add single quotes to each value to be insert/update of column existing in model columns and combine to string
    :param model_cls:
    :param dic_values:
    """
    model_columns = model_cls.Columns.get_column_names()
    return tuple([val for col, val in dic_values.items() if col in model_columns])


def gen_insert_param_str(model_cls, dic_values: Dict):
    model_columns = model_cls.Columns.get_column_names()
    parameter_marker = model_cls.get_parameter_marker()
    return ','.join([parameter_marker for col in dic_values if col in model_columns])


def add_single_quote(val):
    """
    add single quote
    :param val:
    :return:
    """
    if not val:
        return val

    return f"'{val}'"


def add_double_quote(val):
    """
    add single quote
    :param val:
    :return:
    """
    if not val:
        return val

    return f'"{val}"'


def gen_insert_sql(model_cls, dic_values, return_cols: List = None, on_conflict_update=None):
    """
    generate insert sql
    :param model_cls: BridgeStationModel
    :param dic_values:
    :param on_conflict_update:
    :param return_cols:
    :return:
    """

    # if ServerConfig.get_server_type() == ServerType.EdgeServer and ID_STR not in dic_values:
    #     new_id = IdManagementUtils.get_increase_id(model_cls)
    #     if new_id:
    #         dic_values[model_cls.Columns.id.name] = new_id

    col_str = gen_insert_col_str(model_cls, dic_values)
    val_str = gen_insert_param_str(model_cls, dic_values)
    tuple_params = gen_insert_val_str(model_cls, dic_values)

    table_name = add_double_quote(model_cls.get_table_name(dic_values))  # extract partition value from dic_values
    sql = f'INSERT INTO {table_name}({col_str}) VALUES({val_str})'
    parameter_marker = model_cls.get_parameter_marker()  # %s

    if on_conflict_update:
        pri_keys = model_cls.get_pk_column_names()
        pri_keys_str = gen_insert_col_str(model_cls, pri_keys)

        dic_on_updates = {col: val for col, val in dic_values.items() if col not in pri_keys}
        if dic_on_updates:
            sql += f' ON CONFLICT({pri_keys_str}) DO UPDATE SET '
            sql += ', '.join([f'{col} = {parameter_marker}' for col in dic_on_updates])
            tuple_params = tuple_params + tuple(dic_on_updates.values())

    if return_cols:
        return_cols_str = gen_insert_col_str(model_cls, return_cols)
        sql = f'{sql} RETURNING {return_cols_str}'

    return sql, tuple_params


def gen_delete_sql(model_cls, dic_conditions, return_cols=None):
    """
    generate delete sql
    :param return_cols:
    :param model_cls:
    :param dic_conditions:
    :return:
    """
    condition_str, params_condition = _gen_condition_str(model_cls, dic_conditions)
    table_name = add_double_quote(model_cls.get_table_name(dic_conditions))  # extract partition value from dic_values
    sql = f'DELETE FROM {table_name} WHERE {condition_str}'

    if return_cols:
        return_cols_str = gen_insert_col_str(model_cls, return_cols)
        sql = f'{sql} RETURNING {return_cols_str}'

    return sql, params_condition


def gen_update_sql(model_cls, dic_values, dic_conditions, extend=None, return_cols: List = None):
    """
    generate update sql
    :param model_cls:
    :param dic_values:
    :param dic_conditions:
    :param extend: key-value which key not existing in model
    :param return_cols: return updated value of columns.
    :return:
    """
    params = []
    val_str, params_set_value = gen_update_value_str(model_cls, dic_values)
    parameter_marker = model_cls.get_parameter_marker()  # %s

    if extend is not None:
        val_str = val_str + ',' + ','.join([f'"{col}" = {parameter_marker}' for col in extend.keys() if col in extend])
        params_set_value = params_set_value + list(extend.values())

    condition_str, params_condition = _gen_condition_str(model_cls, dic_conditions)
    table_name = add_double_quote(model_cls.get_table_name(dic_values))  # extract partition value from dic_values

    # UPDATE table SET col_a = %s, col_b = %s WHERE col_c = %s
    sql = f'UPDATE {table_name} SET {val_str} WHERE {condition_str}'

    params.extend(params_set_value)
    params.extend(params_condition)

    if return_cols:
        return_cols_str = gen_insert_col_str(model_cls, return_cols)
        sql = f'{sql} RETURNING {return_cols_str}'
    return sql, tuple(params)


def gen_truncate_sql(model_cls, with_cascade=True):
    """
    generate truncate sql
    :param model_cls:
    :param with_cascade:
    :return:
    """
    table_name = add_double_quote(model_cls.get_table_name())  # TODO can not truncate partition table?
    sql = f'TRUNCATE TABLE {table_name}'
    if with_cascade:
        sql = f'{sql} CASCADE'

    return sql


def gen_check_exist_sql(model_cls, dic_conditions=None):
    sql = f'SELECT 1 FROM {add_double_quote(model_cls.get_table_name(dic_conditions))}'
    params_condition = None
    if dic_conditions:
        condition_str, params_condition = _gen_condition_str(model_cls, dic_conditions)
        sql += f' WHERE {condition_str}'
        params_condition = tuple(params_condition)

    sql += ' LIMIT 1'
    return sql, params_condition


def gen_select_col_str(model_cls=None, column_names: Union[Dict, List] = None, is_add_double_quote=True):
    """
    Add double quotes to each name of column existing in model columns and combine to string
    :param is_add_double_quote:
    :param model_cls:
    :param column_names:
    """
    if column_names is None:
        return '*'

    model_columns = model_cls.Columns.get_column_names() if model_cls else column_names

    select_cols = [col for col in column_names if col in model_columns]

    if is_add_double_quote:
        return ','.join([add_double_quote(col) for col in select_cols])
    else:
        return ','.join(list(select_cols))


def gen_select_aggregate_function(dict_aggregate_function: Dict):
    if not dict_aggregate_function:
        return None
    # output
    # MAX(col1),MIN(col2),FUNC_A(col3,col4,const)
    return {
        f'"{key}"': f'{value[0]}({",".join(value[1:])}) as "{key}"' for key, value in dict_aggregate_function.items()
    }


def gen_select_all_sql(model_cls, select_cols=None):
    col_str = gen_select_col_str(model_cls, select_cols)
    table_str = add_double_quote(model_cls.get_table_name(select_cols))
    sql = f'SELECT {col_str} FROM {table_str}'
    deleted_at_str = gen_deleted_at_condition(model_cls)
    if deleted_at_str:
        sql += f'WHERE {deleted_at_str}'

    return sql


def gen_select_by_condition_sql(
    model_cls,
    dic_conditions=None,
    select_cols=None,
    dict_aggregate_function=None,
    dic_order_by=Union[List, Dict, None],
    limit=None,
    is_or_operation=False,
    filter_deleted: bool = True,
):
    model_columns = model_cls.Columns.get_column_names()
    select_col_names = [add_double_quote(col) for col in select_cols if col in model_columns] if select_cols else []

    table_str = add_double_quote(model_cls.get_table_name(dic_conditions))  # extract partition value from dic_values

    if dict_aggregate_function:
        select_aggregate_function = gen_select_aggregate_function(dict_aggregate_function)
        for key_alias, value_function in select_aggregate_function.items():
            if key_alias in select_col_names:
                select_col_names[select_col_names.index(key_alias)] = value_function
            else:
                select_col_names.append(value_function)

    select_statement = ','.join(select_col_names) if select_col_names else '*'
    sql = f'SELECT {select_statement} FROM {table_str}'

    params_condition = None
    condition_str = None

    if dic_conditions:
        condition_str, params_condition = _gen_condition_str(model_cls, dic_conditions, is_or_operation)
        condition_str = f'({condition_str})'

    # WHERE
    deleted_at_str = gen_deleted_at_condition(model_cls)
    where_condition = [condition_str, deleted_at_str] if filter_deleted else [condition_str]
    condition_str = ' AND '.join(cond_str for cond_str in where_condition if cond_str)
    if condition_str:
        sql += f' WHERE {condition_str}'

    # ORDER BY
    if dic_order_by:
        order_by_str = None
        if isinstance(dic_order_by, dict):
            order_by_str = ', '.join(
                [f'{col} {order_by_type or OrderBy.ASC.name}' for col, order_by_type in dic_order_by.items()],
            )
        elif isinstance(dic_order_by, list):
            order_by_str = ', '.join(list(dic_order_by))

        if order_by_str:
            sql += f' ORDER BY {order_by_str}'

    # LIMIT
    if limit:
        sql += f' LIMIT {limit}'

    if params_condition:
        params_condition = tuple(params_condition)

    return sql, params_condition


def gen_deleted_at_condition(model_cls):
    deleted_at_col = model_cls.get_deleted_logical_column()
    if not deleted_at_col:
        return ''
    return f' "{deleted_at_col}" IS NULL'


def get_proc_id_from_condition(dic_condition):
    for key, val in dic_condition.items():
        if key in ('self_process_id', 'process_id'):
            return val

    return None


def gen_sql_filter_query(marker, filters, is_oracle=False):
    params = None
    where_clause = ''
    if filters:
        params = ()
        where_clause = 'where'
        for i, filter in enumerate(filters):
            and_condition = ' and ' if i != 0 else ' '
            marker_name = filter[0] if is_oracle else ''
            if isinstance(filter[2], list):
                # multiple filter, eg. between
                where_clause += (
                    f'{and_condition}{filter[0]} {filter[1]} {marker}{marker_name} AND {marker}{marker_name}'
                )
                params += (filter[2][0], filter[2][-1])
            else:
                where_clause += f'{and_condition}{filter[0]} {filter[1]} {marker}{marker_name}'
                params += (filter[2],)
        if is_oracle:
            where_clause += ' and'
    if where_clause == '' and is_oracle:
        where_clause = 'where'

    return where_clause, params


def db_instance_exec(
    db_instance,
    select=None,
    from_table=None,
    limit=None,
    order=None,
    filter=None,
    with_run=True,
    is_count=None,
    row_is_dict=False,
):
    """
    :param row_is_dict:
    :param is_count:
    :param with_run:
    :param filter:
    :param order:
    :param limit:
    :param from_table:
    :param select:
    :param db_instance CfgDataSourceDB
    :filter [(compare_name, compare_character, compare_value)]
    :return cols, rows
    """
    if is_count:
        select = 'COUNT(1)'

    _select = select or '*'
    _order = order or ''
    _from = from_table or None
    _limit = f'limit {limit}' if limit else ''
    if not _from:
        # prevent query without table
        return None, None

    if isinstance(db_instance, mssqlserver.MSSQLServer):
        _limit = f'TOP {limit}' if limit else ''
        _marker = '%s'
        _filter, _params = gen_sql_filter_query(_marker, filter)
        # "select TOP 1 * from table_name order by id"
        sql_query = f'select {_limit} {_select} from "{_from}" {_filter} {_order}'
    elif isinstance(db_instance, oracle.Oracle):
        _marker = ':'
        # "select * from table_name where a=1 and rownum <= limit order by id"
        _filter, _params = gen_sql_filter_query(_marker, filter, is_oracle=True)
        sql_query = f'select {_select} from "{_from}" {_filter} rownum <= {limit} {_order}'
    else:
        _marker = '?'  # sqlite3
        if isinstance(db_instance, (mysql.MySQL, postgresql.PostgreSQL)):
            _marker = '%s'
        _filter, _params = gen_sql_filter_query(_marker, filter)
        if _params and not isinstance(db_instance, mysql.MySQL):
            _params = [convert_time(tm, format_str=DATE_FORMAT_STR_SQLITE) for tm in _params]
        sql_query = f'select {_select} from "{_from}" {_filter} {_order} {_limit}'

    if with_run:
        cols, rows = db_instance.run_sql(sql_query, row_is_dict=row_is_dict, params=_params)
        return cols, rows

    return sql_query, _params


def test_db_exec():
    """
    for test only
    """
    mssql_db = mssqlserver.MSSQLServer('localhost', 'test', 'test', '')
    mysql_db = mysql.MySQL('localhost', 'test', 'test', '')
    oracle_db = oracle.Oracle('localhost', 'test', 'test', '')
    postgres_db = postgresql.PostgreSQL('localhost', 'test', 'test', '')
    sqlite3_db = sqlite.SQLite3('localhost')

    all = [mssql_db, mysql_db, oracle_db, postgres_db, sqlite3_db]
    for db in all:
        query, params = db_instance_exec(
            db,
            select=1,
            from_table='table_name',
            limit=1000,
            order='order by column_name',
            filter=[
                ('col_1', '=', 'val_1'),
                ('col_2', '>=', 'val_2'),
            ],
            test_mode=True,
        )
        print(query)
        print(params)
