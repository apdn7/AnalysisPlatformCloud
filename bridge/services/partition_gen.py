from datetime import datetime

from ap.common.common_utils import DATE_FORMAT_STR_PARTITION_VALUE, add_months
from ap.common.constants import DEFAULT_POSTGRES_SCHEMA
from ap.common.pydn.dblib.postgresql import PostgreSQL
from bridge.models.bridge_station import BridgeStationModel

PARTITION_RATIO = 100
SERVER_ID_LEN = 11


def check_exist_partition(db_instance, partition_name):
    all_table = db_instance.list_tables()
    return partition_name in all_table


def get_server_id(proc_id):
    return str(proc_id)[:SERVER_ID_LEN]


def get_table_partitions(db_instance: PostgreSQL, table_name):
    param_symbol = BridgeStationModel.get_parameter_marker()
    sql = f'''
        SELECT child.relname
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
        JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
        WHERE nmsp_parent.nspname = {param_symbol}
          AND parent.relname = {param_symbol}
    '''
    params = [DEFAULT_POSTGRES_SCHEMA, table_name]
    _, rows = db_instance.run_sql(sql, params=params, row_is_dict=False)

    return [r[0] for r in rows]


def gen_process_partition(db_instance: PostgreSQL, table_name, proc_id):
    idx = get_partition_by_id(proc_id)
    partition_name = gen_partition_table_name(table_name, proc_id)
    if not check_exist_partition(db_instance, partition_name):
        from_proc_id = proc_id // (PARTITION_RATIO * 10) * (PARTITION_RATIO * 10) + idx
        to_proc_id = from_proc_id + (PARTITION_RATIO * 100)
        procs_str = ','.join(str(id) for id in range(from_proc_id, to_proc_id, PARTITION_RATIO))

        sql = f'''
CREATE TABLE IF NOT EXISTS { partition_name } PARTITION OF { table_name } FOR
VALUES
    IN ({ procs_str }) PARTITION BY RANGE ("time")
    '''
        try:
            db_instance.execute_sql(sql)
            db_instance.connection.commit()
        except Exception:
            db_instance.connection.rollback()

    return partition_name


def gen_time_partition(db_instance: PostgreSQL, table_name, proc_id, year=None, month=None, year_month=None):
    if not year or not month:
        year_month = str(year_month)
        year = int(year_month[:4])
        month = int(year_month[4:])
    else:
        year = int(year)
        month = int(month)
        year_month = f'{year}{str(month).zfill(2)}'

    proc_partition_name = gen_partition_table_name(table_name, proc_id)
    partition_name = gen_partition_table_name(table_name, proc_id, year_month)
    if not check_exist_partition(db_instance, partition_name):
        from_date = datetime(year=year, month=month, day=1)
        to_date = add_months(from_date, 1)

        from_date = datetime.strftime(from_date, DATE_FORMAT_STR_PARTITION_VALUE)
        to_date = datetime.strftime(to_date, DATE_FORMAT_STR_PARTITION_VALUE)
        sql = f'''
CREATE TABLE IF NOT EXISTS { partition_name } PARTITION OF { proc_partition_name } FOR
VALUES
FROM
    ('{from_date}') TO ('{to_date}')
'''
        try:
            db_instance.execute_sql(sql)
            db_instance.connection.commit()
        except Exception:
            db_instance.connection.rollback()

    return partition_name


def get_partition_by_id(proc_id):
    return proc_id % PARTITION_RATIO


def gen_partition_table_name(table_name, proc_id, year_month=None):
    server_id = get_server_id(proc_id)
    idx = get_partition_by_id(proc_id)
    partition_name = f'{table_name}_{server_id}_{idx}'
    if year_month:
        partition_name = f'{partition_name}_{year_month}'

    return partition_name
