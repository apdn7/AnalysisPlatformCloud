#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Author: Masato Yasuda (2018/01/04)
# Copy from analysis_interface, sprint 83 commit d2da6c71305fba1d8f867cffacbcdf46874577aa (2021/02/10)

import select

import psycopg2
import psycopg2.extras
from apscheduler.schedulers.base import STATE_STOPPED
from psycopg2.extras import execute_values
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import Select

from ap.common.common_utils import strip_all_quote
from ap.common.logger import log_execution_time, logger


class PostgreSQL:
    def __init__(self, host, dbname, username, password, port=5432, read_only=False):
        self.host = host
        self.port = port
        self.dbname = dbname
        # postgresqlはdbの下にschemaという概念がある。
        # http://d.hatena.ne.jp/sheeg/20070906/1189083744
        self.schema = None
        self.username = username
        self.password = password
        self.is_connected = False
        self.connection = None
        self.read_only = read_only

    def dump(self):
        print('===== DUMP RESULT =====')
        print('DB Type: PostgreSQL')
        print('self.host: ' + self.host)
        print('self.port: ' + str(self.port))
        print('self.dbname: ' + self.dbname)
        print('self.username: ' + self.username)
        print('self.is_connected: ', self.is_connected)
        print('=======================')

    def connect(self):
        dsn = 'host={} '.format(self.host)
        dsn += 'port={} '.format(self.port)
        dsn += 'dbname={} '.format(self.dbname)
        dsn += 'user={} '.format(self.username)
        dsn += 'password={}'.format(self.password)
        options = f'-c search_path={self.schema}' if self.schema else None
        try:
            self.connection = psycopg2.connect(dsn, options=options)
            self.connection.set_session(readonly=self.read_only)
            cur = self.connection.cursor()
            self.is_connected = True

            if self.schema:
                cur.execute(
                    f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{self.schema}';",
                )
                if cur.rowcount:
                    cur.execute(f"SET search_path TO '{self.schema}'")
                else:
                    logger.info('Schema is not exists!!!')
                    self.disconnect()
            else:
                # Get current schema
                cur.execute('SELECT current_schema();')
                default_schema = cur.fetchone()
                # Save default schema as constant, use to list current schema's tables
                self.schema = default_schema[0]
            cur.close()
            return self.connection
        except Exception as e:
            logger.exception(e)
            return False

    def disconnect(self):
        if not self._check_connection():
            return False
        # https://stackoverflow.com/questions/1281875/
        # making-sure-that-psycopg2-database-connection-alive
        self.connection.close()
        self.is_connected = False

    def create_table(self, tblname, colnames):
        if not self._check_connection():
            return False
        sql = 'create table {0:s}('.format(tblname)
        for idx, val in enumerate(colnames):
            if idx > 0:
                sql += ','
            sql += val['name'] + ' ' + val['type']
        sql += ')'
        logger.info(sql)
        cur = self.connection.cursor()
        cur.execute(sql)
        cur.close()
        self.connection.commit()
        logger.info(tblname + ' created!')

    # テーブル名を配列として返す
    def list_tables(self):
        if not self._check_connection():
            return False
        sql = 'select table_name from information_schema.tables '
        sql += "where table_type = 'BASE TABLE' and table_schema = '{0:s}'".format(self.schema)
        cur = self.connection.cursor()
        cur.execute(sql)
        # cursor.descriptionはcolumnの配列
        # そこから配列名(column[0])を取り出して配列columnsに代入
        cols = [column[0] for column in cur.description]
        # columnsは取得したカラム名、rowはcolumnsをKeyとして持つ辞書型の配列
        # rowは取得したカラムに対応する値が順番にrow[0], row[1], ...として入っている
        # それをdictでまとめてrowsに取得
        rows = []
        for row in cur.fetchall():
            rows.append(dict(zip(cols, row)))
        cur.close()
        # キーに"table_name"を持つ要素を配列として返す
        return [row['table_name'] for row in rows]

    def list_tables_and_views(self):
        if not self._check_connection():
            return False
        sql = 'select table_name from information_schema.tables '
        sql += "where table_schema = '{0:s}'".format(self.schema)
        cur = self.connection.cursor()
        cur.execute(sql)
        # cursor.descriptionはcolumnの配列
        # そこから配列名(column[0])を取り出して配列columnsに代入
        cols = [column[0] for column in cur.description]
        # columnsは取得したカラム名、rowはcolumnsをKeyとして持つ辞書型の配列
        # rowは取得したカラムに対応する値が順番にrow[0], row[1], ...として入っている
        # それをdictでまとめてrowsに取得
        rows = []
        for row in cur.fetchall():
            rows.append(dict(zip(cols, row)))
        cur.close()
        # キーに"table_name"を持つ要素を配列として返す
        return [row['table_name'] for row in rows]

    def drop_table(self, tblname):
        if not self._check_connection():
            return False
        sql = 'drop table if exists ' + tblname
        cur = self.connection.cursor()
        cur.execute(sql)
        cur.close()
        self.connection.commit()
        print(tblname + ' dropped!')

    def list_table_columns(self, tblname):
        if not self._check_connection():
            return False

        sql = 'select * from information_schema.columns '
        sql += "where table_schema = '{0}' and table_name = '{1}'".format(self.schema, tblname)
        cur = self.connection.cursor()
        cur.execute(sql)
        # cursor.descriptionはcolumnの配列
        # そこから配列名(column[0])を取り出して配列columnsに代入
        cols = [column[0] for column in cur.description]
        # columnsは取得したカラム名、rowはcolumnsをKeyとして持つ辞書型の配列
        # rowは取得したカラムに対応する値が順番にrow[0], row[1], ...として入っている
        # それをdictでまとめてrowsに取得
        rows = []
        for row in cur.fetchall():
            rows.append(dict(zip(cols, row)))
        cur.close()
        results = []

        for row in rows:
            results.append({'name': row['column_name'], 'type': row['data_type']})
        return results

    def get_data_type_by_colname(self, tbl, col_name):
        col_name = strip_all_quote(col_name)
        cols = self.list_table_columns(tbl)
        data_type = [col['type'] for col in cols if col['name'] == col_name]
        return data_type[0] if data_type else None

    # list_table_columnsのうちcolumn nameだけ必要な場合
    def list_table_colnames(self, tblname):
        if not self._check_connection():
            return False
        columns = self.list_table_columns(tblname)
        colnames = []
        for column in columns:
            colnames.append(column['name'])
        return colnames

    # 元はinsert_table関数
    def insert_table_records(self, tblname, names, values, add_comma_to_value=True):
        if not self._check_connection():
            return False

        sql = 'insert into {0:s}'.format(tblname)

        # Generate column names fields
        sql += '('
        for idx, name in enumerate(names):
            if idx > 0:
                sql += ','
            sql += name
        sql += ') '

        # Generate values field
        if not values:
            return False

        sql += 'values '
        for idx1, value in enumerate(values):
            if idx1 > 0:
                sql += ','
            sql += '('
            for idx2, name in enumerate(names):
                if idx2 > 0:
                    sql += ','

                if value[name] in ('', None):
                    sql += 'Null'
                elif add_comma_to_value:
                    sql += "'" + str(value[name]) + "'"
                else:
                    sql += str(value[name])

            sql += ')'

        # print(sql)
        cur = self.connection.cursor()
        cur.execute(sql)
        cur.close()
        self.connection.commit()
        logger.info('Dummy data was inserted to {}!'.format(tblname))

    # SQLをそのまま実行
    # colsとdict形式のrowsを返す
    # cols, rows = db1.run_sql("select * from tbl01")
    # という形で呼び出す
    @log_execution_time(prefix='POSTGRES')
    def run_sql(self, sql, row_is_dict=True, params=None):
        if not self._check_connection():
            return False, None
        cur = self.connection.cursor()
        # https://stackoverflow.com/questions/10252247/
        # how-do-i-get-a-list-of-column-names-from-a-psycopg2-cursor
        # カラム名がRenameされた場合も対応出来る形に処理を変更

        logger.debug(sql)
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        # cursor.descriptionはcolumnの配列
        # そこから配列名(column[0])を取り出して配列columnsに代入
        if not cur.description:
            return [], []

        cols = [column[0] for column in cur.description]
        # columnsは取得したカラム名、rowはcolumnsをKeyとして持つ辞書型の配列
        # rowは取得したカラムに対応する値が順番にrow[0], row[1], ...として入っている
        # それをdictでまとめてrowsに取得
        rows = [dict(zip(cols, row)) for row in cur.fetchall()] if row_is_dict else cur.fetchall()

        cur.close()
        return cols, rows

    def fetch_many_by_condition(self, tblname, condition_columns, condition_values, select_cols=None, size=10_000):
        if select_cols is None:
            select_cols = '*'
        sql = 'SELECT {} FROM {} WHERE '.format(','.join(select_cols), tblname)

        # Generate condition (default: AND)
        sql += ' and '.join(['{} = {}'.format(name, value) for name, value in zip(condition_columns, condition_values)])

        yield from self.fetch_many(sql, size)

    @log_execution_time(prefix='POSTGRES')
    def fetch_many(self, sql, size=10_000, params=None):
        if not self._check_connection():
            return False

        logger.debug(sql)
        with self.connection.cursor() as cur:
            if params:
                logger.debug(params)
                cur.execute(sql, params)
            else:
                cur.execute(sql)

            cols = [column[0] for column in cur.description]
            yield cols
            while True:
                rows = cur.fetchmany(size)
                if not rows:
                    break

                yield rows

    @log_execution_time(prefix='POSTGRES')
    def execute_sql(self, sql, params=None):
        """For executing any query requires commit action
        :param sql: SQL to be executed
        :param params:
        :return: Execution result
        """
        if not self._check_connection():
            return False
        logger.debug(sql)
        cur = self.connection.cursor()

        if params is None:
            res = cur.execute(sql)
        else:
            logger.debug(params)
            res = cur.execute(sql, params)
        cur.close()

        return res

    # 現時点ではSQLをそのまま実行するだけ
    def select_table(self, sql):
        return self.run_sql(sql)

    def get_timezone(self):
        try:
            _, rows = self.run_sql('show timezone')
            tz_offset = str(rows[0]['TimeZone'])
            return tz_offset
        except Exception as e:
            logger.info(e)
            return None

    # private functions
    def _check_connection(self):
        if self.is_connected:
            return True
        # 接続していないなら
        if self.connect():
            return True

        logger.exception('Connection is not Initialized. Please run connect() to connect to DB')
        return False

    def is_timezone_hold_column(self, tbl, col):
        data_type = self.get_data_type_by_colname(tbl, col)

        if 'WITH TIME ZONE' in data_type.upper():
            return True

        return False

    @log_execution_time(prefix='POSTGRES')
    def bulk_insert(self, tblname, columns, rows, parameter_marker=None):
        """
        Insert bulk data to db (best performance )
        At end of your logic. Make sure you call
            PostgresSequence.set_last_id_by_table_name(db_instance, tblname, max_id)
        to update latest id to next sequence
        :param tblname:
        :param columns:
        :param rows:
        :param parameter_marker: BridgeStationModel.get_parameter_marker()
        :return:
        """
        if not self._check_connection():
            return False

        cols = ','.join([f'"{col}"' for col in columns])
        # params = ','.join(['%s'] * len(columns))
        if not parameter_marker:
            parameter_marker = '%s'
        sql = f'INSERT INTO {tblname} ({cols}) VALUES {parameter_marker}'

        cur = self.connection.cursor()
        # cur.executemany(sql, rows)
        logger.debug(sql)
        execute_values(cur, sql, rows)
        cur.close()

        return True

    def chanel_listen(self, sche):
        self.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = self.connection.cursor()
        cur.execute('LISTEN ap;')

        while True:
            if sche.state == STATE_STOPPED:
                break

            select.select([self.connection], [], [], 10)
            self.connection.poll()
            while self.connection.notifies:
                notification = self.connection.notifies.pop()
                yield notification

        cur.close()
        yield None

    @staticmethod
    def gen_sql_and_params(stmt: Select) -> tuple[str, dict[str, str]]:
        compiled_stmt = stmt.compile(dialect=postgresql.dialect())
        return compiled_stmt.string, compiled_stmt.params
