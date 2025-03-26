from datetime import datetime
from typing import Union

from ap import dic_config
from ap.common.common_utils import add_seconds
from ap.common.constants import (
    DEFAULT_POSTGRES_SCHEMA,
    MSG_DB_CON_FAILED,
    MSG_NOT_SUPPORT_DB,
    UNIVERSAL_DB_FILE,
    DBType,
)
from ap.common.logger import logger
from ap.common.pydn.dblib import sqlite
from ap.common.pydn.dblib.mssqlserver import MSSQLServer
from ap.common.pydn.dblib.mysql import MySQL
from ap.common.pydn.dblib.oracle import Oracle
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.pydn.dblib.sqlite import SQLite3
from ap.setting_module.models import CfgDataSource, CfgDataSourceDB
from config import get_db_mode


class DbProxy:
    """
    An interface for client to connect to many type of database
    """

    db_instance: Union[SQLite3, None, PostgreSQL, Oracle, MySQL, MSSQLServer]
    db_basic: CfgDataSource
    db_detail: CfgDataSourceDB
    dic_last_connect_failed_time = {}

    def __init__(self, data_src, immediate_isolation_level=False, force_connect=False):
        self.isolation_level = immediate_isolation_level
        self.force_connect = force_connect
        self.data_src = data_src
        if isinstance(data_src, CfgDataSource):
            self.db_basic = data_src
            self.db_detail = data_src.db_detail
        elif isinstance(data_src, CfgDataSourceDB):
            self.db_basic = data_src.cfg_data_source
            self.db_detail = data_src
        else:
            self.db_basic = CfgDataSource.query.get(data_src)
            self.db_detail = self.db_basic.db_detail

    def __enter__(self):
        if not self.force_connect:
            last_failed_time = DbProxy.dic_last_connect_failed_time.get(self.db_basic.id)
            if last_failed_time and last_failed_time > add_seconds(seconds=-60):
                raise Exception(MSG_DB_CON_FAILED)

        self.db_instance = self._get_db_instance()
        conn = self.db_instance.connect()
        if conn in (None, False):
            DbProxy.dic_last_connect_failed_time[self.db_basic.id] = datetime.utcnow()
            raise Exception(MSG_DB_CON_FAILED)
        return self.db_instance

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        try:
            if _exc_type:
                self.db_instance.connection.rollback()
            else:
                self.db_instance.connection.commit()

        except Exception as e:
            if self.db_instance.connection is not None:
                self.db_instance.connection.rollback()
            logger.exception(e)
            raise e
        finally:
            self.db_instance.disconnect()
        return False

    def _get_db_instance(self):
        db_type = self.db_basic.type.lower()
        if db_type == DBType.SQLITE.value.lower():
            return sqlite.SQLite3(self.db_detail.dbname, isolation_level=self.isolation_level)

        if db_type == DBType.POSTGRESQL.value.lower():
            target_db_class = PostgreSQL
        elif db_type == DBType.ORACLE.value.lower():
            target_db_class = Oracle
        elif db_type == DBType.MYSQL.value.lower():
            target_db_class = MySQL
        elif db_type == DBType.MSSQLSERVER.value.lower():
            target_db_class = MSSQLServer
        elif db_type == DBType.SOFTWARE_WORKSHOP.value.lower():
            target_db_class = PostgreSQL
        else:
            raise Exception(MSG_NOT_SUPPORT_DB)

        db_instance = target_db_class(
            self.db_detail.host,
            self.db_detail.dbname,
            self.db_detail.username,
            self.db_detail.get_password(True),
        )

        # use custom port or default port
        if self.db_detail.port:
            db_instance.port = self.db_detail.port

        if self.db_detail.schema:
            db_instance.schema = self.db_detail.schema

        return db_instance


def gen_data_source_of_universal_db():
    """
    create data source cfg object that point to our application universal db
    :return:
    """
    db_src = CfgDataSource()
    db_detail = CfgDataSourceDB()

    db_src.type = DBType.SQLITE.name
    db_src.db_detail = db_detail

    db_detail.dbname = dic_config[UNIVERSAL_DB_FILE]

    return db_src


def gen_data_source_of_bridge_webpage():
    """
    create data source cfg object that point to our application universal db
    :return:
    """
    db_src = CfgDataSource()
    db_detail = CfgDataSourceDB()

    db_mode = get_db_mode()
    db_detail.dbname = db_mode.dbname
    db_detail.host = db_mode.host
    db_detail.port = db_mode.port
    db_detail.username = db_mode.username
    db_detail.password = db_mode.password
    db_detail.hashed = False
    db_detail.schema = DEFAULT_POSTGRES_SCHEMA

    db_src.type = DBType.POSTGRESQL.name
    db_src.db_detail = db_detail

    return db_src


def get_db_proxy():
    """
    Connect to db.session.
    Use when need handle by raw sql
    """

    return DbProxy(gen_data_source_of_bridge_webpage())
