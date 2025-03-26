from typing import Union

from ap.common.common_utils import check_exist, parse_int_value
from ap.common.constants import DBType
from ap.common.pydn.dblib.mssqlserver import MSSQLServer
from ap.common.pydn.dblib.mysql import MySQL
from ap.common.pydn.dblib.oracle import Oracle
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.pydn.dblib.sqlite import SQLite3
from ap.setting_module.models import CfgDataSource, CfgDataSourceDB
from bridge.common.cryptography_utils import decrypt, decrypt_pwd


class ReadOnlyDbProxy:
    """
    An interface for client to connect to many type of database
    """

    db_instance: Union[PostgreSQL, Oracle, MySQL, MSSQLServer]
    db_type: str
    db_host: str
    db_name: str
    db_schema: str
    db_port: str
    db_user: str
    db_hashed: bool
    db_password: str

    def __init__(self, cfg_data_source_db):
        """
        cfg_data_source_db: CfgDataSourceDB object
        """
        self.db_type = cfg_data_source_db.type
        self.db_host = cfg_data_source_db.db_detail.host
        self.db_name = cfg_data_source_db.db_detail.dbname
        self.db_user = cfg_data_source_db.db_detail.username
        self.db_password = cfg_data_source_db.db_detail.password

        self.db_schema = cfg_data_source_db.db_detail.schema
        self.db_port = cfg_data_source_db.db_detail.port
        self.db_hashed = cfg_data_source_db.db_detail.hashed

    def __enter__(self):
        self.db_instance = self._get_db_instance()
        self.db_instance.connect()
        return self.db_instance

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db_instance.disconnect()
        return False

    def _get_db_instance(self):
        if self.db_type == DBType.POSTGRESQL.name:
            target_db_class = PostgreSQL
        elif self.db_type == DBType.ORACLE.name:
            target_db_class = Oracle
        elif self.db_type == DBType.MYSQL.name:
            target_db_class = MySQL
        elif self.db_type == DBType.MSSQLSERVER.name:
            target_db_class = MSSQLServer
        elif self.db_type == DBType.SOFTWARE_WORKSHOP.name:
            target_db_class = PostgreSQL
        elif self.db_type == DBType.SQLITE.name:
            if check_exist(self.db_name):
                target_db_class = SQLite3
            else:
                return None
        else:
            return None

        if self.db_hashed and self.db_password not in ('', None):
            self.db_password = decrypt_pwd(self.db_password)

        args = {'dbname': self.db_name, 'read_only': True}
        if self.db_type != DBType.SQLITE.name:
            args.update(
                {
                    'dbname': self.db_name,
                    'host': self.db_host,
                    'username': self.db_user,
                    'password': self.db_password,
                    'port': self.db_port,
                    'read_only': True,
                },
            )

        db_instance = target_db_class(**args)

        if self.db_schema:
            db_instance.schema = self.db_schema

        return db_instance


def check_db_con(id, db_type, host, port, dbname, schema, username, password):
    parsed_int_port = parse_int_value(port)
    if parsed_int_port is None and db_type.lower() != DBType.SQLITE.name.lower():
        return False
    datasource = CfgDataSourceDB.query.get(id) if id else None
    same_pwd = password == datasource.password if datasource else False
    if same_pwd and datasource.hashed:
        password = decrypt(password).decode()
    # 　オブジェクトを初期化する
    db_source_detail = CfgDataSourceDB()
    db_source_detail.host = host
    db_source_detail.port = parsed_int_port
    db_source_detail.dbname = dbname
    db_source_detail.schema = schema
    db_source_detail.username = username
    db_source_detail.password = password

    db_source = CfgDataSource()
    db_source.db_detail = db_source_detail
    if db_type.lower() == DBType.SOFTWARE_WORKSHOP.name.lower():
        db_type = DBType.POSTGRESQL.name

    db_source.type = db_type

    # 戻り値の初期化
    result = False

    # コネクションをチェックする
    try:
        with ReadOnlyDbProxy(db_source) as db_instance:
            if db_instance.is_connected:
                result = True

        if same_pwd and not result:
            result = False
    except Exception:
        pass

    return result
