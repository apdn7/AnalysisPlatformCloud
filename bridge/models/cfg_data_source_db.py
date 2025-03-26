import os
from typing import Dict, Tuple

from ap.common.constants import DataType, Environment, MasterDBType, ServerType
from ap.common.pydn.dblib.db_common import gen_delete_sql, gen_select_by_condition_sql
from bridge.common.server_config import ServerConfig
from bridge.models.bridge_station import ConfigModel
from bridge.models.cfg_data_source import CfgDataSource
from bridge.models.model_utils import TableColumn


class CfgDataSourceDB(ConfigModel):
    class Columns(TableColumn):
        id = (1, DataType.INTEGER)  # type: CfgDataSource.Columns.id
        host = (2, DataType.TEXT)
        port = (3, DataType.INTEGER)
        dbname = (4, DataType.TEXT)
        schema = (5, DataType.TEXT)
        username = (6, DataType.TEXT)
        password = (7, DataType.TEXT)
        hashed = (8, DataType.BOOLEAN)
        use_os_timezone = (9, DataType.BOOLEAN)
        master_type = (10, DataType.TEXT)  # MasterDBType Enum
        created_at = (11, DataType.DATETIME)
        updated_at = (12, DataType.DATETIME)

    _table_name = 'cfg_data_source_db'
    primary_keys = [Columns.id]

    cfg_data_source: CfgDataSource = None
    localhost_alias = {
        ServerType.BridgeStationGrpc: 'host.docker.internal',
        ServerType.BridgeStationWeb: 'host.docker.internal',
        ServerType.EdgeServer: 'localhost',
    }

    def __init__(self, dict_db_source):
        self.id = dict_db_source.get(CfgDataSourceDB.Columns.id.name)
        if self.id is None:
            del self.id
        self.host = dict_db_source.get(CfgDataSourceDB.Columns.host.name)
        self.port = dict_db_source.get(CfgDataSourceDB.Columns.port.name)
        self.dbname = dict_db_source.get(CfgDataSourceDB.Columns.dbname.name)
        self.schema = dict_db_source.get(CfgDataSourceDB.Columns.schema.name)
        self.username = dict_db_source.get(CfgDataSourceDB.Columns.username.name)
        self.password = dict_db_source.get(CfgDataSourceDB.Columns.password.name)
        self.hashed = dict_db_source.get(CfgDataSourceDB.Columns.hashed.name)
        self.use_os_timezone = dict_db_source.get(CfgDataSourceDB.Columns.use_os_timezone.name)
        self.master_type = dict_db_source.get(CfgDataSourceDB.Columns.master_type.name)
        self.created_at = dict_db_source.get(CfgDataSourceDB.Columns.created_at.name)
        self.updated_at = dict_db_source.get(CfgDataSourceDB.Columns.updated_at.name)

    def __hash__(self):
        # Not all columns is ok
        return hash((self.id, self.host, self.port, self.dbname, self.schema, self.username))

    @classmethod
    def get_by_master_type(cls, db_instance, master_type: MasterDBType):
        dict_condition = {CfgDataSourceDB.Columns.master_type.name: str(master_type)}
        sql, params = gen_select_by_condition_sql(cls, dict_condition)
        _col, rows = db_instance.run_sql(sql, params=params)
        if not rows:
            return None
        cfg_data_source_db = CfgDataSourceDB(rows[0])

        dict_condition = {CfgDataSource.Columns.id.name: cfg_data_source_db.id}
        sql, params = gen_select_by_condition_sql(cls, dict_condition)
        _col, rows = db_instance.run_sql(sql, params=params)
        if rows:
            cfg_data_source_db.cfg_data_source = CfgDataSource(rows[0])

        return cfg_data_source_db

    @classmethod
    def get_by_id(cls, db_instance, db_source_id: int):
        dict_condition = {CfgDataSourceDB.Columns.id.name: db_source_id}
        sql, params = gen_select_by_condition_sql(cls, dict_condition)
        _col, rows = db_instance.run_sql(sql, params=params)
        if not rows:
            return None
        cfg_data_source_db = CfgDataSourceDB(rows[0])

        dict_condition = {CfgDataSource.Columns.id.name: cfg_data_source_db.id}
        sql, params = gen_select_by_condition_sql(CfgDataSource, dict_condition)
        _col, rows = db_instance.run_sql(sql, params=params)
        if rows:
            cfg_data_source_db.cfg_data_source = CfgDataSource(rows[0], db_instance=db_instance)

        return cfg_data_source_db

    @classmethod
    def delete_by_master_type(cls, db_instance, master_type: str):
        dict_condition = {CfgDataSourceDB.Columns.master_type.name: master_type}
        sql, params = gen_delete_sql(cls, dict_condition)
        db_instance.execute_sql(sql, params=params)
        return True

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
        env = os.environ.get('BRIDGE_STATION_ENV', 'DEV')
        if env in (Environment.TEST.name, Environment.DEV.name):
            dict_ret = cls.convert_localhost_alias(dict_ret)
        return dict_ret

    @classmethod
    def convert_localhost_alias(cls, dict_record):
        localhost = cls.localhost_alias[ServerConfig.get_server_type()]
        if dict_record[cls.Columns.host.name] in list(cls.localhost_alias.values()):
            dict_record[cls.Columns.host.name] = localhost
        return dict_record
