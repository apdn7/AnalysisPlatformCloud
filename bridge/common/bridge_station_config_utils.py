from ap.common.constants import (
    HALF_WIDTH_SPACE,
    MAX_RESERVED_NAME_ID,
    CfgConstantType,
    DiskUsageStatus,
    ServerType,
)
from ap.setting_module.models import CfgConstant, MDataGroup, make_session
from bridge.common.epoch_base_time_utils import generate_server_id
from bridge.common.server_config import ServerConfig
from bridge.models.bridge_station import BridgeStationModel

DEFAULT_WARNING_DISK_USAGE = 80
DEFAULT_ERROR_DISK_USAGE = 90


class PostgresSequence:
    @classmethod
    def set_sequence_latest_id(cls, db_instance, next_id: int = None, dict_table_with_latest_id: dict = None):
        order_by_sql = 'ORDER BY "sequencename"'
        where = f"WHERE schemaname = '{db_instance.schema}'"
        select_sequences_sql = 'SELECT "sequencename", "data_type" FROM "pg_sequences"'
        sql = HALF_WIDTH_SPACE.join([select_sequences_sql, where, order_by_sql])
        _cols, rows = db_instance.run_sql(sql)
        for records in rows:
            pm = BridgeStationModel.get_parameter_marker()
            sequence_name = records['sequencename']
            table_name = sequence_name.replace('_id_seq', '')
            update_sql = f'select setval({pm}, {pm}, true)'  # true or false is not important

            db_latest_id = (
                cls.get_db_latest_id(db_instance, table_name) if cls.is_table_exist(db_instance, table_name) else None
            )
            sequence_latest_id = cls.get_sequence_latest_id_by_sequence_name(db_instance, sequence_name)
            latest_id = dict_table_with_latest_id.get(sequence_name, next_id) if dict_table_with_latest_id else next_id

            if sequence_latest_id and (not latest_id or sequence_latest_id > latest_id):
                latest_id = sequence_latest_id
            if sequence_name == f'{MDataGroup.get_table_name()}_id_seq' and (
                not latest_id or latest_id < MAX_RESERVED_NAME_ID
            ):
                latest_id = MAX_RESERVED_NAME_ID
            if db_latest_id and (not latest_id or db_latest_id > latest_id):
                latest_id = db_latest_id

            params = (sequence_name, latest_id)
            _, _ = db_instance.run_sql(update_sql, params=params)

    @classmethod
    def get_sequence_latest_id_by_sequence_name(cls, db_instance, seq_name):
        where_sql = f"WHERE sequencename = '{seq_name}' AND \"schemaname\" = '{db_instance.schema}'"
        limit_sql = 'LIMIT 1'
        select_sequences_sql = 'SELECT "last_value" FROM "pg_sequences" '
        sql = HALF_WIDTH_SPACE.join([select_sequences_sql, where_sql, limit_sql])
        _cols, rows = db_instance.run_sql(sql, row_is_dict=False)
        return rows[0][0] if rows else None

    @classmethod
    def set_sequence_latest_id_by_table_name(cls, db_instance, table_name, latest_id):
        """
        Supports db_instance.bulk_insert
        Update latest id to sequence

        :param db_instance:
        :param table_name:
        :param latest_id:
        :return:
        """
        sequence_name = f'{table_name}_id_seq'
        sql = f"SELECT setval('{sequence_name}', {latest_id}, TRUE);"
        return db_instance.execute_sql(sql)

    @classmethod
    def get_db_latest_id(cls, db_instance, table_name):
        sql = f'select max("id") from "{table_name}"'
        cols, rows = db_instance.run_sql(sql)
        return list(rows[0].values())[0] or None

    @classmethod
    def set_max_sequence_id(cls, db_instance, table_name):
        """
        Update latest id base on max id in table to sequence

        :param db_instance:
        :param table_name:
        :return:
        """
        latest_id = cls.get_db_latest_id(db_instance, table_name)
        if latest_id:
            if table_name == MDataGroup.get_table_name() and (not latest_id or latest_id < MAX_RESERVED_NAME_ID):
                latest_id = MAX_RESERVED_NAME_ID
            return cls.set_sequence_latest_id_by_table_name(db_instance, table_name, latest_id)

        return None

    @classmethod
    def is_table_exist(cls, db_instance, table_name):
        sql = f'''SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{db_instance.schema}'
            AND table_name = '{table_name}'
        )'''
        cols, rows = db_instance.run_sql(sql)
        return list(rows[0].values())[0]

    @classmethod
    def get_next_id_by_table(cls, db_instance, table_name, step=None):
        if not step:
            step = 1

        max_id = cls.get_db_latest_id(db_instance, table_name) or 0
        sequence_name = f'{table_name}_id_seq'
        latest_db_id = cls.get_sequence_latest_id_by_sequence_name(db_instance, sequence_name) or 0

        latest_db_id = max_id if max_id > latest_db_id else latest_db_id
        next_val = latest_db_id + 1

        result = list(range(next_val, next_val + step))
        new_latest_db_id = max(result)
        cls.set_sequence_latest_id_by_table_name(db_instance, table_name, new_latest_db_id)

        return result


class ServerInfo:
    """
    Manages id
    """

    # bridge station config constant name
    EDGE_SERVER_ID = 'EDGE_SERVER_ID'
    BRIDGE_SERVER_ID = 'BRIDGE_SERVER_ID'
    INTEGRATION_SERVER_ID = 'INTEGRATION_SERVER_ID'
    BRIDGE_SERVER_WEBPAGE_ID = 'BRIDGE_SERVER_WEBPAGE_ID'

    _constant_name_table = {
        ServerType.EdgeServer: 'EDGE_SERVER_ID',
        ServerType.StandAlone: 'EDGE_SERVER_ID',
        ServerType.BridgeStationGrpc: 'BRIDGE_SERVER_ID',
        ServerType.BridgeStationWeb: 'BRIDGE_SERVER_WEBPAGE_ID',
        ServerType.IntegrationServer: 'INTEGRATION_SERVER_ID',
    }

    _server_id = None
    _warning_disk_usage = None
    _error_disk_usage = None

    @classmethod
    def get_server_id_config(cls) -> int:
        if not cls._server_id:
            constant_name = cls._constant_name_table[ServerConfig.get_server_type()]
            cls._server_id = CfgConstant.get_value_by_type_name(
                CfgConstantType.BRIDGE_STATION_CONFIG,
                constant_name,
                parse_val=int,
            )
        return cls._server_id

    @classmethod
    def get_error_disk_usage(cls) -> int:
        if not cls._error_disk_usage:
            with BridgeStationModel.get_db_proxy() as db_instance:
                cls._error_disk_usage = CfgConstant.get_value_by_type_name(
                    db_instance,
                    CfgConstantType.DISK_USAGE_CONFIG,
                    DiskUsageStatus.Full,
                    parse_val=int,
                )
        return cls._error_disk_usage

    @classmethod
    def initialize(cls):
        """
        Initialize edge id, server id if not exist
        :return:
        """
        cls._server_id = cls.get_server_id_config()
        if not cls._server_id:
            from ap.common.common_utils import get_data_path
            from ap.common.services.import_export_config_and_master_data import (
                delete_file_and_folder_by_path,
            )

            cls.initialize_server_id()
            data_path = get_data_path()
            delete_file_and_folder_by_path(data_path, ignore_folder='preview')

        cls.initialize_disk_usage_limit()
        if not cls._server_id:
            raise Exception('Can not initialize server id')

    @classmethod
    def initialize_server_id(cls):
        with BridgeStationModel.get_db_proxy() as db_instance:
            server_type = ServerConfig.get_server_type()
            cls._server_id = generate_server_id(server_type)

            # default_id_setter may be PostgresSequence or SqliteSequence. depend on server_type
            # default_id_setter = PostgresSequence if server_type in (
            #     ServerType.BridgeStationGrpc, ServerType.BridgeStationWeb) else SqliteSequence
            PostgresSequence.set_sequence_latest_id(db_instance)

            constant_name = cls._constant_name_table[server_type]
        with make_session() as session:
            CfgConstant.create_or_update_by_type(
                session,
                CfgConstantType.BRIDGE_STATION_CONFIG,
                cls._server_id,
                constant_name,
            )

    @classmethod
    def initialize_disk_usage_limit(cls):
        """
        Sets default disk usage limit constants.
            - Warning: 80% (No terminate jobs)
            - Error: 90% (Terminate jobs)

        :param db_instance:
        :return:
        """
        constants_type = CfgConstantType.DISK_USAGE_CONFIG
        warning_percent = CfgConstant.get_warning_disk_usage()
        if not warning_percent:  # insert of not existing
            warning_percent = DEFAULT_WARNING_DISK_USAGE
            with make_session() as session:
                CfgConstant.create_or_update_by_type(
                    session,
                    constants_type,
                    warning_percent,
                    const_name=DiskUsageStatus.Warning,
                )

        error_percent = CfgConstant.get_error_disk_usage()
        if not error_percent:  # insert of not existing
            error_percent = DEFAULT_ERROR_DISK_USAGE
            with make_session() as session:
                CfgConstant.create_or_update_by_type(
                    session,
                    constants_type,
                    error_percent,
                    const_name=DiskUsageStatus.Full,
                )
