import os
from typing import Dict, Tuple

from ap.common.logger import logger

# Note: Do not use BridgeStationModel in this file


class ServerConfig:
    """
    Manages cross environment objects
        - Bridge dev
        - Bridge test
        - Edge dev
        - Edge test
    """

    SQLITE_CONFIG_DIR = 'SQLITE_CONFIG_DIR'
    METADATA_FILE = 'METADATA_FILE'
    METADATA_FILE_NAME = 'edge_db_metadata.pickle'  # Edge Server's meta data file name
    LOCK = 'lock'
    DB_PROXY = 'db_proxy'
    PARTITION_NUMBER = 'PARTITION_NUMBER'
    SERVER_TYPE = 'SERVER_TYPE'
    DISK_USAGE_CHECK = 'DISK_USAGE_CHECK'
    REDIS_HOST = 'redis_host'
    REDIS_PORT = 'redis_port'
    BROWSER_MODE = 'browser_mode'

    # app config. See config/database_dev.ini
    POSTGRES_MOUNT_DIR = 'postgres_mounted_on_dir'

    # shadow of ap.dic_config, and more
    dic_config = {
        LOCK: None,
        DB_PROXY: None,
        PARTITION_NUMBER: None,
        SERVER_TYPE: None,
        METADATA_FILE: None,
        DISK_USAGE_CHECK: (),
        REDIS_HOST: None,
        REDIS_PORT: None,
        BROWSER_MODE: None,
    }

    is_main_thread = False
    current_app = None

    @classmethod
    def set_server_config(cls, dic_config: Dict = None):
        if dic_config:
            cls.dic_config.update(dic_config)
        instance_dir = cls.dic_config.get(cls.SQLITE_CONFIG_DIR, os.path.join(os.getcwd(), 'instance'))
        cls.dic_config[cls.METADATA_FILE] = os.path.join(instance_dir, cls.METADATA_FILE_NAME)

    @classmethod
    def get_db_proxy(cls):
        db_proxy_func = cls.dic_config.get(ServerConfig.DB_PROXY, None)
        if db_proxy_func:
            return db_proxy_func()
        sample = 'ServerConfig.set_server_config(dic_config={ServerConfig.DB_PROXY: DbProxy})'
        raise Exception(f'DbProxy was not set. Sample: {sample}')

    @classmethod
    def get_partition_number(cls) -> int:
        return cls.dic_config.get(ServerConfig.PARTITION_NUMBER, None)

    @classmethod
    def get_lock(cls):
        return cls.dic_config.get(ServerConfig.LOCK, None)

    @classmethod
    def get_server_type(cls):
        return cls.dic_config.get(ServerConfig.SERVER_TYPE, None)

    @classmethod
    def get_postgres_mounted_dir(cls) -> str:
        return cls.dic_config.get(ServerConfig.POSTGRES_MOUNT_DIR, None)

    @classmethod
    def get_disk_usage_rule(cls) -> Tuple:
        return cls.dic_config.get(ServerConfig.DISK_USAGE_CHECK, ())

    @classmethod
    def get_redis_server(cls):
        if 'redis' not in cls.dic_config:
            logger.info('Missing redis config. See main: ServerConfig.set_server_config(dic_config)')
            return None, None
        redis_host = cls.dic_config['redis'].get(ServerConfig.REDIS_HOST, 'localhost')
        redis_port = cls.dic_config['redis'].get(ServerConfig.REDIS_PORT, 6379)
        logger.info(f'redis_host:{redis_host}')
        logger.info(f'redis_port:{redis_port}')
        return redis_host, redis_port

    @classmethod
    def get_browser_debug(cls):
        return cls.dic_config.get(ServerConfig.BROWSER_MODE, False)
