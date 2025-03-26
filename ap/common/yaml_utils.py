import os
import traceback
from collections import OrderedDict
from typing import Optional

from ruamel import yaml
from ruamel.yaml import add_constructor, resolver

from ap.common.common_utils import (
    check_exist,
    convert_json_to_ordered_dict,
    detect_encoding,
    dict_deep_merge,
    init_config,
)
from ap.common.constants import AP_TILE, DN7_TILE, SEARCH_USAGE, TILE_JUMP_CFG, TILE_MASTER, ServerType

# yaml config files name
YAML_CONFIG_BASIC_FILE_NAME = 'basic_config.yml'
YAML_CONFIG_DB_FILE_NAME = 'db_config.yml'
YAML_CONFIG_PROC_FILE_NAME = 'proc_config.yml'
YAML_CONFIG_AP_FILE_NAME = 'ap_config.yml'
YAML_TILE_MASTER = 'tile_master.yml'
YAML_TILE_INTERFACE_DN7 = 'tile_interface_dn7.yml'
YAML_TILE_INTERFACE_AP = 'tile_interface_analysis_platform.yml'
YAML_TILE_INTERFACE_USAGE = 'tile_interface_search_by_use.yml'
YAML_TILE_JUMP = 'tile_interface_jump.yml'
YAML_START_UP_FILE_NAME = 'startup.yaml'


# class Singleton(type):
#     """ Metaclass that creates a Singleton base type when called.  """
#
#     def __call__(cls, *args, **kwargs):
#         try:
#             instances = g.setdefault(FlaskGKey.YAML_CONFIG, {})
#         except Exception:
#             # for unit test
#             instances = {}
#
#         if cls not in instances:
#             instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
#
#         return instances[cls]


class YamlConfig:
    """
    Common Config Yaml class
    """

    def __init__(self, fname_config_yaml):
        self.fname_config_yaml = fname_config_yaml
        self.dic_config = self.read_yaml() if check_exist(fname_config_yaml) else {}

    def read_yaml(self):
        # Read YAML and return dic
        # https://qiita.com/konomochi/items/f5f53ba8efa07ec5089b
        # 入力時に順序を保持する
        add_constructor(
            resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            lambda loader, node: OrderedDict(loader.construct_pairs(node)),
        )

        # get encoding
        encoding = detect_encoding(self.fname_config_yaml)

        with open(self.fname_config_yaml, 'r', encoding=encoding) as f:
            data = yaml.load(f, Loader=yaml.Loader)

        return data

    def write_json_to_yml_file(self, dict_obj, output_file: str = None):
        # get encoding
        encoding = detect_encoding(self.fname_config_yaml)

        try:
            dict_obj = convert_json_to_ordered_dict(dict_obj)
            with open(output_file if output_file else self.fname_config_yaml, 'w', encoding=encoding) as outfile:
                yaml.dump(dict_obj, outfile, default_flow_style=False, allow_unicode=True)

            return True
        except Exception:
            print('>>> traceback <<<')
            traceback.print_exc()
            print('>>> end of traceback <<<')
            return False

    def update_yaml(self, dict_obj, key_paths=None):
        try:
            self.clear_specified_parts(key_paths)
            updated_obj = dict_deep_merge(dict_obj, self.dic_config)

            self.write_json_to_yml_file(updated_obj)
            return True
        except Exception:
            return False

    def save_yaml(self):
        """
        save yaml object to file
        Notice: This method will save exactly yaml obj to file, so in case many users save the same time,
        the last one will win. so use update_yaml is better in these case. update_yaml only save changed node(key) only.
        :return:
        """

        try:
            self.write_json_to_yml_file(self.dic_config)
            return True
        except Exception:
            return False

    # get node . if node not exist return None (no error)
    @staticmethod
    def get_node(dict_obj, keys, default_val=None):
        node = dict_obj
        if not isinstance(keys, (list, tuple)):
            keys = [keys]

        for key in keys:
            if not node or not isinstance(node, dict):
                if default_val is None:
                    return node
                else:
                    return default_val

            node = node.get(key, default_val)

        return node

    # clear node by a specified key array
    def clear_node_by_key_path(self, keys):
        node = self.dic_config
        for key in keys:
            # if node is empty, we dont need to care its children
            if not node:
                return

            # use parent node reference to delete its children
            if key == keys[-1] and key in node:
                del node[key]
                return

            # move current node reference to 1 layer deeper
            node = node.get(key)

    # clear specified key in specified node.
    def clear_specified_parts(self, key_paths):
        if not key_paths:
            return

        for keys in key_paths:
            self.clear_node_by_key_path(keys)


def get_config_file_if_not_exist():
    # check and copy basic config file if not existing
    basic_config_name = 'basic_config.yml'
    init_config(os.path.join('ap', 'config', basic_config_name), os.path.join('init', basic_config_name))


class BasicConfigYaml(YamlConfig):
    """
    Basic Config Yaml class
    """

    # keywords
    INFO = 'info'
    VERSION = 'version'
    MODE = 'mode'
    BRIDGE_STATION = 'bridge_station'
    HOST = 'host'
    PORT = 'port'
    WEB_HOST = 'web_host'
    WEB_PORT = 'web_port'
    UUID = 'uuid'
    IS_STAND_ALONE = 'is_stand_alone'
    BRIDGE_DATA = 'bridge_data'
    REDIS = 'redis'
    DATABASE = 'database'
    DBNAME = 'dbname'
    USERNAME = 'username'
    PASSWORD = 'password'
    EDGE_PORT = 'port-no'
    PROXY = 'proxy'
    FILE_MODE = 'file_mode'
    SETTING_APP = 'setting_app'
    BROWSER_DEBUG = 'browser_debug'

    def __init__(self, file_name=None):
        get_config_file_if_not_exist()
        if file_name is None:
            file_name = os.path.join('ap', 'config', YAML_CONFIG_BASIC_FILE_NAME)

        super().__init__(file_name)

    def get_proxy(self):
        return YamlConfig.get_node(self.dic_config, [self.INFO, self.PROXY], None)

    def get_server_port(self):
        return YamlConfig.get_node(self.dic_config, [self.INFO, self.EDGE_PORT], None)

    def get_version(self):
        return YamlConfig.get_node(self.dic_config, [self.INFO, self.VERSION], '0')

    def set_version(self, ver):
        self.dic_config[self.INFO][self.VERSION] = ver

    def get_node(self, keys, default_val=None):
        return YamlConfig.get_node(self.dic_config, keys, default_val)

    def get_current_mode(self):
        mode = YamlConfig.get_node(self.dic_config, [self.INFO, self.MODE])
        return ServerType(mode)

    def is_postgres_db(self):
        return self.get_current_mode().is_postgres_db()

    def get_db_name(self):
        dbname = self.get_node([self.DATABASE, self.DBNAME])
        return dbname

    def get_db_host(self):
        host = self.get_node([self.DATABASE, self.HOST])
        return host

    def get_db_port(self):
        port = self.get_node([self.DATABASE, self.PORT])
        return port

    def get_db_username(self):
        username = self.get_node([self.DATABASE, self.USERNAME])
        return username

    def get_db_password(self):
        password = self.get_node([self.DATABASE, self.PASSWORD])
        return password

    def get_bridge_station_host(self):
        return YamlConfig.get_node(self.dic_config, [self.BRIDGE_STATION, self.HOST])

    def get_bridge_station_port(self):
        return YamlConfig.get_node(self.dic_config, [self.BRIDGE_STATION, self.PORT])

    def get_bridge_station_web_host(self):
        return YamlConfig.get_node(self.dic_config, [self.BRIDGE_STATION, self.WEB_HOST])

    def get_bridge_station_web_port(self):
        return YamlConfig.get_node(self.dic_config, [self.BRIDGE_STATION, self.WEB_PORT])

    def get_uuid(self):
        return YamlConfig.get_node(self.dic_config, [self.BRIDGE_STATION, self.UUID])

    def set_uuid(self, uuid):
        dic_bridge_station = YamlConfig.get_node(self.dic_config, [self.BRIDGE_STATION])
        dic_bridge_station.update({self.UUID: uuid})
        self.dic_config.update({self.BRIDGE_STATION: dic_bridge_station})

    def get_data_from_bridge(self):
        return YamlConfig.get_node(self.dic_config, [self.BRIDGE_STATION, self.BRIDGE_DATA])

    def get_redis_config(self):
        redis_host = YamlConfig.get_node(self.dic_config, [self.REDIS, self.HOST])
        redis_port = YamlConfig.get_node(self.dic_config, [self.REDIS, self.PORT])
        if not redis_host or not redis_port:
            raise Exception('Missing redis config in basic_config.yml')
        return redis_host, redis_port

    def get_file_mode(self) -> Optional[bool]:
        if not self.dic_config:
            return None

        return self.get_node([self.SETTING_APP, self.FILE_MODE], None)

    def get_browser_debug(self) -> Optional[bool]:
        default_value = None
        if not self.dic_config:
            return default_value

        return self.get_node([self.SETTING_APP, self.BROWSER_DEBUG], None)


def parse_bool_value(value):
    variants = {'true': True, 't': True, '1': True, 'false': False, 'f': False, '0': False}

    if isinstance(value, bool):
        return value
    lower_value = str(value).strip().lower()
    return variants.get(lower_value) or False


class TileInterfaceYaml(YamlConfig):
    """
    Tile Interface Yaml class
    """

    YML_CFG = {
        None: YAML_TILE_INTERFACE_DN7,
        DN7_TILE: YAML_TILE_INTERFACE_DN7,
        AP_TILE: YAML_TILE_INTERFACE_AP,
        SEARCH_USAGE: YAML_TILE_INTERFACE_USAGE,
        TILE_MASTER: YAML_TILE_MASTER,
        TILE_JUMP_CFG: YAML_TILE_JUMP,
    }

    def __init__(self, file_name):
        yml_file_name = self.YML_CFG[file_name]
        file_name = os.path.join('ap', 'config', yml_file_name)
        super().__init__(file_name)

    def get_node(self, keys, default_val=None):
        return YamlConfig.get_node(self.dic_config, keys, default_val)
