import pickle
from enum import auto
from functools import wraps

from ap import get_basic_yaml_obj
from ap.common.constants import BaseEnum, ServerType
from bridge.common.server_config import ServerConfig
from bridge.redis_utils.pubsub import RedisChannel, redis_connection


def only_run_on_mode(modes):
    def inner1(fn):
        @wraps(fn)
        def inner2(*args, **kwargs):
            basic_config_yaml = get_basic_yaml_obj()
            result = None
            if basic_config_yaml.get_current_mode() in modes:
                result = fn(*args, **kwargs)

            return result

        return inner2

    return inner1


def send_changed_msg_to_redis(conn, msg):
    if conn is None:
        return None

    # check server type
    server_type = ServerConfig.get_server_type()
    if server_type in [ServerType.StandAlone, ServerType.EdgeServer]:
        return None

    # connect with redis server
    # json_msg = msg.to_json()
    msg_data = msg.to_pickle()
    conn.publish(RedisChannel.BRIDGE_CHANGE_CHANNEL.name, msg_data)
    print(f'publish: {RedisChannel.BRIDGE_CHANGE_CHANNEL.name} {msg_data}')

    return True


class ChangedType(BaseEnum):
    MASTER_CONFIG = auto()
    CONFIG = auto()
    '''Sync config data from Bridge to Edge.'''

    MASTER_IMPORT = auto()
    '''Sync master data from Bridge to Edge.'''

    TRANSACTION_IMPORT = auto()
    '''Sync transaction data from Bridge to Edge'''

    PROC_LINK = auto()
    '''Sync proc link data from Bridge to Edge'''

    SHOW_PROC_LINK_COUNT = auto()  # todo: maybe be able to change to DIRECT_SSE_MESSAGE
    DIRECT_SSE_MESSAGE = auto()
    '''When Bridge sends SSE message to GUI. See SSEPublishSupporter'''

    CONTINUE_CSV_IMPORT = auto()
    '''Request next bundle of csv files (10 files)'''

    SCAN_MASTER = auto()
    '''Scan maser done'''

    SCAN_DATA_TYPE = auto()
    GENERATE_CONFIG = auto()


class DbChangedMsg:
    def __init__(
        self,
        changed_type=None,
        process_id=None,
        target_process_id=None,
        json_content=None,
        table_name=None,
        crud_type=None,
        id=None,
        is_reset=None,
        updated_at=None,
    ):
        self.id = id
        self.changed_type = changed_type
        self.process_id = process_id
        self.target_process_id = target_process_id
        self.json_content = json_content  # only use for ChangedType.DIRECT_SSE_MESSAGE
        self.table_name = table_name
        self.crud_type = crud_type
        self.is_reset = is_reset
        self.updated_at = updated_at

    def set_changed_config(self):
        self.changed_type = ChangedType.CONFIG.name

    def set_changed_master(self):
        self.changed_type = ChangedType.MASTER_IMPORT.name

    def set_changed_transaction(self):
        self.changed_type = ChangedType.TRANSACTION_IMPORT.name

    def set_changed_process_link(self):
        # sse show proc link count
        self.changed_type = ChangedType.SHOW_PROC_LINK_COUNT.name

    # def to_json(self):
    #     """
    #     return json
    #     :return:
    #     """
    #     return json.dumps(self.__dict__, ensure_ascii=False, default=http_content.json_serial)

    def to_pickle(self):
        """
        return json
        :return:
        """
        return pickle.dumps(self)


@only_run_on_mode([ServerType.BridgeStationWeb])
def publish_master_config_changed(table_name, crud_type=None, id=None, dict_data=None, updated_at=None):
    """
    check changed on bridge
    :return:
    """
    conn = redis_connection()
    # make object
    msg = DbChangedMsg(
        changed_type=ChangedType.MASTER_CONFIG.name,
        table_name=table_name,
        crud_type=crud_type,
        id=id,
        json_content=dict_data,
        updated_at=updated_at,
    )
    # connect with redis server
    send_changed_msg_to_redis(conn, msg)

    return True


@only_run_on_mode([ServerType.BridgeStationWeb])
def publish_transaction_changed(process_id, change_type: ChangedType):
    """
    check changed on bridge
    :return:
    """
    conn = redis_connection()
    # make object
    msg = DbChangedMsg(changed_type=str(change_type), process_id=process_id)

    # if change_type == ChangedType.SCAN_DATA_TYPE:
    #     from bridge.services.data_import import generate_config_process
    #     generate_config_process()
    #     # Send SSE messages from Bridge to GUI
    #     background_announcer.announce(process_id, AnnounceEvent.PROC_ID)

    # connect with redis server
    send_changed_msg_to_redis(conn, msg)
    return True


@only_run_on_mode([ServerType.BridgeStationWeb])
def publish_proc_link_changed(self_proc_id, target_proc_id, is_reset=None):
    """
    check changed on bridge
    :return:
    """
    conn = redis_connection()
    # make object
    change_type = ChangedType.PROC_LINK.name
    msg = DbChangedMsg(
        changed_type=change_type,
        process_id=self_proc_id,
        target_process_id=target_proc_id,
        is_reset=is_reset,
    )

    # connect with redis server
    send_changed_msg_to_redis(conn, msg)
    return True
