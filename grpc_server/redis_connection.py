import pickle
from datetime import datetime

import redis
from apscheduler.triggers.interval import IntervalTrigger
from pytz import utc

from ap import get_basic_yaml_obj, scheduler
from ap.api.setting_module.services.common import sync_user_setting_to_bs
from ap.common.constants import (
    REQUEST_MAX_TRIED,
    BridgeChannelResponseMsg,
    BridgeMessageType,
    JobType,
    ServerType,
)
from ap.common.logger import logger
from ap.common.scheduler import scheduler_app_context
from bridge.common.server_config import ServerConfig
from bridge.redis_utils.db_changed import ChangedType
from bridge.redis_utils.pubsub import RedisChannel, redis_connection
from grpc_server.sync_config_master import (
    save_config_master_from_redis,
    sync_config_jobs,
    sync_master_jobs,
)
from grpc_server.sync_transaction import start_sync_proc_link_job, start_sync_transaction_job

redis_conn = None


# is_bridge_changed_listen = True


def redis_connection_REMOVE():
    """
    make connection to redis
    :return:
    """
    global redis_conn
    # connect with redis server
    if redis_conn is None:
        # redis_conn = redis.Redis(host='localhost', port=6379)
        basic_yaml = get_basic_yaml_obj()
        redis_host, redis_port = basic_yaml.get_redis_config()
        redis_conn = redis.Redis(host=redis_host, port=redis_port)
        logger.info(f'redis_host:{redis_host}')
        logger.info(f'redis_port:{redis_port}')

    return redis_conn


def bridge_changed_listen_job():
    """
    start job to listen changed from bridge
    :return:
    """
    job_id = f'{JobType.BRIDGE_CHANGED_LISTEN.name}'
    scheduler.add_job(
        job_id,
        start_redis_listener,
        replace_existing=True,
        trigger=IntervalTrigger(seconds=60, timezone=utc),
        next_run_time=datetime.now().astimezone(utc),
        kwargs=dict(_job_id=job_id, _job_name=job_id),
    )
    return True


@scheduler_app_context
def start_redis_listener(_job_id, _job_name):
    """
    run when start app
    :return:
    """
    # TODO : remove, because we must listen all the time
    # global is_bridge_changed_listen
    # if not is_bridge_changed_listen:
    #     return

    # check server type
    server_type = ServerConfig.get_server_type()
    if server_type is ServerType.StandAlone:
        return None

    # sync data when start app
    sync_master_jobs()
    sync_config_jobs(True)
    # sync_user_setting from es to bs
    sync_user_setting_to_bs()

    conn = redis_connection()
    print(f'{start_redis_listener.__name__}: _job_id : {_job_id}')

    # connect with redis server
    pubsub = conn.pubsub()
    try:
        if ServerConfig.get_server_type() is ServerType.EdgeServer:
            pubsub.subscribe([RedisChannel.BRIDGE_CHANGE_CHANNEL.name])
    except redis.exceptions.ConnectionError:
        # is_bridge_changed_listen = False
        return False

    for message in pubsub.listen():
        _handle_message(message)


def _handle_message(dict_plain_msg):
    response = BridgeChannelResponseMsg(dict_plain_msg)
    if response.type != BridgeMessageType.Message.value:
        return

    # message
    # json_obj = response.data
    # if isinstance(json_obj, bytes):
    #     json_obj = json.loads(json_obj)
    bridge_msg = response.data
    # bridge_msg = DbChangedMsg(**msg_data)

    if isinstance(bridge_msg, bytes):
        bridge_msg = pickle.loads(bridge_msg)

    logger.info(f'Handle redis change type: {bridge_msg.changed_type}')
    print(f'Receive: {bridge_msg.changed_type}')

    # master data changed
    if bridge_msg.changed_type == ChangedType.MASTER_CONFIG.name:
        save_config_master_from_redis(
            bridge_msg.table_name, bridge_msg.crud_type, bridge_msg.id, bridge_msg.json_content
        )
        return

    # repo ids
    # repo_id = BridgeStationConfigUtil.get_repository()
    # sub_repo_ids = BridgeStationConfigUtil.get_sub_repo_ids() or []
    # repo_ids = sub_repo_ids + [repo_id]

    # check repo id
    # if bridge_msg.repo_id and bridge_msg.repo_id not in repo_ids:
    #     return

    # transaction data changed
    if bridge_msg.changed_type == ChangedType.TRANSACTION_IMPORT.name:
        start_sync_transaction_job(JobType.SYNC_TRANSACTION, bridge_msg.process_id)
        return

    if bridge_msg.changed_type == ChangedType.PROC_LINK.name:
        start_sync_proc_link_job(
            JobType.SYNC_PROC_LINK,
            bridge_msg.process_id,
            bridge_msg.target_process_id,
            bridge_msg.is_reset,
            delay=REQUEST_MAX_TRIED,
        )
        return

    # Send SSE messages from Bridge to GUI
    # if bridge_msg.changed_type == ChangedType.DIRECT_SSE_MESSAGE.name:
    #     dic_res = json.loads(bridge_msg.json_content)
    #     background_announcer.announce(dic_res['data'], dic_res['event'])
    #     return

    # Process link changed
    # if bridge_msg.changed_type == ChangedType.SHOW_PROC_LINK_COUNT.name:
    #     # Send event to Client to update process link count
    #     background_announcer.announce(True, AnnounceEvent.PROC_LINK.name)

    # Take a event to continue import csv files
    # if bridge_msg.changed_type == ChangedType.CONTINUE_CSV_IMPORT.name:
    #     # TODO: check later ( not used )
    #     # proc_id = bridge_msg.process_id
    #     # add_continue_import_csv_job(proc_id)
    #     return
