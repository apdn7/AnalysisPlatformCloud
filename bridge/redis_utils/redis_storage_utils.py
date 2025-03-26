import pickle
import threading
from enum import Enum, auto
from multiprocessing import Lock
from time import sleep
from typing import Any, Tuple

from bridge.redis_utils.pubsub import redis_connection

_redis_lock = Lock()


class RedisDataKey(Enum):
    # DIC_RUNNING_JOB = auto()
    IS_APP_RUNNING = auto()
    FACTORY_DB_TABLE_NAME = auto()


class FactoryDbUtil:
    @staticmethod
    def get_table_names(data_source_id):
        with _redis_lock:
            dic_table_names = get_redis_data(RedisDataKey.FACTORY_DB_TABLE_NAME)
            dic_table_names = pickle.loads(dic_table_names) if dic_table_names else {}
            return dic_table_names.get(data_source_id, ())

    @staticmethod
    def set_pair_data_source_table_names(data_source_id, table_names: Tuple = None):
        with _redis_lock:
            dic_table_names = get_redis_data(RedisDataKey.FACTORY_DB_TABLE_NAME)
            dic_table_names = pickle.loads(dic_table_names) if dic_table_names else {}
            dic_table_names[data_source_id] = table_names
            set_redis_data(RedisDataKey.FACTORY_DB_TABLE_NAME, pickle.dumps(dic_table_names))


def clear_redis_storage():
    for key in RedisDataKey.__members__:
        delete_redis_data(RedisDataKey[key])


def clear_all_keys():
    redis_conn = redis_connection()
    redis_conn.flushall()


def get_redis_data(key: RedisDataKey):
    redis_conn = redis_connection()
    return redis_conn.get(key.name)


def set_redis_data(key: RedisDataKey, value: Any):
    redis_conn = redis_connection()
    return redis_conn.set(key.name, value)


def delete_redis_data(key: RedisDataKey):
    redis_conn = redis_connection()
    return redis_conn.delete(key.name)


# Utilize

# def set_dic_running_job(key, value):
#     with _redis_lock:
#         dic_running_job: Any = get_dic_running_job()
#         if not dic_running_job:
#             dic_running_job = {}
#
#         dic_running_job[key] = value
#         set_redis_data(RedisDataKey.DIC_RUNNING_JOB, pickle.dumps(dic_running_job))


# def pop_dic_running_job(key):
#     with _redis_lock:
#         dic_running_job: Any = get_dic_running_job()
#         if dic_running_job:
#             dic_running_job.pop(key, None)
#             set_redis_data(RedisDataKey.DIC_RUNNING_JOB, pickle.dumps(dic_running_job))


# def get_dic_running_job():
#     dic_running_job = get_redis_data(RedisDataKey.DIC_RUNNING_JOB)
#     return pickle.loads(dic_running_job) if dic_running_job else {}


def app_heartbeat():
    thread = threading.Thread(target=set_heartbeat)
    thread.start()
    return thread


def set_heartbeat():
    redis_conn = redis_connection()
    expire_time = 15
    sleep_time = expire_time - 5
    while True:
        redis_conn.setex(RedisDataKey.IS_APP_RUNNING.name, expire_time, 1)
        sleep(sleep_time)


def get_heartbeat():
    redis_conn = redis_connection()
    return redis_conn.get(RedisDataKey.IS_APP_RUNNING.name)
