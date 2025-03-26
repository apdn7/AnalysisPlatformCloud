from enum import Enum, auto

import redis

from ap.common.constants import ServerType
from bridge.common.server_config import ServerConfig

redis_conn = None


class RedisChannel(Enum):
    BRIDGE_CHANGE_CHANNEL = auto()


def redis_connection():
    """
    make connection to redis
    :return:
    """
    global redis_conn

    # check server type
    server_type = ServerConfig.get_server_type()
    if server_type is ServerType.StandAlone:
        return None

    # connect with redis server
    if redis_conn is None:
        redis_host, redis_port = ServerConfig.get_redis_server()
        try:
            redis_conn = redis.Redis(host=redis_host, port=redis_port)
        except Exception:
            print('CAN NOT CONNECT TO REDIS')
            return None

    print('CONNECTED TO REDIS')
    return redis_conn


# class CustomRedisLRU(RedisLRU):  # todo rename
#     def _decorator_key(self, func: types.FunctionType, *args, **kwargs):
#         try:
#             hash_arg = tuple([hash(arg) for arg in args])
#             hash_kwargs = tuple([hash(value) for value in kwargs.values()])
#         except TypeError:
#             # TODO: replace to use custom Exception instead ArgsUnhashable in redis_lru lib
#             from redis_lru.lru import ArgsUnhashable
#             raise ArgsUnhashable()
#
#         return '{}:{}:{}{!r}:{!r}'.format(self.key_prefix, func.__module__,
#                                           func.__qualname__, hash_arg, hash_kwargs)


# TODO: don't use this cache because STAND-ALONE can't not use redis
# redis cache
# def cache(maxsize=100, ttl=60 * 60):
#     """
#     Please use this function as a decorator, methods that use this decorator will be cache the result in first call.
#     From second call, it will obtain result in RedisLRU (Redis less recently used) and return instead of process.
#     :param maxsize: Redis to use a specified amount of memory for the data set. (Default 100 megabytes)<br>
#                     Setting maxmemory to zero results into no memory limits. This is the default behavior for
#                     64 bit systems, while 32 bit systems use an implicit memory limit of 3GB.<br><br>
#     :param ttl: The remaining time to live of a key that has a timeout. (Default 1 hour)<br>
#                 This introspection capability allows a Redis client to check how many seconds a given key will
#                 continue to be part of the dataset.
#     :return: result of calling function
#     """
#     if ServerConfig.get_server_type() == ServerType.BridgeStation:
#         redis_host, redis_port = ServerConfig.get_redis_server()
#         client = redis.StrictRedis(host=redis_host, port=redis_port)
#         _cache = CustomRedisLRU(client, max_size=maxsize, default_ttl=ttl)
#     else:
#         # tunghh note: ban đầu định dùng cái cached cho nó có param ttl nhưng mà k work
#         lock = ServerConfig.get_lock()
#         return lru_cache(maxsize)
#         # return cached(cache=TTLCache(maxsize=maxsize, ttl=ttl), lock=lock)
#
#     return _cache
