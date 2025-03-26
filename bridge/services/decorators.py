import traceback
from functools import wraps
from types import GeneratorType

from ap.common.logger import log_exec_time_inside_func, logger
from bridge.models.bridge_station import BridgeStationModel

# GRPC grpc_server hides all server's error trace. Error is put in side response object.
# Put this decorators to grpc grpc_server method to see error on bridge server


def api_middleware():
    """When GRPC API occur an error, error message was hidden and send to Client Server.
    This decorator supports to catch error and print trace stack without modify logic of the API.
    Only use for unary grpc_server
    """

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            logger.info('Start ' + fn.__qualname__)
            with BridgeStationModel.get_db_proxy() as db_instance:
                try:
                    context = args[2]
                    context.db_instance = db_instance
                    from main import app  # không import ở ngoài được

                    with app.app_context():
                        result = fn(*args, **kwargs)
                    if isinstance(result, GeneratorType):
                        raise Exception('Unsupported type')
                    logger.info('End ' + fn.__qualname__)
                    return result
                except Exception as error:
                    traceback.print_exc()
                    logger.error(error)
                    raise error

            logger.info('End ' + fn.__qualname__)

        return wrapper

    return decorator


def api_middleware_stream():
    """When GRPC API occur an error, error message was hidden and send to Client Server.
    This decorator supports to catch error and print trace stack without modify logic of the API.
    Only use for stream grpc_server
    """

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            logger.info('Start ' + fn.__qualname__)
            with BridgeStationModel.get_db_proxy() as db_instance:
                try:
                    context = args[2]
                    context.db_instance = db_instance
                    from main import app  # không import ở ngoài được

                    with app.app_context():
                        result = fn(*args, **kwargs)
                    if not isinstance(result, GeneratorType):
                        raise Exception('Unsupported type')

                    func = log_exec_time_inside_func('api_middleware_stream', fn.__qualname__)
                    for iterator in result:
                        yield iterator
                    func()

                except Exception as error:
                    traceback.print_exc()
                    logger.error(error)
                    raise error
            logger.info('End ' + fn.__qualname__)

        return wrapper

    return decorator


def consume_generator(generator_func):
    while True:
        try:
            _ = next(generator_func)
        except StopIteration as stop_instance:
            return stop_instance.value


def run_once(f):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)

    wrapper.has_run = False
    return wrapper


def run_once_async(func):  # not support return value of func (chưa nghĩ ra cách)
    def wrapper(*args, **kwargs):
        job_id = kwargs.get('_job_id', None)
        if not job_id:
            raise Exception('function must have "_job_id"')
        if job_id in wrapper.dict_has_run:
            return
        else:
            wrapper.dict_has_run[job_id] = True  # must before func, to supports check_disk_available raise exception
            return func(*args, **kwargs)

    # if ServerConfig.get_server_type() is ServerType.BridgeStationGrpc:
    #     manager = multiprocessing.Manager()
    #     wrapper.dict_has_run = manager.dict()
    # else:
    wrapper.dict_has_run = {}
    return wrapper
