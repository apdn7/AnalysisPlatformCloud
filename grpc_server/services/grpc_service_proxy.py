import inspect
import pickle
from functools import wraps
from inspect import signature

import grpc
from grpc._channel import _InactiveRpcError

from ap import get_basic_yaml_obj
from ap.common.common_utils import end_of_minute, start_of_minute
from ap.common.constants import GRPCResponseStatus, ServerType
from ap.common.logger import logger
from bridge.common.server_config import ServerConfig
from grpc_src.connection.services_pb2_grpc import ConnectionStub
from grpc_src.models.common_pb2 import GenericRequestParams
from grpc_src.models.connection_pb2 import RequestCheckConnection
from grpc_src.setting.services_pb2_grpc import SettingServicer, SettingStub

__FORCE_OFFLINE_MODE__ = '__FORCE_OFFLINE_MODE__'
# __FORCE_ONLINE_MODE__ = '__FORCE_ONLINE_MODE__'
__IF_DISCONNECT_RUN_OFFLINE_MODE__ = '__IF_DISCONNECT_RUN_OFFLINE_MODE__'

bridge_connection_status = False


def grpc_api(stub_class=None, method_name=None, show_graph=False):
    """If cannot connect bridge server (have config or lost connection) call method locally
    Current: Only support method with param: list, tuple, primitive type. Not support dict param
    Arguments:
        fn {function} -- [description]
        prefix {string} -- prefix set to logged message
    Returns:
        fn {function} -- [description]
    """

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if check_forced_offline(kwargs):
                logger.info(f'Call method {fn.__name__} via locally')
                result = fn(*args, **kwargs)
                return result
            if show_graph:
                dict_param = kwargs.get('dict_param')
                if not dict_param:
                    dict_param = args[0]
                start_tm = start_of_minute(dict_param['COMMON']['START_DATE'], dict_param['COMMON']['START_TIME'])
                end_tm = end_of_minute(dict_param['COMMON']['END_DATE'], dict_param['COMMON']['END_TIME'])
                from ap.setting_module.models import FactoryImport

                query_offline = FactoryImport.is_range_time_imported(
                    dict_param['COMMON']['start_proc'], start_tm, end_tm
                )
                if query_offline:
                    logger.info(f'Call method {fn.__name__} via locally')
                    result = fn(*args, **kwargs)
                    return result

            logger.info(f'Call method {fn.__name__} via Bridge Station')
            request_content = build_parameter(fn, args, kwargs)
            fn_fully_qualname = f'{fn.__module__}.{fn.__qualname__}'
            request_msg = GenericRequestParams(method=fn_fully_qualname, binParams=pickle.dumps(request_content))
            # see GenericGrpcMethod
            _cls = stub_class or SettingStub
            _method = method_name or SettingServicer.GenericGrpcMethod.__name__
            try:
                with get_grpc_channel() as channel:
                    stub_instance = _cls(channel)
                    method = getattr(stub_instance, _method)
                    response = method(request_msg)
            except _InactiveRpcError as e:
                logger.error('Error Occurred on Bridge Station')
                logger.error(e.details())
                raise e

            result = pickle.loads(response.binResponse)
            return result

        return wrapper

    return decorator


def grpc_api_stream(stub_class=None, method_name=None):
    """If cannot connect bridge server (have config or lost connection) call method locally
    Current: Only support method with param: list, tuple, primitive type. Not support dict param
    Arguments:
        fn {function} -- [description]
        prefix {string} -- prefix set to logged message
    Returns:
        fn {function} -- [description]
    """

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            # see GenericGrpcMethod
            _cls = stub_class or SettingStub
            _method = method_name or SettingServicer.GenericGrpcMethodStream.__name__
            if check_forced_offline(kwargs):
                logger.info(f'Call method {fn.__name__} via locally')
                result = fn(*args, **kwargs)
                yield from result
            else:
                logger.info(f'Call method {fn.__name__} via Bridge Station')
                request_content = build_parameter(fn, args, kwargs)
                fn_fully_qualname = f'{fn.__module__}.{fn.__qualname__}'
                request_msg = GenericRequestParams(method=fn_fully_qualname, binParams=pickle.dumps(request_content))
                with get_grpc_channel() as channel:
                    stub_instance = _cls(channel)
                    method = getattr(stub_instance, _method)
                    response = method(request_msg)
                    for res in response:
                        yield pickle.loads(res.binResponse)

        return wrapper

    return decorator


def build_parameter(fn, args, kwargs):
    """
    sample method signature : fn(a,b,c=None,d=None,e=None)
    caller: fn(1,2, e=3)
    ==> Then we have
    ==>  args  = [1,2]
    ==>  kwargs = {'e':3}
    this method will build dict {'a':1, 'b':2, 'c':None, 'd':None, 'e':3}
    :param fn:
    :param args:
    :param kwargs:
    :return:
    """
    signature_params = signature(fn).parameters
    parameter_names = list(signature_params.keys())

    # supports to determine exactly position of parameter
    dict_params = dict.fromkeys(parameter_names, None)
    for name in parameter_names:
        idx = parameter_names.index(name)
        if idx < len(args):
            dict_params[name] = args[parameter_names.index(name)]

    # Assign param from kwargs dict_params
    for param_name in parameter_names:
        if param_name in kwargs:
            dict_params[param_name] = kwargs[param_name]

    # Apply default if any
    parameter_defaults = [param.default for param in list(signature_params.values())]
    dict_default_param = dict(zip(parameter_names, parameter_defaults))
    for param_name, param in dict_params.items():
        if param is None:
            default_value = dict_default_param.get(param_name)
            if default_value is not None and default_value is not inspect._empty:
                dict_params[param_name] = default_value

    # Convert job to dict
    for param_name, param in dict_params.items():
        if isinstance(param, (list, tuple)):
            dict_params[param_name] = [item.as_dict() if getattr(item, 'as_dict', None) else item for item in param]
        else:
            # not list, tuple. Do nothing.
            pass

    # return tuple(dict_params.values())
    return dict_params


def is_bridge_server_type():
    server_type = ServerConfig.get_server_type()
    return server_type in (
        ServerType.BridgeStationGrpc,
        ServerType.BridgeStationWeb,
        ServerType.StandAlone,
    )


def check_bridge_connection():
    return check_connection_to_server()  # todo rename check_connection_to_server


def check_forced_offline(kwargs):
    is_forced_offline_mode = kwargs.pop(__FORCE_OFFLINE_MODE__, False) or is_bridge_server_type()
    is_flexible = kwargs.pop(__IF_DISCONNECT_RUN_OFFLINE_MODE__, False)
    if is_flexible and not is_forced_offline_mode:
        if not check_bridge_connection():
            is_forced_offline_mode = True

    return is_forced_offline_mode


def check_connection_to_server():
    global bridge_connection_status

    bridge_connection_status = False
    with get_grpc_channel() as channel:
        request = RequestCheckConnection(name=edge_name)
        stub = ConnectionStub(channel)
        try:
            response = stub.CheckConnection(request, timeout=3)
            if response and response.status == GRPCResponseStatus.OK.name:
                bridge_connection_status = True
        except _InactiveRpcError:
            # If can not ping Bridge server, _InactiveRpcError is raised
            bridge_connection_status = False

    print('Bridge-Server status : ', bridge_connection_status)
    return bridge_connection_status


def get_grpc_channel():
    # Basic yaml config information ( bridge station host, port ...)
    basic_yaml = get_basic_yaml_obj()
    bridge_station_host = basic_yaml.get_bridge_station_host()
    bridge_station_port = basic_yaml.get_bridge_station_port()
    bridge_station_url = f'{bridge_station_host}:{bridge_station_port}'
    return grpc.insecure_channel(bridge_station_url)
