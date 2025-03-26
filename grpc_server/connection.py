import socket

from grpc._channel import _InactiveRpcError

from ap import get_basic_yaml_obj
from ap.common.constants import GRPCResponseStatus
from grpc_server.services.grpc_service_proxy import get_grpc_channel
from grpc_src.connection.services_pb2_grpc import ConnectionStub
from grpc_src.models.connection_pb2 import RequestCheckConnection
from grpc_src.models.setting_pb2 import RequestCheckDataBaseConnection
from grpc_src.setting.services_pb2_grpc import SettingStub

edge_name = 'analysis_interface'
bridge_connection_status = False


def check_connection_to_server():
    global bridge_connection_status

    bridge_connection_status = False
    with get_grpc_channel() as channel:
        request = RequestCheckConnection(name=edge_name)
        stub = ConnectionStub(channel)
        try:
            response = stub.CheckConnection(request)
            if response and response.status == GRPCResponseStatus.OK.name:
                bridge_connection_status = True
        except _InactiveRpcError:
            # If can not ping Bridge server, _InactiveRpcError is raised
            bridge_connection_status = False

    print('Bridge-Server status : ', bridge_connection_status)
    return bridge_connection_status


def get_local_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 1))  # connect() for UDP doesn't send packets
    return s.getsockname()[0]


def is_call_grpc_api():
    # Check bridge_data flag from yaml
    basic_yaml = get_basic_yaml_obj()
    is_use_bridge = basic_yaml.get_data_from_bridge()
    return is_use_bridge and check_connection_to_server()


# unused todo remove
def check_db_con_on_server(db_type, host, port, dbname, schema, username, password):
    """
    Check database connection on Bridge Server with input configuration
    :param db_type: database type
    :param host: database host name or ip
    :param port: connect port
    :param dbname: database name
    :param schema: database schema
    :param username: username
    :param password: password
    :return: True if database can connect. Otherwise
    """
    database_connection_status = False
    with get_grpc_channel() as channel:
        request = RequestCheckDataBaseConnection(
            db_type=db_type,
            host=host,
            port=port,
            dbname=dbname,
            schema=schema,
            username=username,
            password=password,
        )
        response = SettingStub(channel).CheckDataBaseConnection(request)
        if response and response.status == GRPCResponseStatus.OK.name:
            database_connection_status = response.is_connected

    print('Database connection status : ', database_connection_status)
    return database_connection_status
