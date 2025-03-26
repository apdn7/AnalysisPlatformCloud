import importlib
import pickle

from ap.common.constants import GRPCResponseStatus
from ap.common.logger import logger
from bridge.common.disk_usage import get_disk_capacity
from bridge.services.data_import import check_db_con
from bridge.services.decorators import api_middleware, api_middleware_stream
from bridge.services.process_config import query_database_tables
from grpc_src.models.common_pb2 import GenericRequestParams, GenericResponse
from grpc_src.models.setting_pb2 import (
    GetDataBaseTableRequest,
    GetDataBaseTableResponse,
    RequestCheckDataBaseConnection,
    RequestCheckDiskCapacity,
    ResponseCheckDataBaseConnection,
    ResponseCheckDiskCapacity,
)
from grpc_src.setting.services_pb2_grpc import SettingServicer

dic_imported_modules = {}


class SettingController(SettingServicer):
    @api_middleware()
    # support chức năng bấm review thì thông qua bridge station access vào db source. chứ không access trực tiếp từ edge
    # todo remove
    def GetDataBaseTable(self, request: GetDataBaseTableRequest, context) -> GetDataBaseTableResponse:
        db_id = request.cfg_data_source_id
        if not db_id:
            dict_response = dict({'tables': [], 'msg': 'Invalid data source id'})
            return GetDataBaseTableResponse(**dict_response)

        tables = query_database_tables(db_id)

        if tables is None:
            dict_response = dict({'tables': [], 'msg': 'Invalid data source id'})
        else:
            dict_response = dict(tables)

        return GetDataBaseTableResponse(**dict_response)

    @api_middleware()
    # todo remove
    def CheckDataBaseConnection(
        self, request: RequestCheckDataBaseConnection, context
    ) -> ResponseCheckDataBaseConnection:
        """
        Check database connection with request's configuration.
        :param request: database configuration request message
        :param context: context that include db_instance
        :return: TRUE message if connect success, otherwise
        """
        is_connected = check_db_con(
            request.db_type,
            request.host,
            request.port,
            request.dbname,
            request.schema,
            request.username,
            request.password,
        )
        return ResponseCheckDataBaseConnection(status=GRPCResponseStatus.OK.name, is_connected=is_connected)

    @api_middleware()
    def CheckDiskCapacity(self, request: RequestCheckDiskCapacity, context) -> ResponseCheckDiskCapacity:
        """
        Check disk capacity of Bridge Station & Postgres DB.
        :param request: RequestCheckDiskCapacity object, current not pass any properties yet.
        :param context: context that include db_instance
        :return: Response message include disk status, used percent and message if have.
        """
        disk_capacity = get_disk_capacity()
        return ResponseCheckDiskCapacity(**disk_capacity.to_dict())

    @api_middleware()
    def GenericGrpcMethod(self, request: GenericRequestParams, context) -> GenericResponse:
        # see grpc_api_proxy
        fn_fully_qualname = request.method
        module_name, fn_name = (
            '.'.join(fn_fully_qualname.split('.')[0:-1]),
            fn_fully_qualname.split('.')[-1],
        )
        module = dic_imported_modules.get(module_name)
        if module is None:
            module = importlib.import_module(module_name)
            dic_imported_modules[module_name] = module

        fn = getattr(module, fn_name)

        logger.info(f'Execute function {fn_fully_qualname} via gRPC API {self.GenericGrpcMethod.__name__}')
        params = pickle.loads(request.binParams)
        if 'db_instance' in params:
            params['db_instance'] = context.db_instance
        response_content = fn(**params)
        return GenericResponse(binResponse=pickle.dumps(response_content))

    @api_middleware_stream()
    def GenericGrpcMethodStream(self, request: GenericRequestParams, context) -> GenericResponse:
        # see grpc_api_proxy
        fn_fully_qualname = request.method
        module_name, fn_name = (
            '.'.join(fn_fully_qualname.split('.')[0:-1]),
            fn_fully_qualname.split('.')[-1],
        )
        module = dic_imported_modules.get(module_name)
        if module is None:
            module = importlib.import_module(module_name)
            dic_imported_modules[module_name] = module
        fn = getattr(module, fn_name)

        logger.info(f'Execute function {fn_fully_qualname} via gRPC API {self.GenericGrpcMethod.__name__}')
        params = pickle.loads(request.binParams)
        if 'db_instance' in params:
            params['db_instance'] = context.db_instance
        response_content = fn(**params)
        for response in response_content:
            yield GenericResponse(binResponse=pickle.dumps(response))
