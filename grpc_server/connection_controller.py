from ap.common.constants import GRPCResponseStatus
from bridge.models.cfg_edge import CfgEdge
from bridge.services.decorators import api_middleware
from grpc_src.connection.services_pb2_grpc import ConnectionServicer
from grpc_src.models.connection_pb2 import (
    GrpcSession,
    RequestCheckConnection,
    RequestConnection,
    ResponseCheckConnection,
    ResponseConnection,
)


class ConnectionController(ConnectionServicer):
    @api_middleware()
    # tunghh: mục đích để khởi tạo cfg repository.
    # edge gửi repo id lên bridge. bridge tạo repo nếu chưa có.
    # maybe không cần nữa # todo remove
    def InitializeEdge(self, request: RequestConnection, context) -> ResponseConnection:
        """
        Updates the record and returns repository id if existing, create and returns if not existing.
        :param request:
        :param context:
        :return:
        """
        db_instance = context.db_instance
        edge_id = request.edge.edge_id
        CfgEdge.insert_or_update(db_instance, CfgEdge.to_dict(request.edge))

        session = GrpcSession(edge_id=edge_id)
        return ResponseConnection(status=GRPCResponseStatus.OK.name, repository=request.repository, session=session)

    def CheckConnection(self, request: RequestCheckConnection, context) -> ResponseCheckConnection:
        """
        Just a ping to Bridge Server.
        :param request:
        :param context:
        :return:
        """
        return ResponseCheckConnection(status=GRPCResponseStatus.OK.name, message=GRPCResponseStatus.OK.name)
