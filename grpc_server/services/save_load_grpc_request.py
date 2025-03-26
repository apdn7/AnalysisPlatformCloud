from enum import Enum, auto


class RequestStatus(Enum):
    PENDING = auto()
    PROCESSING = auto()
    FAILED = auto()
    DONE = auto()


# def save_request(service_name, method_name, msg=None):
#     """
#     save gRPC request to table.
#     :param service_name: class name
#     :param method_name: string
#     :param msg: message instance
#     :return:
#     """
#     with EdgeDbProxy() as meta_session:
#         rec = TBridgeStationRequestHistory()
#         rec.service = service_name
#         rec.method = method_name
#         if msg:
#             rec.message = pickle.dumps(msg)
#
#         rec.status = RequestStatus.PENDING.name
#         meta_session.add(rec)
#
#     return rec.id


# def load_request(meta_session, req_id):
#     """
#     select grpc request from table
#     :param meta_session:
#     :param req_id:
#     :return:
#     """
#     rec = meta_session.query(TBridgeStationRequestHistory).get(req_id)
#     return rec


# def get_last_config_sync_order():
#     """
#     get last sync order ( bridge to edge )
#     :return:
#     """
#     recs = TBridgeStationRequestHistory.get_by_status(service_name=SyncConfigStub.__name__,
#                                                       method_name=SyncConfigServicer.SyncBridgeToEdge.__name__,
#                                                       status=RequestStatus.DONE.name)
#     last_sync_order = None
#     if recs:
#         last_sync_order = recs[-1].sync_order
#
#     return last_sync_order
