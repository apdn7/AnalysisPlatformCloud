# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: grpc_src/models/sync_master.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n!grpc_src/models/sync_master.proto\x12\x12models.sync_master\x1a\x1cgrpc_src/models/master.proto"\x99\x01\n\x11RequestSyncMaster\x12\x0f\n\x07repo_id\x18\x01 \x01(\x03\x12%\n\x06insert\x18\x02 \x01(\x0b\x32\x15.models.master.Insert\x12%\n\x06\x64\x65lete\x18\x03 \x01(\x0b\x32\x15.models.master.Delete\x12%\n\x06update\x18\x04 \x01(\x0b\x32\x15.models.master.Update"5\n\x12ResponseSyncMaster\x12\x0e\n\x06status\x18\x01 \x01(\t\x12\x0f\n\x07message\x18\x02 \x01(\t"-\n\x17RequestSyncBridgeToEdge\x12\x12\n\nsync_order\x18\x01 \x01(\x05"\xe0\x01\n\x18ResponseSyncBridgeToEdge\x12%\n\x06insert\x18\x02 \x01(\x0b\x32\x15.models.master.Insert\x12%\n\x06\x64\x65lete\x18\x03 \x01(\x0b\x32\x15.models.master.Delete\x12%\n\x06update\x18\x04 \x01(\x0b\x32\x15.models.master.Update\x12\x1f\n\x17last_request_history_id\x18\x05 \x01(\x05\x12\x17\n\x0fis_get_all_data\x18\x06 \x01(\x08\x12\x15\n\ris_end_stream\x18\x63 \x01(\x08"\xfc\x01\n$ResponseSyncBridgeToEdge_FactoryMode\x12\x0f\n\x07repo_id\x18\x01 \x01(\x03\x12\x12\n\nprocess_id\x18\x02 \x01(\x03\x12\x12\n\ntable_name\x18\x03 \x01(\t\x12\x14\n\x0c\x63olumn_names\x18\x04 \x03(\t\x12\x0c\n\x04\x64\x61ta\x18\x05 \x01(\x0c\x12\x1f\n\x17last_request_history_id\x18\x06 \x01(\x05\x12\x17\n\x0fis_get_all_data\x18\x07 \x01(\x08\x12\x13\n\x0bimport_from\x18\x08 \x01(\t\x12\x11\n\timport_to\x18\t \x01(\t\x12\x15\n\ris_end_stream\x18\x63 \x01(\x08"c\n#RequestSyncBridgeToEdge_FactoryMode\x12\x12\n\nprocess_id\x18\x01 \x01(\x03\x12\x14\n\x0crequest_from\x18\x02 \x01(\t\x12\x12\n\nrequest_to\x18\x03 \x01(\t"3\n!RequestSyncBridgeToEdgeEtlMapping\x12\x0e\n\x06tables\x18\x01 \x03(\t"n\n"ResponseSyncBridgeToEdgeEtlMapping\x12\x12\n\ntable_name\x18\x01 \x01(\t\x12\x0f\n\x07\x63olumns\x18\x02 \x03(\t\x12\x0c\n\x04\x64\x61ta\x18\x03 \x01(\x0c\x12\x15\n\ris_end_stream\x18\x63 \x01(\x08"/\n\x15RequestSyncNayoseData\x12\x16\n\x0enayose_data_id\x18\x01 \x01(\x03"\x8f\x01\n\x16ResponseSyncNayoseData\x12\x16\n\x0enayose_data_id\x18\x01 \x01(\x03\x12\x18\n\x10nayose_data_part\x18\x02 \x01(\x0c\x12"\n\x1anayose_data_factorymachine\x18\x03 \x01(\x0c\x12\x1f\n\x17nayose_data_processdata\x18\x04 \x01(\x0c\x62\x06proto3'
)


_REQUESTSYNCMASTER = DESCRIPTOR.message_types_by_name['RequestSyncMaster']
_RESPONSESYNCMASTER = DESCRIPTOR.message_types_by_name['ResponseSyncMaster']
_REQUESTSYNCBRIDGETOEDGE = DESCRIPTOR.message_types_by_name['RequestSyncBridgeToEdge']
_RESPONSESYNCBRIDGETOEDGE = DESCRIPTOR.message_types_by_name['ResponseSyncBridgeToEdge']
_RESPONSESYNCBRIDGETOEDGE_FACTORYMODE = DESCRIPTOR.message_types_by_name['ResponseSyncBridgeToEdge_FactoryMode']
_REQUESTSYNCBRIDGETOEDGE_FACTORYMODE = DESCRIPTOR.message_types_by_name['RequestSyncBridgeToEdge_FactoryMode']
_REQUESTSYNCBRIDGETOEDGEETLMAPPING = DESCRIPTOR.message_types_by_name['RequestSyncBridgeToEdgeEtlMapping']
_RESPONSESYNCBRIDGETOEDGEETLMAPPING = DESCRIPTOR.message_types_by_name['ResponseSyncBridgeToEdgeEtlMapping']
_REQUESTSYNCNAYOSEDATA = DESCRIPTOR.message_types_by_name['RequestSyncNayoseData']
_RESPONSESYNCNAYOSEDATA = DESCRIPTOR.message_types_by_name['ResponseSyncNayoseData']
RequestSyncMaster = _reflection.GeneratedProtocolMessageType(
    'RequestSyncMaster',
    (_message.Message,),
    {
        'DESCRIPTOR': _REQUESTSYNCMASTER,
        '__module__': 'grpc_src.models.sync_master_pb2'
        # @@protoc_insertion_point(class_scope:models.sync_master.RequestSyncMaster)
    },
)
_sym_db.RegisterMessage(RequestSyncMaster)

ResponseSyncMaster = _reflection.GeneratedProtocolMessageType(
    'ResponseSyncMaster',
    (_message.Message,),
    {
        'DESCRIPTOR': _RESPONSESYNCMASTER,
        '__module__': 'grpc_src.models.sync_master_pb2'
        # @@protoc_insertion_point(class_scope:models.sync_master.ResponseSyncMaster)
    },
)
_sym_db.RegisterMessage(ResponseSyncMaster)

RequestSyncBridgeToEdge = _reflection.GeneratedProtocolMessageType(
    'RequestSyncBridgeToEdge',
    (_message.Message,),
    {
        'DESCRIPTOR': _REQUESTSYNCBRIDGETOEDGE,
        '__module__': 'grpc_src.models.sync_master_pb2'
        # @@protoc_insertion_point(class_scope:models.sync_master.RequestSyncBridgeToEdge)
    },
)
_sym_db.RegisterMessage(RequestSyncBridgeToEdge)

ResponseSyncBridgeToEdge = _reflection.GeneratedProtocolMessageType(
    'ResponseSyncBridgeToEdge',
    (_message.Message,),
    {
        'DESCRIPTOR': _RESPONSESYNCBRIDGETOEDGE,
        '__module__': 'grpc_src.models.sync_master_pb2'
        # @@protoc_insertion_point(class_scope:models.sync_master.ResponseSyncBridgeToEdge)
    },
)
_sym_db.RegisterMessage(ResponseSyncBridgeToEdge)

ResponseSyncBridgeToEdge_FactoryMode = _reflection.GeneratedProtocolMessageType(
    'ResponseSyncBridgeToEdge_FactoryMode',
    (_message.Message,),
    {
        'DESCRIPTOR': _RESPONSESYNCBRIDGETOEDGE_FACTORYMODE,
        '__module__': 'grpc_src.models.sync_master_pb2'
        # @@protoc_insertion_point(class_scope:models.sync_master.ResponseSyncBridgeToEdge_FactoryMode)
    },
)
_sym_db.RegisterMessage(ResponseSyncBridgeToEdge_FactoryMode)

RequestSyncBridgeToEdge_FactoryMode = _reflection.GeneratedProtocolMessageType(
    'RequestSyncBridgeToEdge_FactoryMode',
    (_message.Message,),
    {
        'DESCRIPTOR': _REQUESTSYNCBRIDGETOEDGE_FACTORYMODE,
        '__module__': 'grpc_src.models.sync_master_pb2'
        # @@protoc_insertion_point(class_scope:models.sync_master.RequestSyncBridgeToEdge_FactoryMode)
    },
)
_sym_db.RegisterMessage(RequestSyncBridgeToEdge_FactoryMode)

RequestSyncBridgeToEdgeEtlMapping = _reflection.GeneratedProtocolMessageType(
    'RequestSyncBridgeToEdgeEtlMapping',
    (_message.Message,),
    {
        'DESCRIPTOR': _REQUESTSYNCBRIDGETOEDGEETLMAPPING,
        '__module__': 'grpc_src.models.sync_master_pb2'
        # @@protoc_insertion_point(class_scope:models.sync_master.RequestSyncBridgeToEdgeEtlMapping)
    },
)
_sym_db.RegisterMessage(RequestSyncBridgeToEdgeEtlMapping)

ResponseSyncBridgeToEdgeEtlMapping = _reflection.GeneratedProtocolMessageType(
    'ResponseSyncBridgeToEdgeEtlMapping',
    (_message.Message,),
    {
        'DESCRIPTOR': _RESPONSESYNCBRIDGETOEDGEETLMAPPING,
        '__module__': 'grpc_src.models.sync_master_pb2'
        # @@protoc_insertion_point(class_scope:models.sync_master.ResponseSyncBridgeToEdgeEtlMapping)
    },
)
_sym_db.RegisterMessage(ResponseSyncBridgeToEdgeEtlMapping)

RequestSyncNayoseData = _reflection.GeneratedProtocolMessageType(
    'RequestSyncNayoseData',
    (_message.Message,),
    {
        'DESCRIPTOR': _REQUESTSYNCNAYOSEDATA,
        '__module__': 'grpc_src.models.sync_master_pb2'
        # @@protoc_insertion_point(class_scope:models.sync_master.RequestSyncNayoseData)
    },
)
_sym_db.RegisterMessage(RequestSyncNayoseData)

ResponseSyncNayoseData = _reflection.GeneratedProtocolMessageType(
    'ResponseSyncNayoseData',
    (_message.Message,),
    {
        'DESCRIPTOR': _RESPONSESYNCNAYOSEDATA,
        '__module__': 'grpc_src.models.sync_master_pb2'
        # @@protoc_insertion_point(class_scope:models.sync_master.ResponseSyncNayoseData)
    },
)
_sym_db.RegisterMessage(ResponseSyncNayoseData)

if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    _REQUESTSYNCMASTER._serialized_start = 88
    _REQUESTSYNCMASTER._serialized_end = 241
    _RESPONSESYNCMASTER._serialized_start = 243
    _RESPONSESYNCMASTER._serialized_end = 296
    _REQUESTSYNCBRIDGETOEDGE._serialized_start = 298
    _REQUESTSYNCBRIDGETOEDGE._serialized_end = 343
    _RESPONSESYNCBRIDGETOEDGE._serialized_start = 346
    _RESPONSESYNCBRIDGETOEDGE._serialized_end = 570
    _RESPONSESYNCBRIDGETOEDGE_FACTORYMODE._serialized_start = 573
    _RESPONSESYNCBRIDGETOEDGE_FACTORYMODE._serialized_end = 825
    _REQUESTSYNCBRIDGETOEDGE_FACTORYMODE._serialized_start = 827
    _REQUESTSYNCBRIDGETOEDGE_FACTORYMODE._serialized_end = 926
    _REQUESTSYNCBRIDGETOEDGEETLMAPPING._serialized_start = 928
    _REQUESTSYNCBRIDGETOEDGEETLMAPPING._serialized_end = 979
    _RESPONSESYNCBRIDGETOEDGEETLMAPPING._serialized_start = 981
    _RESPONSESYNCBRIDGETOEDGEETLMAPPING._serialized_end = 1091
    _REQUESTSYNCNAYOSEDATA._serialized_start = 1093
    _REQUESTSYNCNAYOSEDATA._serialized_end = 1140
    _RESPONSESYNCNAYOSEDATA._serialized_start = 1143
    _RESPONSESYNCNAYOSEDATA._serialized_end = 1286
# @@protoc_insertion_point(module_scope)
