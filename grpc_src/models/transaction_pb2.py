# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: grpc_src/models/transaction.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n!grpc_src/models/transaction.proto\x12\x12models.transaction"v\n\x12MsgTEdgeServerInfo\x12\x0f\n\x07repo_id\x18\x01 \x01(\x03\x12\x0f\n\x07\x65\x64ge_id\x18\x02 \x01(\x03\x12\x0c\n\x04name\x18\x03 \x01(\t\x12\x14\n\x0clast_sync_id\x18\x04 \x01(\t\x12\x1a\n\x12last_sync_datetime\x18\x05 \x01(\tb\x06proto3'
)


_MSGTEDGESERVERINFO = DESCRIPTOR.message_types_by_name['MsgTEdgeServerInfo']
MsgTEdgeServerInfo = _reflection.GeneratedProtocolMessageType(
    'MsgTEdgeServerInfo',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGTEDGESERVERINFO,
        '__module__': 'grpc_src.models.transaction_pb2'
        # @@protoc_insertion_point(class_scope:models.transaction.MsgTEdgeServerInfo)
    },
)
_sym_db.RegisterMessage(MsgTEdgeServerInfo)

if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    _MSGTEDGESERVERINFO._serialized_start = 57
    _MSGTEDGESERVERINFO._serialized_end = 175
# @@protoc_insertion_point(module_scope)
