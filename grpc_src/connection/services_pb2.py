# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: grpc_src/connection/services.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n"grpc_src/connection/services.proto\x12\x13\x63onnection.services\x1a grpc_src/models/connection.proto2\xd9\x01\n\nConnection\x12_\n\x0eInitializeEdge\x12$.models.connection.RequestConnection\x1a%.models.connection.ResponseConnection"\x00\x12j\n\x0f\x43heckConnection\x12).models.connection.RequestCheckConnection\x1a*.models.connection.ResponseCheckConnection"\x00\x62\x06proto3'
)


_CONNECTION = DESCRIPTOR.services_by_name['Connection']
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    _CONNECTION._serialized_start = 94
    _CONNECTION._serialized_end = 311
# @@protoc_insertion_point(module_scope)
