# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Tests for the Redis client codec."""

from trpc_sdk.abc import TrpcContext
from trpc_sdk.codec import Message
from trpc_redis._codec import ClientCodec


def _new_context() -> TrpcContext:
    ctx = TrpcContext()
    ctx.msg = Message()
    return ctx


def test_encode_data():
    """encode_data returns empty bytes and fills the caller service name."""
    redis_codec = ClientCodec()
    ctx = _new_context()
    assert redis_codec.encode_data(ctx, b'hello world') == b''
    assert ctx.msg.caller_service_name.startswith('trpc.redis.')


def test_encode_data_keeps_caller_service_name():
    """An existing caller service name is not overwritten."""
    redis_codec = ClientCodec()
    ctx = _new_context()
    ctx.msg.set_caller_service_name('trpc.app.server.service')
    redis_codec.encode_data(ctx, b'')
    assert ctx.msg.caller_service_name == 'trpc.app.server.service'


def test_decode_data():
    """decode_data turns any reply into UTF-8 bytes."""
    redis_codec = ClientCodec()
    ctx = _new_context()
    assert redis_codec.decode_data(ctx, None) == b''
    assert redis_codec.decode_data(ctx, 'OK') == b'OK'
    req_body = 'hello world'
    assert not redis_codec.decode_data(ctx, req_body) == req_body
    req_body = b''
    assert not redis_codec.decode_data(ctx, req_body) == req_body
    req_body = [b'aa', 'this is a test']
    assert not redis_codec.decode_data(ctx, req_body) == req_body
