# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""
Redis protocol codec module for tRPC framework.

This module provides a codec for encoding and decoding messages using the Redis protocol.
It registers a custom codec plugin for Redis, which is used by the tRPC client to handle Redis-specific message
serialization and deserialization.

Usage:
    The codec is registered automatically as a plugin and is used internally by the tRPC Redis client.
"""

import os
from typing import Any

from trpc_sdk.abc import BaseCodec
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.codec import get_message
from trpc_sdk.plugin import register_plugin

from ._constants import REDIS_NAME


@register_plugin(PluginType.CLIENT_CODEC, REDIS_NAME, init_pos=PluginInitPos.WORKER)
class ClientCodec(BaseCodec):
    """Unary client codec for the Redis protocol.

    Redis I/O is done entirely by the Redis transport through ``redis-py``,
    so this codec produces no wire bytes. It only fills in client metadata on
    encode and turns the transport reply into bytes on decode.
    """

    def encode_data(self, context: TrpcContext, body: Any) -> bytes:
        """Encode a request frame; the entry the client invoke path calls.

        Sets a default caller service name when the message has none.

        Args:
            context: RPC context whose client message is encoded.
            body: Request body. Unused, the request lives in
                ``message.client_req_head``.

        Returns:
            Empty bytes; the transport sends the actual Redis command.
        """
        message = get_message(context)
        if message.caller_service_name == "":
            message.set_caller_service_name(f"trpc.redis.{os.path.basename(os.getcwd())}.service")
        return b""

    def decode_data(self, context: TrpcContext, buffer: Any) -> bytes:
        """Decode the transport reply into bytes; the entry the client invoke path calls.

        Args:
            context: RPC context of the call. Unused.
            buffer: Reply returned by the Redis transport; may be ``None``.

        Returns:
            ``b""`` for ``None``, otherwise ``str(buffer)`` encoded as UTF-8.
        """
        if buffer is None:
            return b""
        return bytes(str(buffer), encoding='utf8')
