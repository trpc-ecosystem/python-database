# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""MySQL client codec.

SQL and its result travel on the tRPC message head (``client_req_head`` /
``client_rsp_head``), so the body codec is a pass-through. Importing this
module registers the codec under ``MYSQL_NAME``.
"""

from trpc_sdk.abc import BaseCodec
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.plugin import register_plugin

from .constants import MYSQL_NAME


@register_plugin(PluginType.CLIENT_CODEC, MYSQL_NAME, init_pos=PluginInitPos.WORKER)
class ClientCodec(BaseCodec):
    """No-op body codec for the MySQL protocol plugin."""

    def encode_data(self, context: TrpcContext, body: bytes) -> bytes:
        """Drop the body. The SQL request is already on the message head.

        The client invoke path calls this context-first entry. It does not
        read the message: the SQL request is already on the client head.

        Args:
            context: RPC context. Unused.
            body: Body bytes. Unused.

        Returns:
            Empty bytes.
        """
        return bytes()

    def decode_data(self, context: TrpcContext, buffer: bytes) -> bytes:
        """Return the transport body unchanged.

        Args:
            context: RPC context. Unused.
            buffer: Bytes returned by the transport, or ``None``.

        Returns:
            ``buffer`` when it is present, otherwise empty bytes.
        """
        if buffer is not None:
            return buffer
        return bytes()
