# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Kafka produce client.

``produce`` stores a ``Request`` on ``client_req_head``, invokes the
tRPC client, and returns the broker response from ``client_rsp_head``.
"""
from trpc_sdk.client import Client as TrpcClient
from trpc_sdk.codec import clone_client_message

from .types import Request
from .utils import create_rsp_object
from .utils import get_options


class Client:
    """Produce records through the tRPC client stack.

    Attributes:
        client: Underlying tRPC client used for ``invoke``.
        server_name: Callee service name placed on the client message.
        opts: Kafka protocol options plus any caller-supplied options.
    """

    def __init__(self, name: str, opts=None):
        """Bind this client to one callee service.

        Args:
            name: Callee service name.
            opts: Extra client options appended after the Kafka defaults.
                ``None`` means no extras.
        """
        if opts is None:
            opts = []
        self.client = TrpcClient()
        self.server_name = name
        self.opts = get_options(opts)

    async def produce(self, ctx, topic: str, key: str, value: str):
        """Send one record and return the broker produce result.

        The request is placed on ``client_req_head``. The RPC name is
        ``/{server_name}/send``. Serialization is left to the transport,
        which reads the request head rather than the encoded body.

        Args:
            ctx: Caller context. A client message is cloned from it.
            topic: Kafka topic.
            key: Record key.
            value: Record value.

        Returns:
            The object the transport stored on ``client_rsp_head``.
        """
        req = Request(topic, key, value)
        ctx, msg = clone_client_message(ctx)
        msg.set_compress_type(0)
        msg.set_serialization_type(-1)
        msg.set_client_rpc_name(f"/{self.server_name}/send")
        msg.set_callee_service_name(self.server_name)
        msg.set_client_req_head(req)

        await self.client.invoke(ctx, req, create_rsp_object, self.opts)
        return msg.client_rsp_head
