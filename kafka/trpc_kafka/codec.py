# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Kafka server and client codecs.

The server codec does not frame a payload. It stamps the tRPC message
so ``Service.handle`` can find the method registered as
``/{svc}/handle``.

The SDK calls the context-first ``encode_data`` / ``decode_data``; they
forward to the message-first ``encode`` / ``decode``.
"""
import os

from trpc_sdk.abc import BaseCodec
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import TrpcMessage
from trpc_sdk.codec import get_message
from trpc_sdk.plugin import register_plugin
from .constants import DEFAULT_SVC_NAME
from .constants import KAFKA_NAME


@register_plugin(PluginType.SERVER_CODEC, KAFKA_NAME, init_pos=PluginInitPos.WORKER)
class ServerKafkaCodec(BaseCodec):
    """Server codec that names the Kafka handle method.

    ``decode`` sets a fixed caller and the RPC name
    ``/{svc}/handle``. ``encode`` returns the body unchanged because
    the consumer path has no client response frame.
    """

    def encode_data(self, context: TrpcContext, body: bytes) -> bytes:
        """Encode the response; the entry ``Service.handle`` calls.

        Args:
            context: Server context.
            body: Response body.

        Returns:
            ``body``.
        """
        return self.encode(get_message(context), body)

    async def decode_data(self, context: TrpcContext, buffer: bytes) -> bytes:
        """Stamp the RPC name; the entry ``Service.handle`` awaits.

        Args:
            context: Server context whose message is updated in place.
            buffer: Raw body.

        Returns:
            ``buffer``.
        """
        return await self.decode(get_message(context), buffer)

    def encode(self, message: TrpcMessage, body: bytes) -> bytes:
        """Return the response body unchanged.

        Args:
            message: Server message. Unused.
            body: Response body.

        Returns:
            ``body``.
        """
        return body

    async def decode(self, message: TrpcMessage, buffer: bytes) -> bytes:
        """Stamp caller, callee, and the ``/{svc}/handle`` RPC name.

        The caller service is fixed. The service segment is
        ``message.callee_service_name`` when that name is non-empty,
        otherwise ``DEFAULT_SVC_NAME``.

        Args:
            message: Server message updated in place.
            buffer: Raw body. Kafka does not frame it here.

        Returns:
            ``buffer``.
        """
        message.set_caller_service("trpc.kafka.noserver.noservice")
        svc_name = DEFAULT_SVC_NAME
        callee_svc_name = message.callee_service_name
        if callee_svc_name is not None and len(callee_svc_name) > 0:
            svc_name = callee_svc_name
        message.set_server_rpc_name(f"/{svc_name}/handle")
        message.set_callee_app(KAFKA_NAME)
        message.set_callee_server(os.path.basename(os.getcwd()))
        message.set_callee_service("service")
        return buffer


@register_plugin(PluginType.CLIENT_CODEC, KAFKA_NAME, init_pos=PluginInitPos.WORKER)
class ClientKafkaCodec(BaseCodec):
    """Client codec that fills a missing caller service name.

    Produce bytes are not framed. ``encode`` only sets
    ``caller_service_name`` when it is empty. ``decode`` returns the
    buffer unchanged.
    """

    def encode_data(self, context: TrpcContext, body: bytes) -> bytes:
        """Encode the request; the entry the client invoke path calls.

        Args:
            context: Client context.
            body: Request body passed through.

        Returns:
            ``body``.
        """
        return self.encode(get_message(context), body)

    def decode_data(self, context: TrpcContext, buffer: bytes) -> bytes:
        """Decode the response; the entry the client invoke path calls.

        Args:
            context: Client context.
            buffer: Response body.

        Returns:
            ``buffer``.
        """
        return self.decode(get_message(context), buffer)

    def encode(self, message: TrpcMessage, body: bytes) -> bytes:
        """Fill an empty caller service name and return ``body``.

        Args:
            message: Client message. ``caller_service_name`` is set
                when it is ``""``.
            body: Request body passed through.

        Returns:
            ``body``.
        """
        if message.caller_service_name == "":
            message.caller_service_name = f"trpc.kafka.{os.path.basename(os.getcwd())}.service"
        return body

    def decode(self, message: TrpcMessage, buffer: bytes) -> bytes:
        """Return the response buffer unchanged.

        Args:
            message: Client message. Unused.
            buffer: Response body.

        Returns:
            ``buffer``.
        """
        return buffer
