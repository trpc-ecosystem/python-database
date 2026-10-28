# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Register Kafka servicers on a tRPC server.

Each helper builds one ``ServiceDesc`` whose only method is
``/{svc_name}/handle``, then passes that descriptor to ``Server.register``.
"""
from typing import List

from trpc_sdk.abc import TrpcContext
from trpc_sdk.server import Method
from trpc_sdk.server import Server
from trpc_sdk.server import ServiceDesc

from . import constants
from .handler import batch_handle
from .handler import handle
from .servicer import TrpcKafkaBatchConsumerServicer
from .servicer import TrpcKafkaServicer
from .types import TrpcKafkaMessage


def register_trpc_kafka_server(svr: Server, servicer: TrpcKafkaServicer, servicer_name: str = None):
    """Register one Kafka consumer servicer on ``svr``.

    ``Server.register`` only accepts a ``ServiceDesc``. The single method
    ``/{svc_name}/handle`` is the name ``ServerKafkaCodec.decode`` sets and
    the name ``Service.handle`` looks up in ``unary_handlers``. The consumer
    calls that service handle, not the servicer, so the descriptor is the
    registration contract that attaches the user handler.

    A custom ``servicer_name`` also sets ``USER_DEFINED_SVC_NAME`` so the
    worker passes the configured callee name into the consumer.

    Args:
        svr: tRPC server that already has the Kafka service attached.
        servicer: User implementation of ``TrpcKafkaServicer``.
        servicer_name: Service name. Defaults to
            ``constants.DEFAULT_SVC_NAME`` when omitted.
    """

    async def func(ctx: TrpcContext, message: TrpcKafkaMessage):
        """Forward the call to the registered single-record servicer.

        Args:
            ctx: Request context whose ``server_req_head`` holds the record.
            message: Unused placeholder required by the ``Method`` signature.
        """
        await handle(ctx, servicer)

    svc_name = constants.DEFAULT_SVC_NAME
    if servicer_name is not None:
        svc_name = servicer_name
        constants.USER_DEFINED_SVC_NAME.value = True

    desc = ServiceDesc(svc_name,
                       [
                           Method(
                               f"/{svc_name}/handle",
                               TrpcKafkaMessage,
                               None,
                               func,
                           )]
                       )

    svr.register(desc)


def register_trpc_kafka_batch_consumer_server(svr: Server, servicer: TrpcKafkaBatchConsumerServicer,
                                              servicer_name: str = None):
    """Register one Kafka batch-consumer servicer on ``svr``.

    Same registration contract as :func:`register_trpc_kafka_server`: one
    ``ServiceDesc`` method named ``/{svc_name}/handle``. Batch mode differs
    only in the closure, which forwards the polled records through
    ``batch_handle``.

    Args:
        svr: tRPC server that already has the Kafka service attached.
        servicer: User implementation of ``TrpcKafkaBatchConsumerServicer``.
        servicer_name: Service name. Defaults to
            ``constants.DEFAULT_SVC_NAME`` when omitted.
    """

    async def func(ctx: TrpcContext, message: List[TrpcKafkaMessage]):
        """Forward the call to the registered batch servicer.

        Args:
            ctx: Request context whose ``server_req_head`` holds the
                polled records.
            message: Unused placeholder required by the ``Method`` signature.
        """
        await batch_handle(ctx, servicer)

    svc_name = constants.DEFAULT_SVC_NAME
    if servicer_name is not None:
        svc_name = servicer_name
        constants.USER_DEFINED_SVC_NAME.value = True

    desc = ServiceDesc(svc_name,
                       [
                           Method(
                               f"/{svc_name}/handle",
                               TrpcKafkaMessage,
                               None,
                               func,
                           )]
                       )

    svr.register(desc)
