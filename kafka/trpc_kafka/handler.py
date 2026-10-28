# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Dispatch Kafka messages from ``server_req_head`` to servicers."""

from aiokafka.structs import ConsumerRecord
from trpc_sdk.abc import TrpcContext
from trpc_sdk.exceptions import TrpcErrorCode
from trpc_sdk.exceptions import TrpcErrorType
from trpc_sdk.exceptions import TrpcException
from trpc_sdk.log import logger

from .servicer import TrpcKafkaBatchConsumerServicer
from .servicer import TrpcKafkaServicer


async def handle(ctx: TrpcContext, servicer: TrpcKafkaServicer = None):
    """Pass one ``ConsumerRecord`` on ``server_req_head`` to ``servicer``.

    Args:
        ctx: Request context. ``ctx.msg.server_req_head`` must be one
            ``ConsumerRecord``.
        servicer: User ``TrpcKafkaServicer``. ``None`` fails when
            ``handle`` is called.

    Raises:
        TrpcException: ``server_req_head`` is not a ``ConsumerRecord``.
    """

    msg = ctx.msg

    kafak_msg = msg.server_req_head

    if not isinstance(kafak_msg, ConsumerRecord):
        logger.error("consumer msg type error")
        raise TrpcException(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_SERVER_VALIDATE_ERR, "kafka msg type error")

    await servicer.handle(ctx, kafak_msg.key, kafak_msg.value, kafak_msg.topic,
                          kafak_msg.partition, kafak_msg.offset, kafak_msg.timestamp)


async def batch_handle(ctx: TrpcContext, servicer: TrpcKafkaBatchConsumerServicer = None):
    """Pass the record list on ``server_req_head`` to ``servicer``.

    Args:
        ctx: Request context. ``ctx.msg.server_req_head`` must be a list
            of records.
        servicer: User ``TrpcKafkaBatchConsumerServicer``. ``None`` fails
            when ``handle`` is called.

    Raises:
        TrpcException: ``server_req_head`` is not a list.
    """

    msg = ctx.msg

    kafak_msgs = msg.server_req_head

    if not isinstance(kafak_msgs, list):
        logger.error("consumer msg type error")
        raise TrpcException(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_SERVER_VALIDATE_ERR, "kafka msg type error")

    await servicer.handle(ctx, kafak_msgs)
