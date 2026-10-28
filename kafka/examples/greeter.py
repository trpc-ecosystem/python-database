# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Greeter service whose handler produces each greeting to Kafka.

The Kafka client is built at import time, before the server forks, so the
example also covers a producer used from every serving thread and process.
"""

import os
import sys
import threading

from trpc_sdk.abc import TrpcContext
from trpc_sdk.log import logger

# add the stub to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "stub")))  # noqa

from trpc_trpc_python_group_helloworld import pb
from trpc_trpc_python_group_helloworld import rpc

import trpc_kafka

TOPIC = os.environ.get("TRPC_KAFKA_TOPIC", "test-topic")

kafka_client = trpc_kafka.Client("trpc.kafka.producer.service")


class GreeterServicer(rpc.GreeterServicer):
    """Greeter whose reply carries where the record landed, the pid, and the handler thread."""

    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Produce the greeting to Kafka and report who served the call.

        Args:
            ctx: tRPC request context, passed on to the Kafka client.
            request: Incoming hello request.

        Returns:
            A reply whose ``msg`` is
            ``hello: <msg> partition=<p> offset=<o> pid=<pid> thread=<name>``.
        """
        meta = await kafka_client.produce(ctx, TOPIC, f"rpc-{request.msg}", f"rpc-{request.msg}")
        thread_name = threading.current_thread().name
        logger.info("SayHello req: %s partition: %s offset: %s pid: %s thread: %s", request.msg, meta.partition,
                    meta.offset, os.getpid(), thread_name)
        rsp = pb.HelloReply()
        rsp.msg = (f"hello: {request.msg} partition={meta.partition} offset={meta.offset} pid={os.getpid()} "
                   f"thread={thread_name}")
        return rsp
