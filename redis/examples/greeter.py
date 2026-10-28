# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Greeter service that counts requests in Redis.

The Redis client is built at import time, before the server forks, so the
example also covers a transport created in the parent process.
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

from trpc_redis import Client as RedisClient

COUNTER_KEY = "trpc_redis_example:counter"

redis_client = RedisClient("trpc.redis.test_example")


class GreeterServicer(rpc.GreeterServicer):
    """Greeter whose reply carries the Redis counter, pid, and handler thread."""

    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Increment the counter in Redis and report who served the call.

        ``INCR`` returns the new value, so concurrent calls see distinct counts.

        Args:
            ctx: tRPC request context, passed on to the Redis client.
            request: Incoming hello request.

        Returns:
            A reply whose ``msg`` is
            ``hello: <msg> count=<n> pid=<pid> thread=<name>``.
        """
        count = await redis_client.incr(ctx, COUNTER_KEY)
        thread_name = threading.current_thread().name
        logger.info("SayHello req: %s count: %s pid: %s thread: %s", request.msg, count, os.getpid(), thread_name)
        rsp = pb.HelloReply()
        rsp.msg = f"hello: {request.msg} count={count} pid={os.getpid()} thread={thread_name}"
        return rsp
