# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""
Redis transport module for tRPC framework.

This module provides a transport implementation for Redis connections, which is used by the tRPC client
to handle Redis-specific message serialization and deserialization.

Usage:
    The transport is registered automatically as a plugin and is used internally by the tRPC Redis client.

One transport instance serves every thread of a process. Connection pools are
cached per thread and per event loop, because a ``redis.asyncio`` pool is
bound to the loop it first runs on. A forked child starts with an empty cache.
"""

import asyncio
import os
import threading
import time
import weakref
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple

import redis.asyncio as redis
from redis import exceptions as redis_exceptions

from trpc_sdk.abc import BaseClientTransport
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.exceptions import ClientDecodeError
from trpc_sdk.exceptions import ClientEncodeError
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.plugin import register_plugin
from trpc_sdk.transport import RoundTripOptions

from ._constants import DECODE_RESPONSES
from ._constants import DECODE_RESPONSES_DEFAULT
from ._constants import HOST
from ._constants import IDLE_TIMEOUT_DEFAULT
from ._constants import MAX_CONNECTIONS
from ._constants import MAX_CONNECTIONS_DEFAULT
from ._constants import PORT
from ._constants import PASSWORD
from ._constants import REDIS_NAME
from ._constants import RequestMode
from ._constants import SOCKET_CONNECT_TIMEOUT
from ._types import Request
from ._types import Response


class _LoopPools(threading.local):
    """Per-thread map from event loop to that loop's connection pools.

    Loops are keyed by object rather than ``id()`` so a closed loop's id
    cannot be reused to return a pool bound to the dead loop.
    """

    def __init__(self):
        """Start the calling thread with no pools."""
        super().__init__()
        self.pools: Dict[asyncio.AbstractEventLoop, Dict[str, redis.ConnectionPool]] = {}

    def for_loop(self, loop: asyncio.AbstractEventLoop) -> Dict[str, redis.ConnectionPool]:
        """Return the pool dict of ``loop``, creating it on first use.

        Entries of closed loops are dropped when a new loop is seen.

        Args:
            loop: The running event loop of the calling thread.

        Returns:
            Mutable mapping from pool key to connection pool.
        """
        pools = self.pools.get(loop, None)
        if pools is None:
            for stale in [old for old in self.pools if old.is_closed()]:
                del self.pools[stale]
            pools = {}
            self.pools[loop] = pools
        return pools


_TRANSPORTS: "weakref.WeakSet[RedisClientTransport]" = weakref.WeakSet()


def _reset_pools_after_fork():
    """Drop every pool inherited from the parent process."""
    for transport in list(_TRANSPORTS):
        transport.reset_pools()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_pools_after_fork)


@register_plugin(PluginType.CLIENT_TRANSPORT, REDIS_NAME, init_pos=PluginInitPos.WORKER)
class RedisClientTransport(BaseClientTransport):
    """
    Transport implementation for Redis connections in the tRPC framework.
    Handles connection pooling, command, pipeline, and pubsub operations.
    """

    def __init__(self):
        """
        Initialize the RedisClientTransport with the default idle timeout and an empty pool cache.
        """
        super().__init__()
        self.idle_timeout = IDLE_TIMEOUT_DEFAULT
        self._loop_pools = _LoopPools()
        _TRANSPORTS.add(self)

    def reset_pools(self):
        """
        Forget every cached connection pool, in all threads.
        """
        self._loop_pools = _LoopPools()

    async def _process_pipeline(self, ctx: TrpcContext, pool: redis.ConnectionPool):
        """
        Process pipeline requests and return a pipe object.

        Args:
            ctx (TrpcContext): The context object.
            pool (redis.ConnectionPool): The connection pool.

        Returns:
            str: An empty string (pipe object is set in response).
        """
        pipe = await self.get_pipe(pool)
        ctx.msg.set_client_rsp_head(Response(pipe))
        return ""

    async def _process_command(self, ctx: TrpcContext, pool: redis.ConnectionPool):
        """
        Process command requests and return a response object.

        Args:
            ctx (TrpcContext): The context object.
            pool (redis.ConnectionPool): The connection pool.

        Returns:
            Any: The reply from Redis.
        """
        begin = time.time() * 1000
        req: Request = ctx.msg.client_req_head
        timeout = None
        if ctx.deadline > 0:
            remain = (ctx.deadline - begin) / 1000.0
            if remain <= 0:
                raise ClientTimeoutError("reach deadline")
            timeout = remain
        try:
            reply = await asyncio.wait_for(self.get_response(pool, req), timeout)
            ctx.msg.set_client_rsp_head(Response(reply))
        except asyncio.TimeoutError as exc:
            raise ClientTimeoutError(f"redis client timeout {timeout}, command {req.command} args {req.args}") from exc
        except TypeError as exc:
            raise ClientEncodeError(f"redis client args error {exc}, command {req.command} args {req.args}") from exc
        except redis_exceptions.ResponseError as exc:
            raise ClientDecodeError(f"redis client reply error {exc} command {req.command} args {req.args}") from exc
        except redis_exceptions.RedisError as exc:
            raise ClientInvokeError(f"redis client protocol error {exc} command {req.command} args {req.args}") from exc
        except Exception as exc:  # pylint: disable=broad-except
            raise ClientInvokeError(f"redis client unknown error {exc} command {req.command} args {req.args}") from exc
        return reply

    async def _process_pubsub(self, ctx: TrpcContext, pool: redis.ConnectionPool):
        """
        Process pubsub requests and return a pubsub object.

        Args:
            ctx (TrpcContext): The context object.
            pool (redis.ConnectionPool): The connection pool.

        Returns:
            str: An empty string (pubsub object is set in response).
        """
        req: Request = ctx.msg.client_req_head
        async with redis.Redis(connection_pool=pool) as conn:
            pubsub = conn.pubsub()
            await pubsub.subscribe(*req.args, **req.kwargs)
            ctx.msg.set_client_rsp_head(Response(pubsub))
            return ""

    async def round_trip(self, ctx: TrpcContext, _: bytes, options: RoundTripOptions):
        """
        Send a request to Redis and return the response or pipe object.

        Args:
            ctx (TrpcContext): The client context.
            _ (bytes): The request data (unused).
            options (RoundTripOptions): The round trip options.

        Returns:
            Any: The response or pipe object, depending on the request mode.
        """
        msg = ctx.msg
        req: Request = msg.client_req_head
        address = options.address
        redis_config = getattr(options, REDIS_NAME, {})
        pool = await self.get_conn(address, options.password, redis_config)
        if req.mode == RequestMode.PIPELINE:
            return await self._process_pipeline(ctx, pool)
        elif req.mode == RequestMode.COMMAND:
            return await self._process_command(ctx, pool)
        elif req.mode == RequestMode.PUBSUB:
            return await self._process_pubsub(ctx, pool)
        else:
            raise ClientInvokeError(f"redis client unknown mode {req.mode}")

    @staticmethod
    def _set_default(key: str, value: Any, redis_config: Dict, force: bool = False):
        """
        Set a default value in the redis_config dictionary.

        Args:
            key (str): The configuration key.
            value (Any): The value to set.
            redis_config (Dict): The configuration dictionary.
            force (bool): If True, overwrite existing value.
        """
        if force:
            redis_config[key] = value
        elif key not in redis_config:
            redis_config[key] = value

    async def get_conn(self, address: Tuple[str, int], password: Optional[str],
                       redis_config: Dict) -> redis.ConnectionPool:
        """
        Get or create a Redis connection pool for the running event loop.

        Lookup and creation do not await, so no lock is needed within one loop;
        other threads use their own cache.

        Args:
            address (Tuple[str, int]): The address tuple (host, port).
            password (Optional[str]): The password for Redis.
            redis_config (Dict): The Redis configuration dictionary. Not modified.

        Returns:
            redis.ConnectionPool: The connection pool object.
        """
        redis_pool = self._loop_pools.for_loop(asyncio.get_running_loop())
        key = f"{address[0]}:{address[1]}:{password or 'none'}"
        conn_pool = redis_pool.get(key, None)
        if conn_pool is not None:
            return conn_pool
        pool_config = dict(redis_config or {})
        self._set_default(SOCKET_CONNECT_TIMEOUT, self.idle_timeout, pool_config)
        self._set_default(MAX_CONNECTIONS, MAX_CONNECTIONS_DEFAULT, pool_config)
        self._set_default(DECODE_RESPONSES, DECODE_RESPONSES_DEFAULT, pool_config)
        self._set_default(HOST, address[0], pool_config, force=True)
        self._set_default(PORT, address[1], pool_config, force=True)
        if password is not None:
            self._set_default(PASSWORD, password, pool_config, force=True)
        conn_pool = redis.ConnectionPool(**pool_config)
        redis_pool[key] = conn_pool
        return conn_pool

    @staticmethod
    async def get_response(pool: redis.ConnectionPool, req: Request) -> Any:
        """
        Get the response data from Redis.

        Args:
            pool (redis.ConnectionPool): The connection pool.
            req (Request): The request object.

        Returns:
            Any: The response data.
        """
        async with redis.Redis(connection_pool=pool) as conn:
            return await conn.execute_command(req.command, *req.args, **req.kwargs)

    @staticmethod
    async def get_pipe(pool: redis.ConnectionPool) -> Any:
        """
        Get a pipeline object from Redis.

        Args:
            pool (redis.ConnectionPool): The connection pool.

        Returns:
            Any: The pipeline object.
        """
        async with redis.Redis(connection_pool=pool) as conn:
            pipe = conn.pipeline()
            return pipe
