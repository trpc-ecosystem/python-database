# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""
Redis client module for accessing Redis servers via the tRPC framework.

This module provides a Client class that wraps the tRPC client for Redis operations, supporting command, pipeline,
and pubsub modes. It also provides utilities for configuring Redis client options.

Usage:
    from trpc_redis import Client as RedisClient
    client = RedisClient("redis", opts)
    await client.execute_command(ctx, "set", ["key", "value"])
    await client.create_pipeline(ctx)
    await client.create_pubsub(ctx, "channel")
"""

import threading
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

from redis.asyncio.client import Pipeline
from redis.asyncio.client import PubSub
from redis.typing import ResponseT as RedisResponse
from redis.typing import KeyT as RedisKey
from redis.typing import ChannelT as RedisChannelT

from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Client as TrpcClient
from trpc_sdk.client import Options
from trpc_sdk.codec import clone_client_message
from trpc_sdk.config import TrpcConfig
from trpc_sdk.serialization import SerializationType
from ._constants import PASSWORD
from ._constants import REDIS_NAME
from ._constants import RequestMode
from ._types import Request
from ._types import Response


def create_rsp_object():
    """
    Construct a new response object. This returns None because the transport will override it.

    Returns:
        None
    """
    return None


def redis_config_option(redis_config: Dict) -> Callable:
    """
    Option setter for specifying Redis backend configuration.

    Args:
        redis_config (Dict): Redis configuration dictionary.

    Returns:
        Callable: Option setter function.
    """

    def func(opts: Options):
        setattr(opts.call_options, REDIS_NAME, redis_config)

    return func


def get_options(opts: List) -> List:
    """
    Get options for the Redis client, including required Redis fields.

    Args:
        opts (List): Additional options to include.

    Returns:
        List: List of options for the client.
    """
    needed = (Options()
              .set_protocol(REDIS_NAME)
              .set_serialization_type(SerializationType.Noop)
              .set_disable_service_router(True))
    options = [needed]
    if opts:
        options.extend(opts)
    return options


class Client:
    """
    Redis client wrapper for the tRPC framework.

    This class wraps the tRPC client to provide Redis command, pipeline, and pubsub operations.

    Attributes:
        client (TrpcClient): The underlying tRPC client object.
        __service_name (str): The service name.
        __opts (List): List of client options.
    """

    def __init__(self, name: str, opts: Optional[List] = None):
        """
        Initialize the Redis client.

        Args:
            name (str): Service name.
            opts (Optional[List]): List of options.
        """
        self.client = TrpcClient()
        self.__service_name = name
        self.__opts = get_options(opts or [])
        self._is_load_config = False
        self._config_lock = threading.Lock()

    @property
    def service_name(self):
        """
        Get the service name.

        Returns:
            str: The service name.
        """
        return self.__service_name

    def __getattr__(self, item):
        """
        Override attribute access to provide dynamic command methods.

        Args:
            item (str): The attribute name (command name).

        Returns:
            Callable: An async function that calls execute_command(ctx, item, args).
        """

        async def func(ctx, *args, **kwargs):
            return await self.execute_command(ctx, item, *args, **kwargs)

        return func

    def _get_config(self):
        """
        Load configuration from global config if available.

        Safe to call from several handler threads at once: the options are
        appended once, before the loaded flag is set.
        """
        if self._is_load_config:
            return
        with self._config_lock:
            if self._is_load_config:
                return
            self.__opts.extend(self._config_options())
            self._is_load_config = True

    def _config_options(self) -> List:
        """
        Build the options that the global config adds for this service.

        Returns:
            List: Password and Redis backend options; empty when not configured.
        """
        parser = TrpcConfig.config_parser()
        if parser is None:
            return []
        client_redis_config_dict = {}
        client_conf_dict = parser.get("client", {}) or {}
        services = client_conf_dict.get('service', []) or []
        for service in services:
            if service.get('name') == self.__service_name:
                client_redis_config_dict = service
                break
        if not client_redis_config_dict:
            return []
        options = []
        password = client_redis_config_dict.get(PASSWORD, None)
        if password:
            options.append(Options().set_password(password))
        redis_config = client_redis_config_dict.get(REDIS_NAME, {})
        if redis_config:
            options.append(redis_config_option(redis_config))
        return options

    async def execute_command(self, ctx: TrpcContext, cmd: str, *args, **kwargs) -> bytes:
        """
        Execute a Redis command via tRPC.

        Args:
            ctx (TrpcContext): The client context.
            cmd (str): The Redis command.
            args (Optional[List]): Arguments for the command.

        Returns:
            bytes: The reply as bytes.
        """
        self._get_config()
        req = Request(command=cmd, mode=RequestMode.COMMAND, args=args, kwargs=kwargs)
        ctx, msg = clone_client_message(ctx)
        msg.set_compress_type(0)
        msg.set_serialization_type(-1)
        msg.set_client_rpc_name(f"/{self.__service_name}/{cmd}")
        msg.set_callee_service_name(self.__service_name)
        msg.set_client_req_head(req)

        await self.client.invoke(ctx, req, create_rsp_object, self.__opts)
        return msg.client_rsp_head.reply

    async def delete(self, ctx: TrpcContext, *names: RedisKey) -> RedisResponse:
        """
        Delete a key from Redis.
        """
        return await self.execute_command(ctx, "DEL", *names)

    async def create_pipeline(self, ctx: TrpcContext) -> Pipeline:
        """
        Create a Redis pipeline via tRPC.

        Args:
            ctx (TrpcContext): The client context.

        Returns:
            Pipeline: The pipeline object.
        """
        self._get_config()
        req = Request(mode=RequestMode.PIPELINE)
        resp = Response(None)
        ctx, msg = clone_client_message(ctx)
        msg.set_compress_type(0)
        msg.set_serialization_type(-1)
        msg.set_client_rpc_name(f"/{self.__service_name}/pipeline")
        msg.set_callee_service_name(self.__service_name)
        msg.set_client_req_head(req)
        msg.set_client_rsp_head(resp)

        await self.client.invoke(ctx, req, create_rsp_object, self.__opts)
        return msg.client_rsp_head.reply

    async def create_pubsub(self, ctx: TrpcContext, *args: RedisChannelT, **kwargs: Callable) -> PubSub:
        """
        Create a Redis pubsub object via tRPC and subscribe to the given channels.

        Args:
            ctx (TrpcContext): The client context.
            *args (RedisChannelT): Channels to subscribe to.
            **kwargs (Callable): Channels mapped to their message handlers.

        Returns:
            PubSub: The pubsub object.
        """
        self._get_config()
        req = Request(mode=RequestMode.PUBSUB, args=args, kwargs=kwargs)
        resp = Response(None)
        ctx, msg = clone_client_message(ctx)
        msg.set_compress_type(0)
        msg.set_serialization_type(-1)
        msg.set_client_rpc_name(f"/{self.__service_name}/pubsub")
        msg.set_callee_service_name(self.__service_name)
        msg.set_client_req_head(req)
        msg.set_client_rsp_head(resp)

        await self.client.invoke(ctx, req, create_rsp_object, self.__opts)
        return msg.client_rsp_head.reply
