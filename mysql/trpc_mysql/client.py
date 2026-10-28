# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""tRPC-facing MySQL client.

This client does not open sockets itself. It fills a ``Request``, invokes the
framework client, and reads the row or write result back from
``client_rsp_head``. The transport owns the ``aiomysql`` pools.
"""

from typing import Callable
from typing import List
from typing import Text

from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Client as TrpcClient
from trpc_sdk.client import Options
from trpc_sdk.codec import clone_client_message
from trpc_sdk.log import logger
from trpc_sdk.serialization import SerializationType

from .constants import MYSQL_NAME
from .constants import Mode
from .types import Request
from .types import Response


class Client:
    """MySQL client that rides on the tRPC client stack.

    Attributes:
        client: Framework client that performs the actual invoke.
        service_name: Callee name put on the message and used to load config
            when no option sets a target.
        options: Option callables passed to every invoke, including the MySQL
            protocol defaults.
        database_uri: Target URI resolved once at construction.
    """

    def __init__(self, service_name: Text, options: List[Callable] = None):
        """Build a client for one backend service.

        Args:
            service_name: Callee service name. Also the key passed to
                ``load_client_config`` when ``options`` do not set a target.
            options: Callables such as ``Options().set_target(uri)``. The URI
                looks like ``dsn://user:password@host:port/database``; the
                ``dsn`` scheme selects the framework selector that passes the
                whole DSN to the transport. Defaults to ``None``.
        """
        self.client = TrpcClient()
        self.service_name = service_name
        self.options = self._build_options(service_name, options)

    def _build_options(self, service_name: Text, options: List[Callable]):
        """Merge MySQL defaults with caller options and capture the target.

        Protocol, noop serialization, and disabling the service router are
        always applied first. Caller options run after them, so an explicit
        target wins. When the merged options still have no target, the URI is
        loaded from framework config for ``service_name``.

        The object returned is the list of callables, not the filled
        ``Options`` instance. Invoke applies that list again later.

        Args:
            service_name: Config key used only when no target was set.
            options: Extra option callables. May be ``None``.

        Returns:
            Option callables stored on this client.
        """
        _options = [
            Options()
            .set_protocol(MYSQL_NAME)
            .set_serialization_type(SerializationType.Noop)
            .set_disable_service_router(True),
        ]

        if options:
            _options.extend(options)

        client_options = Options()
        for option in _options:
            option(client_options)

        if not client_options.target:
            client_options.load_client_config(service_name)

        self.database_uri = client_options.target

        return _options

    async def fetchone(self, context: TrpcContext, query: Text, args=None):
        """Run a query and return one row.

        Args:
            context: Caller context. A client message is cloned from it.
            query: SQL text.
            args: Bound parameters. Defaults to ``None``.

        Returns:
            The ``result`` stored on the response head, or ``None``.
        """
        return await self._invoke(context, Request(Mode.FETCHONE, query, args))

    async def fetchall(self, context: TrpcContext, query: Text, args=None, size=None):
        """Run a query and return many rows.

        Args:
            context: Caller context. A client message is cloned from it.
            query: SQL text.
            args: Bound parameters. Defaults to ``None``.
            size: Maximum rows. ``None`` asks the driver for every row.

        Returns:
            The ``result`` stored on the response head, or ``None``.
        """
        return await self._invoke(context, Request(Mode.FETCHALL, query, args, size))

    async def execute(self, context: TrpcContext, query: Text, args=None,
                      ret_affected_num: bool = False):
        """Run one statement outside an explicit transaction.

        Args:
            context: Caller context. A client message is cloned from it.
            query: SQL text.
            args: Bound parameters. Defaults to ``None``.
            ret_affected_num: When true, return the affected-row count
                instead of ``lastrowid``. Defaults to ``False``.

        Returns:
            The ``result`` stored on the response head, or ``None``.
        """
        return await self._invoke(context, Request(Mode.EXECUTE, query, args,
                                                   ret_affected_num=ret_affected_num))

    async def transaction(self, context: TrpcContext, query: Text, args=None,
                          ret_affected_num: bool = False):
        """Run one statement and roll it back if the driver raises.

        Args:
            context: Caller context. A client message is cloned from it.
            query: SQL text.
            args: Bound parameters. Defaults to ``None``.
            ret_affected_num: When true, return the affected-row count
                instead of ``lastrowid``. Defaults to ``False``.

        Returns:
            The ``result`` stored on the response head, or ``None``.
        """
        return await self._invoke(context, Request(Mode.TRANSACTION, query, args,
                                                   ret_affected_num=ret_affected_num))

    async def _invoke(self, context: TrpcContext, request: Request, response: Response = None):
        """Send ``request`` through the framework client.

        Compression is cleared and serialization is set to ``-1`` because the
        codec does not encode a body. The RPC name is
        ``/{service_name}/{mode}`` and is only a callee label. The URI copied
        onto ``request`` is what the transport uses to find a pool.

        ``response`` is not sent. The value returned is ``result`` from
        ``client_rsp_head`` after invoke, when that head is a ``Response``.

        Args:
            context: Caller context cloned before the message is filled in.
            request: SQL call. ``uri`` is overwritten with ``database_uri``.
            response: Ignored. Kept so existing callers can pass a placeholder.

        Returns:
            ``Response.result`` from the response head, or ``None`` when the
            head is missing or not a ``Response``.
        """
        context, message = clone_client_message(context)
        message.set_compress_type(0)
        message.set_serialization_type(-1)
        message.set_callee_service_name(self.service_name)
        message.set_client_rpc_name(f"/{self.service_name}/{request.mode.value}")

        request.uri = self.database_uri
        message.set_client_req_head(request)

        logger.debug("Invoke trpc client with request: %s", request.mode)
        await self.client.invoke(context, request, Response, self.options)

        resp = message.client_rsp_head
        if resp and isinstance(resp, Response):
            return resp.result

        return None

    def close(self):
        """Release nothing.

        Pools live on ``MysqlClientTransport``, not on this object, so close
        is a no-op kept for callers that treat the client as a resource.
        """
        pass
