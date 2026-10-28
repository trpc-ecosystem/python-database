# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""MySQL client transport.

One process-wide plugin holds a map of ``MySqlClient`` pools per thread and
event loop, because an ``aiomysql`` pool is bound to the loop it was created
on. Within a loop the key is the md5 of the request URI, so repeated calls to
the same database share a pool. A forked child starts with no pools, and a
stopping serving loop closes its own pools. Importing this module registers
the transport under ``MYSQL_NAME`` and the serving-loop stop hook.
"""

import asyncio
import os
import threading
import time
import weakref
from typing import Dict

from trpc_sdk.abc import BaseClientTransport
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.exceptions import ClientEncodeError
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.log import logger
from trpc_sdk.plugin import register_plugin
from trpc_sdk.runtime import HandlerThreadInfo
from trpc_sdk.runtime import register_handler_thread_hook
from trpc_sdk.transport import RoundTripOptions

from . import utils
from .constants import MYSQL_NAME
from .mysql_client import MySqlClient
from .types import DatabaseUrl
from .types import Request
from .types import Response


class _LoopClients(threading.local):
    """Per-thread map from event loop to that loop's ``MySqlClient`` pools.

    Loops are keyed by object rather than ``id()`` so a closed loop's id
    cannot be reused to return a pool bound to the dead loop.
    """

    def __init__(self):
        """Start the calling thread with no pools."""
        super().__init__()
        self.clients: Dict[asyncio.AbstractEventLoop, Dict[str, MySqlClient]] = {}

    def for_loop(self, loop: asyncio.AbstractEventLoop) -> Dict[str, MySqlClient]:
        """Return the client dict of ``loop``, creating it on first use.

        Entries of closed loops are dropped when a new loop is seen.

        Args:
            loop: The running event loop of the calling thread.

        Returns:
            Mutable mapping from ``md5(uri)`` to ``MySqlClient``.
        """
        clients = self.clients.get(loop, None)
        if clients is None:
            for stale in [old for old in self.clients if old.is_closed()]:
                del self.clients[stale]
            clients = {}
            self.clients[loop] = clients
        return clients


_TRANSPORTS: "weakref.WeakSet[MysqlClientTransport]" = weakref.WeakSet()


def _reset_clients_after_fork():
    """Drop every pool inherited from the parent process."""
    for transport in list(_TRANSPORTS):
        transport.reset_clients()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_clients_after_fork)


async def _close_loop_clients(info: HandlerThreadInfo):
    """Close the stopping serving loop's pools while that loop still runs.

    Args:
        info: The serving loop being stopped. Unused.
    """
    for transport in list(_TRANSPORTS):
        await transport.close_loop_clients()


register_handler_thread_hook(on_stop=_close_loop_clients)


@register_plugin(PluginType.CLIENT_TRANSPORT, MYSQL_NAME, init_pos=PluginInitPos.WORKER)
class MysqlClientTransport(BaseClientTransport):
    """Transport that turns a ``Request`` head into an ``aiomysql`` call.

    Attributes:
        _clients: Per-thread, per-loop pools keyed by ``md5(request.uri)``.
    """

    def __init__(self):
        """Start with an empty pool cache."""
        super().__init__()
        self._clients = _LoopClients()
        _TRANSPORTS.add(self)

    def reset_clients(self):
        """Forget every cached pool without closing it.

        Used in a forked child, whose inherited pools belong to the parent.
        """
        self._clients = _LoopClients()

    async def close_loop_clients(self):
        """Close and forget the running loop's pools; a failed close is logged."""
        clients = self._clients.clients.pop(asyncio.get_running_loop(), {})
        for client in clients.values():
            try:
                await client.close()
            except Exception as error:  # pylint: disable=broad-except
                logger.warning(f"close mysql pool error: {error}")

    async def round_trip(self, ctx: TrpcContext, req_data: bytes, options: RoundTripOptions):
        """Execute the SQL on ``client_req_head`` and store a ``Response``.

        The body in ``req_data`` is ignored. On success the response head
        carries the driver result and this method returns ``b"success"``.
        Timeout, ``TypeError``, and any other error are re-raised as client
        errors, so the trailing ``b"fail"`` return is not used.

        Args:
            ctx: Context whose message holds the ``Request``. ``deadline`` is
                milliseconds since the epoch; ``0`` means no deadline.
            req_data: Unused body bytes.
            options: Round-trip options. The pool lookup does not read them.

        Returns:
            ``b"success"`` after the response head is set.

        Raises:
            ClientTimeoutError: The deadline has passed or the call exceeds it.
            ClientEncodeError: Building or invoking the driver raised
                ``TypeError``.
            ClientInvokeError: Any other driver or client failure.
        """
        message = ctx.msg
        timeout = self._timeout(ctx)

        # Original request lives on the head, not in req_data.
        request = message.client_req_head

        try:
            mysql_client = self._mysql_client(request, message.remote_addr, options)

            result = await asyncio.wait_for(self._invoke(mysql_client, request), timeout)
            message.set_client_rsp_head(Response.build(0, "success", result))

            return b"success"
        except asyncio.TimeoutError:
            raise ClientTimeoutError("Invoke mysql client timeout")
        except TypeError as error:
            raise ClientEncodeError("Invoke mysql client encode error")
        except Exception as error:
            raise ClientInvokeError(str(error))

        return b"fail"

    def _mysql_client(self, request: Request, uri: str, options: RoundTripOptions):
        """Return the running loop's pool for ``request.uri``, creating it on a miss.

        Must be called on the loop that will use the pool. The ``uri`` argument and ``options`` are not the cache key. Callers
        pass ``message.remote_addr`` as ``uri``, but the pool is selected from
        ``request.uri``, which the client copied from its resolved target.

        Args:
            request: Head whose ``uri`` selects the pool.
            uri: Unused address kept for the log line's historical signature.
            options: Unused round-trip options.

        Returns:
            Existing or newly constructed ``MySqlClient``. The new client's
            pool stays closed until the first statement.
        """
        logger.debug(f"client for mysql://{uri}")

        uri = request.uri
        unique_key = utils.md5(uri)

        clients = self._clients.for_loop(asyncio.get_running_loop())
        _client = clients.get(unique_key, None)
        if _client is not None:
            return _client

        _client = MySqlClient(DatabaseUrl.make_url(uri))
        clients[unique_key] = _client
        return _client

    @staticmethod
    async def _invoke(client: MySqlClient, request: Request):
        """Dispatch one request to ``MySqlClient.invoke``.

        Args:
            client: Pool selected for this URI. ``None`` skips the call.
            request: SQL call. ``size`` and ``uri`` come from ``params``,
                and ``ret_affected_num`` is added on top.

        Returns:
            Driver result, or ``None`` when ``client`` is missing.
        """
        if not client:
            return None
        params = request.params()
        params["ret_affected_num"] = request.ret_affected_num
        return await client.invoke(request.mode, request.query, request.args, **params)

    @staticmethod
    def _timeout(ctx: TrpcContext):
        """Convert ``ctx.deadline`` into a second count for ``wait_for``.

        Args:
            ctx: Context whose ``deadline`` is milliseconds since the epoch.
                A non-positive deadline means the caller set no limit.

        Returns:
            Remaining seconds, or ``None`` when there is no deadline.

        Raises:
            ClientTimeoutError: The deadline is already in the past.
        """
        timeout = None
        if ctx.deadline > 0:
            remain = (ctx.deadline - time.time() * 1000) / 1000.0
            if remain <= 0:
                raise ClientTimeoutError("reach deadline")
            timeout = remain
        return timeout
