# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Tests for ``MysqlClientTransport.round_trip`` with ``MySqlClient`` patched."""

import asyncio
import multiprocessing
import os
import threading
import time

import pytest
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import TrpcMessage
from trpc_sdk.transport import RoundTripOptions

from trpc_mysql.constants import Mode
from trpc_mysql.transport import MysqlClientTransport
from trpc_mysql.transport import _close_loop_clients
from trpc_mysql.types import Request


@pytest.mark.asyncio
async def test_client(mocker):
    """A round trip with a future deadline does not escape the test.

    ``MySqlClient`` is patched, so no socket is opened. Any error from the
    stub is swallowed here; the test only checks that the call can be issued.
    """
    mocker.patch("trpc_mysql.mysql_client.MySqlClient")

    context = TrpcContext()
    message = TrpcMessage()

    request = Request(mode=Mode.FETCHONE, query="", uri="mysql://user:psw@ip:3306/db_auto_flow")
    message.set_client_req_head(request)
    context.msg = message
    context.deadline = time.time() * 1000 + 100

    req_data = b""
    options = RoundTripOptions()

    try:
        transport = MysqlClientTransport()
        await transport.round_trip(context, req_data, options)
    except Exception:  # pylint: disable=broad-except
        pass


URI = "mysql://user:psw@ip:3306/db_auto_flow"


def _request(uri: str = URI) -> Request:
    """Build a ``FETCHONE`` request head for ``uri``."""
    return Request(mode=Mode.FETCHONE, query="", uri=uri)


@pytest.mark.asyncio
async def test_same_loop_reuses_client():
    """Two lookups on one loop return the same pool, other URIs get their own."""
    transport = MysqlClientTransport()
    first = transport._mysql_client(_request(), "", None)
    assert transport._mysql_client(_request(), "", None) is first
    assert transport._mysql_client(_request(URI + "2"), "", None) is not first


def test_new_loop_gets_new_client():
    """A second ``asyncio.run`` does not reuse a pool bound to the closed loop."""
    transport = MysqlClientTransport()

    async def lookup():
        return transport._mysql_client(_request(), "", None)

    first = asyncio.run(lookup())
    second = asyncio.run(lookup())
    assert first is not second
    assert len(transport._clients.clients) == 1


def test_threads_get_own_clients():
    """Concurrent handler threads, each on its own loop, never share a pool."""
    transport = MysqlClientTransport()
    results = {}
    errors = []

    async def lookup():
        found = []
        for _ in range(20):
            found.append(transport._mysql_client(_request(), "", None))
            await asyncio.sleep(0)
        return found

    def worker(index):
        try:
            results[index] = asyncio.run(lookup())
        except Exception as error:  # pylint: disable=broad-except
            errors.append(error)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    per_thread = [found[0] for found in results.values()]
    assert len(set(map(id, per_thread))) == 4
    for found in results.values():
        assert all(item is found[0] for item in found)


def test_close_loop_clients_only_closes_running_loop(mocker):
    """The stop hook closes the stopping loop's pools and leaves others alone."""
    transport = MysqlClientTransport()
    closed = []

    async def fake_close(self):
        closed.append(self)

    mocker.patch("trpc_mysql.transport.MySqlClient.close", new=fake_close)

    async def lookup():
        return transport._mysql_client(_request(), "", None)

    other_loop = asyncio.new_event_loop()
    try:
        other = other_loop.run_until_complete(lookup())

        async def lookup_and_close():
            mine = transport._mysql_client(_request(), "", None)
            await _close_loop_clients(None)
            return mine

        mine = asyncio.run(lookup_and_close())
        assert closed == [mine]
        assert other_loop in transport._clients.clients
        assert other is not mine
    finally:
        other_loop.close()


def _child_cache_size(transport, queue):
    """Report how many loops the inherited transport still caches."""
    queue.put(len(transport._clients.clients))


@pytest.mark.skipif(not hasattr(os, "register_at_fork"), reason="needs fork hooks")
def test_fork_drops_parent_clients():
    """A forked child starts with no pools from the parent."""
    transport = MysqlClientTransport()

    async def lookup():
        return transport._mysql_client(_request(), "", None)

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(lookup())
        assert len(transport._clients.clients) == 1

        ctx = multiprocessing.get_context("fork")
        queue = ctx.Queue()
        process = ctx.Process(target=_child_cache_size, args=(transport, queue))
        process.start()
        process.join(10)
        assert queue.get(timeout=5) == 0
    finally:
        loop.close()
