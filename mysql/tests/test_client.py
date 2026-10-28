# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Tests for the tRPC ``Client`` with the framework invoke mocked out."""

from typing import Any
from typing import List

import pytest
from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.codec import get_message

from trpc_mysql import Response
from trpc_mysql.client import Client

# Rebuilt for each test so cases do not share a client.
CLIENT = None


def setup_module():
    """No module fixture. Clients are created per test."""
    pass


def teardown_module():
    """No module fixture to tear down."""
    pass


def setup_function():
    """Point a client at a placeholder URI. Invoke is mocked, so it is never dialed."""
    db_uri = "mysql://user:psw@ip:port/db_auto_flow?charset=utf8"
    options = [
        Options().set_target(db_uri)
    ]
    global CLIENT
    CLIENT = Client("mysql", options)


def teardown_function():
    """Drop the client built for this test."""
    global CLIENT
    CLIENT.close()
    CLIENT = None


def get_client() -> Client:
    """Return the client built in ``setup_function``.

    Returns:
        The module-level client.
    """
    return CLIENT


async def _mock_client_invoke(ctx: TrpcContext, req: Any, rsp_cls: Any, options: List) -> Any:
    """Pretend the framework invoke stored a successful response head.

    Args:
        ctx: Context whose message receives the response head.
        req: Unused request body.
        rsp_cls: Class returned to the framework. Not the value ``Client`` reads.
        options: Unused option callables.

    Returns:
        An instance of ``rsp_cls``. ``Client`` ignores it and reads the head.
    """
    message = get_message(ctx)
    resp = Response.build(0, "", True)
    message.set_client_rsp_head(resp)
    return rsp_cls()


def mock_client_invoke(mocker):
    """Replace ``TrpcClient.invoke`` with ``_mock_client_invoke``.

    Args:
        mocker: pytest-mock fixture.
    """
    mocker.patch.object(get_client().client, "invoke", new=_mock_client_invoke)


@pytest.mark.asyncio
async def test_fetchone(mocker):
    """``fetchone`` returns the boolean stored on the mocked response head."""
    mock_client_invoke(mocker)

    context = TrpcContext()
    resp = await get_client().fetchone(context, "")
    assert resp


@pytest.mark.asyncio
async def test_fetchall(mocker):
    """``fetchall`` returns the boolean stored on the mocked response head."""
    mock_client_invoke(mocker)
    context = TrpcContext()
    resp = await get_client().fetchall(context, "")
    assert resp


@pytest.mark.asyncio
async def test_execute(mocker):
    """``execute`` returns the boolean stored on the mocked response head."""
    mock_client_invoke(mocker)
    context = TrpcContext()
    resp = await get_client().execute(context, "")
    assert resp


@pytest.mark.asyncio
async def test_transaction(mocker):
    """``transaction`` returns the boolean stored on the mocked response head."""
    mock_client_invoke(mocker)
    context = TrpcContext()
    resp = await get_client().transaction(context, "")
    assert resp


@pytest.mark.asyncio
async def test_rpc_name(mocker):
    """The callee RPC name is ``/{service}/{mode}``, not a body path."""
    rpc_names = []

    async def _invoke(ctx, req, rsp_cls, options):
        rpc_names.append(get_message(ctx).client_rpc_name)

    mocker.patch.object(get_client().client, "invoke", new=_invoke)
    await get_client().fetchone(TrpcContext(), "")
    assert rpc_names == ["/mysql/fetchone"]


def test_response_defaults():
    """A bare ``Response`` is unset until ``build`` overwrites it."""
    resp = Response()
    assert resp.code == -1
    assert resp.message == "empty"
    assert resp.result is None
