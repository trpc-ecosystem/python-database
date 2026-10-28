# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Test redis client module"""

import threading
import time
from typing import Any, List

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Client as TrpcClient
from trpc_sdk.client import Options
from trpc_sdk.codec import get_message

from trpc_redis._client import Client as RedisClient
from trpc_redis._client import create_rsp_object
from trpc_redis._constants import RequestMode
from trpc_redis._types import Request
from trpc_redis._types import Response


@pytest.fixture
def mock_response(monkeypatch):
    """Mock response"""

    async def mock_invoke(ctx: TrpcContext, req: Any, rsp_cls: Any, options: List):  # pylint: disable=unused-argument
        msg = get_message(ctx)
        resp = Response(None)
        msg.set_client_rsp_head(resp)
        return

    trpc_client = TrpcClient()
    setattr(trpc_client, "invoke", mock_invoke)


# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
@pytest.mark.asyncio
@patch("redis.asyncio.Redis.execute_command", new_callable=AsyncMock)
async def test_set(mock_execute_command):
    mock_execute_command.return_value = b"OK"
    target = "ip://127.0.0.1:6379"
    password = "test"
    opts = [Options().set_target(target).set_password(password)]
    redis_client = RedisClient("redis", opts)
    ctx = TrpcContext()
    reply = await redis_client.execute_command(ctx, "set", ["key", "data"])
    print(f"set success, reply: {reply}")
    assert reply == b"OK"


# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
@pytest.mark.asyncio
@patch("redis.asyncio.Redis.pipeline")
async def test_pipeline(mock_pipeline):
    # The pipeline mock exposes set and execute directly.
    mock_pipe = MagicMock()
    mock_pipe.set = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=b"OK")
    mock_pipeline.return_value = mock_pipe
    target = "ip://127.0.0.1:6379"
    password = "test"
    opts = [Options().set_target(target).set_password(password)]
    redis_client = RedisClient("redis", opts)
    ctx = TrpcContext()
    pipe = await redis_client.create_pipeline(ctx)
    pipe.set(name="test2", value="test_value")
    pipe.set(name="test3", value="test_valuexxxx")
    reply = await pipe.execute()
    assert reply == b"OK"


def test_request():
    """Test request"""
    req = Request(mode=RequestMode.COMMAND)
    assert req.command is None
    assert req.args == tuple()
    assert req.mode == RequestMode.COMMAND
    req.set_pipeline(True)
    assert req.mode == RequestMode.PIPELINE
    assert create_rsp_object() is None


def test_response():
    """Test response"""
    rsp = Response(None)
    assert rsp.reply is None
    rsp.reply = b"OK"
    assert rsp.reply == b"OK"
    rsp = Response(None)
    assert rsp.reply is None


def test_client():
    """Test client"""
    client = RedisClient("xxx")
    assert client.get is not None


class _SlowParser(dict):
    """Config parser whose lookups yield the GIL, to widen the race window."""

    def get(self, key, default=None):
        time.sleep(0.02)
        return super().get(key, default)


def test_get_config_concurrent(monkeypatch):
    """Concurrent first calls append the configured options exactly once."""
    parser = _SlowParser(client={"service": [{"name": "redis_svc", "password": "pwd", "redis": {"db": 1}}]})
    monkeypatch.setattr("trpc_redis._client.TrpcConfig.config_parser", lambda: parser)
    client = RedisClient("redis_svc")
    base_num = len(client._Client__opts)
    thread_num = 8
    barrier = threading.Barrier(thread_num)
    seen = []

    def run():
        barrier.wait()
        client._get_config()
        seen.append(len(client._Client__opts))

    threads = [threading.Thread(target=run) for _ in range(thread_num)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(client._Client__opts) == base_num + 2
    assert seen == [base_num + 2] * thread_num
