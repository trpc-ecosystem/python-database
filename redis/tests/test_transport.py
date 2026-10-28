# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Test redis transport module"""

import time
import asyncio
import multiprocessing
import threading
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from trpc_sdk.abc import TrpcContext
from trpc_sdk.codec import Message
from trpc_sdk.exceptions import ClientDecodeError
from trpc_sdk.exceptions import ClientEncodeError
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.transport import RoundTripOptions
from trpc_redis._constants import RequestMode
from trpc_redis._transport import RedisClientTransport
from trpc_redis._types import Request


@pytest.fixture(autouse=True)
def mock_redis(monkeypatch):
    """Mock Redis objects"""
    mock_pubsub = MagicMock()
    mock_pubsub.subscribe = AsyncMock()
    mock_pipe = MagicMock()
    mock_redis_instance = MagicMock()
    mock_redis_instance.execute_command = AsyncMock(return_value=b'OK')
    mock_redis_instance.pipeline = MagicMock(return_value=mock_pipe)
    mock_redis_instance.pubsub = MagicMock(return_value=mock_pubsub)
    mock_redis_cls = MagicMock(return_value=mock_redis_instance)
    monkeypatch.setattr('redis.asyncio.Redis', mock_redis_cls)
    # Mock ConnectionPool
    monkeypatch.setattr('redis.asyncio.ConnectionPool', MagicMock())


@pytest.mark.asyncio
def test_process_pipeline(monkeypatch):
    """Test process pipeline"""
    ctx = TrpcContext()
    msg = Message()
    req = Request(command="set", args=["test"], mode=RequestMode.PIPELINE)
    msg.set_client_req_head(req)
    ctx.msg = msg
    redis_transport = RedisClientTransport()
    pool = MagicMock()
    # mock get_pipe
    monkeypatch.setattr(redis_transport, 'get_pipe', AsyncMock(return_value=MagicMock()))
    result = asyncio.run(redis_transport._process_pipeline(ctx, pool))
    assert result == ""


@pytest.mark.asyncio
def test_process_command(monkeypatch):
    """Test process command"""
    ctx = TrpcContext()
    msg = Message()
    req = Request(command="set", args=["test"], mode=RequestMode.COMMAND)
    msg.set_client_req_head(req)
    ctx.msg = msg
    ctx.deadline = time.time() * 1000 + 100
    redis_transport = RedisClientTransport()
    pool = MagicMock()
    # mock get_response
    monkeypatch.setattr(redis_transport, 'get_response', AsyncMock(return_value=b'OK'))
    result = asyncio.run(redis_transport._process_command(ctx, pool))
    assert result == b'OK'


@pytest.mark.asyncio
def test_process_command_exceptions(monkeypatch):
    """Test process command exceptions"""
    ctx = TrpcContext()
    msg = Message()
    req = Request(command="set", args=["test"], mode=RequestMode.COMMAND)
    msg.set_client_req_head(req)
    ctx.msg = msg
    ctx.deadline = time.time() * 1000 + 100
    redis_transport = RedisClientTransport()
    pool = MagicMock()
    # Timeout
    monkeypatch.setattr(redis_transport, 'get_response', AsyncMock(side_effect=asyncio.TimeoutError))
    with pytest.raises(ClientTimeoutError):
        asyncio.run(redis_transport._process_command(ctx, pool))
    # TypeError
    monkeypatch.setattr(redis_transport, 'get_response', AsyncMock(side_effect=TypeError))
    with pytest.raises(ClientEncodeError):
        asyncio.run(redis_transport._process_command(ctx, pool))
    # DecodeError
    from redis import exceptions as redis_exceptions
    monkeypatch.setattr(redis_transport, 'get_response', AsyncMock(side_effect=redis_exceptions.ResponseError("error")))
    with pytest.raises(ClientDecodeError):
        asyncio.run(redis_transport._process_command(ctx, pool))
    # ProtocolError
    monkeypatch.setattr(redis_transport, 'get_response', AsyncMock(side_effect=redis_exceptions.RedisError()))
    with pytest.raises(ClientInvokeError):
        asyncio.run(redis_transport._process_command(ctx, pool))
    # Unknown Exception
    monkeypatch.setattr(redis_transport, 'get_response', AsyncMock(side_effect=Exception("unknown")))
    with pytest.raises(ClientInvokeError):
        asyncio.run(redis_transport._process_command(ctx, pool))


@pytest.mark.asyncio
def test_process_pubsub(monkeypatch):
    """Test process pubsub"""
    ctx = TrpcContext()
    msg = Message()
    req = Request(command="subscribe", args=["test"], mode=RequestMode.PUBSUB)
    msg.set_client_req_head(req)
    ctx.msg = msg
    redis_transport = RedisClientTransport()
    pool = MagicMock()
    # mock pubsub.subscribe
    with patch('redis.asyncio.Redis') as mock_redis_cls:
        mock_redis_instance = MagicMock()
        mock_pubsub = MagicMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_redis_instance.pubsub.return_value = mock_pubsub
        mock_redis_cls.return_value.__aenter__.return_value = mock_redis_instance
        result = asyncio.run(redis_transport._process_pubsub(ctx, pool))
        assert result == ""


@pytest.mark.asyncio
def test_round_trip_pipeline(monkeypatch):
    """Test round trip pipeline"""
    ctx = TrpcContext()
    msg = Message()
    req = Request(command="set", args=["test"], mode=RequestMode.PIPELINE)
    msg.set_client_req_head(req)
    ctx.msg = msg
    options = RoundTripOptions()
    options.set_address(('127.0.0.1', 8000))
    options.password = 'test'
    redis_transport = RedisClientTransport()
    monkeypatch.setattr(redis_transport, 'get_conn', AsyncMock(return_value=MagicMock()))
    monkeypatch.setattr(redis_transport, '_process_pipeline', AsyncMock(return_value=''))
    result = asyncio.run(redis_transport.round_trip(ctx, b'', options))
    assert result == ''


@pytest.mark.asyncio
def test_round_trip_command(monkeypatch):
    """Test round trip command"""
    ctx = TrpcContext()
    msg = Message()
    req = Request(command="set", args=["test"], mode=RequestMode.COMMAND)
    msg.set_client_req_head(req)
    ctx.msg = msg
    options = RoundTripOptions()
    options.set_address(('127.0.0.1', 8000))
    options.password = 'test'
    redis_transport = RedisClientTransport()
    monkeypatch.setattr(redis_transport, 'get_conn', AsyncMock(return_value=MagicMock()))
    monkeypatch.setattr(redis_transport, '_process_command', AsyncMock(return_value='ok'))
    result = asyncio.run(redis_transport.round_trip(ctx, b'', options))
    assert result == 'ok'


@pytest.mark.asyncio
def test_round_trip_pubsub(monkeypatch):
    """Test round trip pubsub"""
    ctx = TrpcContext()
    msg = Message()
    req = Request(command="subscribe", args=["test"], mode=RequestMode.PUBSUB)
    msg.set_client_req_head(req)
    ctx.msg = msg
    options = RoundTripOptions()
    options.set_address(('127.0.0.1', 8000))
    options.password = 'test'
    redis_transport = RedisClientTransport()
    monkeypatch.setattr(redis_transport, 'get_conn', AsyncMock(return_value=MagicMock()))
    monkeypatch.setattr(redis_transport, '_process_pubsub', AsyncMock(return_value='pubsub'))
    result = asyncio.run(redis_transport.round_trip(ctx, b'', options))
    assert result == 'pubsub'


@pytest.mark.asyncio
def test_round_trip_unknown_mode(monkeypatch):
    """Test round trip unknown mode"""
    ctx = TrpcContext()
    msg = Message()
    req = Request(command="set", args=["test"], mode=None)
    req.mode = 'UNKNOWN'  # type: ignore
    msg.set_client_req_head(req)
    ctx.msg = msg
    options = RoundTripOptions()
    options.set_address(('127.0.0.1', 8000))
    options.password = 'test'
    redis_transport = RedisClientTransport()
    monkeypatch.setattr(redis_transport, 'get_conn', AsyncMock(return_value=MagicMock()))
    with pytest.raises(ClientInvokeError):
        asyncio.run(redis_transport.round_trip(ctx, b'', options))


class _FakePool:
    """Stand-in for ``redis.asyncio.ConnectionPool`` that records its kwargs and loop."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.loop = asyncio.get_running_loop()


@pytest.fixture
def fake_pool(monkeypatch):
    """Make every new connection pool a distinct :class:`_FakePool`."""
    monkeypatch.setattr('redis.asyncio.ConnectionPool', _FakePool)
    return _FakePool


@pytest.mark.asyncio
async def test_get_conn(fake_pool):
    """A second call on the same loop returns the cached pool."""
    redis_transport = RedisClientTransport()
    address = ('127.0.0.1', 6379)
    pool = await redis_transport.get_conn(address, 'test', {})
    assert isinstance(pool, fake_pool)
    assert pool.kwargs['host'] == '127.0.0.1'
    assert pool.kwargs['port'] == 6379
    assert pool.kwargs['password'] == 'test'
    assert await redis_transport.get_conn(address, 'test', {}) is pool
    assert await redis_transport.get_conn(address, None, {}) is not pool
    assert await redis_transport.get_conn(('127.0.0.2', 6379), 'test', {}) is not pool


def test_get_conn_new_loop(fake_pool):
    """Each event loop gets its own pool, and closed loops are dropped from the cache."""
    redis_transport = RedisClientTransport()
    address = ('127.0.0.1', 6379)
    pool1 = asyncio.run(redis_transport.get_conn(address, None, {}))
    pool2 = asyncio.run(redis_transport.get_conn(address, None, {}))
    assert pool1 is not pool2
    assert pool1.loop is not pool2.loop
    assert list(redis_transport._loop_pools.pools) == [pool2.loop]


@pytest.mark.asyncio
async def test_get_conn_keeps_config(fake_pool):
    """The caller's redis config is copied, not modified."""
    redis_transport = RedisClientTransport()
    redis_config = {'db': 1}
    pool = await redis_transport.get_conn(('127.0.0.1', 6379), 'pwd', redis_config)
    assert redis_config == {'db': 1}
    assert pool.kwargs['db'] == 1
    assert pool.kwargs['host'] == '127.0.0.1'


def test_get_conn_multithread(fake_pool):
    """Threads sharing one transport each get pools bound to their own loop."""
    redis_transport = RedisClientTransport()
    redis_config = {'db': 0}
    thread_num = 4
    barrier = threading.Barrier(thread_num)
    results = {}
    errors = []

    async def worker(index):
        port = 6000 + index % 2
        first = await redis_transport.get_conn(('127.0.0.1', port), None, redis_config)
        await asyncio.sleep(0)
        second = await redis_transport.get_conn(('127.0.0.1', port), None, redis_config)
        return port, first, second

    def run(index):
        try:
            barrier.wait()
            results[index] = asyncio.run(worker(index))
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(thread_num)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert redis_config == {'db': 0}
    pools = set()
    for port, first, second in results.values():
        assert first is second
        assert first.kwargs['port'] == port
        assert first.kwargs['host'] == '127.0.0.1'
        pools.add(id(first))
    assert len(pools) == thread_num


def _child_cache_is_empty(transport, queue):
    queue.put(transport._loop_pools.pools == {})


def test_get_conn_after_fork(fake_pool):
    """A forked child does not inherit the parent's pools."""
    redis_transport = RedisClientTransport()
    asyncio.run(redis_transport.get_conn(('127.0.0.1', 6379), None, {}))
    assert redis_transport._loop_pools.pools
    ctx = multiprocessing.get_context("fork")
    queue = ctx.Queue()
    proc = ctx.Process(target=_child_cache_is_empty, args=(redis_transport, queue))
    proc.start()
    proc.join(10)
    assert proc.exitcode == 0
    assert queue.get(timeout=5) is True
    assert redis_transport._loop_pools.pools


@pytest.mark.asyncio
def test_set_default():
    """Test set default"""
    from trpc_redis._transport import RedisClientTransport
    redis_config = {}
    RedisClientTransport._set_default('foo', 'bar', redis_config)
    assert redis_config['foo'] == 'bar'
    RedisClientTransport._set_default('foo', 'baz', redis_config)
    assert redis_config['foo'] == 'bar'
    RedisClientTransport._set_default('foo', 'baz', redis_config, force=True)
    assert redis_config['foo'] == 'baz'
