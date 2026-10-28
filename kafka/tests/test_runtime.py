# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Tests for the Kafka protocol starter, serving-loop hooks, and per-loop producers."""

import asyncio
import os
import threading
from types import SimpleNamespace
from typing import List

import pytest
from aiokafka import AIOKafkaProducer
from aiokafka.errors import ConsumerStoppedError
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.runtime import HandlerThreadInfo
from trpc_sdk.runtime import ThreadModel
from trpc_sdk.runtime import use_thread_model
from trpc_sdk.runtime._threadmodel import WorkerLoopView
from trpc_sdk.transport import UserProtocolTransport
from trpc_sdk.transport import get_protocol_handle

from trpc_kafka import KafkaServerTransport
from trpc_kafka import TrpcKafkaBatchConsumer
from trpc_kafka import TrpcKafkaConsumer
from trpc_kafka import _runtime
from trpc_kafka import start_kafka
from trpc_kafka.client_transport import KafkaClientTransport
from trpc_kafka.client_transport import stop_producers_for_current_loop

UNARY_TARGET = "localhost:9092?topics=t&group=g&enable_auto_commit=false"
BATCH_TARGET = "localhost:9092?topics=t&group=g&enable_auto_commit=false&batch_num=3"


class FakeConsumer:
    """``AIOKafkaConsumer`` stand-in that returns ``records`` and then waits for records forever."""

    instances: List["FakeConsumer"] = []
    records: List[str] = []

    def __init__(self, *topics, **kwargs):
        self.topics = topics
        self.kwargs = kwargs
        self.loop = asyncio.get_running_loop()
        self.pending = list(FakeConsumer.records)
        self.started = 0
        self.stopped = 0
        self.commits = 0
        FakeConsumer.instances.append(self)

    async def start(self):
        self.started += 1

    async def stop(self):
        self.stopped += 1

    async def commit(self):
        self.commits += 1

    async def _next(self):
        if self.stopped:
            raise ConsumerStoppedError()
        if self.pending:
            return self.pending.pop(0)
        await asyncio.Event().wait()

    async def getone(self):
        return await self._next()

    async def getmany(self, **kwargs):
        record = await self._next()
        return {"tp": [record]}


@pytest.fixture
def fake_consumer(monkeypatch):
    """Patch the consumer class ``build_consumer`` uses."""
    FakeConsumer.instances = []
    FakeConsumer.records = []
    monkeypatch.setattr("trpc_kafka.worker.AIOKafkaConsumer", FakeConsumer)
    yield FakeConsumer
    _runtime._REGISTRY.reset()


def other_starter(worker, index):
    """Starter of a protocol other than Kafka."""


def new_worker(handler=None, targets=(UNARY_TARGET, BATCH_TARGET)) -> SimpleNamespace:
    """Build a stand-in worker with the given Kafka addresses plus one address of another user protocol."""

    async def noop(ctx, req):
        return None

    tran_opts = []
    transports = []
    for target in targets:
        opts = BaseServerTransportOption()
        opts.set_target(target)
        tran_opts.append(opts)
        transports.append(UserProtocolTransport(start_kafka))
    tran_opts.append(BaseServerTransportOption())
    transports.append(UserProtocolTransport(other_starter))
    count = len(tran_opts)
    return SimpleNamespace(tran_opts=tran_opts, transports=transports, handlers=[handler or noop] * count,
                           exit_callbacks=[], loop=None)


def info_for_running_loop(index=0, count=1) -> HandlerThreadInfo:
    """Identity of the running loop as a serving loop."""
    return HandlerThreadInfo(index=index, count=count, loop=asyncio.get_running_loop())


def test_protocol_registered():
    """Importing trpc_kafka registers start_kafka for the kafka protocol."""
    assert get_protocol_handle("kafka") is start_kafka
    assert KafkaServerTransport().starter is start_kafka
    assert isinstance(KafkaServerTransport(), UserProtocolTransport)


def test_start_kafka_adds_exit_callback_once():
    """Every Kafka address calls the starter; the exit callback is added once."""
    worker = new_worker()
    start_kafka(worker, 0)
    start_kafka(worker, 1)
    assert worker.exit_callbacks == [_runtime.request_stop_all]


def test_start_kafka_through_worker_loop_view():
    """On a serving thread the starter sees a WorkerLoopView; the callback lands on the real worker."""
    worker = new_worker()
    loop = asyncio.new_event_loop()
    try:
        start_kafka(WorkerLoopView(worker, loop), 0)
    finally:
        loop.close()
    assert worker.exit_callbacks == [_runtime.request_stop_all]


@pytest.mark.asyncio
async def test_on_start_builds_one_consumer_per_kafka_address(fake_consumer):
    """Only Kafka addresses get a consumer, each its own, started and consuming on this loop."""
    worker = new_worker()
    info = info_for_running_loop()
    with use_thread_model(ThreadModel.single(info.loop, worker=worker)):
        await _runtime._on_start(info)

    group = _runtime._REGISTRY.groups()[0]
    assert group.loop is info.loop
    assert [type(c) for c in group.consumers] == [TrpcKafkaConsumer, TrpcKafkaBatchConsumer]
    assert len(fake_consumer.instances) == 2
    assert group.consumers[0].consumer is not group.consumers[1].consumer
    assert all(c.started == 1 and c.loop is info.loop for c in fake_consumer.instances)
    assert all(not task.done() for task in group.tasks)

    await _runtime._on_stop(info)
    assert _runtime._REGISTRY.groups() == []
    assert all(task.done() for task in group.tasks)
    assert all(c.stopped >= 1 for c in fake_consumer.instances)


@pytest.mark.asyncio
async def test_on_start_without_kafka_addresses(fake_consumer):
    """A worker with no Kafka address starts nothing."""
    worker = new_worker(targets=())
    info = info_for_running_loop()
    with use_thread_model(ThreadModel.single(info.loop, worker=worker)):
        await _runtime._on_start(info)
    assert _runtime._REGISTRY.groups() == []
    assert fake_consumer.instances == []
    await _runtime._on_stop(info)


@pytest.mark.asyncio
async def test_on_start_failure_stops_started_consumers(fake_consumer, monkeypatch):
    """A consumer that cannot start aborts startup; the ones already started are stopped."""
    calls = []

    async def start(self):
        calls.append(self)
        if len(calls) == 2:
            raise RuntimeError("broker down")

    monkeypatch.setattr(FakeConsumer, "start", start)
    worker = new_worker()
    info = info_for_running_loop()
    with use_thread_model(ThreadModel.single(info.loop, worker=worker)):
        with pytest.raises(RuntimeError, match="broker down"):
            await _runtime._on_start(info)
    assert _runtime._REGISTRY.groups() == []
    assert all(c.stopped == 1 for c in fake_consumer.instances)


@pytest.mark.asyncio
async def test_request_stop_all_from_another_thread(fake_consumer):
    """The exit callback runs on the main thread and stops fetching on the serving loop."""
    worker = new_worker(targets=(UNARY_TARGET,))
    info = info_for_running_loop()
    with use_thread_model(ThreadModel.single(info.loop, worker=worker)):
        await _runtime._on_start(info)
    group = _runtime._REGISTRY.groups()[0]

    thread = threading.Thread(target=_runtime.request_stop_all)
    thread.start()
    thread.join()
    await asyncio.wait_for(asyncio.gather(*group.tasks), 1)
    assert group.consumers[0].closed
    assert fake_consumer.instances[0].stopped == 1
    await _runtime._on_stop(info)


@pytest.mark.asyncio
async def test_in_flight_message_finishes_and_commits(fake_consumer):
    """A record being handled at shutdown finishes and is committed before the consumer stops."""
    entered = asyncio.Event()
    release = asyncio.Event()
    handled = []

    async def handler(ctx, req):
        entered.set()
        await release.wait()
        handled.append(ctx.msg.server_req_head)

    fake_consumer.records = ["record-1"]
    worker = new_worker(handler, targets=(UNARY_TARGET,))
    info = info_for_running_loop()
    with use_thread_model(ThreadModel.single(info.loop, worker=worker)):
        await _runtime._on_start(info)
    await asyncio.wait_for(entered.wait(), 1)

    _runtime.request_stop_all()
    stopping = asyncio.create_task(_runtime._on_stop(info))
    await asyncio.sleep(0.01)
    assert not stopping.done()
    release.set()
    await asyncio.wait_for(stopping, 1)

    consumer = fake_consumer.instances[0]
    assert handled == ["record-1"]
    assert consumer.commits == 1
    assert consumer.stopped >= 1


@pytest.mark.asyncio
async def test_on_stop_cancels_after_timeout(fake_consumer, monkeypatch):
    """A handler that never returns is cancelled after the timeout and the consumer is still stopped."""

    async def stuck(ctx, req):
        await asyncio.Event().wait()

    monkeypatch.setattr(_runtime, "STOP_TIMEOUT_SECONDS", 0.05)
    fake_consumer.records = ["record-1"]
    worker = new_worker(stuck, targets=(UNARY_TARGET,))
    info = info_for_running_loop()
    with use_thread_model(ThreadModel.single(info.loop, worker=worker)):
        await _runtime._on_start(info)
    group = _runtime._REGISTRY.groups()[0]
    await asyncio.sleep(0.01)

    await asyncio.wait_for(_runtime._on_stop(info), 1)
    assert group.tasks[0].cancelled()
    assert fake_consumer.instances[0].stopped >= 1
    assert fake_consumer.instances[0].commits == 0


def test_serving_threads_get_their_own_consumers(fake_consumer):
    """Four serving loops in four threads each build their own consumers; the registry keeps them apart."""
    worker = new_worker()
    seen = {}
    errors = []
    barrier = threading.Barrier(4)

    async def serve(index):
        info = info_for_running_loop(index, 4)
        await _runtime._on_start(info)
        barrier.wait(5)
        group = next(g for g in _runtime._REGISTRY.groups() if g.loop is info.loop)
        seen[index] = (info.loop, [c.consumer for c in group.consumers])
        assert len(_runtime._REGISTRY.groups()) == 4
        barrier.wait(5)
        await _runtime._on_stop(info)

    def run(index):
        try:
            asyncio.run(serve(index))
        except BaseException as err:  # pylint: disable=broad-except
            errors.append(err)

    loop = asyncio.new_event_loop()
    try:
        with use_thread_model(ThreadModel.single(loop, worker=worker)):
            threads = [threading.Thread(target=run, args=(i,)) for i in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(10)
    finally:
        loop.close()

    assert errors == []
    assert len(seen) == 4
    assert len({id(loop) for loop, _ in seen.values()}) == 4
    consumers = [c for _, group in seen.values() for c in group]
    assert len({id(c) for c in consumers}) == 8
    assert all(c.loop is seen_loop for seen_loop, group in seen.values() for c in group)
    assert _runtime._REGISTRY.groups() == []


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
def test_registry_empty_after_fork(fake_consumer):
    """A forked child does not inherit the parent's consumers."""
    _runtime._REGISTRY.add(_runtime._LoopConsumers(object()))
    pid = os.fork()
    if pid == 0:
        os._exit(0 if _runtime._REGISTRY.groups() == [] else 1)
    _, status = os.waitpid(pid, 0)
    assert os.WEXITSTATUS(status) == 0
    assert len(_runtime._REGISTRY.groups()) == 1


@pytest.fixture
def fake_producer(monkeypatch):
    """Patch AIOKafkaProducer start/stop and record which producers were stopped."""
    stopped = []

    async def start(self):
        return None

    async def stop(self):
        stopped.append(self)

    monkeypatch.setattr(AIOKafkaProducer, "start", start)
    monkeypatch.setattr(AIOKafkaProducer, "stop", stop)
    return stopped


@pytest.mark.asyncio
async def test_producer_reused_on_one_loop(fake_producer):
    """One loop reuses its producer."""
    transport = KafkaClientTransport()
    first = await transport.get_producer("k", "localhost:9092")
    second = await transport.get_producer("k", "localhost:9092")
    assert first is second


def test_producer_per_event_loop(fake_producer):
    """Two asyncio.run calls get two producers, each bound to its own loop."""
    transport = KafkaClientTransport()

    async def get():
        return await transport.get_producer("k", "localhost:9092")

    first = asyncio.run(get())
    second = asyncio.run(get())
    assert first is not second


def test_producer_per_thread(fake_producer):
    """Four threads producing at once each get their own producer."""
    transport = KafkaClientTransport()
    producers = {}
    barrier = threading.Barrier(4)

    async def get(index):
        barrier.wait(5)
        found = await asyncio.gather(*[transport.get_producer("k", "localhost:9092") for _ in range(3)])
        assert len({id(p) for p in found}) == 1
        producers[index] = found[0]

    threads = [threading.Thread(target=lambda i=i: asyncio.run(get(i))) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)
    assert len({id(p) for p in producers.values()}) == 4


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
def test_producers_empty_after_fork(fake_producer):
    """A forked child starts with an empty producer cache."""
    transport = KafkaClientTransport()
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(transport.get_producer("k", "localhost:9092"))
        pid = os.fork()
        if pid == 0:
            os._exit(0 if transport._loop_producers.producers == {} else 1)
        _, status = os.waitpid(pid, 0)
        assert os.WEXITSTATUS(status) == 0
        assert len(transport._loop_producers.producers) == 1
    finally:
        loop.close()


def test_stop_producers_for_current_loop_only(fake_producer):
    """stop_producers_for_current_loop stops this loop's producers and leaves other loops' alone."""
    transport = KafkaClientTransport()
    other_loop = asyncio.new_event_loop()
    try:
        other = other_loop.run_until_complete(transport.get_producer("k", "localhost:9092"))

        async def produce_then_stop():
            mine = await transport.get_producer("k", "localhost:9092")
            await stop_producers_for_current_loop()
            return mine

        mine = asyncio.run(produce_then_stop())
        assert mine in fake_producer
        assert other not in fake_producer
        assert len(transport._loop_producers.producers[other_loop]) == 1
    finally:
        other_loop.close()
