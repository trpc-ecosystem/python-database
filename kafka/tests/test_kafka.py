# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Tests for the Kafka client, codecs, and consumers with the broker mocked out."""

import asyncio
import os
import time
from types import SimpleNamespace
from typing import Any
from typing import List

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from aiokafka import ConsumerRecord
from aiokafka.errors import ConsumerStoppedError
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import TrpcMessage
from trpc_sdk.client import Client as TrpcClient
from trpc_sdk.client import Options
from trpc_sdk.codec import get_message
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.server import Server
from trpc_sdk.server import ServerOption
from trpc_sdk.transport import RoundTripOptions

import trpc_kafka
from trpc_kafka import KafkaServerTransport
from trpc_kafka import TrpcKafkaBatchConsumer
from trpc_kafka import TrpcKafkaConsumer
from trpc_kafka.client import Client
from trpc_kafka.client_transport import KafkaClientTransport
from trpc_kafka.config import ClientUserConfig
from trpc_kafka.config import UserConfig
from trpc_kafka.constants import USER_DEFINED_SVC_NAME
from trpc_kafka.types import Request
from trpc_kafka.utils import is_false
from trpc_kafka.utils import is_true
from trpc_kafka.utils import parse_kafka_params
from trpc_kafka.utils import unmarshal_config
from trpc_kafka.worker import build_consumer


@pytest.fixture
def mock_response(monkeypatch):
    """Patch produce and consume so tests do not talk to a broker."""

    async def mock_invoke(ctx: TrpcContext, req: Any, rsp_cls: Any, options: List):
        msg = get_message(ctx)
        resp = "OK"
        msg.set_client_rsp_head(resp)
        return

    monkeypatch.setattr(TrpcClient(), 'invoke', mock_invoke)

    async def mock_kafka_start(self):
        return None

    monkeypatch.setattr(AIOKafkaProducer, 'start', mock_kafka_start)

    async def mock_kafka_send(self, topic, value=None, key=None, partition=None,
                              timestamp_ms=None, headers=None):
        return None

    monkeypatch.setattr(AIOKafkaProducer, 'send_and_wait', mock_kafka_send)

    async def mock_kafka_consumer_start(self):
        return None

    monkeypatch.setattr(AIOKafkaConsumer, 'start', mock_kafka_consumer_start)

    async def mock_kafka_consumer_consume(self):
        return None

    monkeypatch.setattr(TrpcKafkaConsumer, 'consume', mock_kafka_consumer_consume)
    monkeypatch.setattr(TrpcKafkaBatchConsumer, 'consume', mock_kafka_consumer_consume)


@pytest.mark.asyncio
async def test_produce(mock_response):
    """Produce one record and return the mocked broker reply."""
    target = "ip://mock_address"
    password = "test_kafka"
    opts = [Options().set_target(target).set_password(password)]
    kafka_client = Client("kafka", opts)
    ctx = TrpcContext()
    reply = await kafka_client.produce(ctx, "test_topic", "test_key", "test_value")
    assert reply == "OK"


@pytest.mark.asyncio
async def test_produce_rpc_name(monkeypatch):
    """Produce uses the callee service name in the RPC path."""
    rpc_names = []

    async def mock_invoke(ctx: TrpcContext, req: Any, rsp_cls: Any, options: List):
        rpc_names.append(get_message(ctx).client_rpc_name)

    monkeypatch.setattr(TrpcClient(), 'invoke', mock_invoke)
    await Client("trpc.kafka.producer.service").produce(TrpcContext(), "t", "k", "v")
    assert rpc_names == ["/trpc.kafka.producer.service/send"]


def new_round_trip_ctx(key="key", deadline=None) -> TrpcContext:
    """Build a context whose head holds one produce request."""
    ctx = TrpcContext()
    msg = TrpcMessage()
    msg.client_req_head = Request("topic", key, "value")
    ctx.msg = msg
    ctx.deadline = time.time() * 2000 if deadline is None else deadline
    return ctx


@pytest.mark.asyncio
async def test_kafka_transport(mock_response):
    """Round-trip a record through a cached producer."""
    kafka_transport = KafkaClientTransport()
    await kafka_transport.get_producer("my_kafka", "")
    ctx = new_round_trip_ctx()
    opt = RoundTripOptions()
    await kafka_transport.round_trip(ctx, b'test_round_trip', opt)


@pytest.mark.asyncio
async def test_kafka_transport_sends_bytes_and_none_key(mock_response, monkeypatch):
    """Encode the value as bytes and leave a missing key as None."""
    sent = []

    async def mock_send(self, topic, value=None, key=None, **kwargs):
        sent.append((topic, key, value))
        return "record_metadata"

    monkeypatch.setattr(AIOKafkaProducer, 'send_and_wait', mock_send)
    kafka_transport = KafkaClientTransport()
    ctx = new_round_trip_ctx(key=None)
    ret = await kafka_transport.round_trip(ctx, b'', RoundTripOptions())
    assert ret == "record_metadata"
    assert ctx.msg.client_rsp_head == "record_metadata"
    assert sent == [("topic", None, b"value")]


@pytest.mark.asyncio
async def test_kafka_transport_timeout(mock_response, monkeypatch):
    """Raise ClientTimeoutError when the deadline expires during send."""

    async def slow_send(self, topic, value=None, key=None, **kwargs):
        await asyncio.sleep(1)

    monkeypatch.setattr(AIOKafkaProducer, 'send_and_wait', slow_send)
    kafka_transport = KafkaClientTransport()
    ctx = new_round_trip_ctx(deadline=time.time() * 1000 + 50)
    with pytest.raises(ClientTimeoutError, match="kafka client timeout topic topic message value"):
        await kafka_transport.round_trip(ctx, b'', RoundTripOptions())


@pytest.mark.asyncio
async def test_kafka_transport_invoke_error(mock_response, monkeypatch):
    """Wrap a broker failure as ClientInvokeError."""

    async def broken_send(self, topic, value=None, key=None, **kwargs):
        raise RuntimeError("broker down")

    monkeypatch.setattr(AIOKafkaProducer, 'send_and_wait', broken_send)
    kafka_transport = KafkaClientTransport()
    with pytest.raises(ClientInvokeError, match="broker down"):
        await kafka_transport.round_trip(new_round_trip_ctx(), b'', RoundTripOptions())


@pytest.mark.asyncio
async def test_get_producer_concurrent_and_close(mock_response, monkeypatch):
    """Concurrent lookups share one producer, and close stops it."""
    stopped = []

    async def mock_stop(self):
        stopped.append(self)

    monkeypatch.setattr(AIOKafkaProducer, 'stop', mock_stop)
    kafka_transport = KafkaClientTransport()
    producers = await asyncio.gather(*[kafka_transport.get_producer("my_kafka", "") for _ in range(5)])
    assert len({id(p) for p in producers}) == 1
    assert len(kafka_transport.producers) == 1

    await kafka_transport.close()
    assert stopped == [producers[0]]
    assert kafka_transport.producers == {}


@pytest.mark.asyncio
async def test_get_producer_start_failure_not_cached(mock_response, monkeypatch):
    """A producer that fails to start is not cached."""

    async def failed_start(self):
        raise RuntimeError("cannot start")

    monkeypatch.setattr(AIOKafkaProducer, 'start', failed_start)
    kafka_transport = KafkaClientTransport()
    with pytest.raises(RuntimeError):
        await kafka_transport.get_producer("my_kafka", "")
    assert kafka_transport.producers == {}


def test_parser_endpoint():
    """Split brokers and query fields out of a client endpoint."""
    kafka_transport = KafkaClientTransport()
    cfg = kafka_transport.parser_endpoint("h1:9092,h2:9092?client_id=abc&linger_ms=5&enable_idempotence=true")
    assert isinstance(cfg, ClientUserConfig)
    assert cfg.bootstrap_servers == ["h1:9092", "h2:9092"]
    assert cfg.client_id == "abc"
    assert cfg.linger_ms == 5
    assert cfg.enable_idempotence == "true"


def test_parse_kafka_params():
    """Parse a query string and reject a token that is not key=value."""
    assert parse_kafka_params("a=1&b=x") == {"a": "1", "b": "x"}
    with pytest.raises(Exception, match="length of kafka param must be two"):
        parse_kafka_params("a=1&b")


def test_unmarshal_config():
    """Copy known query fields onto UserConfig and ignore the rest."""
    cfg = UserConfig()
    unmarshal_config({"session_timeout_ms": "20000", "topics": "t1,t2", "enable_auto_commit": "false",
                      "unknown": "ignored"}, cfg)
    assert cfg.session_timeout_ms == 20000
    assert cfg.topics == "t1,t2"
    assert cfg.enable_auto_commit == "false"
    assert not hasattr(cfg, "unknown")


def test_unmarshal_config_none_default_int():
    """Fill integer fields whose default is None."""
    cfg = UserConfig()
    unmarshal_config({"rebalance_timeout_ms": "5000", "max_poll_records": "10", "sasl_plain_username": "u"}, cfg)
    assert cfg.rebalance_timeout_ms == 5000
    assert cfg.max_poll_records == 10
    assert cfg.sasl_plain_username == "u"


def test_bool_helpers():
    """Accept true and false in string or bool form."""
    assert is_true("true") and is_true("True") and is_true(True)
    assert not is_true("false") and not is_true("") and not is_true(False)
    assert is_false("false") and is_false("False") and is_false(False)
    assert not is_false("true") and not is_false("")


def new_worker(targets: List[str]) -> SimpleNamespace:
    """Build a stand-in worker with one Kafka address per target."""
    tran_opts = []
    for target in targets:
        opts = BaseServerTransportOption()
        opts.set_target(target)
        opts.set_service_name("custom.service")
        tran_opts.append(opts)
    return SimpleNamespace(tran_opts=tran_opts, handlers=[mock_handler] * len(targets),
                           transports=[KafkaServerTransport() for _ in targets], exit_callbacks=[])


UNARY_TARGET = ("localhost:9092?topics=test-topic&group=test_consumer&enable_auto_commit=true"
                "&session_timeout_ms=20000")
BATCH_TARGET = "localhost:9092?topics=test-topic&group=test_consumer&enable_auto_commit=false&batch_num=3"


@pytest.mark.asyncio
async def test_build_consumer():
    """Build an unstarted unary consumer and batch consumer from their targets."""
    worker = new_worker([UNARY_TARGET, BATCH_TARGET])
    unary = await build_consumer(worker, 0)
    batch = await build_consumer(worker, 1)

    assert isinstance(unary, TrpcKafkaConsumer)
    assert unary.enable_auto_commit
    assert unary.handle is mock_handler
    assert unary.app is worker
    assert unary.service_name is None
    assert isinstance(batch, TrpcKafkaBatchConsumer)
    assert batch.user_config.batch_num == 3
    assert isinstance(unary.consumer, AIOKafkaConsumer)
    assert unary.consumer is not batch.consumer
    await unary.consumer.stop()
    await batch.consumer.stop()


@pytest.mark.asyncio
async def test_build_consumer_custom_service_name(monkeypatch):
    """Pass the YAML service name when registration used a custom name."""
    monkeypatch.setattr(USER_DEFINED_SVC_NAME, "value", True)
    consumer = await build_consumer(new_worker([UNARY_TARGET]), 0)
    assert consumer.service_name == "custom.service"
    await consumer.consumer.stop()


@pytest.mark.asyncio
async def test_build_consumer_flags(monkeypatch):
    """Pass broker flags through to AIOKafkaConsumer."""
    captured = {}

    class FakeConsumer:
        def __init__(self, *topics, **kwargs):
            captured["topics"] = topics
            captured.update(kwargs)

    monkeypatch.setattr("trpc_kafka.worker.AIOKafkaConsumer", FakeConsumer)
    target = ("localhost:9092?topics=t1,t2&group=g&enable_auto_commit=False&check_crcs=False"
              "&exclude_internal_topics=FALSE&rebalance_timeout_ms=5000&batch_num=0&session_timeout_ms=10")
    consumer = await build_consumer(new_worker([target]), 0)

    assert captured["topics"] == ("t1", "t2")
    assert captured["enable_auto_commit"] is False
    assert captured["check_crcs"] is False
    assert captured["exclude_internal_topics"] is False
    assert captured["rebalance_timeout_ms"] == 5000
    assert captured["session_timeout_ms"] == 10000
    assert captured["client_id"] == "g"
    assert isinstance(consumer, TrpcKafkaConsumer)
    assert not consumer.enable_auto_commit


@pytest.mark.asyncio
async def test_build_consumer_lack_of_brokers():
    """Reject a target that has no query section."""
    with pytest.raises(Exception, match="lack of kafka brokers"):
        await build_consumer(new_worker(["localhost:9092"]), 0)


class KafkaServer(trpc_kafka.TrpcKafkaServicer):
    """Servicer used to register a one-record Kafka service."""

    async def handle(self, ctx: TrpcContext, key, value, topic, partition, offset, timestamp):
        return


def test_kafka_servicer(mock_response):
    """Register a one-record servicer on a server."""
    trpc_kafka.register_trpc_kafka_server(Server(ServerOption()), KafkaServer())


def test_kafka_batch_servicer(mock_response):
    """Register a batch consumer service on a server."""
    trpc_kafka.register_trpc_kafka_batch_consumer_server(Server(ServerOption()), KafkaServer())


@pytest.mark.asyncio
async def test_server_kafka_codec(mock_response):
    """Stamp the server RPC name as /{service}/handle."""
    msg = TrpcMessage()
    await trpc_kafka.ServerKafkaCodec().decode(msg, b'')
    assert msg.server_rpc_name == "/trpc.kafka.consumer.service/handle"

    ctx = TrpcContext()
    msg = TrpcMessage()
    msg.callee_service_name = "custom.service"
    ctx.msg = msg
    assert await trpc_kafka.ServerKafkaCodec().decode_data(ctx, b'data') == b'data'
    assert msg.server_rpc_name == "/custom.service/handle"


def test_client_kafka_codec():
    """Pass the body through and fill the caller service name."""
    ctx = TrpcContext()
    msg = TrpcMessage()
    ctx.msg = msg
    codec = trpc_kafka.ClientKafkaCodec()
    assert codec.encode_data(ctx, b'body') == b'body'
    assert msg.caller_service_name == f"trpc.kafka.{os.path.basename(os.getcwd())}.service"
    assert codec.decode_data(ctx, b'rsp') == b'rsp'


class MockKafkaServer(trpc_kafka.TrpcKafkaServicer):
    """Servicer whose handle accepts one record and returns."""

    async def handle(self, ctx: TrpcContext, key, value, topic, partition, offset, timestamp):
        return


class MockBatchKafkaServer(trpc_kafka.TrpcKafkaBatchConsumerServicer):
    """Servicer whose handle accepts one batch and returns."""

    async def handle(self, ctx: TrpcContext, msgs):
        return


@pytest.mark.asyncio
async def test_service_desc(mock_response):
    """Dispatch one ConsumerRecord to the unary handler."""
    ctx = TrpcContext()
    msg = TrpcMessage()
    msg.server_req_head = ConsumerRecord(topic="test_kafka", partition=0, offset=0, timestamp=0, timestamp_type=0,
                                         key=None, value=None, checksum=0, serialized_key_size=0,
                                         serialized_value_size=0, headers=None)
    ctx.msg = msg
    service = MockKafkaServer()
    await trpc_kafka.handle(ctx, service)


@pytest.mark.asyncio
async def test_service_desc_type_error(mock_response):
    """Reject a server head that is not a ConsumerRecord."""
    ctx = TrpcContext()
    msg = TrpcMessage()
    msg.server_req_head = "not a record"
    ctx.msg = msg
    with pytest.raises(Exception, match="kafka msg type error"):
        await trpc_kafka.handle(ctx, MockKafkaServer())


@pytest.mark.asyncio
async def test_batch_service_desc(mock_response):
    """Dispatch a list of records to the batch handler."""
    ctx = TrpcContext()
    msg = TrpcMessage()
    msg.server_req_head = [ConsumerRecord(topic="test_kafka", partition=0, offset=0, timestamp=0, timestamp_type=0,
                                          key=None, value=None, checksum=0, serialized_key_size=0,
                                          serialized_value_size=0, headers=None)]
    ctx.msg = msg
    service = MockBatchKafkaServer()
    await trpc_kafka.batch_handle(ctx, service)


class Record:
    """Poll result with two partitions of one stand-in message each."""

    def values(self):
        return [["kafka msg"], ["kafka msg"]]


class MockAIOKafkaConsumer:
    """Consumer that returns ``polls`` results, then waits forever or reports it was stopped.

    Attributes:
        polls: Results of the next ``getone`` / ``getmany`` calls.
        block: Wait forever once ``polls`` is empty; otherwise raise
            ``ConsumerStoppedError``.
        fetches: Number of ``getone`` / ``getmany`` calls.
        commits: Number of ``commit`` calls.
        stops: Number of ``stop`` calls.
        cancelled: Number of fetches cancelled while waiting.
    """

    def __init__(self, polls=None, block=False):
        self.polls = list(polls or [])
        self.block = block
        self.fetches = 0
        self.commits = 0
        self.stops = 0
        self.cancelled = 0

    async def commit(self):
        self.commits += 1

    async def _next(self):
        self.fetches += 1
        if self.polls:
            return self.polls.pop(0)
        if not self.block:
            raise ConsumerStoppedError()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled += 1
            raise

    async def getone(self):
        return await self._next()

    async def getmany(self, **kwargs):
        return await self._next()

    async def stop(self):
        self.stops += 1


async def mock_handler(ctx: TrpcContext, req):
    """Assert the consumer stored the stand-in message on the request head."""
    assert ctx.msg.server_req_head == "kafka msg"


@pytest.mark.asyncio
async def test_unary_consumer():
    """Consume and commit one record, then end when the consumer reports it was stopped."""
    trpc_consumer = TrpcKafkaConsumer(None, mock_handler, "false", None)
    trpc_consumer.consumer = MockAIOKafkaConsumer(["kafka msg"])
    await trpc_consumer.consume()
    assert trpc_consumer.consumer.commits == 1
    assert trpc_consumer.consumer.stops == 1


@pytest.mark.asyncio
async def test_unary_consumer_auto_commit_skips_commit():
    """Leave commits to aiokafka when auto-commit is on."""
    trpc_consumer = TrpcKafkaConsumer(None, mock_handler, "true", None)
    trpc_consumer.consumer = MockAIOKafkaConsumer(["kafka msg"])
    await trpc_consumer.consume()
    assert trpc_consumer.consumer.commits == 0


@pytest.mark.asyncio
async def test_unary_consumer_stamps_service_name():
    """Put the custom callee service name on the message."""
    names = []

    async def handler(ctx, req):
        names.append(ctx.msg.callee_service_name)

    trpc_consumer = TrpcKafkaConsumer(None, handler, "true", "custom.service")
    trpc_consumer.consumer = MockAIOKafkaConsumer(["kafka msg"])
    await trpc_consumer.consume()
    assert names == ["custom.service"]


@pytest.mark.asyncio
async def test_unary_consumer_handler_error_not_committed():
    """A failing handler is logged and its record is not committed."""

    async def broken(ctx, req):
        raise RuntimeError("boom")

    trpc_consumer = TrpcKafkaConsumer(None, broken, "false", None)
    trpc_consumer.consumer = MockAIOKafkaConsumer(["kafka msg"])
    await trpc_consumer.consume()
    assert trpc_consumer.consumer.commits == 0
    assert trpc_consumer.consumer.stops == 1


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.asyncio
async def test_request_stop_interrupts_fetch(batch):
    """request_stop cancels a fetch still waiting for records and stops the consumer."""
    if batch:
        trpc_consumer = TrpcKafkaBatchConsumer(None, mock_handler, UserConfig(batch_num=3), None)
    else:
        trpc_consumer = TrpcKafkaConsumer(None, mock_handler, "true", None)
    trpc_consumer.consumer = MockAIOKafkaConsumer(block=True)
    task = asyncio.create_task(trpc_consumer.consume())
    await asyncio.sleep(0.01)
    assert trpc_consumer.consumer.fetches == 1

    trpc_consumer.request_stop()
    await asyncio.wait_for(task, 1)
    assert trpc_consumer.consumer.cancelled == 1
    assert trpc_consumer.consumer.stops == 1


@pytest.mark.asyncio
async def test_request_stop_before_consume():
    """A stop requested before consume starts ends it without fetching."""
    trpc_consumer = TrpcKafkaConsumer(None, mock_handler, "true", None)
    trpc_consumer.consumer = MockAIOKafkaConsumer(block=True)
    trpc_consumer.request_stop()
    await asyncio.wait_for(trpc_consumer.consume(), 1)
    assert trpc_consumer.consumer.fetches == 0
    assert trpc_consumer.consumer.stops == 1


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.asyncio
async def test_request_stop_finishes_in_flight(batch):
    """The record or batch being handled when the stop comes finishes and is committed."""
    entered = asyncio.Event()
    release = asyncio.Event()
    handled = []

    async def slow_handler(ctx, req):
        entered.set()
        await release.wait()
        handled.append(ctx.msg.server_req_head)

    if batch:
        trpc_consumer = TrpcKafkaBatchConsumer(None, slow_handler, UserConfig(enable_auto_commit="false",
                                                                                batch_num=3), None)
        polls = [{"tp": ["kafka msg"]}]
    else:
        trpc_consumer = TrpcKafkaConsumer(None, slow_handler, "false", None)
        polls = ["kafka msg"]
    trpc_consumer.consumer = MockAIOKafkaConsumer(polls, block=True)
    task = asyncio.create_task(trpc_consumer.consume())
    await asyncio.wait_for(entered.wait(), 1)

    trpc_consumer.request_stop()
    await asyncio.sleep(0.01)
    assert not task.done()
    release.set()
    await asyncio.wait_for(task, 1)

    assert len(handled) == 1
    assert trpc_consumer.consumer.commits == 1
    assert trpc_consumer.consumer.fetches == 1
    assert trpc_consumer.consumer.stops == 1


@pytest.mark.asyncio
async def test_batch_consumer():
    """Commit once for each partition batch when auto-commit is off."""
    user_config = UserConfig(enable_auto_commit="false", batch_num=3)
    trpc_consumer = TrpcKafkaBatchConsumer(None, mock_handler_batch, user_config, None)
    trpc_consumer.consumer = MockAIOKafkaConsumer([Record()])
    await trpc_consumer.consume()
    # Each partition batch is committed by hand after it is handled.
    assert trpc_consumer.consumer.commits == 2
    assert trpc_consumer.consumer.stops == 1


@pytest.mark.asyncio
async def test_batch_consumer_auto_commit():
    """Leave commits to the broker when auto-commit is on."""
    user_config = UserConfig(enable_auto_commit="true", batch_num=3)
    trpc_consumer = TrpcKafkaBatchConsumer(None, mock_handler_batch, user_config, None)
    trpc_consumer.consumer = MockAIOKafkaConsumer([Record()])
    await trpc_consumer.consume()
    assert trpc_consumer.consumer.commits == 0


async def mock_handler_batch(ctx: TrpcContext, req):
    """Assert the batch consumer stored a key group of stand-in messages."""
    assert all(msg == "kafka msg" for msg in ctx.msg.server_req_head)


def test_split_msgs_to_group():
    """Split a poll by key hash, or keep one group when concurrency is unset."""
    records = []
    total_record_cnt = 100
    for i in range(total_record_cnt):
        key = bytes(str(i), "utf-8")
        records.append(ConsumerRecord(topic="test_kafka", partition=0, offset=0, timestamp=0, timestamp_type=0,
                                      key=key, value=None, checksum=0, serialized_key_size=0,
                                      serialized_value_size=0, headers=None))
    consumer = TrpcKafkaBatchConsumer(None, None, UserConfig(enable_auto_commit="false", parallel_num=7), None)
    groups = consumer._split_msgs_to_group(records)
    assert len(groups) == 7
    sum = 0
    for g in groups:
        sum = sum + len(g)
    assert sum == total_record_cnt
    # Without a concurrency setting, every record stays in one group.
    consumer = TrpcKafkaBatchConsumer(None, None, UserConfig(), None)
    groups = consumer._split_msgs_to_group(records)
    assert len(groups) == 1
    assert len(groups[0]) == total_record_cnt
