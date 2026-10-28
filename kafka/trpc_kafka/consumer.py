# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Kafka consumers that feed messages into tRPC handlers.

``TrpcKafkaConsumer`` handles one record at a time.
``TrpcKafkaBatchConsumer`` polls a batch, splits it into key-stable
groups, and runs those groups concurrently.

Both stop on ``request_stop``: a fetch still waiting for records is
cancelled, while a record or batch already being handled runs to the end
and is committed. ``consume`` always stops the ``AIOKafkaConsumer``, which
commits (with auto-commit on) and leaves the group.
"""
import asyncio
import json
import time
import traceback
from typing import Any
from typing import Awaitable
from typing import List
from typing import Optional

from aiokafka.errors import ConsumerStoppedError
from aiokafka.structs import ConsumerRecord
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import TrpcMessage
from trpc_sdk.log import logger
from trpc_sdk.telemetry import report_attr
from .config import UserConfig
from .utils import is_true

# Returned by ``_fetch_unless_stopped`` when the stop request won the race.
_STOPPED = object()


async def _fetch_unless_stopped(fetch: Awaitable[Any], stop: asyncio.Event) -> Any:
    """Await ``fetch`` unless ``stop`` is set first; a stopped fetch is cancelled.

    Records the fetch already returned are kept: when both finish in the
    same turn, the records win and the caller handles them.

    Args:
        fetch: ``getone()`` or ``getmany()`` coroutine.
        stop: Set by ``request_stop``.

    Returns:
        What ``fetch`` returned, or ``_STOPPED``.

    Raises:
        Exception: Whatever ``fetch`` raised.
    """
    fetch_task = asyncio.ensure_future(fetch)
    stop_task = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait((fetch_task, stop_task), return_when=asyncio.FIRST_COMPLETED)
    finally:
        stop_task.cancel()
        if not fetch_task.done():
            fetch_task.cancel()
            await asyncio.wait((fetch_task,))
    if fetch_task.cancelled():
        return _STOPPED
    return fetch_task.result()


class _Stoppable:
    """Stop flag shared by both consumers.

    The event is created by ``consume`` so it belongs to the consuming loop;
    a stop requested earlier is kept in ``closed``.
    """

    closed: bool
    _stop_event: Optional[asyncio.Event]

    def request_stop(self) -> None:
        """Stop fetching; the record or batch in flight still finishes.

        Must be called on the consuming loop.
        """
        self.closed = True
        if self._stop_event is not None:
            self._stop_event.set()

    def _stop_requested(self) -> asyncio.Event:
        """Create the stop event on the running loop, already set if a stop came first."""
        self._stop_event = asyncio.Event()
        if self.closed:
            self._stop_event.set()
        return self._stop_event


class TrpcKafkaConsumer(_Stoppable):
    """Consume one record at a time and dispatch it to a tRPC handler.

    A handler failure is counted and logged. The offset is committed
    only after a successful handler call, and only when auto-commit is
    off. The consume loop always stops the underlying consumer.

    Attributes:
        app: Worker that owns this consumer.
        handle: Service handler invoked as ``handle(ctx, None)``.
        consumer: ``AIOKafkaConsumer`` assigned before ``consume`` runs.
        enable_auto_commit: When false, commit after each successful
            handler call.
        closed: Set by ``request_stop``.
        service_name: Callee service name, or ``None`` when the default
            service name is in use.
    """

    def __init__(self, app, handle, enable_auto_commit: str, service_name: str):
        """Store the handler and the auto-commit flag.

        Args:
            app: Worker that owns this consumer.
            handle: Service handler for each record.
            enable_auto_commit: String flag. Only ``"true"`` enables
                auto-commit.
            service_name: Callee name passed into the message, or
                ``None``.
        """
        self.app = app
        self.handle = handle
        self.consumer = None
        self.enable_auto_commit = is_true(enable_auto_commit)
        self.closed = False
        self._stop_event = None
        self.service_name = service_name

    async def consume(self):
        """Fetch records with ``getone`` and dispatch each one until stopped.

        The record is stored on ``ctx.msg.server_req_head``. On handler
        failure the error is reported and the offset is left uncommitted
        when auto-commit is off.

        The consumer is stopped when the loop ends.
        """
        stop = self._stop_requested()
        try:
            while not self.closed:
                try:
                    kafka_msg = await _fetch_unless_stopped(self.consumer.getone(), stop)
                except ConsumerStoppedError:
                    break
                if kafka_msg is _STOPPED:
                    break
                ctx = TrpcContext()
                msg = TrpcMessage()
                msg.server_req_head = kafka_msg
                if self.service_name is not None:
                    msg.callee_service_name = self.service_name
                ctx.msg = msg
                ctx.recv_time = time.time() * 1000
                try:
                    await self.handle(ctx, None)
                    if not self.enable_auto_commit:
                        await self.consumer.commit()
                except Exception as err:  # pylint: disable=broad-except
                    report_attr("trpc_kafka_handle_err", 1)
                    logger.error(
                        "[executor] failed to send rsp to tcp client, err: %s, trace: %s",
                        repr(err),
                        traceback.format_exc(),
                    )
        finally:
            await self.consumer.stop()


class TrpcKafkaBatchConsumer(_Stoppable):
    """Poll batches and run key-stable groups concurrently.

    ``getmany`` collects one batch. Groups from that batch run at the
    same time, while records inside one group stay serial so one key
    keeps its order. The batch is committed only after every group
    finishes, and only when auto-commit is off.

    Attributes:
        app: Worker that owns this consumer.
        handle: Service handler invoked once per key group.
        consumer: ``AIOKafkaConsumer`` assigned before ``consume`` runs.
        user_config: Batch size, gather timeout, and parallelism.
        enable_auto_commit: When false, commit after a successful batch.
        closed: Set by ``request_stop``.
        service_name: Callee service name, or ``None`` when the default
            service name is in use.
    """

    def __init__(self, app, handle, user_config: UserConfig, service_name: str):
        """Store the handler, batch config, and auto-commit flag.

        Args:
            app: Worker that owns this consumer.
            handle: Service handler for each key group.
            user_config: Parsed server target, including ``batch_num``,
                ``msg_gather_timeout``, and ``parallel_num``.
            service_name: Callee name passed into the message, or
                ``None``.
        """
        self.app = app
        self.handle = handle
        self.consumer = None
        self.user_config = user_config
        self.enable_auto_commit = is_true(user_config.enable_auto_commit)
        self.closed = False
        self._stop_event = None
        self.service_name = service_name
        logger.debug("init TrpcKafkaBatchConsumer, config=%s", json.dumps(user_config.__dict__, default=str))

    async def consume(self):
        """Poll with ``getmany`` and dispatch each partition batch until stopped.

        Groups of one batch run concurrently. The offset is committed
        after ``asyncio.gather`` returns, and only when auto-commit is
        off. A handler failure is counted and logged, and that batch is
        not committed. A stop request is checked before every poll, so the
        batch being handled is finished first.

        The consumer is stopped when the loop ends.
        """
        stop = self._stop_requested()
        try:
            while not self.closed:
                try:
                    kafka_records = await _fetch_unless_stopped(
                        self.consumer.getmany(timeout_ms=self.user_config.msg_gather_timeout,
                                              max_records=self.user_config.batch_num), stop)
                except ConsumerStoppedError:
                    break
                if kafka_records is _STOPPED:
                    break
                for kafka_msgs in kafka_records.values():
                    try:
                        logger.debug("get %d msgs", len(kafka_msgs))
                        msg_groups = self._split_msgs_to_group(kafka_msgs)
                        tasks = []
                        for group in msg_groups:
                            if len(group) == 0:
                                continue
                            # Groups run concurrently; records in one group stay
                            # serial so the same key keeps its order.
                            tasks.append(asyncio.create_task(self._handle_msgs(group)))
                        # Wait until every key group in this batch has been processed.
                        await asyncio.gather(*tasks)
                        if not self.enable_auto_commit:
                            await self.consumer.commit()
                    except Exception as err:  # pylint: disable=broad-except
                        report_attr("trpc_kafka_handle_err", 1)
                        logger.error(
                            "[executor] failed to send rsp to tcp client, err: %s, trace: %s",
                            repr(err),
                            traceback.format_exc(),
                        )
        finally:
            await self.consumer.stop()

    def _split_msgs_to_group(self, kafka_msgs: List[ConsumerRecord]) -> List[List[ConsumerRecord]]:
        """Split one poll batch into key-stable groups.

        ``parallel_num == 1`` keeps the batch as a single group. Otherwise
        records are hashed by key into ``parallel_num`` groups so the same
        key stays together and groups can run concurrently.

        Args:
            kafka_msgs: Records from one ``getmany`` partition batch.

        Returns:
            One list per group. Empty groups are left in place; the caller
            skips them.
        """
        if self.user_config.parallel_num == 1:
            return [kafka_msgs]
        msg_groups: List[List[ConsumerRecord]] = []
        for _ in range(self.user_config.parallel_num):
            msg_groups.append([])
        for msg in kafka_msgs:
            group_id = hash(msg.key) % self.user_config.parallel_num
            msg_groups[group_id].append(msg)
        return msg_groups

    async def _handle_msgs(self, kafka_msgs: List[ConsumerRecord]) -> None:
        """Run the registered service handler for one key group.

        Records are stored on ``ctx.msg.server_req_head``. Dispatch still
        goes through ``Service.handle``, which looks up the method
        registered as ``/{svc}/handle``.

        Args:
            kafka_msgs: Records that share one hash bucket.
        """
        ctx = TrpcContext()
        msg = TrpcMessage()
        msg.server_req_head = kafka_msgs
        if self.service_name is not None:
            msg.callee_service_name = self.service_name
        ctx.msg = msg
        ctx.recv_time = time.time() * 1000
        await self.handle(ctx, None)
