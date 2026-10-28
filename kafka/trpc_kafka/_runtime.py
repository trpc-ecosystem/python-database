# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Run Kafka consumers on every serving loop of the SDK runtime.

Importing this module registers :func:`start_kafka` as the protocol starter
of ``kafka``, so ``ServiceAdapter.run`` serves every Kafka address through a
``UserProtocolTransport``, and registers a serving-loop thread hook pair.

* ``on_start`` runs on each serving loop before it serves: the main loop
  with a single loop, every handle thread in merge multi-thread and
  SeparateThread mode, every rpc task in separate mode, every multi_process
  merge worker.
  It builds one ``AIOKafkaConsumer`` per Kafka address on that loop, all in
  the address's group, and starts consuming. Processes that run no handlers
  (the separate-mode I/O process, task processes) never call it.
* :func:`start_kafka` runs once per address in each serving process and only
  hooks shutdown: :func:`request_stop_all` goes on ``worker.exit_callbacks``.
* On shutdown, :func:`request_stop_all` stops every loop's fetches, so the
  drain only finishes what was already fetched. ``on_stop`` then waits for
  the consume tasks, each of which commits and stops its consumer, and stops
  the producers this loop created.
"""

import asyncio
import os
import threading
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from trpc_sdk.log import logger
from trpc_sdk.runtime import HandlerThreadInfo
from trpc_sdk.runtime import current_thread_model
from trpc_sdk.runtime import register_handler_thread_hook
from trpc_sdk.transport import register_protocol_handle

from .client_transport import stop_producers_for_current_loop
from .constants import KAFKA_NAME
from .worker import build_consumer

# Seconds ``on_stop`` waits for the consume tasks before cancelling them.
# Kept below the SDK's 5 s budget for stopping every serving thread, so the
# consumers still leave their group before the thread is abandoned.
STOP_TIMEOUT_SECONDS = 4.0


class _LoopConsumers:
    """Consumers and consume tasks of one serving loop.

    Attributes:
        loop: The serving loop they run on.
        consumers: ``TrpcKafkaConsumer`` / ``TrpcKafkaBatchConsumer``, one
            per Kafka address.
        tasks: Their ``consume()`` tasks, index-aligned with ``consumers``.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop):
        """Start with no consumers.

        Args:
            loop: The serving loop.
        """
        self.loop = loop
        self.consumers: List[Any] = []
        self.tasks: List[asyncio.Task] = []

    def request_stop(self) -> None:
        """Stop every consumer's fetching; must run on ``loop``."""
        for consumer in self.consumers:
            consumer.request_stop()

    async def stop(self, timeout: float) -> None:
        """Stop fetching, wait for the consume tasks, and make sure every consumer is stopped.

        A task still running after ``timeout`` is cancelled. ``AIOKafkaConsumer.stop``
        is idempotent, so it is called again for every consumer in case its
        task never got to it.

        Args:
            timeout: Seconds to wait for the consume tasks.
        """
        self.request_stop()
        if self.tasks:
            _, pending = await asyncio.wait(self.tasks, timeout=timeout)
            for task in pending:
                logger.warning("[trpc_kafka] consume task still running after %.1f s; cancelling it", timeout)
                task.cancel()
            if pending:
                await asyncio.wait(pending, timeout=timeout)
        for consumer in self.consumers:
            try:
                await consumer.consumer.stop()
            except Exception as err:  # pylint: disable=broad-except
                logger.error("[trpc_kafka] failed to stop consumer: %s", repr(err))


class _Registry:
    """``{loop: _LoopConsumers}`` of this process, shared by every serving thread."""

    def __init__(self):
        """Start empty."""
        self._lock = threading.Lock()
        self._groups: Dict[asyncio.AbstractEventLoop, _LoopConsumers] = {}

    def add(self, group: _LoopConsumers) -> None:
        """Record the consumers of ``group.loop``.

        Args:
            group: Consumers of one serving loop.
        """
        with self._lock:
            self._groups[group.loop] = group

    def pop(self, loop: asyncio.AbstractEventLoop) -> Optional[_LoopConsumers]:
        """Take the consumers of ``loop``.

        Args:
            loop: A serving loop.

        Returns:
            Its consumers, or ``None`` when it has none.
        """
        with self._lock:
            return self._groups.pop(loop, None)

    def groups(self) -> List[_LoopConsumers]:
        """Return a snapshot of every loop's consumers."""
        with self._lock:
            return list(self._groups.values())

    def reset(self) -> None:
        """Forget every loop; the lock is replaced since another thread may have held it at fork."""
        self._lock = threading.Lock()
        self._groups = {}


_REGISTRY = _Registry()

if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_REGISTRY.reset)


def start_kafka(worker: Any, index: int) -> None:
    """SDK protocol starter for a Kafka address; only hooks shutdown.

    The consumers already run, started by the serving-loop ``on_start`` hook.
    Called once per Kafka address in each serving process, so the exit
    callback is added only once. In multi-thread mode ``worker`` is a
    ``WorkerLoopView``, which forwards the list to the real worker.

    Args:
        worker: SDK worker serving the address.
        index: Position of the address in the worker's tables. Unused.
    """
    del index
    if request_stop_all not in worker.exit_callbacks:
        worker.exit_callbacks.append(request_stop_all)


def request_stop_all() -> None:
    """Stop fetching on every serving loop of this process.

    Runs as an exit callback on the main thread, so each loop is reached
    with ``call_soon_threadsafe``.
    """
    for group in _REGISTRY.groups():
        try:
            group.loop.call_soon_threadsafe(group.request_stop)
        except RuntimeError:
            # The loop is already closed; its consumers are gone with it.
            pass


def _kafka_indexes(worker: Any) -> List[int]:
    """Positions of the Kafka addresses in ``worker``'s tables.

    Args:
        worker: SDK worker.

    Returns:
        Indexes whose transport was resolved to :func:`start_kafka`.
    """
    return [i for i, transport in enumerate(worker.transports)
            if getattr(transport, "starter", None) is start_kafka]


def _log_consume_end(task: asyncio.Task) -> None:
    """Done callback of a consume task: log how it ended unless it was cancelled.

    Args:
        task: A finished ``consume()`` task.
    """
    if task.cancelled():
        return
    err = task.exception()
    if err is not None:
        logger.error("[trpc_kafka] consume task failed: %s", repr(err), exc_info=err)
    else:
        logger.info("[trpc_kafka] consumer stopped")


async def _on_start(info: HandlerThreadInfo) -> None:
    """Build, start, and run one consumer per Kafka address on this serving loop.

    A consumer that fails to start aborts startup; the ones already started
    are stopped first.

    Args:
        info: The serving loop being started.

    Raises:
        Exception: Whatever building or starting a consumer raised.
    """
    worker = current_thread_model().worker
    if worker is None:
        return
    indexes = _kafka_indexes(worker)
    if not indexes:
        return
    group = _LoopConsumers(info.loop)
    try:
        for i in indexes:
            consumer = await build_consumer(worker, i)
            group.consumers.append(consumer)
            await consumer.consumer.start()
    except BaseException:
        await group.stop(STOP_TIMEOUT_SECONDS)
        raise
    for consumer in group.consumers:
        task = info.loop.create_task(consumer.consume())
        task.add_done_callback(_log_consume_end)
        group.tasks.append(task)
    _REGISTRY.add(group)
    logger.info("[trpc_kafka] serving loop %s/%s started %s consumer(s)", info.index, info.count,
                len(group.consumers))


async def _on_stop(info: HandlerThreadInfo) -> None:
    """Stop this serving loop's consumers, then the producers it created.

    Args:
        info: The serving loop being stopped.
    """
    group = _REGISTRY.pop(info.loop)
    if group is not None:
        await group.stop(STOP_TIMEOUT_SECONDS)
    await stop_producers_for_current_loop()


register_protocol_handle(KAFKA_NAME, start_kafka)
register_handler_thread_hook(_on_start, _on_stop)
