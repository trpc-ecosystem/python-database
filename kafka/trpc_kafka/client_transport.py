# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Kafka client transport.

One started ``AIOKafkaProducer`` is cached per address and endpoint.
``round_trip`` sends the record on ``client_req_head`` and stores the
broker metadata on ``client_rsp_head``.

One transport instance serves every thread of a process. Producers are
cached per thread and per event loop, because an ``AIOKafkaProducer`` is
bound to the loop it was created on. A forked child starts with an empty
cache.
"""
import asyncio
import os
import threading
import time
import weakref
from typing import Dict

from aiokafka import AIOKafkaProducer
from trpc_sdk import exceptions
from trpc_sdk.abc import BaseClientTransport
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.log import logger
from trpc_sdk.plugin import register_plugin

from .config import ClientUserConfig
from .constants import KAFKA_NAME
from .utils import to_bytes
from .utils import is_true
from .utils import parse_kafka_params
from .utils import unmarshal_config


class _LoopProducers(threading.local):
    """Per-thread map from event loop to that loop's producer start tasks.

    Loops are keyed by object rather than ``id()`` so a closed loop's id
    cannot be reused to return a producer bound to the dead loop.
    """

    def __init__(self):
        """Start the calling thread with no producers."""
        super().__init__()
        self.producers: Dict[asyncio.AbstractEventLoop, Dict[str, asyncio.Task]] = {}

    def for_loop(self, loop: asyncio.AbstractEventLoop) -> Dict[str, asyncio.Task]:
        """Return the start tasks of ``loop``, creating the dict on first use.

        Entries of closed loops are dropped when a new loop is seen.

        Args:
            loop: The running event loop of the calling thread.

        Returns:
            Mutable mapping from cache key to the task starting that producer.
        """
        producers = self.producers.get(loop, None)
        if producers is None:
            for stale in [old for old in self.producers if old.is_closed()]:
                del self.producers[stale]
            producers = {}
            self.producers[loop] = producers
        return producers

    def pop_loop(self, loop: asyncio.AbstractEventLoop) -> Dict[str, asyncio.Task]:
        """Take the start tasks of ``loop``.

        Args:
            loop: The running event loop of the calling thread.

        Returns:
            Mapping from cache key to start task; empty when none were cached.
        """
        return self.producers.pop(loop, {})


_TRANSPORTS: "weakref.WeakSet[KafkaClientTransport]" = weakref.WeakSet()


def _reset_producers_after_fork():
    """Drop every producer inherited from the parent process."""
    for transport in list(_TRANSPORTS):
        transport.reset_producers()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_producers_after_fork)


async def stop_producers_for_current_loop() -> None:
    """Stop the producers every transport created on the running loop.

    Called by the serving-loop ``on_stop`` hook, so records still lingering
    in a producer are flushed before the loop goes away.
    """
    for transport in list(_TRANSPORTS):
        try:
            await transport.close()
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[trpc_kafka] failed to stop producers: %s", repr(err))


def _failed(task: asyncio.Task) -> bool:
    """Whether a start task finished without a producer.

    Args:
        task: A producer start task.
    """
    return task.done() and (task.cancelled() or task.exception() is not None)


@register_plugin(PluginType.CLIENT_TRANSPORT, KAFKA_NAME, init_pos=PluginInitPos.WORKER)
class KafkaClientTransport(BaseClientTransport):
    """Send produce requests with a cached ``AIOKafkaProducer``.

    Attributes:
        opts: Unused option slot kept on the instance.
    """

    def __init__(self):
        """Start with no cached producers."""
        super().__init__()
        self.opts = None
        self._loop_producers = _LoopProducers()
        _TRANSPORTS.add(self)

    @property
    def producers(self) -> Dict[str, asyncio.Task]:
        """Producer start tasks of the running loop, keyed by ``host:port`` plus endpoint."""
        return self._loop_producers.for_loop(asyncio.get_running_loop())

    def reset_producers(self):
        """Forget every cached producer, in all threads."""
        self._loop_producers = _LoopProducers()

    async def round_trip(self, ctx: TrpcContext, req_data: bytes, options):
        """Produce the record on ``ctx.msg.client_req_head``.

        The producer is cached by address plus endpoint. A positive
        ``ctx.deadline`` is converted to the remaining seconds and
        passed to ``asyncio.wait_for``. The broker result is stored on
        ``client_rsp_head``.

        Args:
            ctx: Request context. ``deadline`` and ``client_req_head``
                are read from it.
            req_data: Encoded body. The transport sends ``client_req_head``
                instead of this buffer.
            options: Call options. ``address`` and ``endpoint`` select
                the producer.

        Returns:
            The record metadata returned by ``send_and_wait``.

        Raises:
            exceptions.ClientTimeoutError: The deadline is already past,
                or ``send_and_wait`` exceeds the remaining time.
            exceptions.ClientInvokeError: The producer raises any other
                error.
        """
        msg = ctx.msg
        req = msg.client_req_head
        address = options.address
        endpoint = options.endpoint
        producer = await self.get_producer(address[0] + ":" + str(address[1]) + endpoint, endpoint)

        timeout = None
        if ctx.deadline > 0:
            remain = (ctx.deadline - time.time() * 1000) / 1000.0
            if remain <= 0:
                raise exceptions.ClientTimeoutError("reach deadline")
            timeout = remain

        try:
            ret = await asyncio.wait_for(producer.send_and_wait(
                req.topic, key=to_bytes(req.key), value=to_bytes(req.value)), timeout)
        except asyncio.TimeoutError as e:
            raise exceptions.ClientTimeoutError(
                "kafka client timeout topic %s message %s" % (req.topic, req.value)) from e
        except Exception as e:  # pylint: disable=broad-except
            raise exceptions.ClientInvokeError(str(e)) from e
        msg.client_rsp_head = ret
        return ret

    async def get_producer(self, target: str, endpoint):
        """Return a started producer of the running loop, creating it once per target.

        Concurrent first calls on one loop share the same start task, so
        only one producer is started. A start that fails is dropped from
        the cache and the next call tries again.

        Args:
            target: Cache key, ``host:port`` concatenated with the
                endpoint string.
            endpoint: ``brokers?k=v`` string parsed into
                ``ClientUserConfig``.

        Returns:
            The cached or newly started ``AIOKafkaProducer``.

        Raises:
            Exception: Whatever creating or starting the producer raised.
        """
        producers = self.producers
        starting = producers.get(target, None)
        if starting is None or _failed(starting):
            starting = asyncio.ensure_future(self._start_producer(endpoint))
            producers[target] = starting
        try:
            return await asyncio.shield(starting)
        except Exception:
            if producers.get(target, None) is starting and _failed(starting):
                del producers[target]
            raise

    async def _start_producer(self, endpoint: str) -> AIOKafkaProducer:
        """Create and start one producer on the running loop.

        Args:
            endpoint: ``brokers?k=v`` string parsed into ``ClientUserConfig``.

        Returns:
            The started producer.
        """
        client_user_config = self.parser_endpoint(endpoint)
        enable_idempotence = is_true(client_user_config.enable_idempotence)
        producer = AIOKafkaProducer(
            bootstrap_servers=client_user_config.bootstrap_servers,
            client_id=client_user_config.client_id,
            metadata_max_age_ms=client_user_config.metadata_max_age_ms,
            request_timeout_ms=client_user_config.request_timeout_ms,
            api_version=client_user_config.api_version,
            key_serializer=client_user_config.key_serializer,
            value_serializer=client_user_config.value_serializer,
            compression_type=client_user_config.compression_type,
            max_batch_size=client_user_config.max_batch_size,
            max_request_size=client_user_config.max_request_size,
            linger_ms=client_user_config.linger_ms,
            retry_backoff_ms=client_user_config.retry_backoff_ms,
            security_protocol=client_user_config.security_protocol,
            ssl_context=client_user_config.ssl_context,
            connections_max_idle_ms=client_user_config.connections_max_idle_ms,
            enable_idempotence=enable_idempotence,
            transactional_id=client_user_config.transactional_id,
            transaction_timeout_ms=client_user_config.transaction_timeout_ms,
            sasl_mechanism=client_user_config.sasl_mechanism,
            sasl_plain_password=client_user_config.sasl_plain_password,
            sasl_plain_username=client_user_config.sasl_plain_username,
            sasl_kerberos_service_name=client_user_config.sasl_kerberos_service_name,
            sasl_kerberos_domain_name=client_user_config.sasl_kerberos_domain_name,
            sasl_oauth_token_provider=client_user_config.sasl_oauth_token_provider
        )
        await producer.start()
        return producer

    def parser_endpoint(self, endpoint: str):
        """Parse ``brokers?k=v&k=v`` into a ``ClientUserConfig``.

        Args:
            endpoint: Broker list, optionally followed by a query string.

        Returns:
            Config whose ``bootstrap_servers`` come from the broker list.
            Query keys overwrite matching attributes.
        """
        kafka_config = endpoint.split("?")
        client_user_config = ClientUserConfig(bootstrap_servers=kafka_config[0].split(","))
        if len(kafka_config) == 1:
            return client_user_config

        token_map = parse_kafka_params(kafka_config[1])
        unmarshal_config(token_map, client_user_config)
        return client_user_config

    async def close(self):
        """Stop the producers cached for the running loop and drop them.

        Producers of other loops are left alone: they can only be stopped
        on their own loop. A producer still starting is cancelled.
        """
        starting = self._loop_producers.pop_loop(asyncio.get_running_loop())
        for task in starting.values():
            if not task.done():
                task.cancel()
                await asyncio.wait((task,))
            if _failed(task):
                continue
            await task.result().stop()
