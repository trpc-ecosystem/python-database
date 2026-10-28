# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Kafka server transport.

The ``kafka`` protocol is registered with ``register_protocol_handle``, so
``ServiceAdapter.run`` serves its addresses through a ``UserProtocolTransport``
bound to :func:`~trpc_kafka._runtime.start_kafka`. No socket is opened; the
consumers run on the serving loops, see :mod:`trpc_kafka._runtime`.
"""
from trpc_sdk.abc import BasePluginFactory
from trpc_sdk.transport import UserProtocolTransport

from ._runtime import start_kafka


class KafkaServerTransport(UserProtocolTransport):
    """Kafka server transport: a ``UserProtocolTransport`` bound to ``start_kafka``.

    The SDK builds its own ``UserProtocolTransport`` for every Kafka service
    from the registered starter, so this class is not a plugin; it is kept
    for code that constructs the Kafka transport explicitly.
    """

    def __init__(self):
        """Bind the transport to the Kafka starter."""
        super().__init__(start_kafka)


class KafkaServerTransportFactory(BasePluginFactory):
    """Factory for :class:`KafkaServerTransport`.

    Not registered as a plugin, for the same reason as the transport.
    """

    def create(self) -> KafkaServerTransport:
        """Return a new ``KafkaServerTransport``.

        Returns:
            An unstarted transport.
        """
        return KafkaServerTransport()
