# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Kafka produce request and server method placeholder types."""


class Request:
    """Client produce request stored on ``client_req_head``.

    Attributes:
        topic: Kafka topic to produce to.
        key: Record key. May be ``None``, ``str``, or ``bytes``.
        value: Record value.
    """

    def __init__(self, topic: str, key, value: str):
        """Store the topic, key, and value for one produce call.

        Args:
            topic: Kafka topic to produce to.
            key: Record key.
            value: Record value.
        """
        self.topic = topic
        self.key = key
        self.value = value


class TrpcKafkaMessage:
    """Request-type placeholder used when registering a Kafka ``Method``.

    The registered handler does not receive a filled instance of this
    class. The consumer puts the real record, or a list of records, on
    ``ctx.msg.server_req_head``.
    """
    pass
