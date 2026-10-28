# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Kafka servicer interfaces implemented by business code.

The single-record interface receives one consumed record as separate
fields. The batch interface receives one key-stable group.
"""
from trpc_sdk.abc import TrpcContext


class TrpcKafkaServicer:
    """Handle one Kafka record at a time."""

    async def handle(self, ctx: TrpcContext, key, value, topic, partition, offset, timestamp):
        """Process one consumed record.

        Args:
            ctx: Request context for this record.
            key: Record key.
            value: Record value.
            topic: Topic the record was read from.
            partition: Partition the record was read from.
            offset: Record offset.
            timestamp: Record timestamp from the broker.

        Raises:
            NotImplementedError: The subclass did not override this method.
        """
        raise NotImplementedError('Method not implemented!')


class TrpcKafkaBatchConsumerServicer:
    """Handle one key-stable group of Kafka records."""

    async def handle(self, ctx: TrpcContext, msgs):
        """Process one key group from a poll batch.

        Args:
            ctx: Request context for this group.
            msgs: ``ConsumerRecord`` list that shares one key hash
                bucket. Records in the list stay in poll order.

        Raises:
            NotImplementedError: The subclass did not override this method.
        """
        raise NotImplementedError('Method not implemented!')
