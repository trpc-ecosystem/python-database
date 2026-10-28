# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Example Kafka producer: send N numbered records, each with its own key.

Usage::

    python kafka_client.py [--num 200] [--topic test-topic] [--conf trpc_python.yaml]

Records are ``key-<i>`` / ``msg-<i>``, so the consumer log can be checked for
every record being consumed exactly once.
"""

import argparse
import asyncio
import os
import sys

from trpc_sdk.abc import TrpcContext
from trpc_sdk.config import TrpcConfig
from trpc_sdk.plugin import init_plugins

import trpc_kafka
from trpc_kafka.client_transport import stop_producers_for_current_loop

DEFAULT_CONF_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "trpc_python.yaml"))


async def produce(num: int, topic: str) -> None:
    """Send ``num`` records concurrently, then flush and stop the producer.

    Args:
        num: Number of records.
        topic: Kafka topic.
    """
    client = trpc_kafka.Client("trpc.kafka.producer.service")
    try:
        results = await asyncio.gather(*(client.produce(TrpcContext(), topic, f"key-{i}", f"msg-{i}")
                                         for i in range(num)))
    finally:
        await stop_producers_for_current_loop()
    partitions = sorted({meta.partition for meta in results})
    print(f"produced {len(results)} record(s) to {topic}, partitions={partitions}")


def main():
    """Load config, initialize plugins, and produce the records."""
    parser = argparse.ArgumentParser(description='tRPC Kafka example producer')
    parser.add_argument('--num', '-n', type=int, default=200)
    parser.add_argument('--topic', '-t', default='test-topic')
    parser.add_argument('--conf', '-c', default=DEFAULT_CONF_PATH)
    args = parser.parse_args()

    TrpcConfig.load(args.conf, "utf-8")
    init_plugins()
    asyncio.run(produce(args.num, args.topic))


if __name__ == '__main__':
    sys.exit(main())
