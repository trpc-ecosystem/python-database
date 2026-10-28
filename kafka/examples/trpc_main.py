# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Example server that consumes Kafka and serves a Greeter that produces to Kafka.

Run it with any of the ``trpc_python*.yaml`` files in this directory to try
each process/thread model, for example::

    python trpc_main.py -c trpc_python_merge_processes.yaml
    python trpc_main.py -c trpc_python_merge_threads.yaml --batch

Every consumed record is printed and appended as one JSON line to
``$TRPC_KAFKA_CONSUMED_LOG`` (``/tmp/trpc_kafka_consumed.log`` by default),
with the pid and thread that handled it.
"""

import argparse
import json
import os
import sys
import tempfile
import threading
from typing import List
from typing import Tuple

import yaml
from aiokafka.structs import ConsumerRecord
from trpc_sdk.abc import TrpcContext
from trpc_sdk.log import logger
from trpc_sdk.server import create_server

# add the stub to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "stub")))  # noqa
sys.path.append(os.path.abspath(os.path.dirname(__file__)))  # noqa

import trpc_kafka

from trpc_trpc_python_group_helloworld import rpc
from greeter import GreeterServicer

DEFAULT_CONF_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "trpc_python.yaml"))
CONSUMED_LOG = os.environ.get("TRPC_KAFKA_CONSUMED_LOG", "/tmp/trpc_kafka_consumed.log")
BATCH_NUM = 10


def record_consumed(key, value, topic, partition, offset) -> None:
    """Print one consumed record and append it to ``CONSUMED_LOG``.

    One short ``O_APPEND`` write per record, so lines from several processes
    do not interleave.

    Args:
        key: Record key.
        value: Record value.
        topic: Topic the record was read from.
        partition: Partition the record was read from.
        offset: Record offset.
    """
    line = {
        "key": key.decode() if isinstance(key, bytes) else key,
        "value": value.decode() if isinstance(value, bytes) else value,
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "pid": os.getpid(),
        "thread": threading.current_thread().name,
    }
    print(line)
    with open(CONSUMED_LOG, "a", encoding="utf-8") as file:
        file.write(json.dumps(line) + "\n")


class KafkaServer(trpc_kafka.TrpcKafkaServicer):
    """Record each consumed record."""

    async def handle(self, ctx: TrpcContext, key, value, topic, partition, offset, timestamp):
        """Record one consumed record.

        Args:
            ctx: Request context for this record.
            key: Record key.
            value: Record value.
            topic: Topic the record was read from.
            partition: Partition the record was read from.
            offset: Record offset.
            timestamp: Record timestamp from the broker.
        """
        record_consumed(key, value, topic, partition, offset)


class KafkaBatchServer(trpc_kafka.TrpcKafkaBatchConsumerServicer):
    """Record each record of a consumed batch."""

    async def handle(self, ctx: TrpcContext, msgs: List[ConsumerRecord]):
        """Record every record in one batch.

        Args:
            ctx: Request context for this batch.
            msgs: Records delivered together.
        """
        logger.info("batch of %s record(s)", len(msgs))
        for msg in msgs:
            record_consumed(msg.key, msg.value, msg.topic, msg.partition, msg.offset)


def with_batch_targets(conf_path: str) -> str:
    """Write a copy of the config whose Kafka targets consume in batches.

    Batch consumption is selected by ``batch_num`` on the service target.

    Args:
        conf_path: Path of the tRPC config file.

    Returns:
        Path of the copy.
    """
    with open(conf_path, encoding="utf-8") as file:
        conf = yaml.safe_load(file)
    for service in conf["server"]["service"]:
        if service.get("protocol") == "kafka":
            service["target"] = f"{service['target']}&batch_num={BATCH_NUM}"
    fd, path = tempfile.mkstemp(prefix="trpc_kafka_batch_", suffix=".yaml")
    with os.fdopen(fd, "w", encoding="utf-8") as file:
        yaml.safe_dump(conf, file)
    return path


def parse_args(argv: List[str]) -> Tuple[str, bool]:
    """Resolve the server config path and the consume mode from the command line.

    Args:
        argv: Full argument vector, including the script name at index 0.

    Returns:
        Path of a config file that exists, and whether to consume in batches.
    """
    parser = argparse.ArgumentParser(description='tRPC Kafka example server')
    parser.add_argument('--conf',
                        '-c',
                        default=DEFAULT_CONF_PATH,
                        help='config file, for example: `python3 trpc_main.py --conf=trpc_python.yaml`')
    parser.add_argument('--batch', action='store_true', help=f'consume in batches of up to {BATCH_NUM} records')
    args = parser.parse_args(argv[1:])
    if not os.path.exists(args.conf):
        print(f"conf_path: {args.conf} not exists")
        sys.exit(1)
    return args.conf, args.batch


def serve(conf_path: str, batch: bool):
    """Create the server, register the Kafka servicer and Greeter, and block in ``serve``.

    Args:
        conf_path: Path of the tRPC config file.
        batch: Register the batch servicer instead of the one-record one.
    """
    server = create_server(with_batch_targets(conf_path) if batch else conf_path)
    if batch:
        trpc_kafka.register_trpc_kafka_batch_consumer_server(server, KafkaBatchServer())
    else:
        trpc_kafka.register_trpc_kafka_server(server, KafkaServer())
    rpc.register_GreeterServicer_server(server, GreeterServicer(), fix_rpc_name=True)
    server.serve()


def main():
    """Parse arguments and start the server."""
    conf_path, batch = parse_args(sys.argv)
    logger.info("load config %s, batch=%s", conf_path, batch)
    serve(conf_path, batch)


if __name__ == '__main__':
    sys.exit(main())
