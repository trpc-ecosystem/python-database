# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Public Kafka client and server entry points.

Re-exports the client, consumer, codec, and transport symbols that callers
import from this package. The concrete names are listed in ``__all__``.

Importing the package registers the ``kafka`` protocol starter and the
serving-loop hooks that run the consumers, so it must be imported before
``Server.serve()``.
"""

from ._runtime import start_kafka
from .client import Client
from .client_transport import KafkaClientTransport
from .codec import ClientKafkaCodec
from .codec import ServerKafkaCodec
from .config import ClientUserConfig
from .config import UserConfig
from .consumer import TrpcKafkaBatchConsumer
from .consumer import TrpcKafkaConsumer
from .handler import batch_handle
from .handler import handle
from .kafka_transport import KafkaServerTransport
from .kafka_transport import KafkaServerTransportFactory
from .register import register_trpc_kafka_batch_consumer_server
from .register import register_trpc_kafka_server
from .servicer import TrpcKafkaBatchConsumerServicer
from .servicer import TrpcKafkaServicer
from .types import Request
from .types import TrpcKafkaMessage
from .utils import create_rsp_object
from .utils import get_options

__version__ = '0.1.0'

__all__ = [
    'Request',
    'TrpcKafkaMessage',
    'UserConfig',
    'ClientUserConfig',
    'get_options',
    'create_rsp_object',
    'Client',
    'KafkaClientTransport',
    'ServerKafkaCodec',
    'ClientKafkaCodec',
    'TrpcKafkaConsumer',
    'TrpcKafkaBatchConsumer',
    'TrpcKafkaServicer',
    'TrpcKafkaBatchConsumerServicer',
    'handle',
    'batch_handle',
    'register_trpc_kafka_server',
    'register_trpc_kafka_batch_consumer_server',
    'KafkaServerTransport',
    'KafkaServerTransportFactory',
    'start_kafka',
]
