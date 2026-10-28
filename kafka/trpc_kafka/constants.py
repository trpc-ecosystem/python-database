# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Kafka protocol name and service-name flags."""

from multiprocessing import Value

# Protocol and plugin name used to register Kafka components and the protocol starter.
KAFKA_NAME: str = "kafka"

# Service name used when registration is called without a custom name.
DEFAULT_SVC_NAME: str = "trpc.kafka.consumer.service"

# Whether registration was given a custom Kafka service name.
# When set, the server codec matches that name. Otherwise it keeps the
# fixed default. A custom name must also match the service name in YAML.
USER_DEFINED_SVC_NAME = Value('i', False)
