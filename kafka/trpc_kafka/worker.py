# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Build the consumer that serves one Kafka listen address."""

from typing import List
from typing import Union

from aiokafka import AIOKafkaConsumer
from trpc_sdk.log import logger

from .config import UserConfig
from .constants import USER_DEFINED_SVC_NAME
from .consumer import TrpcKafkaBatchConsumer
from .consumer import TrpcKafkaConsumer
from .utils import is_false
from .utils import parse_kafka_params
from .utils import unmarshal_config


async def build_consumer(worker, index: int) -> Union[TrpcKafkaConsumer, TrpcKafkaBatchConsumer]:
    """Build the consumer for the Kafka address at ``index``; it is not started.

    ``batch_num == 0`` uses ``TrpcKafkaConsumer``. Any other value uses
    ``TrpcKafkaBatchConsumer``. The callee service name is passed through
    only when ``USER_DEFINED_SVC_NAME`` was set at registration; otherwise
    the consumer receives ``None`` and the codec keeps ``DEFAULT_SVC_NAME``.

    A coroutine because ``AIOKafkaConsumer`` binds to the running loop when
    it is constructed: call it on the loop that will consume.

    Args:
        worker: SDK worker whose ``tran_opts`` and ``handlers`` describe
            the address.
        index: Position of the address in the worker's tables.

    Returns:
        A consumer whose ``consumer`` attribute holds an unstarted
        ``AIOKafkaConsumer``.

    Raises:
        Exception: The target has no ``?`` query section.
    """
    tran_opt = worker.tran_opts[index]
    kafka_config = tran_opt.target.split("?")
    if len(kafka_config) != 2:
        raise Exception("lack of kafka brokers")

    brokers = kafka_config[0].split(",")

    user_config = UserConfig()
    token_map = parse_kafka_params(kafka_config[1])
    unmarshal_config(token_map, user_config)
    custom_svc_name = bool(USER_DEFINED_SVC_NAME.value)
    logger.debug(f"user set custom_svc_name: {custom_svc_name}")
    service_name = tran_opt.service_name if custom_svc_name else None
    handler = worker.handlers[index]
    if user_config.batch_num == 0:
        trpc_kafka_consumer = TrpcKafkaConsumer(worker, handler, user_config.enable_auto_commit, service_name)
    else:
        trpc_kafka_consumer = TrpcKafkaBatchConsumer(worker, handler, user_config, service_name)
    trpc_kafka_consumer.consumer = new_aiokafka_consumer(user_config, brokers)
    return trpc_kafka_consumer


def new_aiokafka_consumer(user_config: UserConfig, brokers: List[str]) -> AIOKafkaConsumer:
    """Construct the ``AIOKafkaConsumer`` for one target.

    ``session_timeout_ms`` below 10000 is raised to 10000. An empty
    ``clientid`` falls back to ``group`` so Wormhole Kafka still
    accepts the connection.

    Args:
        user_config: Parsed target options.
        brokers: Broker addresses from the target host section.

    Returns:
        An unstarted consumer bound to the running loop.
    """
    enable_auto_commit = not is_false(user_config.enable_auto_commit)
    check_crcs = not is_false(user_config.check_crcs)
    exclude_internal_topics = not is_false(user_config.exclude_internal_topics)
    session_timeout_ms = user_config.session_timeout_ms
    if session_timeout_ms < 10000:
        session_timeout_ms = 10000
    topics = [topic for topic in user_config.topics.split(",")]
    return AIOKafkaConsumer(
        *topics,
        bootstrap_servers=brokers,
        group_id=user_config.group,
        # Wormhole Kafka rejects the connection when client_id is missing.
        # An empty clientid falls back to the group id.
        client_id=user_config.group if user_config.clientid == "" else user_config.clientid,
        enable_auto_commit=enable_auto_commit,
        session_timeout_ms=session_timeout_ms,
        key_deserializer=user_config.key_deserializer,
        value_deserializer=user_config.value_deserializer,
        fetch_max_wait_ms=user_config.fetch_max_wait_ms,
        fetch_max_bytes=user_config.fetch_max_bytes,
        fetch_min_bytes=user_config.fetch_min_bytes,
        max_partition_fetch_bytes=user_config.max_partition_fetch_bytes,
        request_timeout_ms=user_config.request_timeout_ms,
        retry_backoff_ms=user_config.retry_backoff_ms,
        auto_offset_reset=user_config.auto_offset_reset,
        auto_commit_interval_ms=user_config.auto_commit_interval_ms,
        check_crcs=check_crcs,
        metadata_max_age_ms=user_config.metadata_max_age_ms,
        max_poll_interval_ms=user_config.max_poll_interval_ms,
        rebalance_timeout_ms=user_config.rebalance_timeout_ms,
        heartbeat_interval_ms=user_config.heartbeat_interval_ms,
        consumer_timeout_ms=user_config.consumer_timeout_ms,
        max_poll_records=user_config.max_poll_records,
        ssl_context=user_config.ssl_context,
        security_protocol=user_config.security_protocol,
        api_version=user_config.api_version,
        exclude_internal_topics=exclude_internal_topics,
        connections_max_idle_ms=540000,
        isolation_level=user_config.isolation_level,
        sasl_mechanism=user_config.sasl_mechanism,
        sasl_plain_password=user_config.sasl_plain_password,
        sasl_plain_username=user_config.sasl_plain_username,
        sasl_kerberos_service_name=user_config.sasl_kerberos_service_name,
        sasl_kerberos_domain_name=user_config.sasl_kerberos_domain_name,
        sasl_oauth_token_provider=user_config.sasl_oauth_token_provider,
    )
