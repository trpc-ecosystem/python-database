# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Kafka consumer and producer configuration objects.

``UserConfig`` is filled from the server target query string.
``ClientUserConfig`` is filled from the client endpoint query string.
Fields that are not listed below are forwarded unchanged to the
corresponding aiokafka consumer or producer constructor.
"""


class UserConfig:
    """Consumer options parsed from a server target.

    Most attributes are passed through to ``AIOKafkaConsumer``. The
    attributes below are interpreted by this package.

    Attributes:
        group: Consumer group id. Also used as ``client_id`` when
            ``clientid`` is empty.
        clientid: Explicit ``client_id``. Empty falls back to ``group``.
        topics: Comma-separated topic list.
        enable_auto_commit: String flag read two ways. The aiokafka
            consumer turns auto-commit on unless the value is ``"false"``.
            ``TrpcKafkaConsumer`` commits manually unless the value is
            ``"true"``.
        parallel_num: Number of key-stable groups processed concurrently
            in batch mode. ``1`` keeps the poll batch as one group.
        msg_gather_timeout: ``getmany`` timeout in milliseconds. A larger
            value gathers more records before a batch is dispatched.
        batch_num: Maximum records per ``getmany`` call. ``0`` selects
            the single-record consumer instead of batch mode.
    """

    def __init__(
        self,
        group: str = "",
        clientid: str = "",
        topics: str = "",
        enable_auto_commit: str = "",
        session_timeout_ms: int = 0,
        key_deserializer=None,
        value_deserializer=None,
        fetch_max_wait_ms: int = 500,
        fetch_max_bytes: int = 52428800,
        fetch_min_bytes: int = 1,
        max_partition_fetch_bytes: int = 1 * 1024 * 1024,
        request_timeout_ms: int = 40 * 1000,
        retry_backoff_ms: int = 100,
        auto_offset_reset: str = "latest",
        auto_commit_interval_ms: int = 5000,
        metadata_max_age_ms: int = 5 * 60 * 1000,
        max_poll_interval_ms: int = 300000,
        rebalance_timeout_ms: int = None,
        heartbeat_interval_ms: int = 3000,
        consumer_timeout_ms: int = 200,
        max_poll_records: int = None,
        ssl_context=None,
        security_protocol: str = "PLAINTEXT",
        api_version: str = "auto",
        connections_max_idle_ms: int = 540000,
        isolation_level: str = "read_uncommitted",
        sasl_mechanism: str = "PLAIN",
        sasl_plain_password: str = None,
        sasl_plain_username: str = None,
        sasl_kerberos_service_name: str = "kafka",
        sasl_kerberos_domain_name: str = None,
        sasl_oauth_token_provider=None,
        check_crcs: str = "true",
        exclude_internal_topics: str = "true",
        parallel_num: int = 1,
        msg_gather_timeout: int = 1000,
        batch_num: int = 0,
    ):
        """Store consumer options, including aiokafka passthrough fields.

        Package-specific fields are documented on the class. Every other
        argument is an aiokafka consumer option passed on by ``build_consumer``.
        """
        self.group = group
        self.clientid = clientid
        self.topics = topics
        self.enable_auto_commit = enable_auto_commit
        self.session_timeout_ms = session_timeout_ms
        self.key_deserializer = key_deserializer
        self.value_deserializer = value_deserializer
        self.fetch_max_wait_ms = fetch_max_wait_ms
        self.fetch_max_bytes = fetch_max_bytes
        self.fetch_min_bytes = fetch_min_bytes
        self.max_partition_fetch_bytes = max_partition_fetch_bytes
        self.request_timeout_ms = request_timeout_ms
        self.retry_backoff_ms = retry_backoff_ms
        self.auto_offset_reset = auto_offset_reset
        self.auto_commit_interval_ms = auto_commit_interval_ms
        self.metadata_max_age_ms = metadata_max_age_ms
        self.max_poll_interval_ms = max_poll_interval_ms
        self.rebalance_timeout_ms = rebalance_timeout_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.consumer_timeout_ms = consumer_timeout_ms
        self.max_poll_records = max_poll_records
        self.ssl_context = ssl_context
        self.security_protocol = security_protocol
        self.api_version = api_version
        self.connections_max_idle_ms = connections_max_idle_ms
        self.isolation_level = isolation_level
        self.sasl_mechanism = sasl_mechanism
        self.sasl_plain_password = sasl_plain_password
        self.sasl_plain_username = sasl_plain_username
        self.sasl_kerberos_service_name = sasl_kerberos_service_name
        self.sasl_kerberos_domain_name = sasl_kerberos_domain_name
        self.sasl_oauth_token_provider = sasl_oauth_token_provider
        self.check_crcs = check_crcs
        self.exclude_internal_topics = exclude_internal_topics
        self.parallel_num = parallel_num
        self.msg_gather_timeout = msg_gather_timeout
        self.batch_num = batch_num


class ClientUserConfig:
    """Producer options parsed from a client endpoint.

    Most attributes are passed through to ``AIOKafkaProducer``. The
    attributes below are interpreted by this package.

    Attributes:
        bootstrap_servers: Broker list taken from the endpoint host
            section, before ``?``.
        enable_idempotence: String flag. Only the text ``"true"``
            (case-insensitive) turns idempotent produce on.
    """

    def __init__(self,
                 bootstrap_servers,
                 client_id: str = "",
                 metadata_max_age_ms: int = 300000,
                 request_timeout_ms: int = 40000,
                 api_version: str = 'auto',
                 key_serializer=None,
                 value_serializer=None,
                 compression_type=None,
                 max_batch_size: int = 16384,
                 max_request_size: int = 1048576,
                 linger_ms: int = 0,
                 retry_backoff_ms: int = 100,
                 security_protocol: str = "PLAINTEXT",
                 ssl_context=None,
                 connections_max_idle_ms: int = 540000,
                 enable_idempotence: str = 'false',
                 transactional_id=None,
                 transaction_timeout_ms: int = 60000,
                 sasl_mechanism: str = "PLAIN",
                 sasl_plain_password: str = "",
                 sasl_plain_username: str = "",
                 sasl_kerberos_service_name: str = 'kafka',
                 sasl_kerberos_domain_name=None,
                 sasl_oauth_token_provider=None
                 ):
        """Store producer options, including aiokafka passthrough fields.

        Package-specific fields are documented on the class. Every other
        argument is an aiokafka producer option kept until ``get_producer``.
        """
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.metadata_max_age_ms = metadata_max_age_ms
        self.request_timeout_ms = request_timeout_ms
        self.api_version = api_version
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.compression_type = compression_type
        self.max_batch_size = max_batch_size
        self.max_request_size = max_request_size
        self.linger_ms = linger_ms
        self.retry_backoff_ms = retry_backoff_ms
        self.security_protocol = security_protocol
        self.ssl_context = ssl_context
        self.connections_max_idle_ms = connections_max_idle_ms
        self.enable_idempotence = enable_idempotence
        self.transactional_id = transactional_id
        self.transaction_timeout_ms = transaction_timeout_ms
        self.sasl_mechanism = sasl_mechanism
        self.sasl_plain_password = sasl_plain_password
        self.sasl_plain_username = sasl_plain_username
        self.sasl_kerberos_service_name = sasl_kerberos_service_name
        self.sasl_kerberos_domain_name = sasl_kerberos_domain_name
        self.sasl_oauth_token_provider = sasl_oauth_token_provider
