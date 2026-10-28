# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""
Redis constants module for tRPC framework.

This module defines constants used in the Redis client implementation.
It includes configuration keys and default values for Redis connections.

Usage:
    The constants are used to configure Redis client options and connection parameters.
"""

from enum import Enum

# Plugin name of the codec and transport, the client protocol name, and the
# key of the extra ConnectionPool kwargs in YAML and ``call_options``.
REDIS_NAME: str = "redis"

# ``redis.asyncio.ConnectionPool`` kwargs the transport fills with a default
# when the ``redis:`` config block does not set them.
MAX_CONNECTIONS: str = "max_connections"
SOCKET_CONNECT_TIMEOUT: str = "socket_connect_timeout"
DECODE_RESPONSES: str = "decode_responses"

# ConnectionPool kwargs always overwritten from the service target and password.
# ``PASSWORD`` is also the password key of a client service config entry.
HOST: str = "host"
PORT: str = "port"
PASSWORD: str = "password"

# Maximum number of connections in one pool.
MAX_CONNECTIONS_DEFAULT: int = 1024
# Seconds. Used as the default ``socket_connect_timeout``, not as an idle timeout.
IDLE_TIMEOUT_DEFAULT: float = 3.0 * 60
# Replies stay bytes unless the config enables decoding.
DECODE_RESPONSES_DEFAULT: bool = False


class RequestMode(Enum):
    """Dispatch key of a Redis request.

    Attributes:
        COMMAND: Run one Redis command and return its reply.
        PUBSUB: Subscribe and return a ``PubSub`` object.
        PIPELINE: Return a ``Pipeline`` object for batched commands.
    """
    COMMAND = "command"
    PUBSUB = "pubsub"
    PIPELINE = "pipeline"
