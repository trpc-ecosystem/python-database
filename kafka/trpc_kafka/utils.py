# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Kafka helpers shared by the client and the server.

Query-string parsing, client option defaults, and the byte conversion
used before a produce call live here.
"""
import inspect
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from trpc_sdk.client import Options
from trpc_sdk.serialization import SerializationType

from .constants import KAFKA_NAME


def get_options(opts: List) -> List:
    """Build client options that force the Kafka protocol.

    The required option selects the Kafka protocol, disables
    serialization, and skips the service router. Caller options are
    appended after it.

    Args:
        opts: Extra client options. A falsy value keeps only the
            required option.

    Returns:
        The required Kafka option followed by ``opts``.
    """
    needed = (Options()
              .set_protocol(KAFKA_NAME)
              .set_serialization_type(SerializationType.Noop)
              .set_disable_service_router(True))
    options = [needed]
    if opts:
        options.extend(opts)
    return options


def parse_kafka_params(query: str) -> Dict[str, str]:
    """Parse a ``k=v&k=v`` Kafka query string.

    Args:
        query: Query section of a target or endpoint, without ``?``.

    Returns:
        Parameter names mapped to their raw string values.

    Raises:
        Exception: A token is not exactly ``key=value``.
    """
    token_map = dict()
    for token in query.split("&"):
        vals = token.split("=")
        if len(vals) != 2:
            raise Exception("length of kafka param must be two")
        token_map[vals[0]] = vals[1]
    return token_map


def is_true(value: Any) -> bool:
    """Return whether ``value`` is the string ``true``.

    Comparison is case-insensitive. Non-strings are stringified first,
    so only values whose text is ``true`` match.

    Args:
        value: Flag from configuration, usually a string.

    Returns:
        ``True`` when the string form is ``true``.
    """
    return str(value).lower() == "true"


def is_false(value: Any) -> bool:
    """Return whether ``value`` is the string ``false``.

    Comparison is case-insensitive. Non-strings are stringified first.
    An empty string is not false, so unset flags stay at their default.

    Args:
        value: Flag from configuration, usually a string.

    Returns:
        ``True`` when the string form is ``false``.
    """
    return str(value).lower() == "false"


def unmarshal_config(token_map: Dict[str, str], config: Any) -> None:
    """Write query parameters onto attributes the config already has.

    Unknown keys are ignored. A field is converted with ``int`` or
    ``float`` only when the config constructor annotates it as that
    type; every other value stays a string.

    Args:
        token_map: Parsed ``k=v`` parameters.
        config: Object whose existing attributes receive matching keys.
    """
    params = inspect.signature(type(config).__init__).parameters
    for key, value in token_map.items():
        if not hasattr(config, key):
            continue
        param = params.get(key)
        kind = param.annotation if param is not None else inspect.Parameter.empty
        if kind is inspect.Parameter.empty:
            kind = type(getattr(config, key))
        if kind in (int, float):
            value = kind(value)
        setattr(config, key, value)


def to_bytes(value: Union[str, bytes, None]) -> Optional[bytes]:
    """Encode a Kafka key or value as UTF-8 bytes.

    ``None`` and ``bytes`` pass through. Anything else is encoded as UTF-8,
    which is what the producer expects before ``send_and_wait``.

    Args:
        value: Kafka record key or value from the client request.

    Returns:
        The original bytes, ``None``, or a UTF-8 encoding of ``value``.
    """
    if value is None or isinstance(value, bytes):
        return value
    return bytes(value, encoding="utf8")


def create_rsp_object() -> None:
    """Return no client response object.

    ``Client.invoke`` calls this factory as ``rsp_cls()``. The produce
    result is written onto ``msg.client_rsp_head`` by the transport, so
    the invoke path must not allocate a response body.

    Returns:
        ``None``.
    """
    return None
