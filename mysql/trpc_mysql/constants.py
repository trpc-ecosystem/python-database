# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Protocol name and request modes for the MySQL client."""

from enum import Enum

# Name registered with the tRPC plugin system for both the codec and the transport.
MYSQL_NAME: str = "mysql"


class Mode(str, Enum):
    """Dispatch key consumed by ``MySqlClient.invoke``.

    The ``str`` mixin keeps ``Mode.FETCHONE == "fetchone"``, so config and
    tests can pass the raw mode string.

    Attributes:
        FETCHONE: Read a single row.
        FETCHALL: Read many rows, optionally limited by ``size``.
        EXECUTE: Run one statement outside an explicit transaction.
        TRANSACTION: Run one statement and roll it back on error.
        CURSOR: Reserved. ``invoke`` does not branch on this value.
    """

    FETCHONE = "fetchone"
    FETCHALL = "fetchall"
    EXECUTE = "execute"
    TRANSACTION = "transaction"
    CURSOR = "cursor"
