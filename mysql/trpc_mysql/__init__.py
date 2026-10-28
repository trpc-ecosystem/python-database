# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""tRPC MySQL client.

Importing this package registers the MySQL codec and transport plugins and
re-exports the public client types.
"""

from .client import Client
from .codec import ClientCodec
from .constants import MYSQL_NAME
from .constants import Mode
from .mysql_client import MySqlClient
from .transport import MysqlClientTransport
from .types import DatabaseUrl
from .types import Request
from .types import Response

# Read by hatchling from pyproject.toml. Not a runtime feature flag.
__version__ = '0.1.0'

__all__ = [
    "MYSQL_NAME",
    "Mode",
    "Request",
    "Response",
    "DatabaseUrl",
    "Client",
    "ClientCodec",
    "MysqlClientTransport",
    "MySqlClient",
]
