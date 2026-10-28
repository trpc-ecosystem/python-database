# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Helpers shared by the MySQL client transport."""

import hashlib
from typing import Text


def md5(source: Text) -> Text:
    """Hash a connection URI so the transport can reuse one pool per target.

    Args:
        source: Text to hash, typically the database URI.

    Returns:
        Lowercase hex digest. An empty string when ``source`` is empty, so a
        missing URI does not collide with the hash of real text.
    """
    if not source:
        return ""
    return hashlib.md5(source.encode()).hexdigest()
