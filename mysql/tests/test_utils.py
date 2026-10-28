# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Tests for URI hashing used by the transport client cache."""

from trpc_mysql import utils


def test_md5():
    """Hash a stable fixture string to the known digest."""
    string = "trpc-mysql"
    assert utils.md5(string) == "fa8e9603dbb0067def624048b9926448"
