# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Tests for ``MySqlClient`` against a mocked ``aiomysql`` pool.

``sys.path`` includes the mysql project root so these tests can import
``trpc_mysql`` without an editable install.
"""

import asyncio
import os
import sys
from unittest.mock import MagicMock as AsyncMock

import pytest

# Parent of tests/ is the project root that contains the trpc_mysql package.
g_homepath = os.path.realpath(
    "{}/../".format(os.path.dirname(os.path.realpath(__file__))))
sys.path.append(g_homepath)
from trpc_mysql import Mode
from trpc_mysql.mysql_client import MySqlClient
from trpc_mysql.types import DatabaseUrl

# Rebuilt for each test. The pool is opened only when a test invokes SQL.
MYSQL_CLIENT = None


def test_mode_str_compat():
    """``Mode`` compares equal to its value string and accepts that string."""
    assert Mode.FETCHONE == "fetchone"
    assert Mode("execute") is Mode.EXECUTE


def test_database_url():
    """Parse MySQL URIs, rewrite ``dsn://`` prefixes, and reject other schemes."""
    url = DatabaseUrl.make_url(None)
    assert not url

    url = DatabaseUrl.make_url("sqlite:")
    assert not url

    url = DatabaseUrl.make_url("mysql://user:psw@ip:3306/db_auto_flow")
    assert url
    assert url.host == "ip"

    url = DatabaseUrl.make_url("mysql://user:psw@ip:3306/db_auto_flow?charset=utf8mb4")
    assert url
    assert url.charset == "utf8mb4"

    url = DatabaseUrl.make_url("dsn://user:psw@ip:3306/db_auto_flow?charset=utf8mb4")
    assert url

    url = DatabaseUrl.make_url("dsn+mysql://user:psw@ip:3306/db_auto_flow?charset=utf8mb4")
    assert url


def setup_function():
    """Build a client from a placeholder URI. The pool is not opened yet."""
    global MYSQL_CLIENT
    MYSQL_CLIENT = MySqlClient(DatabaseUrl.make_url("mysql://user:psw@ip:3306/db_auto_flow"))


def teardown_function():
    """Leave pool cleanup to the tests that open one."""
    pass


def client() -> MySqlClient:
    """Return the client built in ``setup_function``.

    Returns:
        The module-level client.
    """
    return MYSQL_CLIENT


def test_init_autocommit():
    """Class default is autocommit, and a boolean keyword overrides it."""
    mysql_client = MySqlClient(DatabaseUrl.make_url("mysql://user:psw@ip:3306/db_auto_flow"))
    assert mysql_client.autocommit
    mysql_client = MySqlClient(DatabaseUrl.make_url("mysql://user:psw@ip:3306/db_auto_flow"), autocommit=False)
    assert not mysql_client.autocommit


@pytest.mark.asyncio
async def test_transaction_multi(mocker):
    """A batch still returns after the mocked second ``executemany`` raises.

    The client logs that error and, because autocommit is false, asks the
    connection to roll back instead of raising to the caller.
    """
    mock_pool(mocker)
    mysql_client = MySqlClient(DatabaseUrl.make_url("mysql://user:psw@ip:3306/db_auto_flow"), autocommit=False)
    raw_sqls = [
                {
                    "pre_sql": "INSERT INTO employees (name, phone) VALUES (%s, %s)",
                    "data": [
                        ("Jane1", "555-001"),
                        ("Joe1", "555-002"),
                        ("John1", "555-003")
                    ]
                },
                "INSERT INTO employees(name, phone) VALUES('Jane2', '555-001')",
                "INSERT INTO employees(name, phone) VALUES('Joe2', '555-002')",
                "INSERT INTO employees(name, phone) VALUES('John2', '555-003')",
                {
                    "pre_sql": "INSERT INTO employees (name, phone) VALUES (%s, %s)",
                    "data": [
                        ("Lucus", "555-004"),
                    ]
                },
            ]

    await mysql_client.transaction_multi(raw_sqls=raw_sqls)


async def close_client():
    """Close the module client so later asserts can see ``pool is None``."""
    await MYSQL_CLIENT.close()


async def mock_create_pool(**kwargs):
    """Return a pool whose second ``executemany`` fails.

    The first ``executemany`` resolves to ``0``. The second raises, which is
    how ``transaction_multi`` reaches its rollback branch. Fetch methods
    resolve to a truthy mock so read tests can assert a result.

    Args:
        **kwargs: Pool arguments from ``MySqlClient``. Unused.

    Returns:
        Async mock shaped like an ``aiomysql`` pool.
    """
    dict_cursor = AsyncMock()

    bool_result = asyncio.Future()
    bool_result.set_result(True)

    result = asyncio.Future()
    result.set_result(0)
    dict_cursor.execute.return_value = result
    dict_cursor.executemany.side_effect = [result, Exception("mock exeption")]

    records = asyncio.Future()
    records.set_result(AsyncMock())
    dict_cursor.fetchone.return_value = records
    dict_cursor.fetchmany.return_value = records
    dict_cursor.fetchall.return_value = records

    cursor = AsyncMock()
    cursor.__aenter__.return_value = dict_cursor

    conn = AsyncMock()
    conn.cursor.return_value = cursor

    conn.begin.return_value = bool_result

    conn.ping.return_value = bool_result
    conn.rollback.return_value = bool_result

    acquire = AsyncMock()
    acquire.__aenter__.return_value = conn

    pool = AsyncMock()
    pool.acquire.return_value = acquire

    pool.close.return_value = bool_result
    pool.wait_closed.return_value = bool_result
    return pool


def mock_pool(mocker):
    """Patch ``aiomysql.create_pool`` with ``mock_create_pool``.

    Args:
        mocker: pytest-mock fixture.
    """
    mocker.patch("aiomysql.create_pool", new=mock_create_pool)


@pytest.mark.asyncio
async def test_concurrent_first_use_opens_one_pool(mocker):
    """Concurrent first calls share one pool created on the running loop."""
    calls = []

    async def counting_create_pool(**kwargs):
        calls.append(kwargs["loop"])
        await asyncio.sleep(0.01)
        return await mock_create_pool(**kwargs)

    mocker.patch("aiomysql.create_pool", new=counting_create_pool)

    await asyncio.gather(*[client().invoke(Mode.FETCHONE, "") for _ in range(10)])

    assert calls == [asyncio.get_running_loop()]
    await close_client()


@pytest.mark.asyncio
async def test_failed_pool_open_is_retried(mocker):
    """A failed ``create_pool`` leaves no pool, so the next call opens one."""
    attempts = []

    async def flaky_create_pool(**kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise ConnectionError("mock connect error")
        return await mock_create_pool(**kwargs)

    mocker.patch("aiomysql.create_pool", new=flaky_create_pool)

    with pytest.raises(ConnectionError):
        await client().invoke(Mode.FETCHONE, "")
    assert client().pool is None

    assert await client().invoke(Mode.FETCHONE, "")
    assert len(attempts) == 2
    await close_client()


@pytest.mark.asyncio
async def test_get(mocker):
    """``FETCHONE`` returns the mocked row, and ``close`` drops the pool."""
    mock_pool(mocker)

    result = await client().invoke(Mode.FETCHONE, "")
    assert result

    await close_client()
    assert not client().pool


@pytest.mark.asyncio
async def test_query(mocker):
    """``FETCHALL`` runs through the mocked pool without raising."""
    mock_pool(mocker)

    await client().invoke(Mode.FETCHALL, "")
    await close_client()


@pytest.mark.asyncio
async def test_execute(mocker):
    """``EXECUTE`` runs through the mocked pool without raising."""
    mock_pool(mocker)

    await client().invoke(Mode.EXECUTE, "")
    await close_client()


@pytest.mark.asyncio
async def test_transaction(mocker):
    """``TRANSACTION`` runs through the mocked pool without raising."""
    mock_pool(mocker)

    await client().invoke(Mode.TRANSACTION, "")
    await close_client()


@pytest.mark.asyncio
async def test_table_has(mocker):
    """``table_has`` binds one value and returns the mocked row."""
    mock_pool(mocker)

    result = await client().table_has("t", "name", "value")
    await close_client()
    assert result


@pytest.mark.asyncio
async def test_table_update(mocker):
    """``table_update`` runs through the mocked pool and returns ``lastrowid``."""
    mock_pool(mocker)

    result = await client().table_update("t", {"name": "trpc mysql client"}, "name", "value")
    await close_client()
    assert result


@pytest.mark.asyncio
async def test_table_insert(mocker):
    """``table_insert`` runs through the mocked pool and returns ``lastrowid``."""
    mock_pool(mocker)

    result = await client().table_insert("t", {})
    await close_client()
    assert result
