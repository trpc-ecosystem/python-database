# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Runnable MySQL examples against a local server.

The default target is ``mysql://root:mysql2021@127.0.0.1:3306/mysql``.
Override it with the ``MYSQL_DSN`` environment variable. ``dsn://`` and
``dsn+mysql://`` are accepted because :meth:`DatabaseUrl.make_url` rewrites
them.

``main`` uses :class:`MySqlClient` and does not load framework config.
``demo_client`` uses :class:`Client` with ``Options.set_target``.
``demo_with_config`` loads ``trpc_python.yaml``. Run those two explicitly so
a missing config file does not fail the default entry point.
"""

import asyncio
import os

from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.config import TrpcConfig
from trpc_sdk.plugin import init_plugins

import trpc_mysql  # noqa: F401  registers the codec and transport plugins
from trpc_mysql.client import Client
from trpc_mysql.mysql_client import MySqlClient
from trpc_mysql.types import DatabaseUrl

DEFAULT_DSN = "mysql://root:mysql2021@127.0.0.1:3306/mysql"
"""Local server used when ``MYSQL_DSN`` is unset."""

DEMO_TABLE = "trpc_mysql_demo"
"""Table created and dropped by the examples. Not a system table."""


def database_dsn() -> str:
    """Return the DSN for this process.

    Returns:
        ``MYSQL_DSN`` when set, otherwise :data:`DEFAULT_DSN`.
    """
    return os.environ.get("MYSQL_DSN", DEFAULT_DSN)


def framework_target() -> str:
    """Return :func:`database_dsn` in the form the tRPC ``Client`` accepts.

    The framework picks a selector from the target scheme. Only ``dsn`` has
    one that passes the whole DSN through, and the transport rewrites it back
    to ``mysql://``.

    Returns:
        The DSN with its ``mysql://`` scheme replaced by ``dsn://``.
    """
    dsn = database_dsn()
    if dsn.startswith("mysql://"):
        return "dsn://" + dsn[len("mysql://"):]
    return dsn


async def demo_standalone() -> None:
    """Create a demo table, write a row, read it back, then drop the table.

    The pool opens on the first statement and is closed before return, including
    when a later statement fails.
    """
    client = MySqlClient(DatabaseUrl.make_url(database_dsn()))
    try:
        await client.execute(
            f"CREATE TABLE IF NOT EXISTS {DEMO_TABLE} "
            "(id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), role INT)"
        )
        await client.execute(f"DELETE FROM {DEMO_TABLE}")
        inserted = await client.table_insert(DEMO_TABLE, {"name": "admin", "role": 1})
        print("[standalone] insert:", inserted)

        rows = await client.query(
            f"SELECT id, name, role FROM {DEMO_TABLE} WHERE name = %s",
            ["admin"],
        )
        print("[standalone] query:", rows)

        count_row = await client.get(
            f"SELECT COUNT(*) AS cnt FROM {DEMO_TABLE} WHERE name = %s",
            ["admin"],
        )
        print("[standalone] get:", count_row)

        found = await client.table_has(DEMO_TABLE, "name", "admin")
        print("[standalone] table_has:", found)
    finally:
        await client.execute(f"DROP TABLE IF EXISTS {DEMO_TABLE}")
        await client.close()


async def demo_client() -> None:
    """Write and read the demo table through the tRPC client.

    The service name is only a callee label. The URI comes from
    ``Options.set_target`` with :func:`framework_target`, not from YAML.
    """
    options = [Options().set_target(framework_target())]
    client = Client("trpc.mysql.hello", options)
    context = TrpcContext()
    try:
        await client.execute(
            context,
            f"CREATE TABLE IF NOT EXISTS {DEMO_TABLE} "
            "(id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), role INT)",
        )
        await client.execute(
            context,
            f"INSERT INTO {DEMO_TABLE} (name, role) VALUES (%s, %s)",
            ["admin", 1],
        )
        rows = await client.fetchall(
            context,
            f"SELECT id, name, role FROM {DEMO_TABLE} WHERE name = %s",
            ["admin"],
        )
        print("[client] fetchall:", rows)
    finally:
        await client.execute(context, f"DROP TABLE IF EXISTS {DEMO_TABLE}")
        client.close()


async def demo_with_config() -> None:
    """Read MySQL using the service target declared in ``trpc_python.yaml``.

    Loads framework config and plugins, then calls ``fetchall`` on
    ``trpc.mysql.hello``. This is not invoked by :func:`main`.
    """
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "trpc_python.yaml"))
    TrpcConfig.load(config_path, "utf-8")
    init_plugins()
    client = Client("trpc.mysql.hello")
    context = TrpcContext()
    rows = await client.fetchall(context, "SELECT 1 AS ok")
    print("[config] fetchall:", rows)
    client.close()


async def main() -> None:
    """Run the direct ``MySqlClient`` example."""
    await demo_standalone()


if __name__ == "__main__":
    asyncio.run(main())
