# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""HTTP routes that read and write MySQL through the tRPC ``Client``.

The routes are bound to ``trpc.mysql.examples.http`` (``protocol: http`` in
``trpc_python.yaml``) and registered when ``trpc_main.py`` imports this module.
Each handler passes its request context to ``Client``, so the MySQL call runs
through the framework client stack inside the server process.

Routes:
    GET  /ping  -- ``SELECT 1 AS ok``.
    POST /demo  -- create the demo table, insert the JSON body as one row,
                   read it back with ``fetchall`` and ``fetchone``, then drop
                   the table.
"""

import os
import threading
from typing import Optional

from aiohttp import web
from trpc_sdk.abc import TrpcContext
from trpc_sdk.http import route
from trpc_sdk.log import logger

from trpc_mysql.client import Client

HTTP_SERVICE = "trpc.mysql.examples.http"
"""Server service the routes are bound to."""

MYSQL_SERVICE = "trpc.mysql.hello"
"""Client service whose ``target`` holds the MySQL DSN."""

DEMO_TABLE = "trpc_mysql_demo"
"""Table created and dropped by ``POST /demo``. Not a system table."""

_client: Optional[Client] = None


def get_client() -> Client:
    """Return the shared MySQL client, creating it on first use.

    ``Client`` reads its target from framework config, which is only loaded by
    ``create_server``. Building it at import time would find no target.

    Returns:
        The process-wide ``Client`` for :data:`MYSQL_SERVICE`.
    """
    global _client
    if _client is None:
        _client = Client(MYSQL_SERVICE)
    return _client


@route("/ping", method="GET", service_name=HTTP_SERVICE)
async def ping(ctx: TrpcContext, request: web.Request) -> web.Response:
    """Run ``SELECT 1`` to show the server process can reach MySQL.

    Args:
        ctx: Request context. Passed on to the MySQL client.
        request: Incoming aiohttp request. Not read.

    Returns:
        ``{"rows": [...], "pid": ..., "thread": ...}`` on success, naming the
        process and handler thread that served the call, or status 500 with
        ``{"error": ...}``.
    """
    try:
        rows = await get_client().fetchall(ctx, "SELECT 1 AS ok")
    except Exception as error:  # pylint: disable=broad-except
        logger.error("[http] GET /ping failed: %s", error)
        return web.json_response({"error": str(error)}, status=500)
    logger.info("[http] GET /ping rows=%s", rows)
    return web.json_response({"rows": rows, "pid": os.getpid(), "thread": threading.current_thread().name})


@route("/demo", method="POST", service_name=HTTP_SERVICE)
async def demo(ctx: TrpcContext, request: web.Request) -> web.Response:
    """Write one row from the JSON body and read it back.

    The body looks like ``{"name": "admin", "role": 1}``. Values are bound
    with ``%s``; the table name is a constant.

    Args:
        ctx: Request context. Passed on to the MySQL client.
        request: Incoming aiohttp request with a JSON body.

    Returns:
        ``{"insert_id", "rows", "one"}`` on success, status 400 for a bad
        body, or status 500 with ``{"error": ...}`` when MySQL fails.
    """
    try:
        body = await request.json()
        name = str(body["name"])
        role = int(body["role"])
    except (ValueError, KeyError, TypeError) as error:
        return web.json_response({"error": f"bad body: {error}"}, status=400)

    client = get_client()
    try:
        await client.execute(
            ctx,
            f"CREATE TABLE IF NOT EXISTS {DEMO_TABLE} "
            "(id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), role INT)",
        )
        await client.execute(ctx, f"DELETE FROM {DEMO_TABLE}")
        insert_id = await client.execute(
            ctx,
            f"INSERT INTO {DEMO_TABLE} (name, role) VALUES (%s, %s)",
            [name, role],
        )
        rows = await client.fetchall(
            ctx,
            f"SELECT id, name, role FROM {DEMO_TABLE} WHERE name = %s",
            [name],
        )
        one = await client.fetchone(
            ctx,
            f"SELECT COUNT(*) AS cnt FROM {DEMO_TABLE} WHERE name = %s",
            [name],
        )
    except Exception as error:  # pylint: disable=broad-except
        logger.error("[http] POST /demo failed: %s", error)
        return web.json_response({"error": str(error)}, status=500)
    finally:
        try:
            await client.execute(ctx, f"DROP TABLE IF EXISTS {DEMO_TABLE}")
        except Exception as error:  # pylint: disable=broad-except
            logger.error("[http] POST /demo drop table failed: %s", error)

    logger.info("[http] POST /demo insert_id=%s rows=%s one=%s", insert_id, rows, one)
    return web.json_response({"insert_id": insert_id, "rows": rows, "one": one})
