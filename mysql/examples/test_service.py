# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Call the example HTTP server and check what it read from MySQL.

What it checks
--------------
- ``GET /ping``: the server process ran ``SELECT 1 AS ok`` through
  ``trpc_mysql.client.Client`` and got ``[{"ok": 1}]`` back.
- ``POST /demo``: the server created ``trpc_mysql_demo``, inserted the JSON
  body with bound parameters, read it back with ``fetchall`` and ``fetchone``,
  then dropped the table.

Required server
---------------
- ``examples/trpc_main.py`` started with ``examples/trpc_python.yaml``.
- A MySQL server reachable at the ``trpc.mysql.hello`` target in that config.

How to run
----------
    # Terminal 1: start the server
    python examples/trpc_main.py --conf examples/trpc_python.yaml

    # Terminal 2: run this client
    python examples/test_service.py

Expected output (abridged)
--------------------------
    [ping] 200 {'rows': [{'ok': 1}]}
    [demo] 200 {'insert_id': 1, 'rows': [{'id': 1, 'name': 'admin', 'role': 1}], 'one': [{'cnt': 1}]}
    [OK] MySQL calls through the tRPC server behave as expected
"""

import argparse
import asyncio
import json
import sys
from typing import Any
from typing import Dict
from typing import Tuple

from trpc_sdk.abc import TrpcContext
from trpc_sdk.http import Client as HttpClient

DEFAULT_ADDR = "127.0.0.1:8080"
"""Listen address of ``trpc.mysql.examples.http`` in ``trpc_python.yaml``."""


async def call(cli: HttpClient, method: str, url: str, body: Any = None) -> Tuple[int, Dict]:
    """Send one request and decode the JSON reply.

    Args:
        cli: tRPC HTTP client.
        method: ``GET`` or ``POST``.
        url: Full request URL.
        body: JSON-serializable body for ``POST``. Defaults to ``None``.

    Returns:
        ``(status, payload)``. A transport error comes back as status ``0``
        with ``{"error": ...}``.
    """
    try:
        if method == "GET":
            rsp = await cli.get(TrpcContext(), url)
        else:
            rsp = await cli.post(TrpcContext(),
                                 url,
                                 data=json.dumps(body),
                                 headers={"Content-Type": "application/json"})
        return rsp.status, await rsp.json()
    except Exception as error:  # pylint: disable=broad-except
        return 0, {"error": str(error)}


async def check_ping(cli: HttpClient, addr: str) -> bool:
    """Check that ``GET /ping`` returns ``[{"ok": 1}]``."""
    status, payload = await call(cli, "GET", f"http://{addr}/ping")
    print(f"[ping] {status} {payload}")
    return status == 200 and payload.get("rows") == [{"ok": 1}]


async def check_concurrent_ping(cli: HttpClient, addr: str, concurrency: int) -> bool:
    """Send ``concurrency`` pings at once and report which workers served them."""
    replies = await asyncio.gather(*[call(cli, "GET", f"http://{addr}/ping") for _ in range(concurrency)])
    ok = [payload for status, payload in replies if status == 200 and payload.get("rows") == [{"ok": 1}]]
    servers = sorted({(payload.get("pid"), payload.get("thread")) for payload in ok}, key=str)
    print(f"[concurrent_ping] {len(ok)}/{concurrency} ok, served by (pid, thread): {servers}")
    return len(ok) == concurrency


async def check_demo(cli: HttpClient, addr: str) -> bool:
    """Check that ``POST /demo`` writes one row and reads the same row back."""
    status, payload = await call(cli, "POST", f"http://{addr}/demo", {"name": "admin", "role": 1})
    print(f"[demo] {status} {payload}")
    if status != 200:
        return False
    rows = payload.get("rows") or []
    return (len(rows) == 1 and rows[0].get("id") == payload.get("insert_id") and rows[0].get("name") == "admin"
            and rows[0].get("role") == 1 and payload.get("one") == [{"cnt": 1}])


async def check_bad_body(cli: HttpClient, addr: str) -> bool:
    """Check that ``POST /demo`` rejects a body without ``role``."""
    status, payload = await call(cli, "POST", f"http://{addr}/demo", {"name": "admin"})
    print(f"[bad_body] {status} {payload}")
    return status == 400 or "400" in str(payload.get("error", ""))


async def main() -> None:
    """Run every check and exit non-zero when one fails."""
    parser = argparse.ArgumentParser(description="Call the tRPC-MySQL example server.")
    parser.add_argument("--addr", default=DEFAULT_ADDR, help="host:port of the example HTTP service.")
    parser.add_argument("--concurrency", type=int, default=50, help="Number of concurrent pings.")
    args = parser.parse_args()

    cli = HttpClient()
    results = {
        "ping": await check_ping(cli, args.addr),
        "concurrent_ping": await check_concurrent_ping(cli, args.addr, args.concurrency),
        "demo": await check_demo(cli, args.addr),
        "bad_body": await check_bad_body(cli, args.addr),
    }
    if all(results.values()):
        print("[OK] MySQL calls through the tRPC server behave as expected")
    else:
        print(f"[FAIL] {results}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
