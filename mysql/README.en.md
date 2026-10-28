# tRPC MySQL plugin

[中文](README.md)

MySQL client for tRPC-Python. It wraps [aiomysql](https://github.com/aio-libs/aiomysql) and runs asynchronous SQL. There is no ORM.

There are two call paths. `MySqlClient` owns its pool and fits scripts or any process that does not go through the framework. `Client` sends the same call through the tRPC client stack (selector, codec, transport, and timeout) and fits a tRPC service calling MySQL.

```mermaid
flowchart TD
  subgraph directPath [MySqlClient]
    url["mysql:// DSN"] --> directClient[MySqlClient]
    directClient --> pool[aiomysql pool]
  end
  subgraph trpcPath [Client]
    imp["import trpc_mysql"] --> frameworkClient["Client dsn://"]
    frameworkClient --> stack["selector / codec / transport"]
    stack --> loopPool["one MySqlClient per event loop"]
    loopPool --> pool
  end
```

## Features

- **SQL access**: `fetchone` / `fetchall` / `execute` / `transaction`, with values bound through PyMySQL `%s` placeholders.
- **Direct-client helpers**: `MySqlClient` also offers `transaction_multi`, plus `table_has` / `table_insert` / `table_update`.
- **Rows are dicts**: the default cursor is `DictCursor`.
- **Connection pools**: an `aiomysql` pool is kept per process, thread, and event loop. The same URI on the same loop shares one pool.
- **Framework integration**: `import trpc_mysql` registers the codec and transport for the `mysql` protocol.

## Install

Requirements:

- Python >= 3.9
- `trpc-python` (import name `trpc_sdk`). That dependency does not support Windows
- `aiomysql~=0.0.21`
- `pymysql~=0.9.3`
- `cryptography` (required by MySQL 8's default `caching_sha2_password` plugin)

```bash
pip install trpc-mysql
```

For local development, run this in the `mysql` directory. `uv sync` installs the package in editable mode from `pyproject.toml` and installs the dev dependency group. `trpc-python` must be resolvable from the configured package index.

```bash
uv sync
```

## Quick start

The default target is `mysql://root:mysql2021@127.0.0.1:3306/mysql`. Set `MYSQL_DSN` to override it. `examples/playground.py` reads that variable.

```bash
python examples/playground.py
```

`main` in `playground.py` uses `MySqlClient` directly: it creates a table, writes a row, reads it back, and drops the table. `demo_client` uses the framework `Client` with `Options.set_target`. `demo_with_config` reads `trpc_python.yaml` in the same directory and has to be called on its own, for example `asyncio.run(demo_with_config())`.

A full example that calls MySQL from inside a tRPC service and checks it by starting the server is in [examples/README.md](examples/README.md).

### Direct MySqlClient

The direct client does not go through a selector. Use a `mysql://` URI. `dsn://` and `dsn+mysql://` also work, because `DatabaseUrl.make_url` rewrites them to `mysql://`.

```python
import asyncio

from trpc_mysql.mysql_client import MySqlClient
from trpc_mysql.types import DatabaseUrl


async def main():
    url = DatabaseUrl.make_url("mysql://root:mysql2021@127.0.0.1:3306/mysql")
    client = MySqlClient(url)
    records = await client.query(
        "SELECT id, name FROM trpc_mysql_demo WHERE name = %s",
        ["admin"],
    )
    print(records)
    await client.close()


asyncio.run(main())
```

### Framework Client

`import trpc_mysql` first, so the codec and transport are registered. The framework picks a selector from the target scheme. Only `dsn` hands the whole DSN to the transport, which rewrites it to `mysql://`. A `mysql://` target raises `Client select node selector not exist`.

```python
import asyncio

import trpc_mysql  # registers codec and transport
from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_mysql.client import Client


async def main():
    options = [
        Options().set_target("dsn://root:mysql2021@127.0.0.1:3306/mysql")
    ]
    client = Client("trpc.mysql.hello", options)
    records = await client.fetchall(
        TrpcContext(),
        "SELECT id, name FROM trpc_mysql_demo WHERE name = %s",
        ["admin"],
    )
    print(records)


asyncio.run(main())
```

### Configuration file

See `examples/trpc_python.yaml`. The backend service name is `trpc.mysql.hello`, and `target` points at the database of the default DSN. `timeout` is in milliseconds.

```yaml
client:
  service:
    - name: trpc.mysql.hello
      target: "dsn://root:mysql2021@127.0.0.1:3306/mysql"
      timeout: 5000
      network: tcp
      protocol: mysql
```

`Client` reads its target at construction time. Do not build it from a service name before config is loaded. Inside a server, create it after `create_server`, in the request handler. In a script, call `TrpcConfig.load` and `init_plugins` first.

```python
import asyncio

import trpc_mysql  # registers codec and transport
from trpc_sdk.abc import TrpcContext
from trpc_sdk.config import TrpcConfig
from trpc_sdk.plugin import init_plugins
from trpc_mysql.client import Client


async def main():
    TrpcConfig.load("examples/trpc_python.yaml", "utf-8")
    init_plugins()
    mysql_client = Client("trpc.mysql.hello")
    records = await mysql_client.fetchall(TrpcContext(), "SELECT 1 AS ok")
    print(records)


asyncio.run(main())
```

## Two clients

| | `MySqlClient` | `Client` |
| --- | --- | --- |
| Use | Scripts and non-tRPC processes | Downstream calls inside a tRPC service |
| URI | `mysql://user:password@host:port/db` | `dsn://user:password@host:port/db` |
| Entry points | `query` / `get` / `execute` | `fetchall` / `fetchone` / `execute`, with `TrpcContext` as the first argument |
| Pool | The pool owned by this object, opened on the first statement | Cached by the transport per thread, event loop, and URI |
| Close | `await client.close()` | `close()` does nothing. The transport closes pools when the serving loop stops |

`import trpc_mysql` must run before `create_server` or `init_plugins`. A process that only uses `MySqlClient` and never calls through the framework client can skip that import.

When `options` passed to `Client` already call `set_target`, that URI is used. Otherwise the target is loaded from `client.service` in the framework config under `service_name`. The service name is also the callee of the call. The RPC name is `/{service_name}/{mode}` and is only a label. The pool is chosen from the resolved URI.

## API

Bind values in SQL with `%s`. Pass one string parameter as a one-element list or tuple, such as `["admin"]` or `("admin",)`. A bare string is treated as a sequence of characters.

A failed read `ping`s the connection and runs the statement once more. A second failure propagates. A failed write is logged and swallowed, and the call returns the current `lastrowid`. Failures while opening the pool or acquiring a connection propagate.

### Client

`Client(service_name, options=None)`. The methods below are asynchronous and return the `result` the transport stored on the response head.

- `fetchone(ctx, query, args=None)`: read one row. A hit is a one-element list. No row returns `None`.
- `fetchall(ctx, query, args=None, size=None)`: `size` of `None` or `0` reads every row, `1` reads one row, and any other positive integer is passed to `fetchmany`.
- `execute(ctx, query, args=None, ret_affected_num=False)`: run one statement. The default return value is `lastrowid`. `ret_affected_num=True` returns the affected-row count.
- `transaction(ctx, query, args=None, ret_affected_num=False)`: run one statement and roll it back if the driver raises. The return value matches `execute`.
- `close()`: no-op.

On the framework `Client`, a timeout becomes `ClientTimeoutError`, and any other failure raised by the driver becomes `ClientInvokeError`. Statement errors swallowed by `execute` / `transaction` do not become either of those exceptions.

`transaction_multi` and `table_*` exist only on `MySqlClient`.

### MySqlClient

`MySqlClient(url, loop=None, cursor=DictCursor, **kwargs)`. `url` is the value returned by `DatabaseUrl.make_url`. The pool is created on the first statement and is bound to the running event loop.

The methods that match `Client` take no `ctx`:

- `get(sql, params=None)`: same as `fetchone`.
- `query(sql, params=None, size=None)`: same as `fetchall`.
- `execute(sql, params=None, ret_affected_num=False)`
- `transaction(sql, params=None, ret_affected_num=False)`
- `await close()`: close the pool owned by this object.

`transaction_multi(raw_sqls)` runs several statements in one transaction, in order. Each item is a SQL string, or a dict with `pre_sql` and `data`. A dict is executed with `executemany`. The return value is the affected-row count accumulated before the first error.

```python
url = DatabaseUrl.make_url("mysql://root:mysql2021@127.0.0.1:3306/mysql")
client = MySqlClient(url, autocommit=False)
affected = await client.transaction_multi([
    "DELETE FROM trpc_mysql_demo",
    {
        "pre_sql": "INSERT INTO trpc_mysql_demo (name, role) VALUES (%s, %s)",
        "data": [("admin", 1), ("guest", 2)],
    },
])
```

The default is `autocommit=True`. Both `transaction` and `transaction_multi` start with `BEGIN`. After success, `COMMIT` runs only when `autocommit=False`. A single `transaction` calls `ROLLBACK` when the driver raises. `transaction_multi` rolls back only when `autocommit=False`. Pass `autocommit=False` when the call should commit on success and roll back on failure.

`Mode.CURSOR` is reserved. `invoke` returns `None` for that mode and for any unknown mode, and does not touch the database.

### Table helpers

`table_has`, `table_insert`, and `table_update` interpolate table and column names into the SQL. `table_update` also interpolates the comparison value inside double quotes instead of binding it with `%s`. Those identifiers and the comparison value should be constants in code. User input goes through `%s` on `query` / `execute`.

- `table_has(table, field, value)`: `SELECT field FROM table WHERE field=%s LIMIT 1`. A hit is a one-element list. No row, or a failed lookup, returns `None`.
- `table_insert(table, item, ret_affected_num=False)`: `item` maps column names to values, and the values are bound with `%s`.
- `table_update(table, updates, field_where, value_where, ret_affected_num=False)`: values in `updates` are bound with `%s`, and `value_where` is interpolated.

### Connection pool

The class defaults are `minsize=3`, `maxsize=10`, and `pool_recycle` of 7 hours (`7 * 3600` seconds). `minsize`, `maxsize`, and `pool_recycle` passed to the constructor are overwritten by those class attributes. Change the pool size by setting the class attributes before creating an instance. A boolean `autocommit` keyword is kept.

Each direct `MySqlClient` has its own pool. Pools inside the framework transport are also cached per thread and event loop, keyed by the md5 of the URI. Repeated `Client` calls for the same URI on the same loop share one pool.

## Configuration

Change `client.service[].target` to point at another database. The protocol name is `mysql`.

A `?charset=` suffix may follow the database name. The default charset is `utf8mb4`. A password may contain colons: parsing splits the user and the password at the first colon only.

```text
mysql://user:password@127.0.0.1:3306/app
mysql://user:password@127.0.0.1:3306/app?charset=utf8mb4
dsn://user:password@127.0.0.1:3306/app
```

The two MySQL-related blocks in `examples/trpc_python.yaml`:

```yaml
server:
  service:
    - name: trpc.mysql.examples.http
      ip: 127.0.0.1
      port: 8080
      protocol: http

client:
  service:
    - name: trpc.mysql.hello
      target: "dsn://root:mysql2021@127.0.0.1:3306/mysql"
      timeout: 5000
      protocol: mysql
```

`playground.py` reads `MYSQL_DSN` (`mysql://`) and does not read `target` from this YAML. `demo_with_config` is the exception: it follows the config by service name.

## Process and thread models

An `aiomysql` pool is bound to the event loop that created it. The transport therefore keeps one cache per process, per thread, and per event loop, and MySQL calls work under every `global.processmodel` setting below. A forked child starts with an empty cache, so constructing a `Client` at import time, before the server forks, is fine. When a serving loop stops, the transport closes the pools for that loop.

| Config | `global.processmodel` | Where MySQL calls run |
| --- | --- | --- |
| `trpc_python.yaml` | omitted (merge, one loop) | The main loop of the main process |
| `trpc_python_single_process_merge.yaml` | `single_process`, `handle_num: 2` | Each handler thread, on its own loop and pools |
| `trpc_python_single_process_separate.yaml` | `single_process`, `io_handle_type: separate`, `io_num: 1`, `handle_num: 2` | Each handler thread, on its own loop and pools |
| `trpc_python_multi_process_separate.yaml` | `multi_process`, `io_handle_type: separate`, `handle_num: 2` | Each rpc task process. The I/O process does not call MySQL |
| `trpc_python_multi_process_merge.yaml` | `multi_process`, `io_handle_type: merge`, `handle_num: 2` | Each worker process |

A `MySqlClient` you construct yourself is not stored in that cache. Its pool belongs to the loop that ran the first statement, and the client should be used on that loop only.

With a local MySQL server up, the example script can run all five configs:

```bash
cd examples
./run_examples.sh modes
```

## Examples

Runnable examples and the one-shot script are described in [examples/README.md](examples/README.md).

```bash
cd examples
./run_examples.sh list
./run_examples.sh playground
./run_examples.sh service
./run_examples.sh modes
./run_examples.sh all
```

`service` starts the HTTP server, calls MySQL from the routes through `Client("trpc.mysql.hello")`, runs `test_service.py`, and stops the server. `playground` runs only the direct-client script.

## Tests

```bash
pytest tests/
# or
uv run pytest tests/
```

Coverage:

```bash
pytest --cov=trpc_mysql tests/
```

## Layout

```text
trpc-mysql/
├── trpc_mysql/
│   ├── __init__.py        # Public exports; import registers codec and transport
│   ├── client.py          # Framework Client
│   ├── mysql_client.py    # Direct MySqlClient and table helpers
│   ├── transport.py       # Transport that caches pools per loop
│   ├── codec.py           # mysql codec; the body is passed through
│   ├── constants.py       # Protocol name mysql and Mode
│   ├── types.py           # Request, Response, DatabaseUrl
│   └── utils.py           # md5 used as the pool cache key
├── examples/
│   ├── README.md
│   ├── run_examples.sh
│   ├── playground.py      # Direct client, set_target, and YAML
│   ├── trpc_main.py       # Example HTTP server
│   ├── http_service.py    # /ping and /demo
│   ├── test_service.py
│   └── trpc_python*.yaml  # One config per process/thread model
├── tests/
├── pyproject.toml
├── README.md
└── README.en.md
```

## Notes

1. The framework `Client` target uses `dsn://`. `MySqlClient` uses `mysql://`, and `dsn://` parses as well.
2. `import trpc_mysql` comes before `create_server` and `init_plugins`. A `Client` built from a service name comes after config is loaded.
3. The examples create and drop `trpc_mysql_demo` in the database named by `target`. Watch the table name when that database is a real application database.
4. MySQL 8 and later default to `caching_sha2_password`. `PyMySQL 0.9.3` needs `cryptography`, which is already part of the install dependencies.
5. Use one `MySqlClient` only on the event loop that created its pool. Calling `Client` through several `asyncio.run` calls in one script can log `Event loop is closed` on exit.
6. `Client.close()` does not close pools. Close a direct pool with `await MySqlClient.close()`. Framework pools close when the serving loop that owns them stops.

## License

Apache-2.0. Copyright (C) 2026 Tencent.
