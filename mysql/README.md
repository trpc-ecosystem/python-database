# tRPC MySQL 插件

[English](README.en.md)

tRPC-Python 的 MySQL 客户端。它封装 [aiomysql](https://github.com/aio-libs/aiomysql)，用 SQL 做异步读写，不提供 ORM。

有两条调用路径。`MySqlClient` 自己持有连接池，适合脚本或不经过框架的进程。`Client` 把同一次调用放进 tRPC 客户端链路（selector、codec、transport、超时），适合在 tRPC 服务里访问 MySQL。

```mermaid
flowchart TD
  subgraph directPath [MySqlClient]
    url["mysql:// DSN"] --> directClient[MySqlClient]
    directClient --> pool[aiomysql 连接池]
  end
  subgraph trpcPath [Client]
    imp["import trpc_mysql"] --> frameworkClient["Client dsn://"]
    frameworkClient --> stack["selector / codec / transport"]
    stack --> loopPool["每个事件循环一个 MySqlClient"]
    loopPool --> pool
  end
```

## 功能特性

- **SQL 读写**：`fetchone` / `fetchall` / `execute` / `transaction`，参数用 PyMySQL 的 `%s` 占位符绑定。
- **直连辅助方法**：`MySqlClient` 另外提供 `transaction_multi`，以及 `table_has` / `table_insert` / `table_update`。
- **行是 dict**：默认游标是 `DictCursor`。
- **连接池**：`aiomysql` 连接池按进程、线程、事件循环分开，同一 URI 在同一个循环里共用一个池。
- **框架接入**：`import trpc_mysql` 注册 `mysql` 协议的 codec 和 transport。

## 安装

要求：

- Python >= 3.9
- `trpc-python`（导入名 `trpc_sdk`）。该依赖不支持 Windows
- `aiomysql~=0.0.21`
- `pymysql~=0.9.3`
- `cryptography`（MySQL 8 默认的 `caching_sha2_password` 需要它）

```bash
pip install trpc-mysql
```

本地开发在 `mysql` 目录下执行。`uv sync` 按 `pyproject.toml` 做可编辑安装，并安装 dev 依赖组。包索引里需要能解析到 `trpc-python`。

```bash
uv sync
```

## 快速开始

默认连接 `mysql://root:mysql2021@127.0.0.1:3306/mysql`。环境变量 `MYSQL_DSN` 可以覆盖这个地址，`examples/playground.py` 会读它。

```bash
python examples/playground.py
```

`playground.py` 的 `main` 走 `MySqlClient` 直连，会自己建表、写入、查询再删表。`demo_client` 用 `Options.set_target` 走框架 `Client`。`demo_with_config` 读取同目录的 `trpc_python.yaml`，需要单独调用，例如 `asyncio.run(demo_with_config())`。

在 trpc 服务里访问 MySQL、并起服务验证的完整示例见 [examples/README.md](examples/README.md)。

### 直连 MySqlClient

直连不经过 selector，URI 用 `mysql://`。`dsn://` 和 `dsn+mysql://` 也可以，`DatabaseUrl.make_url` 会改写成 `mysql://`。

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

### 框架 Client

先 `import trpc_mysql`，codec 和 transport 才会注册。框架按 target 的 scheme 找 selector，只有 `dsn` 会把整段 DSN 原样交给 transport，transport 再改写成 `mysql://`。写成 `mysql://` 会报 `Client select node selector not exist`。

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

### 配置文件

见 `examples/trpc_python.yaml`。后端 service 名是 `trpc.mysql.hello`，`target` 指向默认 DSN 对应的库。`timeout` 的单位是毫秒。

```yaml
client:
  service:
    - name: trpc.mysql.hello
      target: "dsn://root:mysql2021@127.0.0.1:3306/mysql"
      timeout: 5000
      network: tcp
      protocol: mysql
```

`Client` 在构造时读取 target。配置还没加载时，不能按服务名构造它。服务进程里等 `create_server` 之后、在请求处理函数里创建；脚本里先 `TrpcConfig.load` 和 `init_plugins`。

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

## 两种客户端

| | `MySqlClient` | `Client` |
| --- | --- | --- |
| 用途 | 脚本、非 tRPC 进程 | tRPC 服务里的下游调用 |
| URI | `mysql://user:password@host:port/db` | `dsn://user:password@host:port/db` |
| 入口 | `query` / `get` / `execute` | `fetchall` / `fetchone` / `execute`，第一个参数是 `TrpcContext` |
| 连接池 | 这个对象自己的池，第一次语句时打开 | transport 按线程、事件循环和 URI 缓存 |
| 关闭 | `await client.close()` | `close()` 是空操作，池在服务循环停止时由 transport 关闭 |

`import trpc_mysql` 必须写在 `create_server` 或 `init_plugins` 之前。只使用 `MySqlClient`、完全不走框架客户端时，可以不导入这个包的插件注册。

`Client` 的构造参数 `options` 里如果已经 `set_target`，就用这份 URI。否则按 `service_name` 从框架配置读取 `client.service`。服务名同时写进调用的 callee，RPC 名是 `/{service_name}/{mode}`，只作为调用标签。真正选池用的是解析后的 URI。

## 接口

SQL 文本里的值用 `%s` 绑定。一个字符串参数要放进单元素的 list 或 tuple，例如 `["admin"]` 或 `("admin",)`。裸字符串会被当成字符序列。

读操作失败时会 `ping` 一次再执行。第二次仍失败则把异常抛出。写操作失败时记日志并吞掉异常，返回当时的 `lastrowid`。打开连接池或取连接的失败会直接抛出。

### Client

`Client(service_name, options=None)`。下面的方法都是异步的，返回 transport 放在响应头里的 `result`。

- `fetchone(ctx, query, args=None)`：读一行。有行时是只含这一行的 list，没有行时是 `None`。
- `fetchall(ctx, query, args=None, size=None)`：`size` 为空或 `0` 时读出全部行，`1` 时读一行，其他正整数交给 `fetchmany`。
- `execute(ctx, query, args=None, ret_affected_num=False)`：执行一条语句。默认返回 `lastrowid`。`ret_affected_num=True` 时返回影响行数。
- `transaction(ctx, query, args=None, ret_affected_num=False)`：执行一条语句，并在驱动抛错时回滚。返回值与 `execute` 相同。
- `close()`：空操作。

框架 `Client` 上，超时变为 `ClientTimeoutError`，其他从驱动抛出的失败变为 `ClientInvokeError`。被 `execute` / `transaction` 吞掉的语句错误不会变成这两种异常。

`transaction_multi` 和 `table_*` 只在 `MySqlClient` 上。

### MySqlClient

`MySqlClient(url, loop=None, cursor=DictCursor, **kwargs)`。`url` 是 `DatabaseUrl.make_url` 的返回值。连接池在第一条语句时创建，并绑定当时的事件循环。

与 `Client` 对应的方法没有 `ctx`：

- `get(sql, params=None)`：对应 `fetchone`。
- `query(sql, params=None, size=None)`：对应 `fetchall`。
- `execute(sql, params=None, ret_affected_num=False)`
- `transaction(sql, params=None, ret_affected_num=False)`
- `await close()`：关闭这个对象自己的池。

`transaction_multi(raw_sqls)` 在一个事务里按顺序执行多条语句。列表项是 SQL 字符串，或带 `pre_sql` 和 `data` 的 dict。dict 走 `executemany`。返回出错前已经累计的影响行数。

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

默认 `autocommit=True`。`transaction` 和 `transaction_multi` 都会先 `BEGIN`。成功之后，只有 `autocommit=False` 才会显式 `COMMIT`。单条 `transaction` 在驱动抛错时会 `ROLLBACK`。`transaction_multi` 只在 `autocommit=False` 时回滚。需要「成功提交、失败回滚」时，构造客户端传入 `autocommit=False`。

`Mode.CURSOR` 保留未用。`invoke` 遇到它或未知 mode 时返回 `None`，不会访问数据库。

### 表辅助方法

`table_has`、`table_insert`、`table_update` 把表名和列名拼进 SQL。`table_update` 的条件值也会拼进双引号，而不是 `%s` 绑定。这些标识符和条件值只应来自代码里的常量。用户输入走 `query` / `execute` 的 `%s`。

- `table_has(table, field, value)`：`SELECT field FROM table WHERE field=%s LIMIT 1`。查到时返回单元素 list，没有行或查询失败时返回 `None`。
- `table_insert(table, item, ret_affected_num=False)`：`item` 是列名到值的 dict，值用 `%s` 绑定。
- `table_update(table, updates, field_where, value_where, ret_affected_num=False)`：`updates` 的值用 `%s` 绑定，`value_where` 拼进 SQL。

### 连接池

类属性的默认值是 `minsize=3`、`maxsize=10`、`pool_recycle=7` 小时（`7 * 3600` 秒）。构造时传入的 `minsize`、`maxsize`、`pool_recycle` 会被这三个类属性盖掉。要改池大小，在创建实例之前改类属性。布尔型的 `autocommit` 关键字会保留。

直连的 `MySqlClient` 每个实例一个池。框架 transport 里的池另外按线程和事件循环缓存，键是 URI 的 md5。同一个循环上对同一个 URI 的多次 `Client` 调用共用一个池。

## 配置

换库时改 `client.service[].target`。协议名固定为 `mysql`。

URI 的数据库名后可以加 `?charset=`。缺省字符集是 `utf8mb4`。密码里可以包含冒号，解析时只在第一个冒号处切开用户名和密码。

```text
mysql://user:password@127.0.0.1:3306/app
mysql://user:password@127.0.0.1:3306/app?charset=utf8mb4
dsn://user:password@127.0.0.1:3306/app
```

`examples/trpc_python.yaml` 里和 MySQL 相关的两段：

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

`playground.py` 读 `MYSQL_DSN`（`mysql://`），不读这份 YAML 的 target。`demo_with_config` 例外，它按服务名走配置。

## 进程与线程模型

`aiomysql` 的池绑定创建它的事件循环。transport 因此为每个进程、每个线程、每个事件循环各留一份缓存，五种 `global.processmodel` 都可以访问 MySQL。fork 出的子进程缓存是空的，所以在 import 时、fork 之前构造 `Client` 没有问题。服务循环停止时，transport 关掉这个循环上的池。

| 配置 | `global.processmodel` | MySQL 调用跑在哪里 |
| --- | --- | --- |
| `trpc_python.yaml` | 未配置（merge，单循环） | 主进程的主循环 |
| `trpc_python_single_process_merge.yaml` | `single_process`，`handle_num: 2` | 每个处理线程自己的循环和池 |
| `trpc_python_single_process_separate.yaml` | `single_process`，`io_handle_type: separate`，`io_num: 1`，`handle_num: 2` | 每个处理线程自己的循环和池 |
| `trpc_python_multi_process_separate.yaml` | `multi_process`，`io_handle_type: separate`，`handle_num: 2` | 每个 rpc task 进程。I/O 进程不访问 MySQL |
| `trpc_python_multi_process_merge.yaml` | `multi_process`，`io_handle_type: merge`，`handle_num: 2` | 每个 worker 进程 |

自己构造的 `MySqlClient` 不进这张缓存。它的池属于第一次执行语句的那个循环，只在那个循环上使用。

本机 MySQL 就绪后，可以用示例脚本把五种配置都跑一遍：

```bash
cd examples
./run_examples.sh modes
```

## 示例

可运行示例和一键脚本见 [examples/README.md](examples/README.md)。

```bash
cd examples
./run_examples.sh list
./run_examples.sh playground
./run_examples.sh service
./run_examples.sh modes
./run_examples.sh all
```

`service` 会启动 HTTP 服务，用 `Client("trpc.mysql.hello")` 在路由里访问 MySQL，再跑 `test_service.py`，最后关掉服务。`playground` 只跑直连脚本。

## 测试

```bash
pytest tests/
# 或者
uv run pytest tests/
```

覆盖率：

```bash
pytest --cov=trpc_mysql tests/
```

## 目录

```text
trpc-mysql/
├── trpc_mysql/
│   ├── __init__.py        # 公共导出；导入时注册 codec 和 transport
│   ├── client.py          # 框架 Client
│   ├── mysql_client.py    # 直连 MySqlClient 和表辅助方法
│   ├── transport.py       # 按循环缓存连接池的 transport
│   ├── codec.py           # mysql 协议 codec，消息体原样通过
│   ├── constants.py       # 协议名 mysql 和 Mode
│   ├── types.py           # Request、Response、DatabaseUrl
│   └── utils.py           # 连接池缓存键用的 md5
├── examples/
│   ├── README.md
│   ├── run_examples.sh
│   ├── playground.py      # 直连、set_target、读 YAML
│   ├── trpc_main.py       # 示例 HTTP 服务
│   ├── http_service.py    # /ping、/demo
│   ├── test_service.py
│   └── trpc_python*.yaml  # 每种进程/线程模型一份
├── tests/
├── pyproject.toml
├── README.md
└── README.en.md
```

## 注意

1. 框架 `Client` 的 target 用 `dsn://`。`MySqlClient` 用 `mysql://`，`dsn://` 同样能解析。
2. `import trpc_mysql` 要早于 `create_server` 和 `init_plugins`。按服务名创建的 `Client` 要晚于配置加载。
3. 示例会在 target 指向的库里创建并删除 `trpc_mysql_demo`。指向业务库时注意表名。
4. MySQL 8 及以上默认认证插件是 `caching_sha2_password`。`PyMySQL 0.9.3` 需要 `cryptography`，安装依赖里已经包含。
5. 同一个 `MySqlClient` 只在创建池的那个事件循环上使用。一个脚本里连续多次 `asyncio.run` 去调 `Client`，退出时可能看到 `Event loop is closed`。
6. `Client.close()` 不关闭连接池。直连池用 `await MySqlClient.close()`。框架池在对应服务循环停止时关闭。

## 许可证

Apache-2.0。Copyright (C) 2026 Tencent.
