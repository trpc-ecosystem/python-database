# tRPC MySQL 示例

本示例做两件事：在 tRPC HTTP 服务里用 `Client` 访问本机 MySQL，以及不启服务时直接调用。HTTP 服务名 `trpc.mysql.examples.http`，监听 `127.0.0.1:8080`。MySQL 后端名 `trpc.mysql.hello`。

## 关键特性

- **查询和写入**：`GET /ping` 执行 `SELECT 1`；`POST /demo` 建表、按 JSON 插入一行、用 `fetchall` 和 `fetchone` 读回，然后删表
- **三种直接调用**：`MySqlClient` 直连、`Client` 加 `Options.set_target`、读 YAML 里的 service name
- **并发看落点**：`test_service.py` 默认并发 50 次 `/ping`，打印处理请求的 pid 和线程名
- **五种进程/线程模式**：同一套服务配五份 YAML，用来看连接池跟事件循环走

关键文件：

- [trpc_main.py](trpc_main.py)：`import trpc_mysql` 注册插件，再导入路由，然后 `create_server` + `serve`
- [http_service.py](http_service.py)：`GET /ping`、`POST /demo`。路由在第一次请求时创建 `Client("trpc.mysql.hello")`
- [test_service.py](test_service.py)：打 `/ping`（含并发）、`/demo`，校验 JSON，失败时非 0 退出
- [playground.py](playground.py)：不启服务。`main` 只跑 `MySqlClient`；另外两个函数要单独调用
- [run_examples.sh](run_examples.sh)：起服务、等 8080、跑客户端、停服务
- `trpc_python*.yaml`：每种进程模型一份。HTTP 都听 `127.0.0.1:8080`，MySQL 都指向 `dsn://root:mysql2021@127.0.0.1:3306/mysql`

调用关系：

```text
test_service.py
└── :8080  trpc.mysql.examples.http
    ├── GET /ping
    │   └── Client.fetchall("SELECT 1 AS ok")
    └── POST /demo
        └── Client.execute / fetchall / fetchone
            └── MySQL 127.0.0.1:3306/mysql

playground.py
├── demo_standalone    MySqlClient，不读 YAML
├── demo_client        Client + Options.set_target
└── demo_with_config   TrpcConfig.load 之后 Client("trpc.mysql.hello")
```

## 关键代码解释

`trpc_main.py` 在 `create_server` 之前导入 `trpc_mysql` 和 `http_service`。codec、transport 在导入 `trpc_mysql` 时注册。`@route` 在导入 `http_service` 时把路由登记到 `trpc.mysql.examples.http`。

`Client` 构造时要从框架配置读 `target`，配置要等 `create_server` 才加载。`http_service.py` 把 `Client("trpc.mysql.hello")` 放到第一次请求里创建。

路由把请求的 `ctx` 传给 `Client`，SQL 走 selector、codec、transport 和超时。行是 `DictCursor` 的 dict。`fetchone` 的单行也会包成单元素 list。

| 路由 | MySQL 调用 | 返回 |
| --- | --- | --- |
| `GET /ping` | `fetchall(ctx, "SELECT 1 AS ok")` | `{"rows": [{"ok": 1}], "pid", "thread"}` |
| `POST /demo` | 建表 `trpc_mysql_demo`、清空、`INSERT ... VALUES (%s, %s)`、`fetchall`、`fetchone(COUNT(*))`、删表 | `{"insert_id", "rows", "one"}` |

`POST /demo` 的请求体是 `{"name": "admin", "role": 1}`。值用 `%s` 绑定，表名是常量。缺字段返回 400，MySQL 出错返回 500 和 `{"error": ...}`。

`playground.py` 的 `main` 只跑 `demo_standalone`：`table_insert`、`query`、`get`、`table_has`，结束时 `close()`。`demo_client` 用 `Options().set_target("dsn://...")`，服务名只是标签。`demo_with_config` 先 `TrpcConfig.load` 再 `init_plugins`，然后 `Client("trpc.mysql.hello")`。后两个函数要单独调用，例如 `asyncio.run(demo_with_config())`。

五份 YAML 里和示例相关的两块相同，换库时每份都要改 `target`：

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

框架按 `target` 的 scheme 找 selector。`dsn` 会把整段 DSN 原样交给 transport，transport 再写成 `mysql://` 交给 `aiomysql`。`MySqlClient` 直连不经过 selector，用 `mysql://`。

## 环境要求

- Python >= 3.9
- macOS 或 Linux
- 本机 MySQL 监听 `127.0.0.1:3306`，账号口令与上面的 DSN 一致。`playground.py` 用环境变量 `MYSQL_DSN` 覆盖，格式是 `mysql://...`
- 本机 8080 空闲
- 安装见上级 [README.md](../README.md)。在 `mysql` 目录执行 `uv sync`，或 `pip install trpc-mysql`

## 运行步骤

在 `examples` 目录下：

```bash
./run_examples.sh list
./run_examples.sh
./run_examples.sh service
./run_examples.sh service trpc_python_multi_process_merge.yaml
./run_examples.sh modes
./run_examples.sh playground
./run_examples.sh all
```

`./run_examples.sh` 和 `service` 一样，用 [trpc_python.yaml](trpc_python.yaml)。`service` 后面可以换成本目录里的另一份 YAML，或一个绝对路径。`modes` 按下面表格的顺序各跑一遍 `service`。`playground` 只跑 [playground.py](playground.py)。`all` 先跑默认的 `service`，再跑 `playground`。

解释器按 `$PYTHON`、`../venv/bin/python`、`python3` 的顺序选。服务日志写在 `examples/.server_run.log`。

手动两步，效果和默认的 `service` 相同：

```bash
# 终端 1，当前目录是 examples
python trpc_main.py -c trpc_python.yaml

# 终端 2
python test_service.py
python test_service.py --addr 127.0.0.1:8080 --concurrency 50
curl -sS http://127.0.0.1:8080/ping
curl -sS -X POST http://127.0.0.1:8080/demo \
  -H 'Content-Type: application/json' -d '{"name":"admin","role":1}'
```

不启服务：

```bash
python playground.py
MYSQL_DSN='mysql://root:mysql2021@127.0.0.1:3306/mysql' python playground.py
python -c "import asyncio; from playground import demo_client; asyncio.run(demo_client())"
python -c "import asyncio; from playground import demo_with_config; asyncio.run(demo_with_config())"
```

| 配置 | `global.processmodel` | HTTP |
| --- | --- | --- |
| `trpc_python.yaml` | 不写。单进程，一个事件循环 | 主线程 `MainThread`，一个连接池 |
| `trpc_python_single_process_merge.yaml` | `single_process`，`handle_num: 2` | 主线程 accept，`trpc-loop-0` 和 `trpc-loop-1` 各一个 aiohttp 和连接池 |
| `trpc_python_single_process_separate.yaml` | `single_process`，`io_handle_type: separate`，`io_num: 1`，`handle_num: 2` | 主线程 accept，路由在 `trpc-handle-0` 和 `trpc-handle-1`。I/O 线程不访问 MySQL |
| `trpc_python_multi_process_separate.yaml` | `multi_process`，`io_handle_type: separate`，`handle_num: 2` | 两个 rpc task 用 `SO_REUSEPORT` 听 8080，各一个连接池。I/O 进程不访问 MySQL |
| `trpc_python_multi_process_merge.yaml` | `multi_process`，`io_handle_type: merge`，`handle_num: 2` | 两个 worker 用 `SO_REUSEPORT` 听 8080，各一个事件循环和连接池 |

`multi_process` 加 `io_handle_type: merge` 在框架里是 peer：每个 worker 自己 accept、自己处理。同一条线程、同一个事件循环上，同一个 URI 共用一个池。fork 出来的子进程池缓存是空的。

## 运行结果

`./run_examples.sh` 对每一项打印 `[PASS]` 或 `[FAIL]`。

`service` 成功时，客户端打印类似下面的内容，并以状态码 0 退出：

```text
[ping] 200 {'rows': [{'ok': 1}], 'pid': 12345, 'thread': 'MainThread'}
[concurrent_ping] 50/50 ok, served by (pid, thread): [(12345, 'MainThread')]
[demo] 200 {'insert_id': 1, 'rows': [{'id': 1, 'name': 'admin', 'role': 1}], 'one': [{'cnt': 1}]}
[bad_body] 0 {'error': 'http client codec StatusCode: 400'}
[OK] MySQL calls through the tRPC server behave as expected
```

`bad_body` 的状态码是 0：tRPC HTTP 客户端遇到非 2xx 会抛异常，`test_service.py` 把它记成 0，并在错误文本里保留 `400`。

默认配置下 50 次 `/ping` 都落在同一个 `(pid, MainThread)`。`single_process` 的两份配置会看到 `trpc-loop-*` 或 `trpc-handle-*`。两份 `multi_process` 配置用 `SO_REUSEPORT` 分连接；macOS 上它不会把连接摊开，请求可能全进同一个进程。

`playground` 成功时打印：

```text
[standalone] insert: 1
[standalone] query: [{'id': 1, 'name': 'admin', 'role': 1}]
[standalone] get: [{'cnt': 1}]
[standalone] table_has: [{'name': 'admin'}]
```

`insert` 是 `lastrowid`。表每次都会 `DROP`，新表上一般是 `1`。`table_has` 返回匹配行，列只有查询里的 `name`。

单独跑 `demo_client` 会打印 `[client] fetchall: [{'id': 1, 'name': 'admin', 'role': 1}]`。单独跑 `demo_with_config` 会打印 `[config] fetchall: [{'ok': 1}]`。

## 适用场景

- 用 `playground` 确认本机 MySQL，以及 `MySqlClient` 的建表、写入、查询
- 用 `service` 确认插件注册、`dsn://` target、服务进程里的 `execute` / `fetchall` / `fetchone`
- 换 YAML 之后看 `/ping` 落在哪个 pid、哪条线程，确认每个事件循环用自己的连接池

## 需要注意什么

1. **框架 `Client` 的 target 用 `dsn://`**。写成 `mysql://` 时框架找不到对应的 selector。`MySqlClient` 和 `MYSQL_DSN` 用 `mysql://`。`DatabaseUrl.make_url` 也接受 `dsn://`。
2. **先 `import trpc_mysql`**。注册发生在 import 时，放在 `create_server` 或 `init_plugins` 之前。
3. **按名字创建的 `Client` 要等配置加载完**。服务里在路由内创建。`demo_with_config` 放在 `TrpcConfig.load` 之后。`Options.set_target` 自带地址，不读 YAML。
4. **示例会建表再删表**。`trpc_mysql_demo` 建在 target 指定的库里，默认是 `mysql`。每次调用结束都会 `DROP`。
5. **端口**。五份配置都听 `8080`。一键脚本发现端口已被占用会直接失败。
6. **MySQL 8 的认证**。默认插件是 `caching_sha2_password`，`PyMySQL 0.9.3` 需要 `cryptography`，依赖里已经带上。
7. **连接池绑事件循环**。池创建在哪条循环上，就只在那条循环上用。一个进程里对 `Client` 连续多次 `asyncio.run`，退出时会看到 `Event loop is closed`。
