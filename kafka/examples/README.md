# tRPC Kafka 示例

本示例在一个 tRPC 进程里消费 Kafka，并用 Greeter 的 `SayHello` 把问候写回 Kafka。消费服务名 `trpc.trpc-python.trpc-kafka.consume`，不监听端口，消费者直接连 broker。Greeter 服务名 `trpc.trpc_python_group.helloworld.Greeter`，监听 `127.0.0.1:8001`。生产者后端名 `trpc.kafka.producer.service`。

## 关键特性

- **消费落日志**：每条消息打印一行，并追加一行 JSON 到 `$TRPC_KAFKA_CONSUMED_LOG`（默认 `/tmp/trpc_kafka_consumed.log`），带上处理它的 pid 和线程名
- **独立生产**：`kafka_client.py` 读 YAML 里的 service name，发送 `key-<i>` / `msg-<i>`
- **请求里生产**：`SayHello` 把问候写到同一 topic，回复里带 partition、offset、pid 和线程名
- **批量消费**：`trpc_main.py --batch` 给 Kafka target 加上 `batch_num=10`，按批交给 servicer
- **五种进程/线程模式**：同一套服务配五份 YAML，用来看消费者落在哪个进程、哪条线程

关键文件：

- [trpc_main.py](trpc_main.py)：`import trpc_kafka` 注册协议，再注册消费者和 Greeter，然后 `create_server` + `serve`
- [greeter.py](greeter.py)：`SayHello` 调用 `Client.produce`。`Client` 在导入时创建，fork 之前就存在
- [kafka_client.py](kafka_client.py)：不启服务。`TrpcConfig.load` 之后按 service name 生产，结束时停掉当前 loop 上的生产者
- [test_service_rpc.py](test_service_rpc.py)：并发打 `SayHello`，汇总 pid 和线程，失败时非 0 退出
- [run_examples.sh](run_examples.sh)：确认本机 Kafka、起服务、等 8001、跑两个客户端、核对消费日志、停服务
- `trpc_python*.yaml`：每种进程模型一份。Greeter 都听 `127.0.0.1:8001`，broker 都是 `localhost:9092`，topic 都是 `test-topic`，消费组都是 `test_consumers`

调用关系：

```text
kafka_client.py
└── Client("trpc.kafka.producer.service")
    └── Kafka localhost:9092  topic test-topic

test_service_rpc.py
└── :8001  trpc.trpc_python_group.helloworld.Greeter
    └── SayHello
        └── Client.produce
            └── Kafka test-topic

trpc_main.py
└── 每个 serving loop 一个消费者，group test_consumers
    └── handle 把记录写入消费日志
```

## 关键代码解释

`trpc_main.py` 在 `create_server` 之前导入 `trpc_kafka`。`kafka` 协议和 serving loop 上的消费者 hook 在导入时注册。`--batch` 会复制一份配置，给 `protocol: kafka` 的 target 加上 `batch_num=10`，并注册批量 servicer。逐条消费和批量消费写的是同一份日志。

`greeter.py` 在模块顶层创建 `Client("trpc.kafka.producer.service")`。构造时只记下服务名，broker 地址在 `produce` 时从已加载的 YAML 里按这个名字读取。服务进程 fork 之后，每个事件循环会各自建一个生产者。

`kafka_client.py` 先 `TrpcConfig.load` 再 `init_plugins`，然后并发 `produce`。脚本结束前调用 `stop_producers_for_current_loop()`，把还留在 linger 里的记录发出去。

五份 YAML 里和示例相关的两块相同，换 broker 或 topic 时每份都要改 `target`：

```yaml
server:
  service:
    - name: trpc.trpc-python.trpc-kafka.consume
      ip: 127.0.0.1
      port: 8000
      target: localhost:9092?topics=test-topic&group=test_consumers&enable_auto_commit=true&session_timeout_ms=20000&check_crcs=False
      protocol: kafka
      timeout: 1000
    - name: trpc.trpc_python_group.helloworld.Greeter
      ip: 127.0.0.1
      port: 8001
      protocol: trpc
      timeout: 5000

client:
  service:
    - name: trpc.kafka.producer.service
      target: ip://localhost:9092?linger_ms=5
      protocol: kafka
      timeout: 5000
```

消费服务的 `ip`、`port`、`network` 不会被监听。`target` 在 `?` 前是 broker，`?` 后是消费者参数。生产者的 `target` 用 `ip://`。

## 环境要求

- Python >= 3.9
- macOS 或 Linux
- 本机 Kafka 监听 `127.0.0.1:9092`。topic `test-topic` 要存在，或由 broker 自动创建。分区数不少于同一消费组里的消费者数，多出来的消费者分不到分区，只会空闲
- 本机 8001 空闲。8000 不会被监听
- 安装见上级 [README.md](../README.md)。在 `kafka` 目录执行 `uv sync`，或 `pip install -e .`

## 运行步骤

在 `examples` 目录下：

```bash
./run_examples.sh list
./run_examples.sh
./run_examples.sh service
./run_examples.sh service trpc_python_merge_processes.yaml
./run_examples.sh batch
./run_examples.sh modes
./run_examples.sh all
```

`./run_examples.sh` 和 `service` 一样，用 [trpc_python.yaml](trpc_python.yaml)。`service` 和 `batch` 后面可以换成本目录里的另一份 YAML，或一个绝对路径。`batch` 给 `trpc_main.py` 加上 `--batch`。`modes` 按下面表格的顺序各跑一遍 `service`。`all` 先跑 `modes`，再用默认配置跑一遍 `batch`。

解释器按 `$PYTHON`、`../.venv/bin/python`、`../venv/bin/python`、`python3` 的顺序选。`RPC_NUM` 默认 20，是 `SayHello` 的并发次数。`PRODUCE_NUM` 默认 20，是 `kafka_client.py` 发送的条数。服务日志写在 `examples/.server_run.log`。脚本把 `TRPC_KAFKA_CONSUMED_LOG` 设为 `examples/.consumed.log`，启动前会清空。上一级目录里有 `trpc_sdk` 包时，脚本把它的父目录放在 `PYTHONPATH` 前面。

手动两步，效果和默认的 `service` 相同。消费日志仍是默认的 `/tmp/trpc_kafka_consumed.log`：

```bash
# 终端 1，当前目录是 examples
python trpc_main.py -c trpc_python.yaml
python trpc_main.py -c trpc_python.yaml --batch

# 终端 2
python kafka_client.py --num 200
python test_service_rpc.py --num 50 --target ip://127.0.0.1:8001
```

| 配置 | `global.processmodel` | 消费者 |
| --- | --- | --- |
| `trpc_python.yaml` | 不写。单进程，一个事件循环 | 主线程一个 |
| `trpc_python_merge_threads.yaml` | `single_process`，`handle_num: 2` | 两个 serving 线程各一个 |
| `trpc_python_separate_thread.yaml` | `single_process`，`io_handle_type: separate`，`io_num: 1`，`handle_num: 2` | 两个 handle 线程各一个。I/O 线程不消费 |
| `trpc_python_separate.yaml` | `multi_process`，`io_handle_type: separate`，`handle_num: 2` | 两个 rpc task 各一个。I/O 进程不消费。Greeter 用 `SO_REUSEPORT` 听 8001 |
| `trpc_python_merge_processes.yaml` | `multi_process`，`io_handle_type: merge`，`handle_num: 2` | 两个 worker 各一个，用 `SO_REUSEPORT` 听 8001 |

## 运行结果

`./run_examples.sh` 对每一项打印 `[PASS]` 或 `[FAIL]`。

`service` 成功时，客户端打印类似下面的内容，消费日志里能找到 `key-0` 到 `key-19`，脚本以状态码 0 退出：

```text
ok=20 errors=0
pids={'12345': 20}
threads={'12345/MainThread': 20}
produced 20 record(s) to test-topic, partitions=[0, 1]
```

`SayHello` 的回复形如 `hello: hello-0 partition=0 offset=1 pid=12345 thread=MainThread`。消费日志的一行形如：

```text
{"key": "key-0", "value": "msg-0", "topic": "test-topic", "partition": 0, "offset": 1, "pid": 12345, "thread": "MainThread"}
```

默认配置下，这些记录都落在同一个 `(pid, MainThread)`。`single_process` 的两份配置会看到 `trpc-loop-*` 或 `trpc-handle-*`。两份 `multi_process` 配置里，Greeter 用 `SO_REUSEPORT` 分连接；macOS 上它不会把连接摊开，请求可能全进同一个进程。分区由 Kafka 分给消费组里的消费者，分不到分区的消费者空闲。

## 适用场景

- 用 `service` 确认插件注册、消费组、`produce`，以及 `SayHello` 在服务进程里能写到 Kafka
- 用 `batch` 确认 `batch_num` 路径仍会把每一条写进消费日志
- 换 YAML 之后看消费日志里的 pid 和线程名，确认每个 serving loop 用自己的消费者和生产者

## 需要注意什么

1. **先 `import trpc_kafka`**。注册发生在 import 时，放在 `create_server` 之前。
2. **消费服务不监听 `8000`**。一键脚本只等 `8001`。`8001` 已被占用时脚本直接失败。
3. **topic 分区数**。同一消费组里的消费者数等于 serving loop 的数量。分区不够时，多出来的消费者空闲，消息仍然可以被分到分区的消费者读到。
4. **消费组再均衡**。`session_timeout_ms` 是 20000。上一次进程没离开组时，新消费者要等它超时。脚本轮询消费日志大约 60 秒。
5. **生产者绑事件循环**。`AIOKafkaProducer` 只在创建它的 loop 上使用。独立脚本要在 loop 结束前调用 `stop_producers_for_current_loop()`。
6. **自动提交**。示例打开了 `enable_auto_commit`。关掉之后，一条消息（或一批）在 handler 返回后才提交位点。
