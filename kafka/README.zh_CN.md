# tRPC Kafka 插件

[English](README.md)

tRPC-Python 框架的 Kafka 插件：提供生产者客户端，以及在 tRPC 服务里运行、把每条消息交给业务 servicer 的消费者。和 broker 的通信由 [aiokafka](https://github.com/aio-libs/aiokafka) 完成。

## 安装

依赖：`trpc-python`、`aiokafka`。

```bash
pip3 install trpc-kafka
```

`import trpc_kafka` 必须在 `Server.serve()` 之前：导入时会注册 `kafka` 协议和运行消费者的 hook。

## 消费

服务端 service 配 `protocol: kafka`。`target` 先写 brokers，再用 `?` 接消费者参数，参数之间用 `&` 分隔，形如 `k=v`。`ip`、`port`、`network` 不会被监听，消费者直接连 broker。

```yaml
server:
  service:
    - name: trpc.trpc-python.trpc-kafka.consume
      ip: 127.0.0.1
      port: 8000
      target: localhost:9092?topics=test-topic&group=test_consumers&enable_auto_commit=true
      network: tcp
      protocol: kafka
      timeout: 1000
```

```python
from trpc_sdk.abc import TrpcContext
from trpc_sdk.server import create_server

import trpc_kafka


class KafkaServer(trpc_kafka.TrpcKafkaServicer):

    async def handle(self, ctx: TrpcContext, key, value, topic, partition, offset, timestamp):
        print(key, value, topic, partition, offset)


server = create_server("trpc_python.yaml")
trpc_kafka.register_trpc_kafka_server(server, KafkaServer())
server.serve()
```

批量消费：在 target 里加 `batch_num=<n>`，并用 `register_trpc_kafka_batch_consumer_server` 注册 `TrpcKafkaBatchConsumerServicer`，它的 `handle(ctx, msgs)` 收到一组 `ConsumerRecord`。`parallel_num` 把一批按 key 分成若干组并发处理，同一个 key 仍然有序；`msg_gather_timeout` 是 `getmany` 的超时，单位毫秒。

`enable_auto_commit=false` 时，一条消息（或一批）在 handler 返回后提交位点；开启自动提交时，由 aiokafka 每隔 `auto_commit_interval_ms` 提交。

## 生产

```yaml
client:
  service:
    - name: trpc.kafka.producer.service
      target: ip://localhost:9092?linger_ms=5
      protocol: kafka
      timeout: 1000
```

```python
import trpc_kafka

client = trpc_kafka.Client("trpc.kafka.producer.service")


async def send(ctx):
    meta = await client.produce(ctx, "test-topic", "key", "value")
    print(meta.partition, meta.offset)
```

`produce` 返回 aiokafka 的 `RecordMetadata`。生产者按 target 和事件循环各建一个，因为 `AIOKafkaProducer` 只能在创建它的 loop 上用；不同 serving 线程或进程里的 handler 各用各的。在服务里，serving loop 停止时会停掉它创建的生产者，并把 linger 中的消息发出去。独立脚本应在 loop 结束前调用 `await trpc_kafka.client_transport.stop_producers_for_current_loop()`。

## 进程/线程模式

进程模式由 `global.processmodel` 决定

每个 serving loop（运行 handler 的事件循环）为每个 Kafka service 各建一个消费者，都在该 service `target` 指定的 group 里，拉到的消息就在这个 loop 上处理，分区由 Kafka 在它们之间分配。

| 模式 | `global.processmodel` | 每个 Kafka service 的消费者数 | 所在位置 |
|---|---|---|---|
| merge 单循环 | 不配置（默认） | 1 | 主 loop |
| merge 多线程 | `single_process`，`handle_num: N` | N | 每个 serving 线程 |
| SeparateThread | `single_process`，`io_handle_type: separate`，`handle_num: N` | N | 每个 handle 线程 |
| separate | `multi_process`，`io_handle_type: separate`，`handle_num: N` | N | 每个 rpc task 进程，I/O 进程不消费 |
| merge 多进程 | `multi_process`，`io_handle_type: merge`，`handle_num: N` | N | 每个 worker 进程 |

topic 的分区数要不少于 group 里的消费者总数，多出来的消费者分不到分区，只会空闲。

收到 SIGTERM 后，每个 serving 进程立即停止拉取；已经拉到的消息在 drain 期间（`server.close_wait_time`）处理完并提交位点（关闭自动提交时），然后停掉消费者使其离开 group，最后停掉每个 serving loop 上的生产者。

## 示例

[`examples/`](examples) 下每种模式一份配置。运行步骤和各配置的说明见 [examples/README.md](examples/README.md)。一键脚本是 [examples/run_examples.sh](examples/run_examples.sh)。

| 配置 | 模式 |
|---|---|
| `trpc_python.yaml` | merge 单循环 |
| `trpc_python_merge_threads.yaml` | merge，两个 serving 线程 |
| `trpc_python_separate_thread.yaml` | 一个 I/O 线程，两个 handle 线程 |
| `trpc_python_separate.yaml` | 一个 I/O 进程，两个 rpc task 进程 |
| `trpc_python_merge_processes.yaml` | 两个 worker 进程 |

```bash
cd examples
python trpc_main.py -c trpc_python_merge_threads.yaml          # 加 --batch 改为批量消费
python kafka_client.py --num 200                               # 发 200 条 key 各不相同的消息
python test_service_rpc.py --num 50                            # 调 SayHello，handler 里会生产消息
```

`trpc_main.py` 同时提供 Kafka 消费和 Greeter 服务，`SayHello` 会向 Kafka 生产一条消息。每条消费到的消息连同处理它的 pid 和线程名，以一行 JSON 追加到 `$TRPC_KAFKA_CONSUMED_LOG`（默认 `/tmp/trpc_kafka_consumed.log`）。
