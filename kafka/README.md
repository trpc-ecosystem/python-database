# tRPC Kafka plugin

[中文](README.zh_CN.md)

Kafka for the tRPC-Python framework: a producer client, and consumers that run inside a tRPC server and hand each record
to your servicer. The broker connection is [aiokafka](https://github.com/aio-libs/aiokafka).

## Install

Requirements: `trpc-python`, `aiokafka`.

```bash
pip3 install trpc-kafka
```

Import `trpc_kafka` before `Server.serve()`: the import registers the `kafka` protocol and the hooks that run the
consumers.

## Consume

Set `protocol: kafka` on a server service. `target` holds the brokers, then `?`, then the consumer options as
`k=v&k=v`. `ip`, `port` and `network` are not listened on; the consumers connect to the brokers.

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

For batches, add `batch_num=<n>` to the target and register a `TrpcKafkaBatchConsumerServicer` with
`register_trpc_kafka_batch_consumer_server`; its `handle(ctx, msgs)` receives a list of `ConsumerRecord`.
`parallel_num` splits a batch into key-stable groups that run concurrently, and `msg_gather_timeout` is the `getmany`
timeout in milliseconds.

With `enable_auto_commit=false`, a record (or a batch) is committed after its handler returns. With auto-commit on,
aiokafka commits every `auto_commit_interval_ms`.

## Produce

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

`produce` returns aiokafka's `RecordMetadata`. One producer is created per target and per event loop, because an
`AIOKafkaProducer` only works on the loop that created it; handlers on different serving threads or processes each get
their own. Inside a server, the producers of a serving loop are stopped (and their lingering records flushed) when the
loop stops. A standalone script should call `await trpc_kafka.client_transport.stop_producers_for_current_loop()`
before its loop ends.

## Process and thread models

The process model comes from `global.processmodel`.

Every serving loop (an event loop that runs handlers) builds its own consumer for each Kafka service, all in the group
of the service's `target`, and handles the records it fetches on that loop. Kafka then spreads the partitions over
them.

| Model | `global.processmodel` | Consumers per Kafka service | Where |
|---|---|---|---|
| merge, single loop | none (default) | 1 | the main loop |
| merge, threads | `single_process`, `handle_num: N` | N | each serving thread |
| SeparateThread | `single_process`, `io_handle_type: separate`, `handle_num: N` | N | each handle thread |
| separate | `multi_process`, `io_handle_type: separate`, `handle_num: N` | N | each rpc task process; the I/O process has none |
| merge, processes | `multi_process`, `io_handle_type: merge`, `handle_num: N` | N | each worker process |

Give the topic at least as many partitions as there are consumers in the group; extra consumers get no partition and
stay idle.

On SIGTERM each serving process stops fetching at once, lets the records already fetched finish during the drain
(`server.close_wait_time`), commits them (with auto-commit off), stops its consumers so they leave the group, and then
stops the producers of each serving loop.

## Examples

[`examples/`](examples) has one config per model. How to run them, and what each config does, is in
[examples/README.md](examples/README.md). The one-step script is [examples/run_examples.sh](examples/run_examples.sh).

| Config | Model |
|---|---|
| `trpc_python.yaml` | merge, single loop |
| `trpc_python_merge_threads.yaml` | merge, two serving threads |
| `trpc_python_separate_thread.yaml` | one I/O thread, two handle threads |
| `trpc_python_separate.yaml` | one I/O process, two rpc task processes |
| `trpc_python_merge_processes.yaml` | two worker processes |

```bash
cd examples
python trpc_main.py -c trpc_python_merge_threads.yaml          # add --batch to consume in batches
python kafka_client.py --num 200                               # produce 200 records with distinct keys
python test_service_rpc.py --num 50                            # SayHello, which produces from the handler
```

`trpc_main.py` serves the Kafka consumer and a Greeter whose `SayHello` produces to Kafka. Every consumed record is
appended as one JSON line, with the pid and thread that handled it, to `$TRPC_KAFKA_CONSUMED_LOG`
(`/tmp/trpc_kafka_consumed.log` by default).
