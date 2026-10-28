# tRPC Redis 示例

本示例演示两件事：不启服务时用 `Client` 访问本机 Redis，以及在 tRPC 服务进程里用 Redis 计数

## 关键特性

- **命令、pipeline、pub/sub**：`set` / `get`、批量 `execute`、订阅一条频道并收到自己发出的消息
- **锁和事务**：`SET NX EX` 分布式锁、`MULTI/EXEC`、`WATCH` 乐观事务，以及锁包住事务的组合
- **服务内调用**：Greeter 的 `SayHello` 对固定 key 做 `INCR`，回复里带上计数值、pid 和线程名
- **五种进程/线程模式**：同一套服务配五份 YAML，用来看连接池跟事件循环走，而不是跟客户端对象走

关键文件：

- [redis_client.py](redis_client.py)：独立脚本，默认读 [trpc_python.yaml](trpc_python.yaml) 里的 `trpc.redis.test_example`
- [trpc_main.py](trpc_main.py)：服务入口，`import trpc_redis` 注册插件后 `create_server` + `serve`
- [greeter.py](greeter.py)：在 import 时构造 `RedisClient`，handler 里 `INCR`
- [test_service_rpc.py](test_service_rpc.py)：并发打 `SayHello`，汇总 pid / 线程分布
- [run_examples.sh](run_examples.sh)：起服务、等端口、跑客户端、停服务
- `trpc_python*.yaml`：每种模式一份，服务都听 `127.0.0.1:8000`，Redis 都指向 `ip://127.0.0.1:6379`

## 关键代码解释

- `redis_client.py` 的 `__main__` 走 `test_redis_with_config`：`TrpcConfig.load` 和 `init_plugins` 之后才 `Client("trpc.redis.test_example")`。同文件里的 `test_redis_without_config` 不读 YAML，用 `Options().set_target("ip://127.0.0.1:6379")`。
- `greeter.py` 在模块顶层创建客户端。fork 出来的子进程连接池缓存是空的，父进程里先建好客户端没有问题。
- `INCR` 的返回值就是新的计数值，所以并发请求的 `count` 互不相同。`test_service_rpc.py` 用这个判断每次调用都真正打到了 Redis。

调用关系：

```text
test_service_rpc.py
└── Greeter.SayHello
    └── RedisClient.incr
        └── Redis 127.0.0.1:6379
```

## 环境要求

- Python >= 3.9，推荐 3.10 及以上
- 本机 Redis 监听 `127.0.0.1:6379`，示例配置没有密码
- 依赖见上级目录 [README.zh_CN.md](../README.zh_CN.md)，安装命令是 `pip install trpc-redis>=0.1.0`

## 运行步骤

在 `examples` 目录下：

```bash
./run_examples.sh list                              # 列出示例
./run_examples.sh                                   # 同 service，用 trpc_python.yaml
./run_examples.sh client                            # 只跑 redis_client.py
./run_examples.sh service                           # 起服务 -> test_service_rpc.py -> 关服务
./run_examples.sh service trpc_python_peer.yaml     # 换成另一份进程模型配置
./run_examples.sh all                               # 先 client，再默认 service
```

解释器按 `$PYTHON`、`../venv/bin/python`、`python3` 的顺序选。服务日志写在 `examples/.server_run.log`。

手动两步，效果和 `service` 相同：

```bash
# 终端 1
python trpc_main.py -c trpc_python.yaml

# 终端 2
python test_service_rpc.py -n 50
```

| 配置 | `global.processmodel` |
| --- | --- |
| `trpc_python.yaml` | 不写（merge，一个事件循环） |
| `trpc_python_merge_threads.yaml` | `single_process`，`handle_num: 2` |
| `trpc_python_separate_thread.yaml` | `single_process`，`io_handle_type: separate`，`io_num: 1`，`handle_num: 2` |
| `trpc_python_separate.yaml` | `multi_process`，`io_handle_type: separate`，`handle_num: 2` |
| `trpc_python_peer.yaml` | `multi_process`，`io_handle_type: peer`，`handle_num: 2` |

## 运行结果

`client` 会按顺序打印每段的标记，例如 `[test_set_and_get]`、`[test_pubsub]`、`[test_complex_operation]`，最后是 `[cleanup_keys] cleanup completed`。脚本里这些 key 会被删掉。

`service` 的客户端汇总类似：

```text
ok=50 errors=0
counts unique=50 min=1 max=50
pids={...}
threads={...}
```

`ok` 等于请求数，且 `counts` 里没有重复，说明每次 `INCR` 都成功了。`pids` 和 `threads` 看出请求落在哪个进程、哪条线程。macOS 上 `SO_REUSEPORT` 不会把连接摊开，peer 模式可能全部进同一个 worker。

## 适用场景

- 先用 `client` 确认本机 Redis 和 `Client` 的命令、pipeline、pubsub、锁、事务
- 再用 `service` 确认插件注册、服务进程里的 `INCR`，以及换 YAML 之后的进程/线程分布

## 需要注意什么

1. **先有 Redis**。两种示例都连 `127.0.0.1:6379`。服务端 YAML 里的 `password` 默认注释掉了，Redis 需要 `AUTH` 时再打开。
2. **先 `import trpc_redis`**。codec 和 transport 在 import 时注册，要在 `create_server` 或 `init_plugins` 之前。
3. **按名字创建的 `Client` 要等配置加载完**。`redis_client.py` 放在 `TrpcConfig.load` 之后。`greeter.py` 写在模块顶层，是因为服务 import 它的时候配置还没加载，真正发命令发生在 `serve` 之后。
4. **连接池绑事件循环**。同一个池只在创建它的那条线程、那个循环上用。`Pipeline` 和 `PubSub` 同样不要交给别的线程。
5. **端口**。五份配置都听 `8000`。一键脚本发现端口已被占用会直接失败。
