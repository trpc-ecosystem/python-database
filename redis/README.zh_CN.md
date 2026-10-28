# tRPC Redis 插件

[English](README.md)

tRPC-Python 框架的 Redis 客户端实现，提供普通命令、pipeline 和 pub/sub 等 Redis 操作。

## 功能特性

- **命令模式**：直接执行 Redis 命令
- **Pipeline 模式**：批量执行多个 Redis 操作
- **Pub/Sub 模式**：订阅 Redis 频道并处理消息
- **连接池**：高效管理连接
- **灵活配置**：支持通过 tRPC 配置文件或直接传入 Options 配置
- **Async/Await**：完整的异步支持，适合高性能应用
- **分布式锁**：内置分布式锁的使用方式
- **事务**：支持 Redis 事务（MULTI/EXEC + WATCH）

## 安装

依赖：
- trpc-python
- redis>=6.2.0
- Python >=3.9（redis 要求，推荐 3.10）

```bash
pip install trpc-redis>=0.1.0
```

使用 [uv](https://docs.astral.sh/uv/) 开发时，在本包目录下执行如下命令。需要确保已配置的包索引中能解析到 `trpc-python`：

```bash
uv sync
```

## 快速开始

### 基本用法

```python
import asyncio
from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_redis import Client as RedisClient

# 初始化客户端
target = "ip://127.0.0.1:6379"
opts = [Options().set_target(target).set_password("xxx")]
redis_client = RedisClient("redis", opts)

async def main():
    ctx = TrpcContext()
    
    # 写入 key
    await redis_client.set(ctx, "key1", "value1")
    
    # 读取 key
    value = await redis_client.get(ctx, "key1")
    print(f"Value: {value}")
    
    # 删除 key
    result = await redis_client.delete(ctx, "key1")
    print(f"Deleted: {result}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Pipeline 操作

```python
async def test_pipeline():
    ctx = TrpcContext()
    pipe = await redis_client.create_pipeline(ctx)
    
    # 向 pipeline 中添加多个操作
    pipe.set(name="key1", value="value1")
    pipe.set(name="key2", value="value2")
    pipe.get("key1")
    
    # 一次性执行所有操作
    results = await pipe.execute()
    print(f"Pipeline results: {results}")
```

### Pub/Sub 操作

```python
async def test_pubsub():
    ctx = TrpcContext()
    
    # 订阅频道
    pubsub = await redis_client.create_pubsub(ctx, ["test_channel"])
    
    # 监听消息
    async for message in pubsub.listen():
        if message["type"] == "message":
            print(f"Received: {message['data']}")
            break  # 收到第一条消息后退出
```

### 动态命令方法

客户端支持以方法调用的形式执行任意 Redis 命令：

```python
async def test_dynamic_commands():
    ctx = TrpcContext()
    
    # 这些方法是动态生成的
    await redis_client.hset(ctx, "hash_key", "field", "value")
    await redis_client.lpush(ctx, "list_key", "item1", "item2")
    await redis_client.zadd(ctx, "sorted_set", 1, "member1")
    
    # 读取结果
    hash_value = await redis_client.hget(ctx, "hash_key", "field")
    list_length = await redis_client.llen(ctx, "list_key")
```

## 高级操作

### 分布式锁

#### 简单的锁实现

```python
import time
import uuid
from trpc_sdk.exceptions import ClientInvokeError

class DistributedLock:
    def __init__(self, redis_client, lock_name, timeout=10):
        self.redis_client = redis_client
        self.lock_name = f"lock:{lock_name}"
        self.timeout = timeout
        self.lock_value = str(uuid.uuid4())
    
    async def acquire(self, ctx, ttl=30):
        """获取分布式锁"""
        # 使用带 NX 和 EX 的 SET 原子地加锁
        result = await self.redis_client.set(
            ctx, self.lock_name, self.lock_value, 
            ex=ttl, nx=True
        )
        return result is True
    
    async def release(self, ctx):
        """用 Lua 脚本原子地释放锁"""
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        result = await self.redis_client.eval(
            ctx, lua_script, 1, self.lock_name, self.lock_value
        )
        return result == 1
    
    async def __aenter__(self):
        ctx = TrpcContext()
        acquired = await self.acquire(ctx)
        if not acquired:
            raise TimeoutError(f"Failed to acquire lock: {self.lock_name}")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        ctx = TrpcContext()
        await self.release(ctx)

# 使用示例
async def critical_section():
    lock = DistributedLock(redis_client, "my_resource")
    async with lock:
        # 这段代码受分布式锁保护
        await redis_client.incr(ctx, "counter")
        await asyncio.sleep(1)  # 模拟耗时操作
```

#### 带重试的加锁

```python
async def acquire_lock_with_retry(redis_client, lock_name, max_retries=5, retry_delay=1):
    """带重试逻辑的加锁"""
    for attempt in range(max_retries):
        lock = DistributedLock(redis_client, lock_name)
        try:
            ctx = TrpcContext()
            if await lock.acquire(ctx):
                return lock
        except Exception as e:
            print(f"Lock acquisition attempt {attempt + 1} failed: {e}")
        
        if attempt < max_retries - 1:
            await asyncio.sleep(retry_delay)
    
    raise TimeoutError(f"Failed to acquire lock after {max_retries} attempts")

# 使用
async def safe_operation():
    lock = await acquire_lock_with_retry(redis_client, "shared_resource")
    try:
        # 执行关键操作
        ctx = TrpcContext()
        await redis_client.incr(ctx, "shared_counter")
    finally:
        ctx = TrpcContext()
        await lock.release(ctx)
```

### 事务

#### 基本事务

```python
async def basic_transaction():
    ctx = TrpcContext()
    
    # 开始事务
    pipe = await redis_client.create_pipeline(ctx)
    
    # WATCH 相关 key，实现乐观锁
    await pipe.watch("account:1", "account:2")
    
    try:
        # 读取当前值
        balance1 = await redis_client.get(ctx, "account:1")
        balance2 = await redis_client.get(ctx, "account:2")
        
        # 解析余额（假设以字符串存储）
        balance1 = int(balance1) if balance1 else 0
        balance2 = int(balance2) if balance2 else 0
        
        # 开启事务
        pipe.multi()
        
        # 执行操作
        pipe.set("account:1", balance1 - 100)
        pipe.set("account:2", balance2 + 100)
        
        # 提交事务
        results = await pipe.execute()
        print(f"Transaction successful: {results}")
        
    except Exception as e:
        # 事务失败，key 已被其他客户端修改
        print(f"Transaction failed: {e}")
        await pipe.reset()
```

#### 基于版本号的乐观锁

```python
async def optimistic_transaction():
    ctx = TrpcContext()
    
    # 读取当前数据及版本号
    user_data = await redis_client.hgetall(ctx, "user:123")
    current_version = int(user_data.get("version", "0"))
    
    # 开始事务
    pipe = await redis_client.create_pipeline(ctx)
    await pipe.watch("user:123")
    
    try:
        # 校验版本号未变化
        pipe.multi()
        pipe.hset("user:123", "name", "new_name")
        pipe.hset("user:123", "version", current_version + 1)
        
        results = await pipe.execute()
        print(f"Update successful: {results}")
        
    except Exception as e:
        print(f"Optimistic lock failed: {e}")
        await pipe.reset()
```

#### 条件事务

```python
async def conditional_transaction():
    ctx = TrpcContext()
    
    # 检查是否满足操作条件
    current_stock = await redis_client.get(ctx, "product:stock")
    current_stock = int(current_stock) if current_stock else 0
    
    if current_stock < 1:
        raise ValueError("Insufficient stock")
    
    # 开始事务
    pipe = await redis_client.create_pipeline(ctx)
    await pipe.watch("product:stock")
    
    try:
        pipe.multi()
        pipe.decr("product:stock")
        pipe.incr("product:sold")
        
        results = await pipe.execute()
        print(f"Purchase successful: {results}")
        
    except Exception as e:
        print(f"Transaction failed: {e}")
        await pipe.reset()
```

#### 在事务中批量操作

```python
async def batch_transaction():
    ctx = TrpcContext()
    
    # 准备批量操作
    operations = [
        ("set", "key1", "value1"),
        ("set", "key2", "value2"),
        ("incr", "counter"),
        ("lpush", "list", "item1"),
        ("lpush", "list", "item2"),
    ]
    
    # 在事务中执行
    pipe = await redis_client.create_pipeline(ctx)
    
    try:
        pipe.multi()
        
        for op, *args in operations:
            getattr(pipe, op)(*args)
        
        results = await pipe.execute()
        print(f"Batch transaction results: {results}")
        
    except Exception as e:
        print(f"Batch transaction failed: {e}")
        await pipe.reset()
```

### 锁与事务结合

```python
async def complex_operation():
    """在复杂操作中同时使用分布式锁和事务"""
    
    # 获取分布式锁
    lock = DistributedLock(redis_client, "complex_operation")
    ctx = TrpcContext()
    
    try:
        if not await lock.acquire(ctx):
            raise TimeoutError("Failed to acquire lock")
        
        # 在锁内开启事务
        pipe = await redis_client.create_pipeline(ctx)
        await pipe.watch("shared_resource")
        
        try:
            # 读取当前状态
            current_value = await redis_client.get(ctx, "shared_resource")
            current_value = int(current_value) if current_value else 0
            
            # 执行复杂操作
            pipe.multi()
            pipe.set("shared_resource", current_value + 1)
            pipe.set("last_updated", time.time())
            pipe.incr("operation_count")
            
            results = await pipe.execute()
            print(f"Complex operation completed: {results}")
            
        except Exception as e:
            print(f"Transaction failed: {e}")
            await pipe.reset()
            
    finally:
        # 始终释放锁
        await lock.release(ctx)
```

## 配置

### 直接配置

```python
from trpc_sdk.client import Options
from trpc_redis import Client as RedisClient

opts = [
    Options()
    .set_target("ip://127.0.0.1:6379")
    .set_password("your_password")
    .set_timeout(5000)  # 5 秒
]

redis_client = RedisClient("redis", opts)
```

### 配置文件

创建 YAML 配置文件：

```yaml
client:
  service:
    - name: trpc.redis.test_example
      namespace: ${namespace}
      target: "ip://127.0.0.1:6379"
      password: "test"
      timeout: 5000
      redis:
        max_connections: 1024
        decode_responses: false
```

`redis:` 下的字段会作为额外参数传给 `redis.asyncio.ConnectionPool`。然后初始化客户端：

```python
from trpc_redis import Client as RedisClient

# 客户端会自动加载配置
redis_client = RedisClient("trpc.redis.test_example")
```

## 高级用法

### 错误处理

```python
from trpc_sdk.exceptions import ClientTimeoutError, ClientInvokeError

async def safe_redis_operation():
    ctx = TrpcContext()
    try:
        result = await redis_client.set(ctx, "key", "value")
        return result
    except ClientTimeoutError:
        print("Operation timed out")
    except ClientInvokeError as e:
        print(f"Redis operation failed: {e}")
```

### 连接池管理

连接池按 Redis 的 host、port 和 password 区分。同一线程、同一事件循环中指向同一服务器的多个客户端共用一个连接池：

```python
client1 = RedisClient("redis", opts)
client2 = RedisClient("redis", opts)
# 在同一个事件循环上运行时，client1 和 client2 共用一个连接池。
```

## 项目结构

```
trpc-redis/
├── trpc_redis/
│   ├── __init__.py            # 公开符号的再导出
│   ├── version.py             # 包版本，由 hatch 和 setup.py 读取
│   ├── _client.py             # 客户端实现
│   ├── _transport.py          # 执行 Redis 操作的传输层
│   ├── _codec.py              # 消息编解码
│   ├── _constants.py          # 配置常量和 RequestMode
│   └── _types.py              # Request / Response 类型
├── examples/
│   ├── README.md              # 示例怎么跑
│   ├── run_examples.sh        # 起服务、跑客户端、停服务
│   ├── redis_client.py        # 独立使用示例
│   ├── trpc_main.py           # 示例服务，Greeter 中调用 Redis
│   ├── greeter.py             # Greeter 实现
│   ├── test_service_rpc.py    # 并发调用 Greeter 的客户端
│   ├── trpc_python*.yaml      # 每种进程/线程模式一份配置
│   └── stub/                  # 生成的 Greeter 桩代码
├── tests/
│   ├── test_codec.py         # Codec 测试
│   ├── test_redis_client.py  # 客户端测试
│   └── test_transport.py     # Transport 测试
├── pyproject.toml            # 包元数据和 uv 开发依赖
├── setup.py                  # 旧版 setuptools 入口
├── README.md
└── README.zh_CN.md
```

## API 参考

### Client 类

#### `__init__(name: str, opts: Optional[List] = None)`
初始化 Redis 客户端。

- `name`：Redis 实例的服务名
- `opts`：可选的配置项列表

#### `execute_command(ctx: TrpcContext, cmd: str, *args) -> bytes`
执行一条 Redis 命令。

- `ctx`：tRPC 上下文
- `cmd`：Redis 命令名
- `*args`：命令参数

#### `create_pipeline(ctx: Context) -> Pipeline`
创建用于批量操作的 Redis pipeline。

#### `create_pubsub(ctx: TrpcContext, *channels) -> PubSub`
创建 Redis pub/sub 订阅。

### 请求模式

- `RequestMode.COMMAND`：执行普通命令
- `RequestMode.PIPELINE`：pipeline 操作
- `RequestMode.PUBSUB`：发布/订阅操作

## 测试

运行测试：

```bash
pytest tests/
# 或使用 uv
uv run pytest tests/
```

查看覆盖率：

```bash
pytest --cov=trpc_redis tests/
```

## 示例

可运行示例和一键脚本见 [examples/README.md](examples/README.md)：

```bash
cd examples
./run_examples.sh list
./run_examples.sh client
./run_examples.sh service
./run_examples.sh all
```

其中包括：

- Redis 基本操作
- Pipeline 用法
- Pub/Sub 模式
- 错误处理
- 配置示例
- 分布式锁
- 事务用法
- 每种进程/线程模式下的 Greeter 服务

## 贡献

1. Fork 本仓库
2. 创建特性分支
3. 提交修改
4. 为新功能补充测试
5. 提交 Pull Request

## 许可证

Copyright @ 2025 Tencent.com
