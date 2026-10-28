# tRPC Redis Plugin

[中文](README.zh_CN.md)

A Redis client implementation for the tRPC Python framework, providing seamless Redis operations including commands, pipelines, and pub/sub functionality.

## Features

- **Command Mode**: Execute Redis commands directly
- **Pipeline Mode**: Batch multiple Redis operations
- **Pub/Sub Mode**: Subscribe to Redis channels and handle messages
- **Connection Pooling**: Efficient connection management
- **Configuration Support**: Flexible configuration via tRPC config or direct options
- **Async/Await Support**: Full async support for high-performance applications
- **Distributed Locks**: Built-in support for distributed locking
- **Transactions**: ACID-compliant transaction support

## Installation

Requirements:
- trpc-python (the `trpc_sdk` package)
- redis>=6.2.0
- Python >=3.9 (required by redis, recommended 3.10)

```bash
pip install trpc-redis>=0.1.0
```

For development with [uv](https://docs.astral.sh/uv/), run this in the package
directory. `trpc-python` must be resolvable from your configured package index:

```bash
uv sync
```

## Quick Start

### Basic Usage

```python
import asyncio
from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_redis import Client as RedisClient

# Initialize client
target = "ip://127.0.0.1:6379"
opts = [Options().set_target(target).set_password("xxx")]
redis_client = RedisClient("redis", opts)

async def main():
    ctx = TrpcContext()
    
    # Set a key
    await redis_client.set(ctx, "key1", "value1")
    
    # Get a key
    value = await redis_client.get(ctx, "key1")
    print(f"Value: {value}")
    
    # Delete a key
    result = await redis_client.delete(ctx, "key1")
    print(f"Deleted: {result}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Pipeline Operations

```python
async def test_pipeline():
    ctx = TrpcContext()
    pipe = await redis_client.create_pipeline(ctx)
    
    # Add multiple operations to pipeline
    pipe.set(name="key1", value="value1")
    pipe.set(name="key2", value="value2")
    pipe.get("key1")
    
    # Execute all operations
    results = await pipe.execute()
    print(f"Pipeline results: {results}")
```

### Pub/Sub Operations

```python
async def test_pubsub():
    ctx = TrpcContext()
    
    # Subscribe to channels
    pubsub = await redis_client.create_pubsub(ctx, ["test_channel"])
    
    # Listen for messages
    async for message in pubsub.listen():
        if message["type"] == "message":
            print(f"Received: {message['data']}")
            break  # Exit after first message
```

### Dynamic Command Methods

The client supports dynamic method calls for any Redis command:

```python
async def test_dynamic_commands():
    ctx = TrpcContext()
    
    # These methods are dynamically created
    await redis_client.hset(ctx, "hash_key", "field", "value")
    await redis_client.lpush(ctx, "list_key", "item1", "item2")
    await redis_client.zadd(ctx, "sorted_set", 1, "member1")
    
    # Get results
    hash_value = await redis_client.hget(ctx, "hash_key", "field")
    list_length = await redis_client.llen(ctx, "list_key")
```

## Advanced Operations

### Distributed Locks

#### Simple Lock Implementation

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
        """Acquire a distributed lock"""
        # Use SET with NX and EX for atomic lock acquisition
        result = await self.redis_client.set(
            ctx, self.lock_name, self.lock_value, 
            ex=ttl, nx=True
        )
        return result is True
    
    async def release(self, ctx):
        """Release the lock using Lua script for atomicity"""
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

# Usage example
async def critical_section():
    lock = DistributedLock(redis_client, "my_resource")
    async with lock:
        # This code is protected by the distributed lock
        await redis_client.incr(ctx, "counter")
        await asyncio.sleep(1)  # Simulate work
```

#### Lock with Retry Logic

```python
async def acquire_lock_with_retry(redis_client, lock_name, max_retries=5, retry_delay=1):
    """Acquire lock with retry logic"""
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

# Usage
async def safe_operation():
    lock = await acquire_lock_with_retry(redis_client, "shared_resource")
    try:
        # Perform critical operation
        ctx = TrpcContext()
        await redis_client.incr(ctx, "shared_counter")
    finally:
        ctx = TrpcContext()
        await lock.release(ctx)
```

### Transactions

#### Basic Transaction

```python
async def basic_transaction():
    ctx = TrpcContext()
    
    # Start a transaction
    pipe = await redis_client.create_pipeline(ctx)
    
    # Watch keys for optimistic locking
    await pipe.watch("account:1", "account:2")
    
    try:
        # Get current values
        balance1 = await redis_client.get(ctx, "account:1")
        balance2 = await redis_client.get(ctx, "account:2")
        
        # Parse balances (assuming they're stored as strings)
        balance1 = int(balance1) if balance1 else 0
        balance2 = int(balance2) if balance2 else 0
        
        # Start transaction
        pipe.multi()
        
        # Perform operations
        pipe.set("account:1", balance1 - 100)
        pipe.set("account:2", balance2 + 100)
        
        # Execute transaction
        results = await pipe.execute()
        print(f"Transaction successful: {results}")
        
    except Exception as e:
        # Transaction failed, keys were modified
        print(f"Transaction failed: {e}")
        await pipe.reset()
```

#### Optimistic Locking with Version

```python
async def optimistic_transaction():
    ctx = TrpcContext()
    
    # Get current data with version
    user_data = await redis_client.hgetall(ctx, "user:123")
    current_version = int(user_data.get("version", "0"))
    
    # Start transaction
    pipe = await redis_client.create_pipeline(ctx)
    await pipe.watch("user:123")
    
    try:
        # Check version hasn't changed
        pipe.multi()
        pipe.hset("user:123", "name", "new_name")
        pipe.hset("user:123", "version", current_version + 1)
        
        results = await pipe.execute()
        print(f"Update successful: {results}")
        
    except Exception as e:
        print(f"Optimistic lock failed: {e}")
        await pipe.reset()
```

#### Conditional Transaction

```python
async def conditional_transaction():
    ctx = TrpcContext()
    
    # Check if we can perform the operation
    current_stock = await redis_client.get(ctx, "product:stock")
    current_stock = int(current_stock) if current_stock else 0
    
    if current_stock < 1:
        raise ValueError("Insufficient stock")
    
    # Start transaction
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

#### Batch Operations with Transaction

```python
async def batch_transaction():
    ctx = TrpcContext()
    
    # Prepare batch operations
    operations = [
        ("set", "key1", "value1"),
        ("set", "key2", "value2"),
        ("incr", "counter"),
        ("lpush", "list", "item1"),
        ("lpush", "list", "item2"),
    ]
    
    # Execute in transaction
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

### Lock and Transaction Combined

```python
async def complex_operation():
    """Combine distributed lock with transaction for complex operations"""
    
    # Acquire distributed lock
    lock = DistributedLock(redis_client, "complex_operation")
    ctx = TrpcContext()
    
    try:
        if not await lock.acquire(ctx):
            raise TimeoutError("Failed to acquire lock")
        
        # Start transaction within lock
        pipe = await redis_client.create_pipeline(ctx)
        await pipe.watch("shared_resource")
        
        try:
            # Get current state
            current_value = await redis_client.get(ctx, "shared_resource")
            current_value = int(current_value) if current_value else 0
            
            # Perform complex operation
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
        # Always release the lock
        await lock.release(ctx)
```

## Configuration

### Direct Configuration

```python
from trpc_sdk.client import Options
from trpc_redis import Client as RedisClient

opts = [
    Options()
    .set_target("ip://127.0.0.1:6379")
    .set_password("your_password")
    .set_timeout(5000)  # 5 seconds
]

redis_client = RedisClient("redis", opts)
```

### Configuration File

Create a YAML configuration file:

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

Then initialize the client:

```python
from trpc_redis import Client as RedisClient

# Client will automatically load configuration
redis_client = RedisClient("trpc.redis.test_example")
```

## Advanced Usage

### Error Handling

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

### Connection Pool Management

Connection pools are keyed by Redis host, port, and password. Clients in the
same thread and event loop that point at the same server share one pool:

```python
client1 = RedisClient("redis", opts)
client2 = RedisClient("redis", opts)
# client1 and client2 share a pool while they run on the same event loop.
```

## Process and Thread Models

A `redis.asyncio` pool is bound to the event loop it first runs on. The
transport therefore keeps one cache per process, per thread, and per event
loop, and works in every `global.processmodel` setting of tRPC-Python:

| Mode | Where Redis calls run |
| --- | --- |
| merge, one loop (default) | The main thread of the single process. |
| merge, `handle_num > 1` | Each serving thread, on its own loop and pools. |
| merge, `io_handle_type: separate` | Each handler thread, on its own loop and pools. |
| `multi_process` + `separate` | Each rpc task process. The I/O process does not call Redis. |
| `multi_process` + `peer` | Each peer worker process. |

Notes:

- A forked child starts with an empty cache, so building a `RedisClient` at
  import time, before the server forks, is fine.
- A `Pipeline` or `PubSub` object belongs to the loop that created it. Use it
  on that loop only; do not hand it to another thread.
- The YAML `redis:` block is copied for each pool and is never modified.
- Closing an event loop drops its pools from the cache. Pools are not
  disconnected on shutdown; process exit closes their sockets.

`examples/` has one config per mode. With Redis on `127.0.0.1:6379`:

```bash
cd examples
./run_examples.sh service trpc_python_peer.yaml   # or any config below
```

The same steps by hand, in two terminals:

```bash
cd examples
python trpc_main.py -c trpc_python_peer.yaml
python test_service_rpc.py -n 50               # prints the pid/thread spread
```

See [examples/README.md](examples/README.md) for the client script and every config.

| Config | `global.processmodel` |
| --- | --- |
| `trpc_python.yaml` | none (merge, one loop) |
| `trpc_python_merge_threads.yaml` | `single_process`, `handle_num: 2` |
| `trpc_python_separate_thread.yaml` | `single_process`, `io_handle_type: separate`, `io_num: 1`, `handle_num: 2` |
| `trpc_python_separate.yaml` | `multi_process`, `io_handle_type: separate`, `handle_num: 2` |
| `trpc_python_peer.yaml` | `multi_process`, `io_handle_type: peer`, `handle_num: 2` |

On macOS, `SO_REUSEPORT` does not balance connections, so in peer mode one
worker may receive all of them.

## Project Structure

```
trpc-redis/
├── trpc_redis/
│   ├── __init__.py            # Public re-exports
│   ├── version.py             # Package version, read by hatch and setup.py
│   ├── _client.py             # Client implementation
│   ├── _transport.py          # Transport for Redis operations
│   ├── _codec.py              # Message encoding/decoding
│   ├── _constants.py          # Configuration constants and RequestMode
│   └── _types.py              # Request / Response types
├── examples/
│   ├── README.md              # How to run the examples
│   ├── run_examples.sh        # Start the server, run a client, stop the server
│   ├── redis_client.py        # Standalone usage examples
│   ├── trpc_main.py           # Example server whose Greeter calls Redis
│   ├── greeter.py             # Greeter implementation
│   ├── test_service_rpc.py    # Concurrent Greeter client
│   ├── trpc_python*.yaml      # One config per process/thread model
│   └── stub/                  # Generated Greeter stub
├── tests/
│   ├── test_codec.py         # Codec tests
│   ├── test_redis_client.py  # Client tests
│   └── test_transport.py     # Transport tests
├── pyproject.toml            # Package metadata and uv dev dependencies
├── setup.py                  # Legacy setuptools entry point
├── README.md
└── README.zh_CN.md
```

## API Reference

### Client Class

#### `__init__(name: str, opts: Optional[List] = None)`
Initialize a Redis client.

- `name`: Service name for the Redis instance
- `opts`: Optional list of configuration options

#### `execute_command(ctx: TrpcContext, cmd: str, *args) -> bytes`
Execute a Redis command.

- `ctx`: tRPC context
- `cmd`: Redis command name
- `*args`: Command arguments

#### `create_pipeline(ctx: Context) -> Pipeline`
Create a Redis pipeline for batch operations.

#### `create_pubsub(ctx: TrpcContext, *channels) -> PubSub`
Create a Redis pub/sub subscription.

### Request Modes

- `RequestMode.COMMAND`: Standard command execution
- `RequestMode.PIPELINE`: Pipeline operations
- `RequestMode.PUBSUB`: Publish/subscribe operations

## Testing

Run the test suite:

```bash
pytest tests/
# or, with uv
uv run pytest tests/
```

For coverage:

```bash
pytest --cov=trpc_redis tests/
```

## Examples

Runnable examples and the one-shot script are described in [examples/README.md](examples/README.md):

```bash
cd examples
./run_examples.sh list
./run_examples.sh client
./run_examples.sh service
./run_examples.sh all
```

Covered there:

- Basic Redis operations
- Pipeline usage
- Pub/Sub patterns
- Error handling
- Configuration examples
- Distributed locks
- Transaction patterns
- A Greeter server for each process/thread model (see [Process and Thread Models](#process-and-thread-models))

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

Copyright @ 2025 Tencent.com
