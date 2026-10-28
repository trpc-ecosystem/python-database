# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Runnable Redis examples for commands, pipelines, pub/sub, locks, and transactions.

``test_redis_without_config`` connects with ``Options.set_target``.
``test_redis_with_config`` loads ``trpc_python.yaml`` and uses the named client.
"""

import os
import time
import asyncio
import uuid
from redis.asyncio.client import Pipeline
from redis.asyncio.client import PubSub
from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.config import TrpcConfig
from trpc_sdk.plugin import init_plugins

from trpc_redis import Client as RedisClient


async def test_pipeline(redis_client: RedisClient):
    """Test pipeline"""
    ctx = TrpcContext()
    pipe: Pipeline = await redis_client.create_pipeline(ctx)
    pipe.set(name="test2", value="test_value")
    pipe.set(name="test3", value="test_valuexxxx")
    await pipe.execute()


async def test_pipe_get(redis_client: RedisClient, key: str):
    """Test pipe get"""
    ctx = TrpcContext()
    reply = await redis_client.execute_command(ctx, "get", key)
    print(f"[test_pipe_get] get success, reply: {reply}")


async def test_set_and_get(redis_client: RedisClient):
    """Test set and get"""
    ctx = TrpcContext()
    reply = await redis_client.get(ctx, "key1")
    print("[test_set_and_get] get: ", reply)
    reply = await redis_client.set(ctx, "key1", "value1")
    print("[test_set_and_get] set: ", reply)
    reply = await redis_client.get(ctx, "key1")
    print("[test_set_and_get] get: ", reply)


async def test_expire(redis_client: RedisClient):
    """Test expire"""
    ctx = TrpcContext()
    reply = await redis_client.set(ctx, "key1", "value1", "ex", 1)
    print("[test_expire] set: ", reply)
    time.sleep(2)
    reply = await redis_client.get(ctx, "key1")
    print("[test_expire] get: ", reply)


async def test_pubsub(redis_client: RedisClient):
    """Subscribe to a channel and print one published message."""
    ctx = TrpcContext()
    channel = "demo_channel"
    pubsub: PubSub = await redis_client.create_pubsub(ctx, channel)
    print("[test_pubsub] pubsub: ", pubsub)

    async def listen_messages():
        async for message in pubsub.listen():
            print("[test_pubsub] message: ", message)
            if message.get("type") == "message":
                print("[test_pubsub] data: ", message["data"])
                return

    listen_task = asyncio.create_task(listen_messages())
    await redis_client.execute_command(ctx, "publish", channel, "hello")
    await listen_task
    await pubsub.unsubscribe(channel)
    print("[test_pubsub] unsubscribed.")


class DistributedLock:
    """Distributed lock implementation using Redis"""

    def __init__(self, redis_client: RedisClient, lock_name: str, timeout: int = 10):
        self.redis_client = redis_client
        self.lock_name = f"lock:{lock_name}"
        self.timeout = timeout
        self.lock_value = str(uuid.uuid4())

    async def acquire(self, ctx, ttl: int = 30):
        """Acquire a distributed lock"""
        # Use SET with NX and EX for atomic lock acquisition
        result = await self.redis_client.set(ctx, self.lock_name, self.lock_value, "ex", ttl, "nx")
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
        result = await self.redis_client.eval(ctx, lua_script, 1, self.lock_name, self.lock_value)
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


async def test_distributed_lock(redis_client: RedisClient):
    """Test distributed lock functionality"""
    print("\n=== Testing Distributed Lock ===")
    ctx = TrpcContext()

    # Test simple lock
    lock = DistributedLock(redis_client, "test_resource")

    # Acquire lock
    acquired = await lock.acquire(ctx)
    print(f"[test_distributed_lock] Lock acquired: {acquired}")

    if acquired:
        # Simulate critical section
        await redis_client.incr(ctx, "lock_counter")
        await asyncio.sleep(0.1)

        # Release lock
        released = await lock.release(ctx)
        print(f"[test_distributed_lock] Lock released: {released}")

    # Test context manager
    async with DistributedLock(redis_client, "test_resource_2"):
        await redis_client.incr(ctx, "lock_counter_2")
        print("[test_distributed_lock] Critical section executed with context manager")


async def acquire_lock_with_retry(redis_client: RedisClient,
                                  lock_name: str,
                                  max_retries: int = 5,
                                  retry_delay: float = 1):
    """Acquire lock with retry logic"""
    for attempt in range(max_retries):
        lock = DistributedLock(redis_client, lock_name)
        try:
            ctx = TrpcContext()
            if await lock.acquire(ctx):
                return lock
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"[acquire_lock_with_retry] Lock acquisition attempt {attempt + 1} failed: {e}")

        if attempt < max_retries - 1:
            await asyncio.sleep(retry_delay)

    raise TimeoutError(f"Failed to acquire lock after {max_retries} attempts")


async def test_lock_with_retry(redis_client: RedisClient):
    """Test lock with retry logic"""
    print("\n=== Testing Lock with Retry ===")
    ctx = TrpcContext()

    try:
        lock = await acquire_lock_with_retry(redis_client, "retry_test", max_retries=3, retry_delay=0.1)
        try:
            # Perform critical operation
            await redis_client.incr(ctx, "retry_counter")
            print("[test_lock_with_retry] Critical operation completed")
        finally:
            await lock.release(ctx)
    except TimeoutError as e:
        print(f"[test_lock_with_retry] {e}")


async def test_basic_transaction(redis_client: RedisClient):
    """Test basic transaction functionality"""
    print("\n=== Testing Basic Transaction ===")
    ctx = TrpcContext()

    # Initialize test data
    await redis_client.set(ctx, "account:1", "1000")
    await redis_client.set(ctx, "account:2", "500")

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

        print(f"[test_basic_transaction] Before transfer - Account1: {balance1}, Account2: {balance2}")

        # Start transaction
        pipe.multi()

        # Perform operations
        pipe.set("account:1", balance1 - 100)
        pipe.set("account:2", balance2 + 100)

        # Execute transaction
        results = await pipe.execute()
        print(f"[test_basic_transaction] Transaction successful: {results}")

        # Verify results
        new_balance1 = await redis_client.get(ctx, "account:1")
        new_balance2 = await redis_client.get(ctx, "account:2")
        print(f"[test_basic_transaction] After transfer - Account1: {new_balance1}, Account2: {new_balance2}")

    except Exception as e:  # pylint: disable=broad-exception-caught
        # Transaction failed, keys were modified
        print(f"[test_basic_transaction] Transaction failed: {e}")
        await pipe.reset()


async def test_optimistic_transaction(redis_client: RedisClient):
    """Test optimistic locking with version"""
    print("\n=== Testing Optimistic Transaction ===")
    ctx = TrpcContext()

    # Initialize user data with version
    await redis_client.hset(ctx, "user:123", "name", "John")
    await redis_client.hset(ctx, "user:123", "version", "1")

    # Get current data with version
    user_data = await redis_client.hgetall(ctx, "user:123")
    current_version = int(user_data.get("version", "0"))

    print(f"[test_optimistic_transaction] Current version: {current_version}")

    # Start transaction
    pipe = await redis_client.create_pipeline(ctx)
    await pipe.watch("user:123")

    try:
        # Check version hasn't changed
        pipe.multi()
        pipe.hset("user:123", "name", "new_name")
        pipe.hset("user:123", "version", current_version + 1)

        results = await pipe.execute()
        print(f"[test_optimistic_transaction] Update successful: {results}")

        # Verify update
        updated_data = await redis_client.hgetall(ctx, "user:123")
        print(f"[test_optimistic_transaction] Updated data: {updated_data}")

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"[test_optimistic_transaction] Optimistic lock failed: {e}")
        await pipe.reset()


async def test_conditional_transaction(redis_client: RedisClient):
    """Test conditional transaction"""
    print("\n=== Testing Conditional Transaction ===")
    ctx = TrpcContext()

    # Initialize product stock
    await redis_client.set(ctx, "product:stock", "10")
    await redis_client.set(ctx, "product:sold", "0")

    # Check if we can perform the operation
    current_stock = await redis_client.get(ctx, "product:stock")
    current_stock = int(current_stock) if current_stock else 0

    print(f"[test_conditional_transaction] Current stock: {current_stock}")

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
        print(f"[test_conditional_transaction] Purchase successful: {results}")

        # Verify results
        new_stock = await redis_client.get(ctx, "product:stock")
        total_sold = await redis_client.get(ctx, "product:sold")
        print(f"[test_conditional_transaction] New stock: {new_stock}, Total sold: {total_sold}")

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"[test_conditional_transaction] Transaction failed: {e}")
        await pipe.reset()


async def test_batch_transaction(redis_client: RedisClient):
    """Test batch operations with transaction"""
    print("\n=== Testing Batch Transaction ===")
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
        print(f"[test_batch_transaction] Batch transaction results: {results}")

        # Verify results
        value1 = await redis_client.get(ctx, "key1")
        value2 = await redis_client.get(ctx, "key2")
        counter = await redis_client.get(ctx, "counter")
        list_items = await redis_client.lrange(ctx, "list", 0, -1)

        print(
            f"[test_batch_transaction] Verification - key1: {value1}, key2: {value2}, counter: {counter}, list: {list_items}"
        )

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"[test_batch_transaction] Batch transaction failed: {e}")
        await pipe.reset()


async def test_complex_operation(redis_client: RedisClient):
    """Combine distributed lock with transaction for complex operations"""
    print("\n=== Testing Complex Operation (Lock + Transaction) ===")

    # Acquire distributed lock
    lock = DistributedLock(redis_client, "complex_operation")
    ctx = TrpcContext()

    try:
        if not await lock.acquire(ctx):
            raise TimeoutError("Failed to acquire lock")

        print("[test_complex_operation] Lock acquired, starting transaction")

        # Start transaction within lock
        pipe = await redis_client.create_pipeline(ctx)
        await pipe.watch("shared_resource")

        try:
            # Get current state
            current_value = await redis_client.get(ctx, "shared_resource")
            current_value = int(current_value) if current_value else 0

            print(f"[test_complex_operation] Current shared_resource value: {current_value}")

            # Perform complex operation
            pipe.multi()
            pipe.set("shared_resource", current_value + 1)
            pipe.set("last_updated", time.time())
            pipe.incr("operation_count")

            results = await pipe.execute()
            print(f"[test_complex_operation] Complex operation completed: {results}")

            # Verify results
            new_value = await redis_client.get(ctx, "shared_resource")
            last_updated = await redis_client.get(ctx, "last_updated")
            op_count = await redis_client.get(ctx, "operation_count")

            print(
                f"[test_complex_operation] Verification - shared_resource: {new_value}, last_updated: {last_updated}, operation_count: {op_count}"
            )

        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"[test_complex_operation] Transaction failed: {e}")
            await pipe.reset()

    finally:
        # Always release the lock
        await lock.release(ctx)
        print("[test_complex_operation] Lock released")


async def cleanup_keys(redis_client: RedisClient):
    """Delete keys created by the examples."""
    ctx = TrpcContext()
    keys = [
        "key1", "test2", "test3", "lock_counter", "lock_counter_2", "retry_counter", "account:1", "account:2",
        "user:123", "product:stock", "product:sold", "key2", "counter", "list", "shared_resource", "last_updated",
        "operation_count"
    ]
    for key in keys:
        await redis_client.delete(ctx, key)
    print("[cleanup_keys] cleanup completed")


async def test_redis_without_config():
    """Redis main without config"""
    # PASSWORD = "test"
    opts = [Options().set_target("ip://127.0.0.1:6379")]
    redis_client = RedisClient("redis", opts)
    await test_set_and_get(redis_client)
    await test_expire(redis_client)
    await test_pipeline(redis_client)
    await test_pipe_get(redis_client, "test2")
    await test_pipe_get(redis_client, "test3")
    await test_pubsub(redis_client)

    # Test advanced operations
    await test_distributed_lock(redis_client)
    await test_lock_with_retry(redis_client)
    await test_basic_transaction(redis_client)
    await test_optimistic_transaction(redis_client)
    await test_conditional_transaction(redis_client)
    await test_batch_transaction(redis_client)
    await test_complex_operation(redis_client)

    await cleanup_keys(redis_client)


async def test_redis_with_config():
    """Redis main with config"""
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "trpc_python.yaml"))
    TrpcConfig.load(config_path, "utf-8")
    init_plugins()
    redis_client = RedisClient("trpc.redis.test_example")
    await test_set_and_get(redis_client)
    await test_expire(redis_client)
    await test_pipeline(redis_client)
    await test_pipe_get(redis_client, "test2")
    await test_pipe_get(redis_client, "test3")
    await test_pubsub(redis_client)

    # Test advanced operations
    await test_distributed_lock(redis_client)
    await test_lock_with_retry(redis_client)
    await test_basic_transaction(redis_client)
    await test_optimistic_transaction(redis_client)
    await test_conditional_transaction(redis_client)
    await test_batch_transaction(redis_client)
    await test_complex_operation(redis_client)

    await cleanup_keys(redis_client)


if __name__ == "__main__":
    asyncio.run(test_redis_with_config())
