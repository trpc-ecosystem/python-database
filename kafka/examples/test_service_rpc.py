# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Send concurrent SayHello calls to the example server and summarize who served them.

Each SayHello produces one record to Kafka from the handler, so this also
exercises the producer on every serving thread and process.

Usage::

    python test_service_rpc.py [--num 50] [--target ip://127.0.0.1:8001]
"""

import argparse
import asyncio
import os
import re
import sys
from collections import Counter

from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.serialization import SerializationType

# add the stub to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "stub")))  # noqa

from trpc_trpc_python_group_helloworld import pb
from trpc_trpc_python_group_helloworld import rpc

_REPLY_RE = re.compile(r"partition=(\d+) offset=(\d+) pid=(\d+) thread=(.+)$")


async def call_many(target: str, num: int) -> int:
    """Send ``num`` concurrent SayHello calls and print a summary.

    Args:
        target: Server address, such as ``ip://127.0.0.1:8001``.
        num: Number of concurrent calls.

    Returns:
        Process exit code: 0 when every call succeeded, 1 otherwise.
    """
    proxy = rpc.GreeterClientProxyImpl()

    async def one(index: int):
        req = pb.HelloRequest()
        req.msg = f"hello-{index}"
        options = (Options().set_target(target).set_protocol('trpc').set_serialization_type(
            SerializationType.PB).set_network('tcp').set_timeout(5000))
        return await proxy.asyncSayHello(TrpcContext(), req, options)

    results = await asyncio.gather(*(one(i) for i in range(num)), return_exceptions=True)
    errors = [r for r in results if isinstance(r, BaseException)]
    produced, pids, threads = set(), Counter(), Counter()
    for result in results:
        if isinstance(result, BaseException):
            continue
        match = _REPLY_RE.search(result.msg)
        if match is None:
            errors.append(ValueError(f"unexpected reply {result.msg!r}"))
            continue
        produced.add((match.group(1), match.group(2)))
        pids[match.group(3)] += 1
        threads[f"{match.group(3)}/{match.group(4)}"] += 1

    print(f"ok={len(produced)} errors={len(errors)}")
    print(f"pids={dict(pids)}")
    print(f"threads={dict(threads)}")
    for err in errors[:5]:
        print(f"error: {err!r}")
    return 0 if not errors and len(produced) == num else 1


def main() -> int:
    """Parse arguments and run the calls."""
    parser = argparse.ArgumentParser(description='Concurrent Greeter client')
    parser.add_argument('--num', '-n', type=int, default=50)
    parser.add_argument('--target', '-t', default='ip://127.0.0.1:8001')
    args = parser.parse_args()
    return asyncio.run(call_many(args.target, args.num))


if __name__ == '__main__':
    sys.exit(main())
