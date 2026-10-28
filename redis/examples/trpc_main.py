# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Example server whose Greeter handler calls Redis.

Run it with any of the ``trpc_python*.yaml`` files in this directory to try
each process/thread model, for example::

    python trpc_main.py -c trpc_python_peer.yaml
"""

import os
import sys
import argparse
from typing import List

from trpc_sdk.log import logger
from trpc_sdk.server import create_server

# add the stub to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "stub")))  # noqa
sys.path.append(os.path.abspath(os.path.dirname(__file__)))  # noqa

# Importing trpc_redis registers the redis codec and transport plugins.
import trpc_redis as _

from trpc_trpc_python_group_helloworld import rpc
from greeter import GreeterServicer

DEFAULT_CONF_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "trpc_python.yaml"))


def parse_args(argv: List[str]) -> str:
    """Resolve the server config path from the command line.

    Args:
        argv: Full argument vector, including the script name at index 0.

    Returns:
        Path of a config file that exists.
    """
    parser = argparse.ArgumentParser(description='tRPC Redis example server')
    parser.add_argument('--conf',
                        '-c',
                        default=DEFAULT_CONF_PATH,
                        help='config file, for example: `python3 trpc_main.py --conf=trpc_python.yaml`')
    args = parser.parse_args(argv[1:])
    if not os.path.exists(args.conf):
        print(f"conf_path: {args.conf} not exists")
        sys.exit(1)
    return args.conf


def serve(conf_path: str):
    """Create the server, register Greeter, and block in ``serve``.

    Args:
        conf_path: Path of the tRPC config file.
    """
    server = create_server(conf_path)
    rpc.register_GreeterServicer_server(server, GreeterServicer(), fix_rpc_name=True)
    server.serve()


def main():
    """Parse arguments and start the server."""
    conf_path = parse_args(sys.argv)
    logger.info("load config %s", conf_path)
    serve(conf_path)


if __name__ == '__main__':
    sys.exit(main())
