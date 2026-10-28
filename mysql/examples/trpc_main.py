# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Example HTTP server whose routes call MySQL through the tRPC client.

Importing ``trpc_mysql`` registers the MySQL codec and transport. Importing
``http_service`` registers the ``/ping`` and ``/demo`` routes. Both must
happen before ``create_server``.
"""

import argparse
import os
import sys
from typing import List

from trpc_sdk.log import logger
from trpc_sdk.server import create_server

sys.path.append(os.path.abspath(os.path.dirname(__file__)))  # noqa

# pylint: disable=unused-import
import trpc_mysql as _
import http_service as _
# pylint: enable=unused-import

DEFAULT_CONF_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "trpc_python.yaml"))


def parse_args(argv: List[str]) -> str:
    """Resolve the server config path from the command line.

    Args:
        argv: Full argument vector, including the script name at index 0.

    Returns:
        Path of a config file that exists. Falls back to the example config
        when the requested path is missing.
    """
    parser = argparse.ArgumentParser(description='tRPC-MySQL example server')
    parser.add_argument('--conf',
                        '-c',
                        default=DEFAULT_CONF_PATH,
                        help='start server with conf, for example: `python3 trpc_main.py --conf=trpc_python.yaml`')
    args = parser.parse_args(argv[1:])
    if os.path.exists(args.conf):
        print(f"conf_path: {args.conf}")
        return args.conf
    print(f"conf_path: {args.conf} not exists")
    if os.path.exists(DEFAULT_CONF_PATH):
        print(f"use default conf path: {DEFAULT_CONF_PATH}")
        return DEFAULT_CONF_PATH
    print(f"default conf path: {DEFAULT_CONF_PATH} not exists, please check the path")
    sys.exit(1)


def serve(conf_path: str):
    """Create the server from config and block in ``serve``.

    Args:
        conf_path: Path of the tRPC config file.
    """
    svr = create_server(conf_path)
    svr.serve()


def main():
    """Parse arguments and start the server."""
    conf_path = parse_args(sys.argv)
    logger.info("load config %s", conf_path)
    serve(conf_path)


if __name__ == '__main__':
    sys.exit(main())
