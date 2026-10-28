# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Install entry point for the trpc-kafka package."""

import os
import re

from setuptools import setup, find_packages


def read_version():
    """Read ``__version__`` from the package init module.

    Returns:
        Version string declared in ``trpc_kafka/__init__.py``.

    Raises:
        RuntimeError: The version assignment is missing or not a supported form.
    """
    regexp = re.compile(r"^__version__\W*=\W*'(\d\.\d\.\d(-(alpha|beta|rc)\.\d)?)'")
    init_py = os.path.join(os.path.dirname(__file__), "trpc_kafka", "__init__.py")
    with open(init_py) as file:
        for line in file:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        raise RuntimeError("Cannot find version in {}".format(init_py))


setup(
    name="trpc_kafka",
    version=read_version(),
    url="",
    project_urls={
        "Documentation": "",
        "Code": "",
        "Issue tracker": "",
    },
    maintainer="",
    description="kafka plugin for trpc framework",
    python_requires=">=3.6",
    install_requires=[
        "trpc-python",
        "aiokafka",
    ],
    packages=find_packages(include=("trpc_kafka", "trpc_kafka.*"))
)
