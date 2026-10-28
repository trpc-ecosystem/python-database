# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Install entry point for the trpc-redis package."""

import os
import re

from setuptools import setup
from setuptools import find_packages


def _read_version():
    """Read the package version from version.py file.

    Parses the version string from the version.py file using regex pattern matching.

    Returns:
        str: The version string in format 'X.Y.Z' or 'X.Y.Z-{alpha|beta|rc}.N'

    Raises:
        RuntimeError: If version string cannot be found in the file.
    """
    regexp = re.compile(r"^__version__\W*=\W*'(\d\.\d\.\d(-(alpha|beta|rc)\.\d)?)'")
    init_py = os.path.join(os.path.dirname(__file__), "trpc_redis", "version.py")
    with open(init_py, encoding="utf-8") as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        raise RuntimeError(f"Cannot find version in {init_py}")


setup(name="trpc_redis",
      version=_read_version(),
      url="",
      project_urls={
          "Documentation": "",
          "Code": "",
          "Issue tracker": "",
      },
      maintainer="",    
      maintainer_email='',
      description="redis client for trpc framework",
      python_requires=">=3.9",
      install_requires=[
          "trpc-python ; sys_platform != 'win32'",
          "redis>=6.2.0",
      ],
      packages=find_packages(include=("trpc_redis", "trpc_redis.*")))
