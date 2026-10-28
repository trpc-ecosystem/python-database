# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""
Redis request and response types for tRPC framework.

The client stores a :class:`Request` in ``msg.client_req_head`` and the transport replies with a
:class:`Response` in ``msg.client_rsp_head``.
"""
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from ._constants import RequestMode


@dataclass
class Request:
    """Mode and command payload for one Redis call.

    ``args`` and ``kwargs`` default to an empty tuple and dict. A single list
    argument is unpacked, so ``args=[["a", "b"]]`` becomes ``("a", "b")``.

    Attributes:
        mode: Command, pipeline, or pub/sub dispatch key.
        command: Redis command name. Unused for pipeline and pub/sub.
        args: Positional arguments for the command.
        kwargs: Keyword arguments for the command.
    """

    mode: RequestMode
    command: Optional[str] = None
    args: Optional[Sequence] = None
    kwargs: Optional[Dict] = None

    def __post_init__(self):
        if self.args and len(self.args) == 1 and isinstance(self.args[0], list):
            self.args = tuple(self.args[0])
        self.args = self.args or tuple()
        self.kwargs = self.kwargs or dict()

    def set_pipeline(self, pipeline: bool):
        """
        Set the request mode to pipeline or command.

        Args:
            pipeline (bool): If True, set mode to PIPELINE; else COMMAND.
        """
        self.mode = RequestMode.PIPELINE if pipeline else RequestMode.COMMAND


@dataclass
class Response:
    """Transport reply for one Redis call.

    Attributes:
        reply: Command reply, or the ``Pipeline`` / ``PubSub`` object for
            pipeline and pub/sub requests.
    """

    reply: Any = None
