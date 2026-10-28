# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""Request, response, and connection-target types carried on the tRPC message."""

from typing import NamedTuple
from typing import Text

from .constants import Mode


class Request:
    """SQL call passed from ``Client`` to the transport on ``client_req_head``.

    Attributes:
        mode: Which ``MySqlClient`` method the transport should call.
        query: SQL text. Placeholders use PyMySQL ``%s`` style.
        args: Bound parameters for ``query``. Not included in ``params``.
        size: Row limit for ``Mode.FETCHALL``. ``1`` selects ``fetchone``.
        uri: Connection URI copied from client options before invoke.
        ret_affected_num: When true, execute paths return the affected-row
            count instead of ``lastrowid``.
    """

    def __init__(self, mode: Mode, query: Text,
                 args=None, size=None, uri=None,
                 ret_affected_num: bool = False):
        """Store one SQL call.

        Args:
            mode: Dispatch key for ``MySqlClient.invoke``.
            query: SQL text.
            args: Bound parameters. Defaults to ``None``.
            size: Fetch limit. Defaults to ``None``, which means fetch all.
            uri: Database URI. The client fills this in before invoke.
            ret_affected_num: Return affected-row count instead of
                ``lastrowid``. Defaults to ``False``.
        """
        self.mode = mode
        self.query = query
        self.args = args
        self.size = size
        self.uri = uri
        self.ret_affected_num = ret_affected_num

    def params(self) -> dict:
        """Return keyword arguments the transport forwards besides SQL args.

        ``args`` and ``ret_affected_num`` are applied by the transport
        separately. Only ``size`` and ``uri`` travel through this dict.

        Returns:
            Mapping with keys ``size`` and ``uri``.
        """
        return dict(size=self.size, uri=self.uri)


class Response:
    """Result stored on ``client_rsp_head`` after a transport round trip.

    Attributes:
        code: ``0`` after a successful ``build``. ``-1`` means unset.
        message: Status text. ``"empty"`` until ``build`` replaces it.
        result: Rows, insert id, or affected-row count from ``MySqlClient``.
    """

    def __init__(self):
        """Create an unset response.

        ``build`` overwrites these defaults. Callers should not treat a bare
        ``Response()`` as a successful query.
        """
        self.code = -1
        self.message = "empty"
        self.result = None

    @staticmethod
    def build(code, message, result):
        """Wrap a driver result for the tRPC message head.

        Args:
            code: Status code. The transport uses ``0`` for success.
            message: Status text stored alongside ``result``.
            result: Value ``Client`` later returns to the caller.

        Returns:
            A response whose fields are the three arguments.
        """
        resp = Response()
        resp.code = code
        resp.message = message
        resp.result = result
        return resp


class DatabaseUrl(NamedTuple):
    """Pieces ``aiomysql.create_pool`` accepts, parsed from a URI.

    Attributes:
        user: Login name.
        password: Login password. May itself contain ``:``.
        host: Server host. Taken from the text before the last ``:port``.
        port: TCP port.
        db: Database name, without a ``?charset=`` suffix.
        charset: Character set. ``utf8mb4`` when the URI omits ``?charset=``.
    """

    user: Text
    password: Text
    host: Text
    port: int
    db: Text
    charset: Text = "utf8mb4"

    @staticmethod
    def make_url(uri: str):
        """Parse a MySQL URI into pool arguments.

        ``dsn://`` and ``dsn+mysql://`` are rewritten to ``mysql://`` before
        parsing, so framework config and direct URIs share one grammar.
        The authority is split on the first ``:`` only, which keeps colons
        inside the password. Anything that is empty or does not start with
        ``mysql://`` after that rewrite yields ``None``.

        Args:
            uri: ``mysql://user:password@host:port/database`` with an optional
                ``?charset=`` on the database name.

        Returns:
            Parsed target, or ``None`` when ``uri`` is missing or not MySQL.
        """
        if not uri:
            return None

        if uri.startswith("dsn://") or uri.startswith("dsn+mysql://"):
            uri = uri.replace("dsn://", "mysql://")
            uri = uri.replace("dsn+mysql://", "mysql://")

        if not uri.startswith("mysql://"):
            return None

        parts = uri.rsplit("/", 1)
        uri = parts[0]
        database = parts[1]

        if "?charset=" in database.lower():
            parts = database.split("?charset=", 1)
            database = parts[0]
            charset = parts[1]
        else:
            charset = "utf8mb4"

        parts = uri.rsplit("@", 1)

        uri = parts[0]
        server = parts[1].split(":", 1)
        host = server[0]
        port = int(server[1])

        parts = uri.rsplit("://", 1)

        auth = parts[1].split(":", 1)
        user = auth[0]
        password = auth[1]

        args = [user, password, host, port, database, charset]
        return DatabaseUrl(*args)
