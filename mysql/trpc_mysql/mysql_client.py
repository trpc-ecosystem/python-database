# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
"""aiomysql pool and the SQL helpers built on top of it.

The pool is not opened in ``MySqlClient.__init__``. The first fetch or
execute creates it. Table and column names in the table helpers are
interpolated, not bound, because PyMySQL placeholders cannot stand in for
identifiers.
"""

import asyncio
from typing import Dict
from typing import List
from typing import Optional
from typing import Text
from typing import Tuple
from typing import Union

import aiomysql
from aiomysql.cursors import Cursor
from aiomysql.cursors import DictCursor

from trpc_sdk.log import logger

from .constants import Mode
from .types import DatabaseUrl


class CommonMixin:
    """Query and execute helpers shared by ``MySqlClient``."""

    async def get(self, raw_sql: Text, params: Optional[Union[Tuple, List]] = None):
        """Fetch a single row.

        Args:
            raw_sql: SQL statement. Placeholders use ``%s``.
            params: Sequence of values for those placeholders. A bare string
                is a sequence of characters, so pass a one-element tuple for
                one string parameter. Defaults to ``None``.

        Returns:
            A one-element list holding the row, or the fetch result from the
            driver. See ``_fetch`` for the retry.
        """
        return await self._fetch(raw_sql, params, 1)

    async def query(self, raw_sql: Text, params: Optional[Union[Tuple, List]] = None,
                    size: Optional[int] = None):
        """Fetch many rows, or every row when ``size`` is omitted.

        Args:
            raw_sql: SQL statement.
            params: Bound parameters. Defaults to ``None``.
            size: ``None`` or ``0`` fetches all rows. ``1`` fetches one row.
                Any other positive value fetches that many.

        Returns:
            Rows from ``_fetch``. A single non-list row is wrapped in a list.
        """
        return await self._fetch(raw_sql, params, size)

    async def execute(self, raw_sql: Text, params: Optional[Union[Tuple, List]] = None,
                      ret_affected_num: bool = False):
        """Run one statement without starting a transaction.

        Args:
            raw_sql: SQL statement.
            params: Bound parameters. Defaults to ``None``.
            ret_affected_num: When true, return the affected-row count
                instead of ``lastrowid``. Defaults to ``False``.

        Returns:
            Affected-row count or ``lastrowid``. Driver errors are logged and
            do not propagate; see ``_execute``.
        """
        return await self._execute(raw_sql, params,
                                   ret_affected_num=ret_affected_num)

    async def transaction(self, raw_sql: Text, params: Optional[Union[Tuple, List]] = None,
                          ret_affected_num: bool = False):
        """Run one statement and roll it back if the driver raises.

        Commit and rollback run only when this client's ``autocommit`` is
        false. With the default ``autocommit=True`` the driver commits on its
        own and a failure does not call ``rollback``.

        Args:
            raw_sql: SQL statement.
            params: Bound parameters. Defaults to ``None``.
            ret_affected_num: When true, return the affected-row count
                instead of ``lastrowid``. Defaults to ``False``.

        Returns:
            Affected-row count or ``lastrowid``.
        """
        return await self._execute(raw_sql, params, transaction=True,
                                   ret_affected_num=ret_affected_num)

    async def transaction_multi(self, raw_sqls: list):
        """Run several statements in one transaction.

        Each item is either a SQL string or a dict with ``pre_sql`` and
        ``data`` for ``executemany``. A raised driver error rolls the
        transaction back when ``autocommit`` is false, then the method
        returns the affected-row count accumulated before the error.

        Args:
            raw_sqls: Statements in order. Dict items look like
                ``{"pre_sql": "... %s ...", "data": [(...), ...]}``.
                String items are executed with an empty parameter tuple.

        Returns:
            Sum of affected-row counts reported before the first error.
        """
        return await self._execute_multi(raw_sqls, transaction=True)


class TableActionMixin:
    """Convenience SQL for a single table.

    Identifiers are interpolated into the statement. Values are bound.
    """

    async def table_has(self, table_name, field, value):
        """Return the first row whose ``field`` equals ``value``, or ``None``.

        Lookup errors are logged and swallowed. ``value`` is always bound as
        one parameter; a string must not be unpacked into characters.

        Args:
            table_name: Table identifier interpolated into the SQL.
            field: Column identifier interpolated into the select list and
                the comparison.
            value: Single bound value for the ``WHERE`` placeholder.

        Returns:
            The fetched row, or ``None`` when the lookup raises.
        """
        raw_sql = "SELECT {} FROM {} WHERE {}=%s limit 1".format(field, table_name, field)
        try:
            result = await self.get(raw_sql, (value,))
            return result
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(f"table has value error: {error}")

        return None

    async def table_insert(self, table_name, item: Dict,
                           ret_affected_num: bool = False):
        """Insert one row from a column-to-value mapping.

        Args:
            table_name: Table identifier interpolated into the SQL.
            item: Column names and bound values. Key order is the column
                order. An empty mapping produces an ``INSERT`` with no columns.
            ret_affected_num: When true, return the affected-row count
                instead of ``lastrowid``. Defaults to ``False``.

        Returns:
            Affected-row count or ``lastrowid``.
        """
        fields = list(item.keys())
        values = list(item.values())
        raw_sql = "INSERT INTO %s (%s) VALUES(%s)" % (
            table_name, ",".join(fields), ",".join(["%s"] * len(item)))

        return await self.execute(raw_sql, values,
                                  ret_affected_num=ret_affected_num)

    async def table_update(self, table_name, updates: Dict, field_where,
                           value_where, ret_affected_num: bool = False):
        """Update rows matched by one literal column comparison.

        ``value_where`` is interpolated in double quotes, not bound. Values
        in ``updates`` are bound.

        Args:
            table_name: Table identifier interpolated into the SQL.
            updates: Columns to set. Each value becomes one ``%s`` parameter.
            field_where: Column identifier used in the ``WHERE`` clause.
            value_where: Comparison literal interpolated as a quoted string.
            ret_affected_num: When true, return the affected-row count
                instead of ``lastrowid``. Defaults to ``False``.

        Returns:
            Affected-row count or ``lastrowid``.
        """
        upsets = []
        values = []
        for key, value in updates.items():
            pair = "%s=%%s" % key
            upsets.append(pair)
            values.append(value)

        upsets = ",".join(upsets)
        raw_sql = 'UPDATE %s SET %s WHERE %s="%s"' % (
            table_name,
            upsets,
            field_where, value_where,
        )
        return await self.execute(raw_sql, values,
                                  ret_affected_num=ret_affected_num)


class MySqlClient(CommonMixin, TableActionMixin):
    """Async MySQL access through one ``aiomysql`` pool.

    Class attributes are the pool settings sealed into every new client.
    Passing ``minsize``, ``maxsize``, or ``pool_recycle`` in ``kwargs`` does
    not stick: those three are copied from the class after ``kwargs`` is
    applied. A boolean ``autocommit`` keyword does stick, because it is
    copied onto the instance before the pool mapping is sealed.

    Attributes:
        minsize: Minimum connections kept in the pool.
        maxsize: Maximum connections the pool may open.
        pool_recycle: Seconds before a pooled connection is recycled.
        autocommit: Driver autocommit flag. When true, explicit commit and
            rollback in the transaction helpers are skipped.
        pool: Open pool, or ``None`` until the first statement.
    """

    minsize: int = 3
    maxsize: int = 10
    pool_recycle: int = 7 * 3600
    autocommit: bool = True

    def __init__(self, url: DatabaseUrl, loop=None, cursor: Cursor = DictCursor, **kwargs):
        """Store pool arguments. The pool itself is created lazily.

        Args:
            url: Parsed connection target. ``_asdict`` becomes the
                ``create_pool`` keywords.
            loop: Event loop stored for ``aiomysql``. When omitted, the
                running loop is captured on first use.
            cursor: Cursor class passed as ``cursorclass``. Defaults to
                ``DictCursor``, so rows are mappings.
            **kwargs: Extra ``create_pool`` arguments. A boolean
                ``autocommit`` also updates this instance.
        """
        self._db_args = url._asdict()
        if loop:
            self._db_args["loop"] = loop
        if cursor:
            self._db_args["cursorclass"] = cursor
        if kwargs:
            self._db_args.update(kwargs)
            if "autocommit" in kwargs and isinstance(kwargs["autocommit"], bool):
                self.autocommit = kwargs["autocommit"]

        self._db_args.update({
            "minsize": self.minsize,
            "maxsize": self.maxsize,
            "pool_recycle": self.pool_recycle,
            "autocommit": self.autocommit
        })
        self.pool = None
        self._pool_lock = None

    async def close(self):
        """Close the pool and wait until its connections are gone.

        A second close, or a close before any statement, does nothing because
        ``pool`` is already ``None``.
        """
        if self.pool is None:
            return

        self.pool.close()
        await self.pool.wait_closed()

        self.pool = None
        self._pool_lock = None

    async def invoke(self, method, raw_sql: Text, params: Optional[Union[Tuple, List]] = None, **kwargs):
        """Dispatch ``method`` onto get, query, execute, or transaction.

        Args:
            method: ``Mode`` value. ``Mode.CURSOR`` and unknown values return
                ``None`` without touching the database.
            raw_sql: SQL statement.
            params: Bound parameters. Defaults to ``None``.
            **kwargs: ``size`` is forwarded for ``FETCHALL``.
                ``ret_affected_num`` is forwarded for execute and transaction.
                Other keys, including ``uri``, are ignored.

        Returns:
            The helper's result, or ``None`` for an unhandled mode.
        """
        ret_affected_num = kwargs.get("ret_affected_num", False)
        if method == Mode.FETCHONE:
            return await self.get(raw_sql, params)

        if method == Mode.FETCHALL:
            return await self.query(raw_sql, params, size=kwargs.get("size"))

        if method == Mode.EXECUTE:
            return await self.execute(raw_sql, params,
                                      ret_affected_num=ret_affected_num)

        if method == Mode.TRANSACTION:
            return await self.transaction(raw_sql, params,
                                          ret_affected_num=ret_affected_num)

        return None

    async def _ensure_pool(self):
        """Open the pool once, even when first calls on the loop run concurrently.

        The pool is bound to the running loop, so one instance must only be
        used on that loop. A failed open leaves ``pool`` unset for a retry.
        """
        if self.pool is not None:
            return
        if self._pool_lock is None:
            self._pool_lock = asyncio.Lock()
        async with self._pool_lock:
            if self.pool is None:
                await self._init_pool()

    async def _init_pool(self):
        """Open the pool on the stored loop, or on the running loop if none was stored."""
        loop = self._db_args.get("loop") or asyncio.get_running_loop()
        self.pool = await aiomysql.create_pool(**{**self._db_args, "loop": loop})

    async def _fetch(self, raw_sql: Text, params: Optional[Union[Tuple, List]] = None,
                     size: Optional[int] = None):
        """Execute a read, retrying once after ``ping`` if the first try fails.

        The second failure propagates. A single non-list row is wrapped so
        callers can always iterate a list when the driver returned one.

        Args:
            raw_sql: SQL statement.
            params: Bound parameters. ``None`` is executed as an empty tuple.
            size: Forwarded to ``_do_fetch``.

        Returns:
            Fetched rows. A bare row becomes a one-element list.
        """
        await self._ensure_pool()

        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                try:
                    await cursor.execute(raw_sql, params or ())
                    result = await self._do_fetch(cursor, size)
                except Exception as error:  # pylint: disable=broad-except
                    logger.error(f"fetch sql exception: {error}")
                    await connection.ping()

                    await cursor.execute(raw_sql, params or ())
                    result = await self._do_fetch(cursor, size)

                if result is not None and not isinstance(result, List):
                    result = [result]

                return result

    @staticmethod
    async def _do_fetch(cursor: Cursor, size: Optional[int] = None):
        """Choose fetchall, fetchone, or fetchmany from ``size``.

        Args:
            cursor: Cursor that already executed the statement.
            size: ``None`` or ``0`` means all rows. ``1`` means one row.
                Any other value is passed to ``fetchmany``.

        Returns:
            Whatever the chosen fetch method returns.
        """
        if not size:
            result = await cursor.fetchall()
        elif size and size == 1:
            result = await cursor.fetchone()
        else:
            result = await cursor.fetchmany(size)

        return result

    async def _execute(self, raw_sql: Text, params: Optional[Union[Tuple, List]] = None,
                       transaction: bool = False, many: bool = False,
                       ret_affected_num: bool = False):
        """Execute one statement, optionally inside a transaction.

        When ``ret_affected_num`` is true the affected-row count is returned
        immediately after a successful execute. Otherwise the return value is
        ``cursor.lastrowid``, including after a logged failure. Failures do
        not propagate. Rollback runs only for a transaction whose client has
        ``autocommit`` left false; commit uses the same condition.

        Args:
            raw_sql: SQL statement. Ignored as the statement text when
                ``many`` is true and ``params`` is a sequence of rows; it is
                still the statement passed to ``executemany``.
            params: One parameter sequence, or a sequence of rows when
                ``many`` is true.
            transaction: When true, ``begin`` before the statement.
            many: When true, call ``executemany`` instead of ``execute``.
            ret_affected_num: Return the driver count instead of
                ``lastrowid``.

        Returns:
            Affected-row count or ``lastrowid``.
        """
        await self._ensure_pool()

        async with self.pool.acquire() as connection:
            if transaction is True:
                await connection.begin()

            async with connection.cursor() as cursor:
                result = None
                try:
                    if many is True:
                        count = await cursor.executemany(raw_sql, params or ())
                    else:
                        count = await cursor.execute(raw_sql, params or ())

                    if transaction is True and not self.autocommit:
                        await connection.commit()
                    logger.debug(f"cursor result: {count}")
                    if ret_affected_num:
                        return count
                except Exception as error:  # pylint: disable=broad-except
                    logger.error(f"execute sql exception: {error}")
                    if transaction is True:
                        await connection.rollback()

                if result is None:
                    result = cursor.lastrowid

                return result

    async def _execute_multi(self, raw_sqls: list, transaction: bool = False):
        """Execute a batch, rolling back when a transactional statement fails.

        Dict items must contain both ``pre_sql`` and ``data``. A missing key
        raises ``ValueError`` from inside the loop, which is then logged and
        swallowed like any other error. Rollback on that path still requires
        ``transaction`` and ``autocommit`` to be false. Commit uses the same
        pair of flags.

        Args:
            raw_sqls: SQL strings or ``{"pre_sql", "data"}`` mappings.
            transaction: When true, ``begin`` before the batch.

        Returns:
            Affected-row count summed from statements that ran before the
            first error.
        """
        await self._ensure_pool()

        count = 0
        async with self.pool.acquire() as connection:
            if transaction is True:
                await connection.begin()

            async with connection.cursor() as cursor:
                try:
                    for sql_info in raw_sqls:
                        if isinstance(sql_info, dict):
                            if "pre_sql" not in sql_info or "data" not in sql_info:
                                raise ValueError("pre sql format error!")
                            affect_row = await cursor.executemany(sql_info["pre_sql"], sql_info["data"] or [])
                            count += affect_row
                        else:
                            affect_row = await cursor.execute(sql_info, ())
                            count += affect_row

                    if transaction is True and not self.autocommit:
                        await connection.commit()
                except Exception as error:  # pylint: disable=broad-except
                    logger.error(f"execute sql exception: {error}")
                    if transaction is True and not self.autocommit:
                        await connection.rollback()
                logger.debug(f"multi transaction result count: {count}")

        return count
