# pinpoint-python-agent
# Copyright (c) 2026-present NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generic DB-API 2.0 instrumentation helpers.

PEP 249 ("Python Database API Specification 2.0") gives every supported
driver — pymysql, mysql-connector, mysqlclient, psycopg2/3, sqlite3,
oracledb, etc. — the same connection/cursor surface. Rather than
reimplement the same trace logic per driver, this module exposes:

- ``trace_cursor_method(span_event, ...)`` — instrumentation primitive that
  records SQL + endpoint annotations on a span event. Used by the per-driver
  hooks below and from the per-driver wrappers in sibling modules.
- ``wrap_cursor_class(cursor_cls, dialect, service_type)`` — install
  wrapt wrappers on a Cursor class's ``execute`` / ``executemany`` /
  ``callproc``. The driver instrumentations for mysql-connector, mysqlclient,
  psycopg, aiopg, and aiomysql all delegate here.

The wrappers don't *automatically* attach to any specific module — there's
no canonical DB-API import. Each driver instrumentation knows its own
cursor class and calls ``wrap_cursor_class`` for it.
"""

from __future__ import annotations

import contextvars
from typing import Any
from collections.abc import Callable

from ..._log import get_logger
from ...context import current_span
from ...errors import safe_try
from .._util import (
    limited_repr,
    memoize_on,
    span_event_scope,
    span_is_sampled,
    sql_bind_values_enabled,
    truncate_text,
    wrap,
)

_log = get_logger("dbapi")

# Cap on bound-parameter blob length recorded in span events. Native caps the
# SQL bytes too; this stops us from copying megabyte BLOBs into trace data on
# every query.
_MAX_PARAMS_LEN = 1024

# Many drivers implement ``Cursor.executemany`` as a Python loop calling
# ``self.execute(query, row)`` per row (PyMySQL, MySQLdb, mysql-connector), and
# ``execute`` is wrapped too — so a 50k-row call would allocate 50k+1 events on one
# span, growing the span buffer and gRPC payload toward a worker OOM.
#
# Set while an ``executemany`` is traced: inner ``execute`` calls see it and skip
# their own event, leaving one event for the statement. A contextvar, so
# suppression survives ``await`` in the async aiomysql/aiopg wrappers.
_in_executemany: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "pinpoint_dbapi_in_executemany", default=False,
)

# SQLAlchemy over an instrumented DBAPI driver would trace a statement twice —
# its ``before_cursor_execute`` event plus a nested one from this module's cursor
# wrapper — doubling native crossings and showing a duplicated DB node per ORM
# query. SQLAlchemy owns the trace for cursors it drives, so it sets this flag
# (:func:`enter_sqlalchemy_execute` / :func:`exit_sqlalchemy_execute`) and the
# inner wrapper no-ops. A contextvar, for async engines. As ``_in_executemany``.
_in_sqlalchemy: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "pinpoint_dbapi_in_sqlalchemy", default=False,
)


def _driver_event_suppressed() -> bool:
    """True when the inner driver wrapper should skip opening its own span
    event because an outer layer (an ``executemany`` loop, or SQLAlchemy)
    already traces this statement."""
    return _in_executemany.get() or _in_sqlalchemy.get()


def enter_sqlalchemy_execute():
    """Mark the current context as executing a statement driven by
    SQLAlchemy, so the DBAPI cursor wrapper for that statement is suppressed.

    Returns a reset token that must be passed to
    :func:`exit_sqlalchemy_execute` once the statement finishes.

    ``_before`` is never legitimately re-entered on a context that is already
    suppressed, so finding the flag set means a previous statement leaked it:
    SQLAlchemy before 1.4.40 does not route a ``BaseException`` (a
    ``gevent.Timeout``, a cancellation) to ``handle_error``, so neither reset
    hook ran. Clear it first — otherwise this statement's token would restore
    the *stale* True and pin suppression on the context for the life of the
    worker, silently dropping every later raw-driver trace.
    """
    if _in_sqlalchemy.get():
        _log.debug("stale sqlalchemy suppression flag; clearing")
        _in_sqlalchemy.set(False)
    return _in_sqlalchemy.set(True)


def exit_sqlalchemy_execute(token) -> None:
    """Undo a previous :func:`enter_sqlalchemy_execute`. Tolerant of a token
    from a different context (defensive: the SQLAlchemy before/after callbacks
    should always pair up, but a leaked flag would silently drop driver
    traces)."""
    try:
        _in_sqlalchemy.reset(token)
    except Exception:  # noqa: BLE001
        _in_sqlalchemy.set(False)


# ---- Per-driver hook installation -------------------------------------------

def wrap_cursor_class(
    module: str,
    cursor_qualname: str,
    *,
    service_type: int,
    extract_target: Callable[[Any], TargetInfo | None] | None = None,
) -> None:
    """Install ``execute`` / ``executemany`` / ``callproc`` wrappers on
    ``module.cursor_qualname`` (e.g. ``"pymysql.cursors", "Cursor"``).

    Parameters
    ----------
    module:
        Dotted module path that owns the cursor class.
    cursor_qualname:
        Class qualname inside that module (e.g. ``"Cursor"`` or
        ``"MySQLCursor"``).
    service_type:
        Pinpoint service-type code shown in the UI (MySQL=2101, etc.).
    extract_target:
        Optional callable ``(cursor) -> TargetInfo | None`` that produces
        ``(host, port, database)`` from the cursor's connection. Drivers
        with non-standard connection layouts override this.
    """
    _wrap_cursor_methods(module, cursor_qualname, service_type,
                         extract_target, _make_wrapper)


def wrap_async_cursor_class(
    module: str,
    cursor_qualname: str,
    *,
    service_type: int,
    extract_target: Callable[[Any], TargetInfo | None] | None = None,
) -> None:
    """Same as :func:`wrap_cursor_class` but produces async wrappers — the
    coroutine ``cursor.execute()`` shape used by aiopg/aiomysql.
    """
    _wrap_cursor_methods(module, cursor_qualname, service_type,
                         extract_target, _make_async_wrapper)


def _wrap_cursor_methods(module, cursor_qualname, service_type,
                         extract_target, make_wrapper) -> None:
    extract = extract_target or _default_extract_target

    for op_kind in ("execute", "executemany", "callproc"):
        target = f"{cursor_qualname}.{op_kind}"
        # ``wrap`` applies ``safe_wrapper``, skips an already-wrapped target, and
        # debug-logs a method missing on this cursor (e.g. no ``callproc``).
        wrap(
            module, target,
            make_wrapper(op_kind, service_type, extract,
                         operation=f"{module}.{target}"),
        )


# ---- Wrapper factories ------------------------------------------------------

def _make_wrapper(op_kind: str, service_type: int, extract, *, operation: str):
    def _wrapper(wrapped, instance, args, kwargs):
        return trace_cursor_call(
            wrapped, instance, args, kwargs,
            operation=operation, op_kind=op_kind,
            service_type=service_type, extract=extract,
        )
    _wrapper.__qualname__ = f"_dbapi_wrapper[{operation}]"
    return _wrapper


def _make_async_wrapper(op_kind: str, service_type: int, extract, *,
                        operation: str):
    async def _wrapper(wrapped, instance, args, kwargs):
        return await _trace_query_async(
            wrapped, instance, args, kwargs,
            operation=operation, op_kind=op_kind,
            service_type=service_type, extract=extract,
        )
    _wrapper.__qualname__ = f"_dbapi_async_wrapper[{operation}]"
    return _wrapper


# ---- The actual trace logic -------------------------------------------------

# Hoisted: ``in ("a", "b")`` on locals emits BUILD_TUPLE per cursor call.
_MULTI_STATEMENT_OP_KINDS = frozenset(("executemany", "callproc"))


class TargetInfo:
    """``(host, port, database)`` extracted from a DB-API connection.

    The derived endpoint/destination strings are computed here: a TargetInfo
    is built once per connection (``_connection_target``) while
    ``_annotate_target`` reads it on every query."""
    __slots__ = ("host", "port", "database", "endpoint", "destination")

    def __init__(self, host: str = "", port: int = 0, database: str = ""):
        self.host = host
        self.port = port
        self.database = database
        self.endpoint = f"{host}:{port}" if host and port else (host or "")
        self.destination = database or self.endpoint


def _open_query_event(cursor, args, kwargs, *, operation, op_kind,
                      service_type, extract):
    """Span event for one cursor call, or ``None`` to run it untraced.

    Untraced: an outer layer already traces the statement (an executemany
    loop's inner execute(), a statement SQLAlchemy drives), no sampled span is
    current, or the setup failed. Suppression is checked before the span
    lookup — two contextvar reads beat the full current_span() resolution every
    suppressed call (each executemany row) would otherwise pay. Setup failures
    are contained here because the async wrappers are unguarded by
    safe_wrapper; the param extraction belongs inside too, since it evaluates
    the caller's params object for truthiness and exotic ones (numpy arrays,
    pandas objects) raise from __bool__.
    """
    if _driver_event_suppressed():
        return None
    span = current_span()
    if span is None or not span_is_sampled(span):
        return None
    try:
        sql, params = _extract_sql_and_params(op_kind, args, kwargs)
        event = span.new_span_event(operation, service_type=service_type)
        _annotate_target(event, cursor, extract)
        _record_sql(event, cursor, sql, params, op_kind=op_kind)
        return event
    except Exception:  # noqa: BLE001
        _log.debug("dbapi query event setup failed", exc_info=True)
        return None


async def _trace_query_async(wrapped, instance, args, kwargs, *, operation,
                             op_kind, service_type, extract):
    event = _open_query_event(
        instance, args, kwargs, operation=operation, op_kind=op_kind,
        service_type=service_type, extract=extract)
    if event is None:
        return await wrapped(*args, **kwargs)
    # See trace_cursor_call: suppress the inner driver events a
    # multi-statement call re-enters.
    token = (_in_executemany.set(True)
             if op_kind in _MULTI_STATEMENT_OP_KINDS else None)
    try:
        with span_event_scope(event):
            return await wrapped(*args, **kwargs)
    finally:
        if token is not None:
            _in_executemany.reset(token)


def trace_cursor_call(target, cursor, args, kwargs, *, operation,
                      op_kind, service_type, extract=None):
    """Trace one already-bound cursor method call.

    The wrapt wrappers from :func:`wrap_cursor_class` route here, as do
    drivers whose C-extension cursor classes reject monkeypatching
    (psycopg2): those callers intercept the method themselves (e.g. via a
    ``wrapt.ObjectProxy``) and delegate here. Instrumentation failures
    fall back to the untraced call; user exceptions propagate exactly
    once — the ``safe_wrapper`` fallback is never needed on this path.
    """
    event = _open_query_event(
        cursor, args, kwargs, operation=operation, op_kind=op_kind,
        service_type=service_type, extract=extract or _default_extract_target)
    if event is None:
        return target(*args, **kwargs)
    # Suppress inner driver events across a multi-statement call: executemany loops
    # call execute() per row, and mysql-connector's callproc calls execute() to bind
    # OUT params — each would otherwise nest a second event.
    token = (_in_executemany.set(True)
             if op_kind in _MULTI_STATEMENT_OP_KINDS else None)
    try:
        with span_event_scope(event):
            return target(*args, **kwargs)
    finally:
        if token is not None:
            _in_executemany.reset(token)


def _extract_sql_and_params(op_kind: str, args, kwargs):
    """``execute(query[, params])``, ``executemany(query, seq_of_params)``,
    ``callproc(procname[, params])`` all share the same signature shape:
    first positional is the SQL/procedure, second optional is params."""
    sql = args[0] if args else (
        kwargs.get("query") or kwargs.get("operation") or kwargs.get("procname") or ""
    )
    params = (
        args[1] if len(args) > 1
        else kwargs.get("params")   # psycopg3, mysql-connector
        or kwargs.get("args")
        or kwargs.get("parameters")
        or kwargs.get("vars")
    )
    return sql, params


# ---- Annotation helpers (re-used by per-driver code via public alias below) -

@safe_try
def _annotate_target(event, cursor, extract) -> None:
    target = extract(cursor)
    if target is None:
        return
    if target.endpoint:
        event.set_end_point(target.endpoint)
    if target.destination:
        event.set_destination(target.destination)


@safe_try
def _record_sql(event, cursor, sql, params, *, op_kind: str) -> None:
    """Record SQL + params on the span event.

    Bound parameter VALUES are captured only when ``sql_trace_bind_values`` is
    enabled (default off — the values routinely hold PII / secrets). With it
    off we record just the SQL template and never call ``mogrify`` (which would
    bake the literals into the statement text itself, defeating any downstream
    redaction). With it on, for the format-paramstyle drivers (pymysql etc.) we
    prefer the rendered SQL via ``cursor.mogrify`` so the trace shows what
    actually went on the wire, else the template + repr(params).
    """
    sql_text = _decode(sql)
    params_text = ""
    if params and sql_bind_values_enabled(event):
        # A params value with a broken __repr__ must cost only the bind
        # values, not the SQL text as well.
        try:
            rendered = _try_mogrify(cursor, sql, params, op_kind=op_kind)
            if rendered is not None:
                sql_text = rendered
            else:
                params_text = _format_params(params)
        except Exception:  # noqa: BLE001
            params_text = ""
    event.set_sql_query(sql_text, params_text)


def _try_mogrify(cursor, sql, params, *, op_kind: str):
    mogrify = getattr(cursor, "mogrify", None)
    if mogrify is None:
        return None
    # mogrify re-renders the full statement (execute() interpolates again, so the
    # work is paid twice) and truncate_text discards everything past
    # _MAX_PARAMS_LEN. Record template + params instead when oversized.
    if isinstance(sql, (str, bytes, bytearray)) and len(sql) > _MAX_PARAMS_LEN:
        return None
    if _params_may_be_large(params):
        return None
    # Only materialized containers may reach mogrify. A generator/iterator
    # passed as params (``executemany(sql, (row for row in rows))``) would be
    # *consumed* by drivers whose mogrify walks it (mysqlclient's
    # ``tuple(map(db.literal, args))``), leaving the real execute with an
    # exhausted iterator: 0 rows written, or StopIteration in the app.
    if not isinstance(params, (list, tuple, dict)):
        return None
    try:
        if op_kind == "executemany" and _is_param_sequence(params):
            first = params[0]
            rendered = truncate_text(_decode(mogrify(sql, first)), _MAX_PARAMS_LEN)
            if len(params) > 1:
                rendered += f"  /* + {len(params) - 1} more rows */"
            return rendered
        return truncate_text(_decode(mogrify(sql, params)), _MAX_PARAMS_LEN)
    except Exception:  # noqa: BLE001
        return None


def _is_param_sequence(params) -> bool:
    return (
        isinstance(params, (list, tuple))
        and bool(params)
        and isinstance(params[0], (list, tuple, dict))
    )


def _decode(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "replace")
    return str(value)


def _format_params(params) -> str:
    if params is None:
        return ""
    if isinstance(params, (list, tuple)) and params and isinstance(params[0], (list, tuple, dict)):
        return limited_repr([params[0], f"...x{len(params)}"], _MAX_PARAMS_LEN)
    return limited_repr(params, _MAX_PARAMS_LEN)


def _params_may_be_large(params) -> bool:
    if params is None:
        return False
    if isinstance(params, (str, bytes, bytearray)):
        return len(params) > _MAX_PARAMS_LEN
    if isinstance(params, dict):
        for idx, (key, value) in enumerate(params.items()):
            if idx >= 16:
                return True
            if _params_may_be_large(key) or _params_may_be_large(value):
                return True
        return False
    if isinstance(params, (list, tuple)):
        for idx, value in enumerate(params):
            if idx >= 16:
                return True
            if _params_may_be_large(value):
                return True
        return False
    return False


# ---- Connection target extractors -------------------------------------------

def _default_extract_target(cursor) -> TargetInfo | None:
    """Best-effort PEP-249 connection inspection: most drivers stash
    ``self.connection`` (DB-API attr) or ``self._connection`` and expose
    ``host``/``port``/``database`` on it."""
    conn = (
        getattr(cursor, "connection", None)
        or getattr(cursor, "_connection", None)
    )
    if conn is None:
        return None
    return _connection_target(conn)


def _connection_target(conn) -> TargetInfo:
    # host/port/database are fixed for a connection's life, so resolve once and
    # stamp it on the connection. psycopg2's C connection type rejects the
    # attribute and lands in memoize_on's weak-key fallback — the driver whose
    # per-query resolve is the most expensive (three ``conn.info`` accesses,
    # each a fresh ConnectionInfo + libpq call).
    return memoize_on(conn, "_pinpoint_target", _resolve_connection_target)


def _resolve_connection_target(conn) -> TargetInfo:
    host = (
        getattr(conn, "host", None)
        or getattr(conn, "_host", None)
        or _info_attr(conn, "host")
        or ""
    )
    port = (
        getattr(conn, "port", None)
        or getattr(conn, "_port", None)
        or _info_attr(conn, "port")
        or 0
    )
    database = (
        getattr(conn, "database", None)
        or getattr(conn, "db", None)
        or getattr(conn, "_database", None)
        or _info_attr(conn, "database")
        or ""
    )
    if isinstance(database, (bytes, bytearray)):
        database = database.decode("utf-8", "replace")
    try:
        port = int(port) if port else 0
    except Exception:  # noqa: BLE001
        port = 0
    return TargetInfo(host=str(host or ""), port=port, database=str(database or ""))


def _info_attr(conn, key: str):
    """Some drivers expose connection metadata via an ``info`` object
    (psycopg3) or a ``get_dsn_parameters`` dict (psycopg2)."""
    info = getattr(conn, "info", None)
    if info is not None:
        v = getattr(info, key, None)
        if v is None and key == "database":
            # psycopg3's ConnectionInfo names it ``dbname`` (libpq PQdb).
            v = getattr(info, "dbname", None)
        if v is not None:
            return v
    get_dsn = getattr(conn, "get_dsn_parameters", None)
    if callable(get_dsn):
        try:
            params = get_dsn() or {}
            if key == "host":
                return params.get("host")
            if key == "port":
                return params.get("port")
            if key == "database":
                return params.get("dbname") or params.get("database")
        except Exception:  # noqa: BLE001
            return None
    return None


# Public aliases — sibling driver modules import these.
extract_default_target = _default_extract_target
connection_target = _connection_target
