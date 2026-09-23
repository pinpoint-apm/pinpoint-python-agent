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

"""asyncpg (native async PostgreSQL) instrumentation.

asyncpg implements the PostgreSQL wire protocol directly — no DB-API,
no psycopg under the hood. The ``Connection`` class exposes a small set
of coroutine query methods we wrap individually:

- ``execute(query, *args, ...)`` — fire-and-forget statement.
- ``executemany(query, args, ...)`` — batched fire-and-forget.
- ``fetch(query, *args, ...)`` / ``fetchmany(query, args, ...)`` — records.
- ``fetchrow(query, *args, ...)`` — single record (or None).
- ``fetchval(query, *args, ...)`` — single value.
- ``cursor(query, *args, ...)`` — server-side cursor (we wrap to record
  the SQL when the cursor is created; iteration itself isn't traced).

We don't reuse the dbapi base because asyncpg's coroutines don't conform
to PEP 249 — different signature, different way to read connection
metadata (``Connection._addr`` for host/port, ``Connection._params``
for the database name).
"""

from __future__ import annotations

from ...context import current_span
from ...errors import safe_try
from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_POSTGRESQL
from .._util import (
    limited_repr,
    memoize_on,
    span_event_scope,
    span_is_sampled,
    sql_bind_values_enabled,
    wrap,
)
from ..dbapi import TargetInfo, _driver_event_suppressed

_OPERATION_PREFIX = "asyncpg.connection.Connection"


class AsyncpgInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        for method in ("execute", "executemany", "fetch", "fetchmany",
                       "fetchrow", "fetchval", "cursor"):
            wrap("asyncpg.connection", f"Connection.{method}",
                 _make_wrapper(method))


def _make_wrapper(kind: str):
    # The operation string is fixed per wrapper; building it per query would
    # be a per-call f-string for a constant.
    operation = f"{_OPERATION_PREFIX}.{kind}"
    if kind == "cursor":
        # Connection.cursor is a sync factory returning a CursorFactory;
        # the actual server-side iteration happens inside the user's loop.
        # We open a span event around the *creation* call only.
        def _sync_wrapper(wrapped, instance, args, kwargs):
            return _trace_sync(wrapped, instance, args, kwargs, operation)
        _sync_wrapper.__qualname__ = f"_asyncpg_{kind}_wrapper"
        return _sync_wrapper

    async def _wrapper(wrapped, instance, args, kwargs):
        return await _trace_async(wrapped, instance, args, kwargs, operation)
    _wrapper.__qualname__ = f"_asyncpg_{kind}_wrapper"
    return _wrapper


def _open_event(instance, args, kwargs, operation: str):
    """Span event for one query, or ``None`` to run it untraced: no sampled
    span is current, an outer layer already traces the statement (SQLAlchemy's
    async engine — its greenlet shares this contextvars context, so
    ``_in_sqlalchemy`` is visible — or an ``executemany`` loop), or the setup
    failed (the async wrappers are unguarded by safe_wrapper, so a native
    failure here would otherwise escape into the user's query)."""
    span = current_span()
    if span is None or not span_is_sampled(span) or _driver_event_suppressed():
        return None
    try:
        event = span.new_span_event(
            operation, service_type=SERVICE_TYPE_POSTGRESQL,
        )
    except Exception:  # noqa: BLE001
        return None
    _annotate(event, instance, args[0] if args else (kwargs.get("query") or ""), args)
    return event


async def _trace_async(wrapped, instance, args, kwargs, operation: str):
    event = _open_event(instance, args, kwargs, operation)
    if event is None:
        return await wrapped(*args, **kwargs)
    with span_event_scope(event):
        return await wrapped(*args, **kwargs)


def _trace_sync(wrapped, instance, args, kwargs, operation: str):
    """For ``Connection.cursor`` which is a synchronous factory."""
    event = _open_event(instance, args, kwargs, operation)
    if event is None:
        return wrapped(*args, **kwargs)
    with span_event_scope(event):
        return wrapped(*args, **kwargs)


@safe_try
def _annotate(event, conn, sql, args) -> None:
    target = _connection_target(conn)
    if target.endpoint:
        event.set_end_point(target.endpoint)
    if target.destination:
        event.set_destination(target.destination)

    # asyncpg's API only accepts ``str`` queries, so no bytes handling needed.
    sql_text = str(sql or "")
    params_text = ""
    if len(args) > 1 and sql_bind_values_enabled(event):
        # asyncpg uses positional placeholders ($1, $2, ...), so the bound values are
        # args[1:]. Rendered only when bind-value capture is on (off by default;
        # values may hold PII/secrets — see Config.sql_trace_bind_values).
        params_text = limited_repr(tuple(args[1:]), 1024)
    event.set_sql_query(sql_text, params_text)


def _connection_target(conn) -> TargetInfo:
    # Fixed for the connection's life. asyncpg's Connection is __slots__, so
    # memoize_on keeps it in its weak-key fallback.
    return memoize_on(conn, "_pinpoint_target", _resolve_connection_target)


def _resolve_connection_target(conn) -> TargetInfo:
    host, port = _connection_addr(conn)
    return TargetInfo(host=host, port=port, database=_connection_database(conn))


def _connection_addr(conn) -> tuple[str, int]:
    """asyncpg.Connection stores the resolved transport address on
    ``_addr`` as ``(host, port)`` for TCP or ``"/path"`` for Unix sockets."""
    addr = getattr(conn, "_addr", None)
    if isinstance(addr, tuple) and len(addr) >= 2:
        try:
            return str(addr[0] or ""), int(addr[1] or 0)
        except Exception:  # noqa: BLE001
            return "", 0
    if isinstance(addr, str):  # Unix socket
        return addr, 0
    return "", 0


def _connection_database(conn) -> str:
    """Database name lives on ``Connection._params.database`` in modern
    asyncpg, or in the ``ConnectionParameters`` namedtuple-ish object."""
    params = getattr(conn, "_params", None)
    if params is None:
        return ""
    name = getattr(params, "database", None) or getattr(params, "dbname", None)
    return str(name) if name else ""


def instrument() -> None:
    AsyncpgInstrumentor().instrument()
