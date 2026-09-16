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

"""asyncpg instrumentation.

Drives the wrappers directly against fakes — no asyncpg dependency.
Validates span event lifecycle for fetch/fetchrow/fetchval/execute/
executemany, error propagation, and the synchronous cursor() factory.
"""

from __future__ import annotations

import asyncio

import pytest

import _fakes
from pinpoint import context as ppctx
from pinpoint.instrumentations import asyncpg as asyncpg_instr
from pinpoint.instrumentations import dbapi as dbapi_instr


class _Params:
    """Stand-in for asyncpg's ConnectionParameters."""
    def __init__(self, database="appdb"):
        self.database = database


class _Connection:
    def __init__(self, host="pg.test", port=5432, database="appdb"):
        self._addr = (host, port)
        self._params = _Params(database)


# ---------------------------------------------------------------------------
# Async query wrappers
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("kind", ["execute", "executemany", "fetch", "fetchmany", "fetchrow", "fetchval"])
def test_async_wrapper_emits_event(sql_push_span, kind, sql_bind_values_on):
    sp, rec = sql_push_span

    async def wrapped(*args, **kwargs):
        return ["row"] if kind == "fetch" else None

    wrapper = asyncpg_instr._make_wrapper(kind)
    asyncio.run(wrapper(
        wrapped, instance=_Connection(),
        args=("SELECT $1", 42), kwargs={},
    ))
    operation = f"asyncpg.connection.Connection.{kind}"
    assert ("event_start", "root", operation) in rec.events
    assert ("event_end", "root", operation) in rec.events
    sql_events = [e for e in rec.events if e[0] == "sql"]
    assert sql_events
    assert sql_events[-1][2] == "SELECT $1"
    assert "(42,)" in sql_events[-1][3]


def test_async_wrapper_omits_bind_values_by_default(sql_push_span, sql_bind_values_off):
    """Secure default: the CQL/SQL template is recorded, the positional bound
    values ($1, ...) are not."""
    sp, rec = sql_push_span

    async def wrapped(*args, **kwargs):
        return None

    wrapper = asyncpg_instr._make_wrapper("execute")
    asyncio.run(wrapper(
        wrapped, instance=_Connection(),
        args=("UPDATE users SET token=$1 WHERE id=$2", "secret-token", 7),
        kwargs={},
    ))
    sql_events = [e for e in rec.events if e[0] == "sql"]
    assert sql_events
    assert sql_events[-1][2] == "UPDATE users SET token=$1 WHERE id=$2"
    assert sql_events[-1][3] == ""
    assert "secret-token" not in sql_events[-1][3]


def test_async_wrapper_records_exception(sql_push_span):
    sp, rec = sql_push_span

    async def boom(*_a, **_kw):
        raise RuntimeError("kaput")

    wrapper = asyncpg_instr._make_wrapper("execute")
    with pytest.raises(RuntimeError, match="kaput"):
        asyncio.run(wrapper(
            boom, instance=_Connection(),
            args=("UPDATE foo",), kwargs={},
        ))
    assert any(e[0] == "event_error" for e in rec.events)


def test_async_wrapper_no_active_span_passes_through():
    """No current span → no event, just delegate."""
    async def wrapped(*_a, **_kw):
        return "ok"

    wrapper = asyncpg_instr._make_wrapper("fetchval")
    out = asyncio.run(wrapper(
        wrapped, instance=_Connection(),
        args=("SELECT 1",), kwargs={},
    ))
    assert out == "ok"


def test_async_wrapper_unsampled_span_passes_through_without_event():
    from pinpoint.agent import UnSampledSpan  # type: ignore[attr-defined]

    rec = _fakes.Recorder()
    sp = UnSampledSpan(object())
    token = ppctx.set_current_span(sp)
    try:
        async def wrapped(*_a, **_kw):
            return "ok"

        wrapper = asyncpg_instr._make_wrapper("fetchval")
        out = asyncio.run(wrapper(
            wrapped, instance=_Connection(),
            args=("SELECT $1", "x" * 5000), kwargs={},
        ))
        assert out == "ok"
        assert not [e for e in rec.events if e[0] in {"event_start", "sql"}]
    finally:
        ppctx.reset_current_span(token)


def test_async_wrapper_suppressed_when_sqlalchemy_owns_statement(sql_push_span):
    """Driver-event suppression must reach asyncpg too. When an outer layer
    already traces the statement (SQLAlchemy's async engine over
    ``postgresql+asyncpg`` sets ``_in_sqlalchemy`` in the shared greenlet
    context, or an ``executemany`` loop), the asyncpg wrapper must open no
    event; unsuppressed, it opens exactly one. Without this the same SQL is
    double-traced — one SQLAlchemy event and one asyncpg event."""
    sp, rec = sql_push_span

    async def wrapped(*_a, **_kw):
        return "ok"

    wrapper = asyncpg_instr._make_wrapper("execute")

    # (1) Suppressed: an outer layer already owns this statement's trace.
    token = dbapi_instr.enter_sqlalchemy_execute()
    try:
        out = asyncio.run(wrapper(
            wrapped, instance=_Connection(), args=("SELECT 1",), kwargs={},
        ))
    finally:
        dbapi_instr.exit_sqlalchemy_execute(token)
    assert out == "ok"
    assert not [e for e in rec.events if e[0] in {"event_start", "sql"}], (
        "asyncpg opened a second event while suppressed → statement double-traced")

    # (2) Not suppressed: raw asyncpg usage still traces exactly once.
    out = asyncio.run(wrapper(
        wrapped, instance=_Connection(), args=("SELECT 2",), kwargs={},
    ))
    assert out == "ok"
    starts = [e for e in rec.events if e[0] == "event_start"]
    assert len(starts) == 1
    assert starts[0][2] == "asyncpg.connection.Connection.execute"


# ---------------------------------------------------------------------------
# Synchronous cursor() factory
# ---------------------------------------------------------------------------

def test_cursor_factory_wrapper_is_synchronous(sql_push_span):
    """Connection.cursor returns a CursorFactory synchronously — the
    wrapper is sync too. Ensure it produces a span event named
    ``asyncpg.cursor``."""
    sp, rec = sql_push_span

    def wrapped(*args, **kwargs):
        return "factory-object"

    wrapper = asyncpg_instr._make_wrapper("cursor")
    out = wrapper(
        wrapped, instance=_Connection(),
        args=("SELECT * FROM t",), kwargs={},
    )
    assert out == "factory-object"
    assert ("event_start", "root", "asyncpg.connection.Connection.cursor") in rec.events
    assert ("event_end", "root", "asyncpg.connection.Connection.cursor") in rec.events


def test_cursor_factory_wrapper_suppressed_when_sqlalchemy_owns_statement(sql_push_span):
    """``_trace_sync`` (the synchronous cursor() factory) has its OWN
    driver-event suppression branch, mirroring the async one. SQLAlchemy's async
    engine opens a server-side cursor for a ``.stream()`` query with
    ``_in_sqlalchemy`` already set — the asyncpg cursor factory must then open no
    event; unsuppressed it opens exactly one. Only the async wrapper's
    suppression was covered before, so a regression dropping the check from
    ``_trace_sync`` would double-trace every streamed query."""
    sp, rec = sql_push_span
    wrapper = asyncpg_instr._make_wrapper("cursor")

    def wrapped(*_a, **_kw):
        return "factory-object"

    # (1) Suppressed: SQLAlchemy already owns this statement's trace.
    token = dbapi_instr.enter_sqlalchemy_execute()
    try:
        out = wrapper(wrapped, instance=_Connection(),
                      args=("SELECT * FROM t",), kwargs={})
    finally:
        dbapi_instr.exit_sqlalchemy_execute(token)
    assert out == "factory-object"
    assert not [e for e in rec.events if e[0] in {"event_start", "sql"}], (
        "asyncpg cursor factory opened an event while suppressed "
        "→ streamed statement double-traced")

    # (2) Not suppressed: raw asyncpg cursor() usage still traces exactly once.
    out = wrapper(wrapped, instance=_Connection(),
                  args=("SELECT * FROM t",), kwargs={})
    assert out == "factory-object"
    starts = [e for e in rec.events if e[0] == "event_start"]
    assert len(starts) == 1
    assert starts[0][2] == "asyncpg.connection.Connection.cursor"


# ---------------------------------------------------------------------------
# Connection metadata extraction
# ---------------------------------------------------------------------------

def test_connection_addr_handles_tcp_tuple():
    conn = _Connection(host="db.test", port=5432)
    assert asyncpg_instr._connection_addr(conn) == ("db.test", 5432)


def test_connection_addr_handles_unix_socket_string():
    """asyncpg sets ``_addr`` to a path string for Unix domain sockets."""
    class _Conn:
        _addr = "/tmp/.s.PGSQL.5432"

    assert asyncpg_instr._connection_addr(_Conn()) == ("/tmp/.s.PGSQL.5432", 0)


def test_connection_addr_handles_missing():
    class _Conn: pass
    assert asyncpg_instr._connection_addr(_Conn()) == ("", 0)


def test_connection_database_reads_from_params():
    assert asyncpg_instr._connection_database(_Connection(database="orders")) == "orders"


def test_connection_database_handles_missing_params():
    class _Conn: pass
    assert asyncpg_instr._connection_database(_Conn()) == ""
