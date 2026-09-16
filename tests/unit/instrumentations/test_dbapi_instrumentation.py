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

"""Generic DB-API base instrumentation.

Validates the trace logic that the per-driver modules depend on:
- Cursor target extraction (host/port/database from various connection
  shapes — direct attrs, ``info``, ``get_dsn_parameters``).
- SQL recording with mogrify when available, repr(params) otherwise.
- Wrapper installation via ``wrap_cursor_class``.

Tests use in-memory fakes: a fake cursor with mogrify, a fake connection
with various attribute layouts, and the in-memory span fakes shared with
the other instrumentation tests.
"""

from __future__ import annotations

import pytest

import _fakes
import pinpoint
from pinpoint import context as ppctx
from pinpoint.instrumentations import dbapi as dbapi_instr


# DB-API doubles -------------------------------------------------------------

class _FakeConn:
    def __init__(self, host="db.test", port=3306, database="appdb"):
        self.host = host
        self.port = port
        self.database = database


class _FakeCursor:
    def __init__(self, conn=None, mogrify_fn=None):
        self.connection = conn or _FakeConn()
        if mogrify_fn is not None:
            self.mogrify = mogrify_fn


# ---------------------------------------------------------------------------
# Target extraction
# ---------------------------------------------------------------------------

def test_extract_default_target_reads_direct_attrs():
    cursor = _FakeCursor(_FakeConn(host="h", port=1234, database="d"))
    target = dbapi_instr.extract_default_target(cursor)
    assert target.host == "h"
    assert target.port == 1234
    assert target.database == "d"


def test_extract_default_target_falls_back_to_get_dsn_parameters():
    """psycopg2-style connection that exposes DSN via a method."""
    class _Psycopg2Conn:
        def get_dsn_parameters(self):
            return {"host": "pg.test", "port": "5432", "dbname": "shop"}

    cursor = _FakeCursor(_Psycopg2Conn())
    target = dbapi_instr.extract_default_target(cursor)
    assert target.host == "pg.test"
    assert target.port == 5432
    assert target.database == "shop"


def test_extract_default_target_falls_back_to_info_object():
    """psycopg3-style connection that exposes attrs via ``info``."""
    class _Info:
        host = "pg3.test"
        port = 5433
        database = "wallet"

    class _Psycopg3Conn:
        info = _Info()

    cursor = _FakeCursor(_Psycopg3Conn())
    target = dbapi_instr.extract_default_target(cursor)
    assert target.host == "pg3.test"
    assert target.port == 5433
    assert target.database == "wallet"


def test_extract_default_target_info_object_dbname_only():
    """Real psycopg3 ``ConnectionInfo`` names the database ``dbname`` (libpq
    PQdb) and has no ``database`` attribute — the extractor must still
    resolve it."""
    class _Info:
        host = "pg3.test"
        port = 5433
        dbname = "wallet"

    class _Psycopg3Conn:
        info = _Info()

    cursor = _FakeCursor(_Psycopg3Conn())
    target = dbapi_instr.extract_default_target(cursor)
    assert target.database == "wallet"


def test_extract_default_target_no_connection_returns_none():
    class _NoConn: pass
    assert dbapi_instr.extract_default_target(_NoConn()) is None


def test_connection_target_caches_when_setattr_rejected(monkeypatch):
    """psycopg2's C ``connection`` rejects attribute assignment, so the
    stash-on-connection cache never sticks there — the weak-key fallback
    must keep the resolve at one per connection, not one per query."""
    class _SlottedConn:
        __slots__ = ("host", "port", "database", "__weakref__")

        def __init__(self):
            self.host = "pg.test"
            self.port = 5432
            self.database = "shop"

    calls = []
    real_resolve = dbapi_instr._resolve_connection_target

    def _counting_resolve(conn):
        calls.append(conn)
        return real_resolve(conn)

    monkeypatch.setattr(
        dbapi_instr, "_resolve_connection_target", _counting_resolve)
    conn = _SlottedConn()
    with pytest.raises(AttributeError):
        conn._pinpoint_target = None  # the premise: setattr rejected

    first = dbapi_instr._connection_target(conn)
    second = dbapi_instr._connection_target(conn)
    assert first is second
    assert len(calls) == 1
    assert first.host == "pg.test"


# ---------------------------------------------------------------------------
# Wrapper installation
# ---------------------------------------------------------------------------

def test_wrap_cursor_class_installs_hooks_on_module(sql_push_span, monkeypatch):
    """Build a fake module with a Cursor class and verify wrap_cursor_class
    installs working wrappers on it."""
    import sys
    fake_mod = type(sys)("fake_dbapi_drv")

    class Cursor:
        def __init__(self, conn):
            self.connection = conn
        def execute(self, sql, params=None):
            return f"executed:{sql}"
        def executemany(self, sql, seq):
            return f"executemany:{sql}:{len(seq)}"
        def callproc(self, name, args=None):
            return f"call:{name}"

    fake_mod.Cursor = Cursor
    monkeypatch.setitem(sys.modules, "fake_dbapi_drv", fake_mod)

    dbapi_instr.wrap_cursor_class(
        "fake_dbapi_drv", "Cursor", service_type=2101,
    )

    # Mock agent so the wrapper short-circuits "agent.enabled" check.
    class _Agent:
        enabled = True
    monkeypatch.setattr(pinpoint.agent, "_instance", _Agent())

    cursor = Cursor(_FakeConn())
    sp, rec = sql_push_span
    out = cursor.execute("SELECT 1")
    assert out == "executed:SELECT 1"
    assert ("event_start", "root", "fake_dbapi_drv.Cursor.execute") in rec.events
    assert ("event_end", "root", "fake_dbapi_drv.Cursor.execute") in rec.events


def test_wrap_cursor_class_is_idempotent_across_reinstrument(sql_push_span, monkeypatch):
    """Re-running ``wrap_cursor_class`` (e.g. instrument→uninstrument→instrument,
    or a partial-install retry) must not stack wrappers: a query still produces
    exactly one span event, not one per wrapper layer."""
    import sys
    fake_mod = type(sys)("fake_dbapi_reinstr")

    class Cursor:
        def __init__(self, conn):
            self.connection = conn
        def execute(self, sql, params=None):
            return f"executed:{sql}"

    fake_mod.Cursor = Cursor
    monkeypatch.setitem(sys.modules, "fake_dbapi_reinstr", fake_mod)

    # Install three times over — mimics repeated instrument() calls.
    for _ in range(3):
        dbapi_instr.wrap_cursor_class(
            "fake_dbapi_reinstr", "Cursor", service_type=2101,
        )

    class _Agent:
        enabled = True
    monkeypatch.setattr(pinpoint.agent, "_instance", _Agent())

    cursor = Cursor(_FakeConn())
    sp, rec = sql_push_span
    cursor.execute("SELECT 1")
    starts = [e for e in rec.events if e[0] == "event_start"]
    assert len(starts) == 1, f"expected 1 DB event after re-instrument, got {len(starts)}"
    assert starts[0][2] == "fake_dbapi_reinstr.Cursor.execute"


# ---------------------------------------------------------------------------
# SQL recording
# ---------------------------------------------------------------------------

def test_record_sql_uses_mogrify_when_available(sql_push_span, sql_bind_values_on):
    sp, rec = sql_push_span
    cursor = _FakeCursor(mogrify_fn=lambda sql, params: f"{sql} -- params={params!r}".encode())

    def wrapped(sql, params=None):
        return None

    wrapper = dbapi_instr._make_wrapper(
        "execute", 2101, dbapi_instr.extract_default_target,
        operation="mysql.execute",
    )
    wrapper(wrapped, cursor, ("SELECT %s", (1,)), {})
    sql_events = [e for e in rec.events if e[0] == "sql"]
    assert sql_events, "SQL event missing"
    sql_text = sql_events[-1][2]
    assert "params=(1,)" in sql_text


def test_record_sql_omits_bind_values_by_default(sql_push_span, sql_bind_values_off):
    """Secure default: with ``sql_trace_bind_values`` off the query TEMPLATE is
    recorded but the bound values never are — mogrify (which would bake the
    literals into the SQL text) is not even called, and params_text is empty.
    A value like an SSN must not reach the trace."""
    sp, rec = sql_push_span
    mogrify_called = []
    cursor = _FakeCursor(
        mogrify_fn=lambda sql, params: mogrify_called.append(1) or b"SHOULD-NOT-BE-USED"
    )

    def wrapped(sql, params=None):
        return None

    wrapper = dbapi_instr._make_wrapper(
        "execute", 2101, dbapi_instr.extract_default_target,
        operation="mysql.execute",
    )
    wrapper(
        wrapped, cursor,
        ("UPDATE users SET ssn=%s WHERE id=%s", ("123-45-6789", 7)), {},
    )
    sql_events = [e for e in rec.events if e[0] == "sql"]
    assert sql_events, "SQL event missing"
    sql_text, params_text = sql_events[-1][2], sql_events[-1][3]
    assert sql_text == "UPDATE users SET ssn=%s WHERE id=%s"
    assert params_text == ""
    assert "123-45-6789" not in sql_text
    assert not mogrify_called, "mogrify must not run when bind capture is off"


def test_sql_bind_values_enabled_reads_agent_config(monkeypatch):
    """The gate reflects ``Config.sql_trace_bind_values`` on the live agent."""
    from pinpoint.config import Config
    from pinpoint.instrumentations import _util

    class _Agent:
        def __init__(self, flag):
            self.config = Config(sql_trace_bind_values=flag)

    monkeypatch.setattr(pinpoint.agent, "_instance", _Agent(True))
    assert _util.sql_bind_values_enabled() is True

    monkeypatch.setattr(pinpoint.agent, "_instance", _Agent(False))
    assert _util.sql_bind_values_enabled() is False


def test_record_sql_falls_back_to_repr_params_without_mogrify(sql_push_span, sql_bind_values_on):
    sp, rec = sql_push_span
    cursor = _FakeCursor()  # no mogrify

    def wrapped(sql, params=None):
        return None

    wrapper = dbapi_instr._make_wrapper(
        "execute", 2501, dbapi_instr.extract_default_target,
        operation="psycopg.execute",
    )
    wrapper(wrapped, cursor, ("SELECT %s", (42,)), {})
    sql_events = [e for e in rec.events if e[0] == "sql"]
    assert sql_events
    sql, params_repr = sql_events[-1][2], sql_events[-1][3]
    assert sql == "SELECT %s"
    assert "(42,)" in params_repr


def test_record_sql_truncates_oversize_params(sql_push_span, sql_bind_values_on):
    sp, rec = sql_push_span
    cursor = _FakeCursor()

    def wrapped(sql, params=None):
        return None

    big = ("X" * 5000,)
    wrapper = dbapi_instr._make_wrapper(
        "execute", 2101, dbapi_instr.extract_default_target,
        operation="mysql.execute",
    )
    wrapper(wrapped, cursor, ("SELECT %s", big), {})
    sql_events = [e for e in rec.events if e[0] == "sql"]
    params_repr = sql_events[-1][3]
    assert len(params_repr) <= 1024
    assert "..." in params_repr


def test_query_unsampled_span_passes_through_without_event():
    from pinpoint.agent import UnSampledSpan  # type: ignore[attr-defined]

    rec = _fakes.Recorder()
    sp = UnSampledSpan(object())
    token = ppctx.set_current_span(sp)
    try:
        cursor = _FakeCursor()

        def wrapped(sql, params=None):
            return "ok"

        wrapper = dbapi_instr._make_wrapper(
            "execute", 2101, dbapi_instr.extract_default_target,
            operation="mysql.execute",
        )
        out = wrapper(wrapped, cursor, ("SELECT %s", ("x" * 5000,)), {})
        assert out == "ok"
        assert not [e for e in rec.events if e[0] in {"event_start", "sql"}]
    finally:
        ppctx.reset_current_span(token)


def test_extract_sql_and_params_reads_params_keyword():
    """psycopg3 and mysql-connector name the bind argument ``params``; it must
    be picked up like the positional form (was silently missed before)."""
    sql, params = dbapi_instr._extract_sql_and_params(
        "execute", ("SELECT %s",), {"params": (7,)},
    )
    assert sql == "SELECT %s"
    assert params == (7,)


def test_callproc_suppresses_inner_execute_event(sql_push_span, monkeypatch):
    """mysql-connector's ``callproc`` internally issues ``execute("SET ...")``
    to bind its parameters; the callproc wrapper must suppress that inner
    execute so one stored-proc call yields a single DB event, not two."""
    import sys
    fake_mod = type(sys)("fake_callproc_drv")

    class Cursor:
        def __init__(self, conn):
            self.connection = conn

        def execute(self, sql, params=None):
            return f"exec:{sql}"

        def callproc(self, name, args=None):
            # Mirror mysql-connector: bind args through an internal execute().
            self.execute("SET @_proc_arg0=%s", (args or [None])[:1])
            return f"call:{name}"

    fake_mod.Cursor = Cursor
    monkeypatch.setitem(sys.modules, "fake_callproc_drv", fake_mod)
    dbapi_instr.wrap_cursor_class(
        "fake_callproc_drv", "Cursor", service_type=2101,
    )

    class _Agent:
        enabled = True
    monkeypatch.setattr(pinpoint.agent, "_instance", _Agent())

    cursor = Cursor(_FakeConn())
    sp, rec = sql_push_span
    cursor.callproc("do_thing", [1])
    starts = [e for e in rec.events if e[0] == "event_start"]
    assert len(starts) == 1, f"expected 1 event for callproc, got {starts}"
    assert starts[0][2] == "fake_callproc_drv.Cursor.callproc"


def test_executemany_renders_first_row_with_count_marker(sql_push_span, sql_bind_values_on):
    sp, rec = sql_push_span
    cursor = _FakeCursor(mogrify_fn=lambda sql, p: f"{sql} {p!r}")

    def wrapped(sql, seq):
        return None

    wrapper = dbapi_instr._make_wrapper(
        "executemany", 2101, dbapi_instr.extract_default_target,
        operation="mysql.executemany",
    )
    wrapper(wrapped, cursor, ("INSERT %s", [(1,), (2,), (3,)]), {})
    sql_text = [e for e in rec.events if e[0] == "sql"][-1][2]
    assert "(1,)" in sql_text
    assert "+ 2 more rows" in sql_text


# ---------------------------------------------------------------------------
# executemany re-entrancy suppression
# ---------------------------------------------------------------------------

def test_executemany_loop_produces_single_event(sql_push_span, monkeypatch):
    """Many drivers implement ``executemany`` as a Python loop that calls
    ``self.execute`` per row. Since ``execute`` is also wrapped, each row would
    re-enter the wrapper and allocate a span event (→ unbounded memory). The
    executemany must produce exactly ONE DB event regardless of row count."""
    import sys
    fake_mod = type(sys)("fake_dbapi_many")

    class Cursor:
        def __init__(self, conn):
            self.connection = conn
        def execute(self, sql, params=None):
            return f"executed:{sql}"
        def executemany(self, sql, seq):
            # Driver-style fallback: loop calling the (wrapped) execute.
            for row in seq:
                self.execute(sql, row)
            return len(seq)

    fake_mod.Cursor = Cursor
    monkeypatch.setitem(sys.modules, "fake_dbapi_many", fake_mod)

    dbapi_instr.wrap_cursor_class(
        "fake_dbapi_many", "Cursor", service_type=2101,
    )

    class _Agent:
        enabled = True
    monkeypatch.setattr(pinpoint.agent, "_instance", _Agent())

    cursor = Cursor(_FakeConn())
    sp, rec = sql_push_span
    rows = [(i,) for i in range(1000)]
    cursor.executemany("UPDATE t SET x=1 WHERE id=%s", rows)

    starts = [e for e in rec.events if e[0] == "event_start"]
    assert len(starts) == 1, f"expected 1 DB event, got {len(starts)}"
    assert starts[0][2] == "fake_dbapi_many.Cursor.executemany"
    # Suppression is reset in finally — a subsequent execute traces normally.
    cursor.execute("SELECT 1")
    starts = [e for e in rec.events if e[0] == "event_start"]
    assert len(starts) == 2
    assert starts[1][2] == "fake_dbapi_many.Cursor.execute"


def test_in_executemany_flag_reset_after_exception(sql_push_span, monkeypatch):
    """If the executemany body raises, the suppression contextvar must still
    be reset so later queries on the same task trace normally."""
    import sys
    fake_mod = type(sys)("fake_dbapi_many_err")

    class Cursor:
        def __init__(self, conn):
            self.connection = conn
        def execute(self, sql, params=None):
            return "ok"
        def executemany(self, sql, seq):
            raise RuntimeError("boom")

    fake_mod.Cursor = Cursor
    monkeypatch.setitem(sys.modules, "fake_dbapi_many_err", fake_mod)

    dbapi_instr.wrap_cursor_class(
        "fake_dbapi_many_err", "Cursor", service_type=2101,
    )

    class _Agent:
        enabled = True
    monkeypatch.setattr(pinpoint.agent, "_instance", _Agent())

    cursor = Cursor(_FakeConn())
    sp, rec = sql_push_span
    with pytest.raises(RuntimeError, match="boom"):
        cursor.executemany("UPDATE t SET x=1 WHERE id=%s", [(1,)])
    assert dbapi_instr._in_executemany.get() is False
    cursor.execute("SELECT 1")
    assert any(
        e[0] == "event_start" and e[2].endswith("Cursor.execute")
        for e in rec.events
    )


def test_async_executemany_loop_produces_single_event(sql_push_span, monkeypatch):
    """Async drivers (aiomysql/aiopg) implement ``executemany`` as a coroutine
    that ``await``s ``self.execute`` per row. Because ``execute`` is also
    wrapped, each row would re-enter the wrapper and allocate a span event
    unless suppression survives the ``await``. The fix keys suppression on a
    *contextvar* (not a plain flag) precisely so it stays set across ``await``;
    the async executemany must emit exactly ONE DB event regardless of row
    count. A plain-flag regression (lost across the await) would emit 1+N."""
    import asyncio
    import sys
    fake_mod = type(sys)("fake_dbapi_async_many")

    class Cursor:
        def __init__(self, conn):
            self.connection = conn

        async def execute(self, sql, params=None):
            return f"executed:{sql}"

        async def executemany(self, sql, seq):
            # Driver-style fallback: a coroutine awaiting the (wrapped) execute.
            for row in seq:
                await self.execute(sql, row)
            return len(seq)

    fake_mod.Cursor = Cursor
    monkeypatch.setitem(sys.modules, "fake_dbapi_async_many", fake_mod)

    dbapi_instr.wrap_async_cursor_class(
        "fake_dbapi_async_many", "Cursor", service_type=2101,
    )

    class _Agent:
        enabled = True
    monkeypatch.setattr(pinpoint.agent, "_instance", _Agent())

    cursor = Cursor(_FakeConn())
    sp, rec = sql_push_span
    rows = [(i,) for i in range(1000)]
    asyncio.run(cursor.executemany("UPDATE t SET x=1 WHERE id=%s", rows))

    starts = [e for e in rec.events if e[0] == "event_start"]
    assert len(starts) == 1, f"expected 1 DB event, got {len(starts)}"
    assert starts[0][2] == "fake_dbapi_async_many.Cursor.executemany"
    # Suppression is reset in ``finally`` — a later execute traces normally.
    asyncio.run(cursor.execute("SELECT 1"))
    starts = [e for e in rec.events if e[0] == "event_start"]
    assert len(starts) == 2
    assert starts[1][2] == "fake_dbapi_async_many.Cursor.execute"


# ---------------------------------------------------------------------------
# Error recording
# ---------------------------------------------------------------------------

def test_query_records_exception_and_reraises(sql_push_span):
    sp, rec = sql_push_span
    cursor = _FakeCursor()

    def boom(sql, params=None):
        raise RuntimeError("kaput")

    wrapper = dbapi_instr._make_wrapper(
        "execute", 2101, dbapi_instr.extract_default_target,
        operation="mysql.execute",
    )
    with pytest.raises(RuntimeError, match="kaput"):
        wrapper(boom, cursor, ("SELECT 1",), {})
    assert any(e[0] == "event_error" for e in rec.events)
    assert ("event_end", "root", "mysql.execute") in rec.events


def test_failed_query_runs_once_through_safe_wrapper(sql_push_span):
    """A failing INSERT must execute exactly once. The query runs inside
    ``span_event_scope``, which re-raises, so the ``safe_wrapper`` fallback
    must NOT re-run it — that would duplicate the write and swallow the
    driver's original error."""
    from pinpoint.instrumentations._util import safe_wrapper

    sp, rec = sql_push_span
    cursor = _FakeCursor()
    calls = []

    def boom(sql, params=None):
        calls.append(sql)
        raise RuntimeError("duplicate key")

    shim = safe_wrapper(dbapi_instr._make_wrapper(
        "execute", 2101, dbapi_instr.extract_default_target,
        operation="mysql.execute",
    ))
    with pytest.raises(RuntimeError, match="duplicate key"):
        shim(boom, cursor, ("INSERT INTO t VALUES (1)",), {})

    assert calls == ["INSERT INTO t VALUES (1)"], \
        "failed query must run exactly once, not be retried"
    assert any(e[0] == "event_error" for e in rec.events)


def test_record_sql_never_hands_an_iterator_to_mogrify(sql_push_span, sql_bind_values_on):
    """A generator passed as params must not reach mogrify: drivers whose
    mogrify walks it (mysqlclient) would consume it, and the real execute
    would then run with an exhausted iterator (0 rows / StopIteration)."""
    sp, rec = sql_push_span
    seen = []

    def mogrify_fn(sql, params):
        seen.append(params)
        return sql.encode()

    cursor = _FakeCursor(mogrify_fn=mogrify_fn)
    rows = ((i,) for i in range(3))
    received = []

    def wrapped(sql, params=None):
        received.extend(params)

    wrapper = dbapi_instr._make_wrapper(
        "executemany", 2101, dbapi_instr.extract_default_target,
        operation="mysql.executemany",
    )
    wrapper(wrapped, cursor, ("INSERT INTO t VALUES (%s)", rows), {})
    assert seen == [], "mogrify must not see an iterator"
    assert received == [(0,), (1,), (2,)], "the driver must get every row"
    assert [e for e in rec.events if e[0] == "sql"], "SQL event missing"
