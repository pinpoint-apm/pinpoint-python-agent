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

"""SQLAlchemy + driver-level DBAPI instrumentation double-trace suppression.

SQLAlchemy's ``before_cursor_execute`` opens a span event, and if the
underlying driver's cursor is *also* instrumented (via the generic dbapi
``wrap_cursor_class``), that wrapper would open a second, nested event for the
same statement. These tests assert a statement driven through SQLAlchemy is
traced exactly once, while raw DBAPI usage (no SQLAlchemy) is still traced.

SQLAlchemy itself is not a test dependency: the ``_before`` / ``_after`` /
``_on_error`` engine-event callbacks are plain functions that only import
``sqlalchemy`` at *install* time, so they can be driven directly with fake
connection/cursor/execution-context objects.
"""

from __future__ import annotations

import pytest

import _fakes
import pinpoint
from pinpoint import context as ppctx
from pinpoint.instrumentations import dbapi as dbapi_instr
from pinpoint.instrumentations import sqlalchemy as sqla_instr
from pinpoint.tracer import Span


@pytest.fixture(autouse=True)
def _enabled_agent(monkeypatch):
    """The dbapi wrapper short-circuits on ``agent.enabled``."""
    class _Agent:
        enabled = True
    monkeypatch.setattr(pinpoint.agent, "_instance", _Agent())


class _FakeConn:
    def __init__(self, host="db.test", port=3306, database="appdb"):
        self.host = host
        self.port = port
        self.database = database


class _FakeCtx:
    """Stand-in for SQLAlchemy's ExecutionContext — the callbacks only stash
    an attribute on it, so a bare object suffices."""


def _install_fake_driver(monkeypatch, name):
    """Register a fake DBAPI driver module and wrap its Cursor, returning the
    Cursor class. Wrapped ``execute`` records a driver-level DB event."""
    import sys
    fake_mod = type(sys)(name)

    class Cursor:
        def __init__(self, conn):
            self.connection = conn
        def execute(self, sql, params=None):
            return f"executed:{sql}"
        def executemany(self, sql, seq):
            for row in seq:
                self.execute(sql, row)
            return len(seq)

    fake_mod.Cursor = Cursor
    monkeypatch.setitem(sys.modules, name, fake_mod)
    dbapi_instr.wrap_cursor_class(
        name, "Cursor", service_type=2101,
    )
    return Cursor


def _simulate_orm_statement(cursor, sql, *, error=None):
    """Drive one statement the way SQLAlchemy's engine does: fire
    ``before_cursor_execute``, run the (instrumented) driver ``execute``, then
    fire ``after_cursor_execute`` (or ``handle_error`` on failure)."""
    ctx = _FakeCtx()
    conn = cursor.connection
    sqla_instr._before(conn, cursor, sql, None, ctx, False)
    try:
        result = cursor.execute(sql)
    except Exception as exc:  # noqa: BLE001
        err_ctx = type("Err", (), {
            "execution_context": ctx,
            "original_exception": error or exc,
        })()
        sqla_instr._on_error(err_ctx)
        raise
    sqla_instr._after(conn, cursor, sql, None, ctx, False)
    return result


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_orm_statement_over_instrumented_driver_traces_once(sql_push_span, monkeypatch):
    """A statement executed through SQLAlchemy over an instrumented DBAPI
    driver must yield exactly ONE DB span event — SQLAlchemy's — not a second
    nested one from the driver wrapper."""
    cursor = _install_fake_driver(monkeypatch, "fake_drv_once")(_FakeConn())
    sp, rec = sql_push_span

    out = _simulate_orm_statement(cursor, "SELECT 1")

    assert out == "executed:SELECT 1"
    starts = [e for e in rec.events if e[0] == "event_start"]
    assert len(starts) == 1, f"expected 1 DB event, got {len(starts)}: {starts}"
    assert starts[0][2] == "sqlalchemy.engine.Engine.before_cursor_execute"


def test_raw_dbapi_without_sqlalchemy_still_traces_once(sql_push_span, monkeypatch):
    """Raw driver usage (no SQLAlchemy wrapping the call) is unaffected: the
    driver wrapper still opens its single event."""
    cursor = _install_fake_driver(monkeypatch, "fake_drv_raw")(_FakeConn())
    sp, rec = sql_push_span

    cursor.execute("SELECT 2")

    starts = [e for e in rec.events if e[0] == "event_start"]
    assert len(starts) == 1
    assert starts[0][2] == "fake_drv_raw.Cursor.execute"


def test_suppression_resets_so_later_raw_query_traces(sql_push_span, monkeypatch):
    """After an ORM statement finishes, the suppression contextvar must be
    reset so a subsequent raw driver call on the same context traces normally."""
    cursor = _install_fake_driver(monkeypatch, "fake_drv_reset")(_FakeConn())
    sp, rec = sql_push_span

    _simulate_orm_statement(cursor, "SELECT 1")
    assert dbapi_instr._in_sqlalchemy.get() is False

    cursor.execute("SELECT 2")  # raw, outside SQLAlchemy
    starts = [e for e in rec.events if e[0] == "event_start"]
    assert len(starts) == 2
    assert starts[0][2] == "sqlalchemy.engine.Engine.before_cursor_execute"
    assert starts[1][2] == "fake_drv_reset.Cursor.execute"


def test_suppression_resets_after_error(sql_push_span, monkeypatch):
    """If the statement raises, ``handle_error`` must still reset suppression
    and there must be exactly one (errored) DB event."""
    cursor = _install_fake_driver(monkeypatch, "fake_drv_err")(_FakeConn())

    def boom(sql, params=None):
        raise RuntimeError("kaput")
    cursor.execute = boom  # type: ignore[method-assign]

    sp, rec = sql_push_span
    with pytest.raises(RuntimeError, match="kaput"):
        _simulate_orm_statement(cursor, "SELECT 1")

    assert dbapi_instr._in_sqlalchemy.get() is False
    starts = [e for e in rec.events if e[0] == "event_start"]
    assert len(starts) == 1
    assert any(e[0] == "event_error" for e in rec.events)


def test_executemany_through_sqlalchemy_traces_once(sql_push_span, monkeypatch):
    """An ORM ``executemany`` (driver loops calling the wrapped ``execute``)
    still collapses to a single SQLAlchemy event."""
    Cursor = _install_fake_driver(monkeypatch, "fake_drv_many")
    cursor = Cursor(_FakeConn())
    sp, rec = sql_push_span

    ctx = _FakeCtx()
    conn = cursor.connection
    sqla_instr._before(conn, cursor, "UPDATE t SET x=1 WHERE id=%s", None, ctx, True)
    cursor.executemany("UPDATE t SET x=1 WHERE id=%s", [(i,) for i in range(100)])
    sqla_instr._after(conn, cursor, "UPDATE t SET x=1 WHERE id=%s", None, ctx, True)

    starts = [e for e in rec.events if e[0] == "event_start"]
    assert len(starts) == 1, f"expected 1 DB event, got {len(starts)}"
    assert starts[0][2] == "sqlalchemy.engine.Engine.before_cursor_execute"


def test_before_failure_resets_suppression_so_later_raw_query_traces(sql_push_span, monkeypatch):
    """If the SQLAlchemy span event fails to open inside ``_before``,
    ``@safe_try`` swallows it — but the suppression flag must still be reset.
    Otherwise the ``enter_`` has no matching ``exit_``, ``_in_sqlalchemy``
    stays pinned ``True`` on this worker's context, and every later
    raw-driver query is silently untraced."""
    cursor = _install_fake_driver(monkeypatch, "fake_drv_before_boom")(_FakeConn())
    sp, rec = sql_push_span

    # Fail ONLY the SQLAlchemy event creation inside ``_before`` (event
    # creation is pure Python now, so inject at the wrapper seam); leave the
    # driver's own event path working so the later raw query still traces.
    real_new_event = Span.new_span_event

    def maybe_boom(self, operation, service_type=0):
        if operation == sqla_instr._OPERATION_CURSOR_EXECUTE:
            raise RuntimeError("span event creation failed")
        return real_new_event(self, operation, service_type=service_type)
    monkeypatch.setattr(Span, "new_span_event", maybe_boom)

    try:
        ctx = _FakeCtx()
        # ``_before`` is @safe_try: the failure is swallowed, it returns None.
        sqla_instr._before(cursor.connection, cursor, "SELECT 1", None, ctx, False)

        # The flag must NOT be pinned True despite the internal failure.
        assert dbapi_instr._in_sqlalchemy.get() is False
        assert dbapi_instr._driver_event_suppressed() is False

        # A subsequent raw driver query on the same context must still trace.
        cursor.execute("SELECT 2")
        starts = [e for e in rec.events if e[0] == "event_start"]
        assert len(starts) == 1, (
            f"raw query after a failed _before must trace once, got {starts}")
        assert starts[0][2] == "fake_drv_before_boom.Cursor.execute"
    finally:
        # Defensive: if the leak regresses, don't pin the contextvar True for
        # the rest of the process and cascade-fail sibling tests.
        dbapi_instr._in_sqlalchemy.set(False)


def test_before_stash_failure_ends_new_event_and_resets_suppression(sql_push_span):
    """A context that rejects the private stash must not strand the event."""
    sp, rec = sql_push_span
    cursor = type("Cursor", (), {"connection": _FakeConn()})()

    class _SlotsOnlyContext:
        __slots__ = ()

    sqla_instr._before(
        cursor.connection, cursor, "SELECT 1", None, _SlotsOnlyContext(), False,
    )

    assert dbapi_instr._in_sqlalchemy.get() is False
    lifecycle = [
        event[0] for event in rec.events
        if event[0] in ("event_start", "event_end")
    ]
    assert lifecycle == ["event_start", "event_end"]
    assert sp._active_events == []


def test_error_recording_failure_still_ends_event(sql_push_span):
    """A broken exception __str__ must not skip the last event cleanup path."""
    sp, rec = sql_push_span
    ctx = _FakeCtx()
    cursor = type("Cursor", (), {"connection": _FakeConn()})()
    sqla_instr._before(
        cursor.connection, cursor, "SELECT 1", None, ctx, False,
    )

    class _BrokenStrError(Exception):
        def __str__(self):
            raise RuntimeError("broken __str__")

    error_ctx = type("Err", (), {
        "execution_context": ctx,
        "original_exception": _BrokenStrError(),
    })()
    sqla_instr._on_error(error_ctx)

    assert dbapi_instr._in_sqlalchemy.get() is False
    lifecycle = [
        event[0] for event in rec.events
        if event[0] in ("event_start", "event_end")
    ]
    assert lifecycle == ["event_start", "event_end"]
    assert sp._active_events == []


def test_orm_statement_records_sql_endpoint_and_service_type(
    sql_push_span, monkeypatch, sql_bind_values_on,
):
    """The SQLAlchemy event suppresses the driver-level event, so it must
    itself carry what that event would have carried: the SQL text (with bind
    params), the connection endpoint/destination, and a per-dialect DB
    service type — otherwise ORM-driven queries appear as empty events with
    no DB node in the server map."""
    from pinpoint.service_type import SERVICE_TYPE_MYSQL

    cursor = _install_fake_driver(monkeypatch, "fake_drv_sql")(_FakeConn())
    sp, rec = sql_push_span

    url = type("Url", (), {"host": "db.test", "port": 3306, "database": "appdb"})()
    engine = type("Engine", (), {"url": url})()
    dialect = type("Dialect", (), {"name": "mysql"})()
    conn = type("Conn", (), {"engine": engine, "dialect": dialect})()

    captured_service_types = []
    real_new_event = sp._native.new_span_event

    def capture(operation, service_type=0):
        captured_service_types.append(service_type)
        return real_new_event(operation, service_type)
    monkeypatch.setattr(sp._native, "new_span_event", capture)

    ctx = _FakeCtx()
    sqla_instr._before(
        conn, cursor, "SELECT * FROM t WHERE id = %s", (42,), ctx, False,
    )
    cursor.execute("SELECT * FROM t WHERE id = %s")
    sqla_instr._after(
        conn, cursor, "SELECT * FROM t WHERE id = %s", (42,), ctx, False,
    )

    assert captured_service_types == [SERVICE_TYPE_MYSQL]
    sqls = [e for e in rec.events if e[0] == "sql"]
    assert len(sqls) == 1
    assert sqls[0][2] == "SELECT * FROM t WHERE id = %s"
    assert "42" in sqls[0][3]
    assert ("end_point", sqls[0][1], "db.test:3306") in rec.events
    assert ("destination", sqls[0][1], "appdb") in rec.events


def test_orm_executemany_records_first_row_and_count(
    sql_push_span, monkeypatch, sql_bind_values_on,
):
    """executemany parameters are summarized as first-row + row count, never
    dumped in full."""
    cursor = _install_fake_driver(monkeypatch, "fake_drv_sql_many")(_FakeConn())
    sp, rec = sql_push_span

    rows = [(i,) for i in range(500)]
    ctx = _FakeCtx()
    sqla_instr._before(
        cursor.connection, cursor, "UPDATE t SET x=%s", rows, ctx, True,
    )
    cursor.executemany("UPDATE t SET x=%s", rows)
    sqla_instr._after(
        cursor.connection, cursor, "UPDATE t SET x=%s", rows, ctx, True,
    )

    sqls = [e for e in rec.events if e[0] == "sql"]
    assert len(sqls) == 1
    assert sqls[0][2] == "UPDATE t SET x=%s"
    assert "...x500" in sqls[0][3]
    assert len(sqls[0][3]) <= 1100  # capped, not the full 500-row dump


def test_orm_statement_omits_bind_values_by_default(sql_bind_values_off):
    """Secure default: SQLAlchemy keeps the query template but does not send
    literal parameters unless value capture was explicitly enabled."""
    rec = _fakes.Recorder()
    event = _fakes.SqlFakeNativeSpanEvent("sqlalchemy", rec)

    sqla_instr._record_statement(
        event, "SELECT * FROM users WHERE token = %s", ("secret-token",))

    sqls = [entry for entry in rec.events if entry[0] == "sql"]
    assert sqls == [(
        "sql",
        "sqlalchemy",
        "SELECT * FROM users WHERE token = %s",
        "",
    )]


def test_unsampled_span_still_pairs_suppression(monkeypatch):
    """When the span is unsampled SQLAlchemy opens no event, but the
    suppression flag must still be set-and-reset (so it never leaks) and the
    driver wrapper opens nothing either."""
    from pinpoint.agent import UnSampledSpan  # type: ignore[attr-defined]

    rec = _fakes.Recorder()
    token = ppctx.set_current_span(UnSampledSpan(object()))
    try:
        cursor = _install_fake_driver(monkeypatch, "fake_drv_unsampled")(_FakeConn())
        _simulate_orm_statement(cursor, "SELECT 1")
        assert dbapi_instr._in_sqlalchemy.get() is False
        assert not [e for e in rec.events if e[0] == "event_start"]
    finally:
        ppctx.reset_current_span(token)


def test_leaked_suppression_flag_self_heals_on_the_next_statement(monkeypatch):
    """SQLAlchemy before 1.4.40 does not route a BaseException (gevent.Timeout,
    a cancellation) to handle_error, so neither reset hook runs and the flag is
    left set. Without a clear, the next statement's token would restore that
    stale True and pin suppression for the life of the worker — silently
    dropping every later raw-driver trace."""
    rec = _fakes.Recorder()
    span = Span(_fakes.FakeNativeSpan("root", recorder=rec))
    token = ppctx.set_current_span(span)
    try:
        cursor = _install_fake_driver(monkeypatch, "fake_drv_leak")(_FakeConn())
        # A statement whose after/handle_error never fires.
        sqla_instr._before(cursor.connection, cursor, "SELECT 1", None,
                           _FakeCtx(), False)
        assert dbapi_instr._in_sqlalchemy.get() is True

        _simulate_orm_statement(cursor, "SELECT 2")

        assert dbapi_instr._in_sqlalchemy.get() is False
    finally:
        ppctx.reset_current_span(token)


def test_driver_events_resume_after_a_leaked_suppression_flag(monkeypatch):
    """The point of the self-heal: raw-driver queries are traced again."""
    rec = _fakes.Recorder()
    span = Span(_fakes.FakeNativeSpan("root", recorder=rec))
    token = ppctx.set_current_span(span)
    try:
        cursor = _install_fake_driver(monkeypatch, "fake_drv_resume")(_FakeConn())
        sqla_instr._before(cursor.connection, cursor, "SELECT 1", None,
                           _FakeCtx(), False)
        _simulate_orm_statement(cursor, "SELECT 2")
        rec.events.clear()

        cursor.execute("SELECT 3")  # raw driver call, no SQLAlchemy around it

        assert [e for e in rec.events if e[0] == "event_start"]
    finally:
        ppctx.reset_current_span(token)
