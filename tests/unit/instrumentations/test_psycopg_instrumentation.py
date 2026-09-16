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

"""psycopg2 / psycopg3 instrumentation.

Verifies the instrumentor wires up the right targets:
- psycopg2.connect (connect-level hook — the C-extension cursor class
  rejects monkeypatching, so cursors are traced via a traced
  ``cursor_factory`` installed on the raw connection)
- psycopg.Cursor (sync, psycopg3)
- psycopg.AsyncCursor (async, psycopg3)
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from unittest import mock

import pytest
import wrapt

from pinpoint.instrumentations import psycopg as psycopg_instr
from pinpoint.service_type import SERVICE_TYPE_POSTGRESQL

try:
    import psycopg2  # noqa: F401

    _HAS_PSYCOPG2 = True
except Exception:  # noqa: BLE001
    _HAS_PSYCOPG2 = False


def test_psycopg2_instrumentor_wraps_connect_only():
    """The psycopg2 instrumentor installs the connect-level hook (its C
    cursor class can't be monkeypatched) and must NOT touch the psycopg3
    (``psycopg``) cursor helpers — doing so would eagerly import the sibling
    driver the application never asked for."""
    with mock.patch.object(psycopg_instr, "wrap_cursor_class") as sync_wrap, \
         mock.patch.object(psycopg_instr, "wrap_async_cursor_class") as async_wrap, \
         mock.patch.object(psycopg_instr, "wrap", return_value=True) as connect_wrap:
        psycopg_instr.Psycopg2Instrumentor()._instrument()

    # Installed through _util.wrap like every other pinpoint patch (safe_wrapper
    # guard, ownership stamp, uninstall registration).
    assert [c.args for c in connect_wrap.call_args_list] == [
        ("psycopg2", "connect", psycopg_instr._psycopg2_connect_wrapper)]
    # No sibling (psycopg3) driver import.
    assert not sync_wrap.call_args_list
    assert not async_wrap.call_args_list


def test_psycopg3_instrumentor_wraps_cursors_only():
    """The psycopg3 instrumentor wraps the sync + async cursor classes and
    must NOT reach for psycopg2's connect (sibling driver)."""
    with mock.patch.object(psycopg_instr, "wrap_cursor_class") as sync_wrap, \
         mock.patch.object(psycopg_instr, "wrap_async_cursor_class") as async_wrap, \
         mock.patch.object(psycopg_instr, "wrap") as connect_wrap:
        psycopg_instr.Psycopg3Instrumentor()._instrument()

    # No sibling (psycopg2) connect wrap.
    assert not connect_wrap.call_args_list

    sync_calls = sync_wrap.call_args_list
    async_calls = async_wrap.call_args_list

    sync_targets = [(call.args, call.kwargs) for call in sync_calls]
    assert (("psycopg", "Cursor"),) == (sync_targets[0][0],)
    assert sync_targets[0][1]["service_type"] == SERVICE_TYPE_POSTGRESQL

    assert async_calls
    assert async_calls[0].args == ("psycopg", "AsyncCursor")
    assert async_calls[0].kwargs["service_type"] == SERVICE_TYPE_POSTGRESQL


def test_psycopg2_wrap_failure_does_not_mark_installed():
    """If the connect wrap fails (target not yet present), the global guard
    must NOT record the instrumentor as installed — otherwise a later hook
    fire (once ``connect`` exists) would be locked out and never retried."""
    import pinpoint.instrumentor as instrumentor_mod
    from pinpoint.instrumentations.psycopg import Psycopg2Instrumentor

    instrumentor_mod._global_installed.pop(Psycopg2Instrumentor, None)
    try:
        # _util.wrap reports a target it could not install as False.
        with mock.patch.object(psycopg_instr, "wrap", return_value=False):
            Psycopg2Instrumentor().instrument()
        assert Psycopg2Instrumentor not in instrumentor_mod._global_installed

        # A subsequent trigger, now that the target resolves, succeeds.
        with mock.patch.object(psycopg_instr, "wrap", return_value=True) as connect_wrap:
            Psycopg2Instrumentor().instrument()
        assert connect_wrap.call_args_list
        assert Psycopg2Instrumentor in instrumentor_mod._global_installed
    finally:
        instrumentor_mod._global_installed.pop(Psycopg2Instrumentor, None)


_SAMPLE_ROWS = [("r1",), ("r2",)]


class _FakeCursor:
    """Stand-in for psycopg2's C cursor: its own iterator, records execute,
    and — like the real C cursor — implements the ``fetch*`` family plus the
    context-manager protocol so we can prove the traced subclass inherits
    them unchanged (no per-row Python frame)."""

    def __init__(self):
        self.executed = []
        self.closed = False
        self._rows = iter(list(_SAMPLE_ROWS))

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return "ok"

    def fetchone(self):
        return next(self._rows, None)

    def fetchall(self):
        return list(self._rows)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.closed = True
        return False


class _DictLikeCursor(_FakeCursor):
    """A distinct user-supplied factory (mirrors ``DictCursor``) so we can
    check a custom connect-time ``cursor_factory`` is still traced and its
    base behaviour preserved."""


class _FakeConnection:
    """Stand-in that honours the settable ``cursor_factory`` attribute the
    way psycopg2's C connection does."""

    def __init__(self, cursor_factory=_FakeCursor):
        # Simulate a connection with a user-configured cursor factory (the
        # branch our wrapper can exercise without real psycopg2 installed).
        self.cursor_factory = cursor_factory

    def cursor(self, *a, **kw):
        factory = kw.get("cursor_factory") or self.cursor_factory
        return factory()


def test_psycopg2_connect_wrapper_returns_raw_connection():
    """The wrapper must return the *genuine* connection object — never a
    wrapt proxy — so psycopg2's C-level ``PyObject_TypeCheck`` (used by
    ``register_default_jsonb`` / ``register_uuid`` / ``quote_ident`` during
    Django & SQLAlchemy connection setup) still passes."""
    raw = _FakeConnection()

    def _connect(*a, **kw):
        return raw

    conn = psycopg_instr._psycopg2_connect_wrapper(_connect, None, (), {})

    # Same object identity — what any psycopg2 C API would receive.
    assert conn is raw
    assert not isinstance(conn, wrapt.ObjectProxy)

    # cursor_factory now points at a traced *subclass* of the original.
    traced_factory = conn.cursor_factory
    assert traced_factory is not _FakeCursor
    assert issubclass(traced_factory, _FakeCursor)
    assert getattr(traced_factory, "__pinpoint_traced_cursor__", False)


def test_psycopg2_traced_cursor_traces_and_supports_next():
    """cursor() yields the traced subclass: execute() is recorded (and
    transparently passes through with no span current) and ``next(cur)``
    still works — a wrapt ObjectProxy cursor would break that."""
    raw = _FakeConnection()

    def _connect(*a, **kw):
        return raw

    conn = psycopg_instr._psycopg2_connect_wrapper(_connect, None, (), {})
    cur = conn.cursor()

    assert isinstance(cur, _FakeCursor)
    # No span current → transparent passthrough, execute still delegates.
    assert cur.execute("SELECT 1") == "ok"
    assert cur.executed == [("SELECT 1", None)]
    # next(cur) works on the traced cursor (ObjectProxy lacked __next__).
    assert next(cur) == ("r1",)
    assert next(cur) == ("r2",)


def test_psycopg2_connect_time_custom_cursor_factory_is_traced():
    """A ``cursor_factory`` supplied at connect time (psycopg2 records it on
    ``connection.cursor_factory``) must be traced too — the traced factory is
    a subclass of the *user's* class, preserving its behaviour."""
    raw = _FakeConnection(cursor_factory=_DictLikeCursor)

    def _connect(*a, **kw):
        return raw

    conn = psycopg_instr._psycopg2_connect_wrapper(_connect, None, (), {})

    traced = conn.cursor_factory
    assert traced is not _DictLikeCursor
    assert issubclass(traced, _DictLikeCursor)
    assert getattr(traced, "__pinpoint_traced_cursor__", False)

    cur = conn.cursor()
    assert isinstance(cur, _DictLikeCursor)
    assert cur.execute("SELECT 1") == "ok"
    assert cur.executed == [("SELECT 1", None)]


class _SpecialCursor(_FakeCursor):
    """Mirrors a psycopg2.extras special cursor (RealDictCursor / DictCursor /
    NamedTupleCursor) — a distinct class the connection defaults to when no
    explicit ``cursor_factory`` is set."""


class _ConnFactoryConnection:
    """Mirrors ``psycopg2.extras.RealDictConnection`` & friends: its
    ``cursor_factory`` starts as ``None`` (the C-level default), and
    ``cursor()`` falls back to its own special cursor class via
    ``self.cursor_factory or _SpecialCursor`` — exactly the ``or`` that a plain
    traced factory would defeat."""

    def __init__(self):
        self.cursor_factory = None

    def cursor(self, *a, **kw):
        kw.setdefault("cursor_factory", self.cursor_factory or _SpecialCursor)
        return kw["cursor_factory"]()


def test_psycopg2_connection_factory_default_cursor_preserved():
    """Regression: a ``connection_factory``-style connection reports
    ``cursor_factory is None`` at connect time but defaults its cursors to a
    special class. Tracing must subclass *that* class — not the plain cursor —
    or ``self.cursor_factory or Special`` picks our plain factory and silently
    turns dict/namedtuple rows back into plain tuples."""
    raw = _ConnFactoryConnection()

    conn = psycopg_instr._psycopg2_connect_wrapper(
        lambda *a, **k: raw, None, (), {},
    )

    traced = conn.cursor_factory
    assert traced is not _SpecialCursor
    # The traced factory subclasses the connection's OWN default cursor, so the
    # ``self.cursor_factory or _SpecialCursor`` fallback keeps producing the
    # special cursor's rows (just traced).
    assert issubclass(traced, _SpecialCursor)
    assert getattr(traced, "__pinpoint_traced_cursor__", False)

    cur = conn.cursor()
    assert isinstance(cur, _SpecialCursor)
    assert cur.execute("SELECT 1") == "ok"


def test_psycopg2_cursor_probe_failure_leaves_connection_untraced():
    """If discovering the default cursor class fails (a custom connection whose
    ``cursor()`` needs args/state), the wrapper must leave the connection
    untouched rather than break it."""

    class _UnprobableConnection:
        def __init__(self):
            self.cursor_factory = None

        def cursor(self, *a, **kw):
            raise RuntimeError("cursor requires initialization")

    raw = _UnprobableConnection()
    conn = psycopg_instr._psycopg2_connect_wrapper(
        lambda *a, **k: raw, None, (), {},
    )
    # Never crashed the connect, and cursor_factory left as-is (untraced).
    assert conn is raw
    assert conn.cursor_factory is None


def test_psycopg2_traced_cursor_fetchone_loop_matches_base():
    """The traced subclass overrides only the execute family — ``fetchone``
    is inherited, so a ``while (row := cur.fetchone())`` loop yields exactly
    the rows the un-traced base cursor would, at native speed."""
    raw = _FakeConnection()

    def _connect(*a, **kw):
        return raw

    conn = psycopg_instr._psycopg2_connect_wrapper(_connect, None, (), {})
    cur = conn.cursor()

    fetched = []
    while (row := cur.fetchone()) is not None:
        fetched.append(row)
    assert fetched == _SAMPLE_ROWS
    # fetchone is not overridden — the traced subclass uses the base's own.
    assert "fetchone" not in vars(type(cur))


def test_psycopg2_traced_cursor_supports_context_manager():
    """``with conn.cursor() as cur:`` keeps working — __enter__/__exit__ come
    straight from the base cursor class, unchanged by tracing."""
    raw = _FakeConnection()

    def _connect(*a, **kw):
        return raw

    conn = psycopg_instr._psycopg2_connect_wrapper(_connect, None, (), {})
    with conn.cursor() as cur:
        assert isinstance(cur, _FakeCursor)
        assert cur.execute("SELECT 1") == "ok"
    assert cur.closed is True


def test_psycopg2_per_call_cursor_factory_is_not_traced():
    """Documented limitation: a per-call ``cursor(cursor_factory=X)`` override
    bypasses ``connection.cursor_factory`` and is NOT traced. Intercepting it
    would require subclassing the immutable C connection, which we reject to
    keep the returned connection the genuine object."""
    raw = _FakeConnection()

    def _connect(*a, **kw):
        return raw

    conn = psycopg_instr._psycopg2_connect_wrapper(_connect, None, (), {})
    cur = conn.cursor(cursor_factory=_DictLikeCursor)

    # Plain user class — not our traced subclass.
    assert type(cur) is _DictLikeCursor
    assert not getattr(type(cur), "__pinpoint_traced_cursor__", False)


def test_psycopg2_connect_wrapper_skips_when_agent_config_disabled():
    """When an agent exists but is configured off for good it can never open
    a span, so the wrapper must leave the host connection's cursor_factory
    untouched rather than mutate every connection for nothing."""
    raw = _FakeConnection()
    original_factory = raw.cursor_factory

    def _connect(*a, **kw):
        return raw

    class _DisabledConfig:
        enabled = False

    class _DisabledAgent:
        config = _DisabledConfig()

    with mock.patch(
        "pinpoint.agent.get_agent", return_value=_DisabledAgent(),
    ):
        conn = psycopg_instr._psycopg2_connect_wrapper(_connect, None, (), {})

    assert conn is raw
    assert raw.cursor_factory is original_factory  # never wrapped


@pytest.mark.skipif(not _HAS_PSYCOPG2, reason="psycopg2 not installed")
def test_psycopg2_traced_factory_is_subclass_of_c_cursor():
    """A cursor produced by the traced factory passes psycopg2's C-level
    ``isinstance(cur, psycopg2.extensions.cursor)`` — because the factory is
    a genuine subclass of the C cursor type (needs no live connection)."""
    import psycopg2.extensions as ext

    traced = psycopg_instr._traced_cursor_factory(ext.cursor)
    assert issubclass(traced, ext.cursor)
    assert getattr(traced, "__pinpoint_traced_cursor__", False)


def test_psycopg2_connect_wrapper_is_idempotent():
    """Re-running connect on a connection that already carries a traced
    factory must not wrap a traced factory inside another."""
    raw = _FakeConnection()

    def _connect(*a, **kw):
        return raw

    psycopg_instr._psycopg2_connect_wrapper(_connect, None, (), {})
    first = raw.cursor_factory
    psycopg_instr._psycopg2_connect_wrapper(_connect, None, (), {})
    assert raw.cursor_factory is first


def test_psycopg2_connect_wrapper_skips_async_connections():
    """aiopg drives the raw psycopg2 async connection itself (and has its
    own instrumentation) — those must not be proxied."""
    sentinel = object()

    def _connect(*a, **kw):
        return sentinel

    conn = psycopg_instr._psycopg2_connect_wrapper(
        _connect, None, (), {"async_": True},
    )
    assert conn is sentinel


def test_target_extraction_works_for_psycopg3_info_object():
    """psycopg3 exposes connection metadata via ``conn.info`` — verify the
    dbapi base picks it up so our integration doesn't need to override."""
    from pinpoint.instrumentations.dbapi import extract_default_target

    class _Info:
        host = "pg.test"
        port = 5432
        database = "shop"

    class _Psycopg3Conn:
        info = _Info()

    class _Cur:
        connection = _Psycopg3Conn()

    target = extract_default_target(_Cur())
    assert target is not None
    assert target.host == "pg.test"
    assert target.port == 5432
    assert target.database == "shop"


def test_target_extraction_works_for_psycopg2_dsn_parameters():
    """psycopg2 exposes DSN via ``conn.get_dsn_parameters()`` — same."""
    from pinpoint.instrumentations.dbapi import extract_default_target

    class _Psycopg2Conn:
        def get_dsn_parameters(self):
            return {"host": "pg2.test", "port": "5433", "dbname": "wallet"}

    class _Cur:
        connection = _Psycopg2Conn()

    target = extract_default_target(_Cur())
    assert target.host == "pg2.test"
    assert target.port == 5433
    assert target.database == "wallet"


@pytest.mark.skipif(not _HAS_PSYCOPG2, reason="psycopg2 not installed")
def test_init_before_import_wraps_psycopg2_connect():
    """Regression: the standard ordering — ``pinpoint.init()`` registers the
    post-import hooks, *then* ``import psycopg2`` happens later (e.g. a lazy
    Django worker import). ``psycopg2/__init__.py`` imports ``extensions``
    before it defines ``connect``, so a hook on ``extensions`` fired mid-import
    and silently failed. The hook is now on ``psycopg2`` itself, which fires
    only after the package finishes importing and ``connect`` exists.

    Runs in a fresh subprocess so psycopg2 is genuinely imported *after* the
    hook is registered (it is a C module already loaded in this process)."""
    script = textwrap.dedent(
        """
        import sys

        import wrapt

        from pinpoint.autoload import _REGISTRY, _make_hook

        assert "psycopg2" not in sys.modules, "psycopg2 imported too early"

        # Simulate pinpoint.init(): register the hook before the import.
        target = _REGISTRY["psycopg2"]
        wrapt.register_post_import_hook(_make_hook(target), "psycopg2")

        import psycopg2

        assert hasattr(psycopg2.connect, "__wrapped__"), (
            "psycopg2.connect was not wrapped after init-before-import ordering"
        )
        print("WRAPPED")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "WRAPPED" in result.stdout


def test_traced_cursor_factory_keeps_one_class_per_base_under_a_race(
        monkeypatch):
    """Two threads can both miss the cache and each build a subclass; the
    factory must still hand every caller the same class.

    ``cursor_factory`` identity is what the connection-level guard and psycopg2's
    own ``cursor_factory or <Special>`` fallback compare on, so two live
    subclasses for one base is exactly what this cache exists to prevent. The
    race is simulated deterministically: the stand-in cache publishes a rival
    class the instant our lookup misses, standing in for the other thread
    winning between the miss and the store.
    """
    class _Base:
        pass

    class _Rival:
        __pinpoint_traced_cursor__ = True

    class _RacingCache(dict):
        def get(self, key, default=None):
            found = super().get(key, default)
            if found is None:
                # The "other thread" publishes between our miss and our store.
                super().__setitem__(key, _Rival)
            return found

    monkeypatch.setattr(psycopg_instr, "_traced_cursor_cache", _RacingCache())

    assert psycopg_instr._traced_cursor_factory(_Base) is _Rival
    # And the loser's class is discarded, not left in the cache.
    assert psycopg_instr._traced_cursor_cache[_Base] is _Rival
    assert psycopg_instr._traced_cursor_factory(_Base) is _Rival
