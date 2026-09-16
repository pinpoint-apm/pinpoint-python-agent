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

"""cassandra-driver instrumentation.

Drives the wrappers against in-memory fakes — no cassandra-driver dep.
Validates execute / execute_async lifecycle, cluster endpoint extraction,
keyspace annotation, and Statement-class CQL extraction.
"""

from __future__ import annotations

import pytest

import _fakes
from pinpoint import context as ppctx
from pinpoint.instrumentations import cassandra as cassandra_instr


class _Cluster:
    def __init__(self, contact_points=("c1", "c2"), port=9042):
        self.contact_points = list(contact_points)
        self.port = port


class _Session:
    def __init__(self, keyspace="orders", cluster=None):
        self.keyspace = keyspace
        self.cluster = cluster or _Cluster()


# ---------------------------------------------------------------------------
# execute / execute_async wrappers
# ---------------------------------------------------------------------------

def test_execute_emits_event_with_keyspace_destination(sql_push_span, sql_bind_values_on):
    sp, rec = sql_push_span

    def wrapped(*args, **kwargs):
        return ["row"]

    out = cassandra_instr._execute_wrapper(
        wrapped, instance=_Session(),
        args=("SELECT * FROM users WHERE id = %s", (1,)),
        kwargs={},
    )
    assert out == ["row"]
    assert ("event_start", "root", "cassandra.cluster.Session.execute") in rec.events
    assert ("event_end", "root", "cassandra.cluster.Session.execute") in rec.events
    sql_events = [e for e in rec.events if e[0] == "sql"]
    assert sql_events
    sql_text, params_text = sql_events[-1][2], sql_events[-1][3]
    assert sql_text == "SELECT * FROM users WHERE id = %s"
    assert "(1,)" in params_text


def test_execute_omits_bind_values_by_default(sql_push_span, sql_bind_values_off):
    """Secure default: the CQL template is recorded, the bound values are not."""
    sp, rec = sql_push_span

    def wrapped(*args, **kwargs):
        return ["row"]

    cassandra_instr._execute_wrapper(
        wrapped, instance=_Session(),
        args=("INSERT INTO users (id, ssn) VALUES (%s, %s)", (1, "123-45-6789")),
        kwargs={},
    )
    sql_events = [e for e in rec.events if e[0] == "sql"]
    assert sql_events
    sql_text, params_text = sql_events[-1][2], sql_events[-1][3]
    assert sql_text == "INSERT INTO users (id, ssn) VALUES (%s, %s)"
    assert params_text == ""
    assert "123-45-6789" not in params_text


def test_execute_records_exception(sql_push_span):
    sp, rec = sql_push_span

    def boom(*_a, **_kw):
        raise RuntimeError("kaput")

    with pytest.raises(RuntimeError, match="kaput"):
        cassandra_instr._execute_wrapper(
            boom, instance=_Session(),
            args=("UPDATE foo",), kwargs={},
        )
    assert any(e[0] == "event_error" for e in rec.events)


def test_execute_async_uses_distinct_op_name(sql_push_span):
    sp, rec = sql_push_span

    def wrapped(*_a, **_kw):
        return "ResponseFuture"

    cassandra_instr._execute_async_wrapper(
        wrapped, instance=_Session(),
        args=("SELECT 1",), kwargs={},
    )
    assert ("event_start", "root", "cassandra.cluster.Session.execute_async") in rec.events


def test_execute_no_active_span_passes_through():
    """No current span → just delegate."""
    def wrapped(*_a, **_kw):
        return "ok"

    out = cassandra_instr._execute_wrapper(
        wrapped, instance=_Session(),
        args=("SELECT 1",), kwargs={},
    )
    assert out == "ok"


def test_execute_unsampled_span_passes_through_without_event():
    from pinpoint.agent import UnSampledSpan  # type: ignore[attr-defined]

    rec = _fakes.Recorder()
    sp = UnSampledSpan(object())
    token = ppctx.set_current_span(sp)
    try:
        def wrapped(*_a, **_kw):
            return "ok"

        out = cassandra_instr._execute_wrapper(
            wrapped, instance=_Session(),
            args=("SELECT * FROM users WHERE blob = ?", ("x" * 5000,)),
            kwargs={},
        )
        assert out == "ok"
        assert not [e for e in rec.events if e[0] in {"event_start", "sql"}]
    finally:
        ppctx.reset_current_span(token)


def test_execute_bounds_large_params_before_recording(sql_push_span):
    sp, rec = sql_push_span

    def wrapped(*_a, **_kw):
        return "ok"

    cassandra_instr._execute_wrapper(
        wrapped, instance=_Session(),
        args=("SELECT * FROM users WHERE blob = ?", ("x" * 5000,)),
        kwargs={},
    )
    sql_events = [e for e in rec.events if e[0] == "sql"]
    assert sql_events
    assert len(sql_events[-1][3]) <= 1024


# ---------------------------------------------------------------------------
# Cluster + Statement extraction helpers
# ---------------------------------------------------------------------------

def test_session_endpoint_joins_contact_points_and_port():
    session = _Session(cluster=_Cluster(("a", "b", "c"), 9043))
    assert cassandra_instr._resolve_session_endpoint(session) == ("a,b,c", 9043)


def test_session_endpoint_handles_no_cluster():
    class _NoCluster: pass
    assert cassandra_instr._resolve_session_endpoint(_NoCluster()) == ("", 0)


def test_cql_text_extracts_query_string_from_simple_statement():
    """SimpleStatement carries the CQL on .query_string."""
    class _SimpleStatement:
        query_string = "SELECT id FROM users"

    assert cassandra_instr._cql_text(_SimpleStatement()) == "SELECT id FROM users"


def test_cql_text_handles_plain_string():
    assert cassandra_instr._cql_text("SELECT 1") == "SELECT 1"


def test_cql_text_handles_none():
    assert cassandra_instr._cql_text(None) == ""


def test_cql_text_caps_repr_fallback_of_exotic_statement():
    """A statement lacking query_string/prepared_statement falls back to repr;
    a huge repr (e.g. an embedded multi-MB blob param) must be capped, not
    copied whole into set_sql_query."""
    class _ExoticStatement:
        def __repr__(self):
            return "Statement(" + "x" * 100000 + ")"

    text = cassandra_instr._cql_text(_ExoticStatement())
    assert len(text) <= cassandra_instr._CQL_MAX_LEN


def test_cql_text_caps_large_plain_string():
    text = cassandra_instr._cql_text("SELECT " + "a" * 100000)
    assert len(text) <= cassandra_instr._CQL_MAX_LEN


def test_cql_text_caps_large_query_string_of_simple_statement():
    """A ``SimpleStatement`` whose ``.query_string`` is large (e.g. a big inline
    batch) must be truncated too. The ``.query_string`` branch was only
    exercised with a short value, so a regression returning it uncapped —
    copying a multi-MB statement into ``set_sql_query`` — would go unnoticed."""
    class _SimpleStatement:
        query_string = "SELECT " + "a" * 100000

    text = cassandra_instr._cql_text(_SimpleStatement())
    assert len(text) <= cassandra_instr._CQL_MAX_LEN
    assert text.startswith("SELECT ")


def test_sync_execute_suppresses_nested_execute_async_event(sql_push_span):
    """cassandra-driver implements ``Session.execute()`` as
    ``execute_async(...).result()`` — the inner (also wrapped) call must
    not emit a second duplicate event."""
    sp, rec = sql_push_span
    session = _Session()

    def inner(*args, **kwargs):
        return "future"

    def wrapped_execute(*args, **kwargs):
        # Simulate the driver: execute() delegates to the *wrapped*
        # execute_async on the same session.
        cassandra_instr._execute_async_wrapper(
            inner, instance=session, args=args, kwargs=kwargs,
        )
        return ["row"]

    out = cassandra_instr._execute_wrapper(
        wrapped_execute, instance=session,
        args=("SELECT * FROM users", None), kwargs={},
    )
    assert out == ["row"]
    starts = [e for e in rec.events if e[0] == "event_start"]
    assert starts == [("event_start", "root", "cassandra.cluster.Session.execute")]


def test_direct_execute_async_still_emits_event(sql_push_span):
    """Suppression only applies inside a wrapped sync execute — direct
    ``execute_async`` calls keep their own event."""
    sp, rec = sql_push_span
    cassandra_instr._execute_async_wrapper(
        lambda *a, **kw: "future", instance=_Session(),
        args=("SELECT 1", None), kwargs={},
    )
    assert (
        "event_start", "root", "cassandra.cluster.Session.execute_async",
    ) in rec.events


def test_extract_cql_keeps_an_empty_batch_passed_by_keyword():
    """``BatchStatement`` defines ``__len__``; an empty one is falsy and used
    to fall through ``kwargs.get("query") or ...`` to None (regression)."""
    class _EmptyBatch:
        def __len__(self):
            return 0

    batch = _EmptyBatch()
    assert cassandra_instr._extract_cql((), {"query": batch}) is batch
    assert cassandra_instr._extract_cql((), {"statement": batch}) is batch
    assert cassandra_instr._extract_cql((batch,), {}) is batch
