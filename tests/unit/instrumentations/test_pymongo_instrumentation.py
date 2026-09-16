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

"""pymongo instrumentation.

Drives the listener callbacks against in-memory event objects — no
pymongo dependency needed. Validates started → succeeded / failed
correlation by request_id, span event lifecycle, collection extraction
from command BSON, and address formatting.
"""

from __future__ import annotations

import pytest

import _fakes
from pinpoint import context as ppctx
from pinpoint.instrumentations import pymongo as pymongo_instr
from pinpoint.tracer import Span


# ---------------------------------------------------------------------------
# Fakes (span fakes + ``push_span`` fixture come from _fakes / conftest)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clear_inflight():
    pymongo_instr._inflight.clear()
    yield
    pymongo_instr._inflight.clear()


@pytest.fixture(autouse=True)
def _enable_listener(monkeypatch):
    """Tests drive the listener callbacks directly (no instrument() call),
    so flip the enabled gate the instrumentor would normally set."""
    monkeypatch.setattr(pymongo_instr, "_listener_enabled", True)


class _StartedEvent:
    """Minimal stand-in for pymongo.monitoring.CommandStartedEvent."""
    def __init__(self, command_name="find", database_name="appdb",
                 connection_id=("mongo.test", 27017),
                 command=None, request_id=42):
        self.command_name = command_name
        self.database_name = database_name
        self.connection_id = connection_id
        self.command = command if command is not None else {command_name: "users"}
        self.request_id = request_id


class _SucceededEvent:
    def __init__(self, request_id=42,
                 connection_id=("mongo.test", 27017)):
        self.request_id = request_id
        self.connection_id = connection_id


class _FailedEvent:
    def __init__(self, request_id=42, failure=None,
                 connection_id=("mongo.test", 27017)):
        self.request_id = request_id
        self.failure = failure
        self.connection_id = connection_id


# ---------------------------------------------------------------------------
# started → succeeded happy path
# ---------------------------------------------------------------------------

def test_started_then_succeeded_opens_and_closes_event(push_span):
    sp, rec = push_span
    started = _StartedEvent()
    pymongo_instr._on_started(started)
    key = pymongo_instr._event_key(started)
    assert key in pymongo_instr._inflight
    # The event is Python-buffered while open; it reaches the fake native
    # only once it ends.
    assert ("event_start", "root", "mongo.find") not in rec.events
    pymongo_instr._on_succeeded(_SucceededEvent())
    assert ("event_start", "root", "mongo.find") in rec.events
    assert ("event_end", "root", "mongo.find") in rec.events
    assert key not in pymongo_instr._inflight


# ---------------------------------------------------------------------------
# started → failed records error
# ---------------------------------------------------------------------------

def test_started_then_failed_with_exception_records_error(push_span):
    sp, rec = push_span
    pymongo_instr._on_started(_StartedEvent())
    err = RuntimeError("kaput")
    pymongo_instr._on_failed(_FailedEvent(failure=err))
    assert any(e[0] == "event_error" for e in rec.events)
    assert ("event_end", "root", "mongo.find") in rec.events


def test_started_then_failed_with_dict_records_string_error(push_span):
    """MongoDB sometimes returns command-level errors as a {errmsg, code}
    dict in the wire reply rather than throwing — surface as a string error."""
    sp, rec = push_span
    pymongo_instr._on_started(_StartedEvent())
    pymongo_instr._on_failed(_FailedEvent(failure={"errmsg": "Auth failed", "code": 18}))
    err_events = [e for e in rec.events if e[0] == "event_error"]
    assert err_events
    args = err_events[-1][2]
    assert "MongoCommandError" in args[0]


# ---------------------------------------------------------------------------
# Annotations
# ---------------------------------------------------------------------------

def test_started_annotates_collection_and_json(push_span, sql_bind_values_on):
    from pinpoint.annotation import (
        ANNOTATION_MONGO_COLLECTION_INFO,
        ANNOTATION_MONGO_JSON_DATA,
    )

    sp, rec = push_span
    event = _StartedEvent(
        command_name="insert",
        database_name="shop",
        command={"insert": "orders", "documents": [{"x": 1}]},
    )
    pymongo_instr._on_started(event)
    pymongo_instr._on_succeeded(_SucceededEvent(request_id=42))

    ev = sp._native.all_events[-1]
    entries = ev.annotations.entries

    # Collection annotation is the collection only — database is already on
    # the span event via set_destination and would be a duplicate here.
    assert ("str", ANNOTATION_MONGO_COLLECTION_INFO, "orders") in entries

    # JSON_DATA is a string-string slot: (marshaled JSON, "").
    json_entries = [e for e in entries
                    if e[0] == "strstr" and e[1] == ANNOTATION_MONGO_JSON_DATA]
    assert len(json_entries) == 1
    payload, second = json_entries[0][2]
    assert second == ""
    assert "insert" in payload and "orders" in payload


def test_started_omits_json_payload_by_default(push_span, sql_bind_values_off):
    """Secure default: the command's JSON payload (insert documents, query
    operands — the Mongo equivalent of SQL bind values) is NOT captured, while
    the operation name and collection annotations still are."""
    from pinpoint.annotation import (
        ANNOTATION_MONGO_COLLECTION_INFO,
        ANNOTATION_MONGO_JSON_DATA,
    )

    sp, rec = push_span
    event = _StartedEvent(
        command_name="insert",
        database_name="shop",
        command={"insert": "orders", "documents": [{"ssn": "123-45-6789"}]},
    )
    pymongo_instr._on_started(event)
    pymongo_instr._on_succeeded(_SucceededEvent(request_id=42))

    ev = sp._native.all_events[-1]
    entries = ev.annotations.entries
    # Collection info is still recorded — trace still shows what was queried.
    assert ("str", ANNOTATION_MONGO_COLLECTION_INFO, "orders") in entries
    # ...but the JSON payload (with the SSN) is not.
    json_entries = [e for e in entries
                    if e[0] == "strstr" and e[1] == ANNOTATION_MONGO_JSON_DATA]
    assert json_entries == []


def test_started_annotates_collection_option_when_command_exposes_it(push_span):
    from pinpoint.annotation import ANNOTATION_MONGO_COLLECTION_OPTION

    sp, _rec = push_span
    event = _StartedEvent(
        command_name="insert",
        database_name="shop",
        command={
            "insert": "orders",
            "documents": [{"x": 1}],
            "writeConcern": {"w": "majority"},
        },
    )
    pymongo_instr._on_started(event)
    pymongo_instr._on_succeeded(_SucceededEvent(request_id=42))

    ev = sp._native.all_events[-1]
    assert (
        "str",
        ANNOTATION_MONGO_COLLECTION_OPTION,
        '{"w": "majority"}',
    ) in ev.annotations.entries


def test_started_truncates_json_to_64k(push_span, sql_bind_values_on):
    """A pathologically large insert must not exceed the 64 KiB annotation cap."""
    from pinpoint.annotation import ANNOTATION_MONGO_JSON_DATA

    sp, rec = push_span
    big = {"insert": "events", "documents": [{"blob": "x" * 100_000}]}
    pymongo_instr._on_started(_StartedEvent(command_name="insert", command=big))
    pymongo_instr._on_succeeded(_SucceededEvent(request_id=42))

    ev = sp._native.all_events[-1]
    json_entries = [e for e in ev.annotations.entries
                    if e[0] == "strstr" and e[1] == ANNOTATION_MONGO_JSON_DATA]
    assert len(json_entries) == 1
    payload, _ = json_entries[0][2]
    assert len(payload.encode("utf-8")) <= 64 * 1024


def test_started_unsampled_span_is_no_op():
    from pinpoint.agent import UnSampledSpan  # type: ignore[attr-defined]

    rec = _fakes.Recorder()
    sp = UnSampledSpan(object())
    token = ppctx.set_current_span(sp)
    try:
        pymongo_instr._on_started(_StartedEvent(request_id=77))
        assert not pymongo_instr._inflight
        assert not [e for e in rec.events if e[0] == "event_start"]
    finally:
        ppctx.reset_current_span(token)


def test_marshal_command_limits_nested_documents_before_stringifying_all_items():
    class _BadString:
        def __str__(self):
            raise AssertionError("should not stringify truncated documents")

    command = {
        "insert": "events",
        "documents": [{"idx": i} for i in range(64)] + [{"bad": _BadString()}],
    }
    payload = pymongo_instr._marshal_command(command)
    assert "insert" in payload
    assert "bad" not in payload


def test_format_address_handles_tuple_string_and_none():
    assert pymongo_instr._format_address(("h", 27017)) == "h:27017"
    assert pymongo_instr._format_address("/var/run/mongo.sock") == "/var/run/mongo.sock"
    assert pymongo_instr._format_address(None) == ""


def test_extract_collection_pulls_value_of_command_key():
    assert pymongo_instr._extract_collection("find", {"find": "users"}) == "users"
    assert pymongo_instr._extract_collection("aggregate", {"aggregate": 1}) == ""
    assert pymongo_instr._extract_collection("", {}) == ""


def test_extract_collection_option_reads_visible_write_or_read_options():
    assert (
        pymongo_instr._extract_collection_option(
            {"insert": "orders", "writeConcern": {"w": "majority"}},
        )
        == '{"w": "majority"}'
    )
    assert (
        pymongo_instr._extract_collection_option(
            {"find": "users", "$readPreference": {"mode": "secondaryPreferred"}},
        )
        == "secondaryPreferred"
    )
    assert pymongo_instr._extract_collection_option({"find": "users"}) == ""


# ---------------------------------------------------------------------------
# No active span
# ---------------------------------------------------------------------------

def test_started_no_active_span_is_no_op():
    """Without a span, the listener shouldn't register an inflight entry."""
    pymongo_instr._on_started(_StartedEvent(request_id=99))
    assert not pymongo_instr._inflight


def test_same_request_and_connection_on_other_thread_cannot_end_foreign_event():
    """Request ids are connection-scoped and can collide. A terminal callback
    on another driver thread must only pop that thread's event."""
    import threading

    main_rec = _fakes.Recorder()
    main_span = Span(_fakes.FakeNativeSpan("main", recorder=main_rec))
    token = ppctx.set_current_span(main_span)
    started = _StartedEvent(request_id=7)
    try:
        pymongo_instr._on_started(started)
        main_key = pymongo_instr._event_key(started)

        worker_rec = _fakes.Recorder()

        def worker():
            worker_span = Span(_fakes.FakeNativeSpan("worker", recorder=worker_rec))
            worker_token = ppctx.set_current_span(worker_span)
            try:
                pymongo_instr._on_started(_StartedEvent(request_id=7))
                pymongo_instr._on_succeeded(_SucceededEvent(request_id=7))
            finally:
                ppctx.reset_current_span(worker_token)

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()

        assert main_key in pymongo_instr._inflight
        assert not any(e[0] == "event_end" for e in main_rec.events)
        assert any(e[0] == "event_end" for e in worker_rec.events)

        pymongo_instr._on_succeeded(_SucceededEvent(request_id=7))
        assert main_key not in pymongo_instr._inflight
        assert any(e[0] == "event_end" for e in main_rec.events)
    finally:
        ppctx.reset_current_span(token)


def test_inflight_table_is_bounded(push_span, monkeypatch):
    """A command whose terminal callback never arrives must not pin its span
    forever. Eviction only drops our reference — the event stays on its parent's
    stack, so the parent's end() still finalizes it, and ending it here could
    touch an event another thread owns."""
    monkeypatch.setattr(pymongo_instr, "_MAX_INFLIGHT", 4)
    span, _ = push_span

    for request_id in range(12):
        pymongo_instr._on_started(_StartedEvent(request_id=request_id))

    assert len(pymongo_instr._inflight) <= 4
    # The most recent commands are the ones still tracked.
    assert {key[0] for key in pymongo_instr._inflight} == {8, 9, 10, 11}
    # Nothing was ended out from under the parent span: it still owns them all.
    assert len(span._active_events) == 12


def test_succeeded_for_unknown_request_id_is_safe():
    pymongo_instr._on_succeeded(_SucceededEvent(request_id=12345))


def test_reinstrument_does_not_register_second_listener(monkeypatch):
    """pymongo has no monitoring.unregister — re-instrument must toggle the
    enabled flag instead of registering a duplicate listener (which would
    fire double callbacks and leak one span event per command).

    Driven against a fake ``pymongo.monitoring`` injected into ``sys.modules``
    so the test honors this file's "no pymongo dependency needed" contract:
    pymongo is an optional extra and absent in minimal environments, where a
    real ``import pymongo.monitoring`` here would ``ModuleNotFoundError`` instead
    of exercising the register-once/toggle logic."""
    import sys
    import types
    from pinpoint.instrumentations import pymongo as pymongo_instr

    registered = []
    fake_monitoring = types.ModuleType("pymongo.monitoring")
    fake_monitoring.register = lambda listener: registered.append(listener)
    fake_pymongo = types.ModuleType("pymongo")
    fake_pymongo.monitoring = fake_monitoring
    monkeypatch.setitem(sys.modules, "pymongo", fake_pymongo)
    monkeypatch.setitem(sys.modules, "pymongo.monitoring", fake_monitoring)
    monkeypatch.setattr(pymongo_instr, "_listener_registered", False)
    monkeypatch.setattr(pymongo_instr, "_listener_enabled", False)

    inst = pymongo_instr.PymongoInstrumentor()
    inst._instrument()
    assert len(registered) == 1
    assert pymongo_instr._listener_enabled is True

    inst._uninstrument()
    assert pymongo_instr._listener_enabled is False

    inst._instrument()
    # Re-enabled, but NOT registered a second time.
    assert len(registered) == 1
    assert pymongo_instr._listener_enabled is True


def test_started_is_noop_while_uninstrumented(push_span, monkeypatch):
    from pinpoint.instrumentations import pymongo as pymongo_instr

    monkeypatch.setattr(pymongo_instr, "_listener_enabled", False)
    sp, rec = push_span
    pymongo_instr._on_started(_StartedEvent())
    assert not [e for e in rec.events if e[0] == "event_start"]
