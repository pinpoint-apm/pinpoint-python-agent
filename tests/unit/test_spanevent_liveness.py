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

"""Post-end liveness guards for ``SpanEvent``/``Span``.

Span events are pure Python: ``end()`` buffers the event's completed record
on the parent span exactly once, and every later recording call is a no-op
that never buffers again. ``Span.end()`` is the single native flush — it must
run exactly once, carrying the buffered event records in its ``span_events``
batch, and every native-touching span method after it must degrade to a no-op
instead of reaching the drained native span.

The fake native spans here deliberately expose no ``new_span_event`` so the
conftest bridge leaves the records on the production batch-flush path.
"""

from __future__ import annotations

import pytest

import threading

from _fakes import _REC_ANNOTATIONS

import pinpoint.http_helper as hh
from pinpoint.tracer import Span


class _RecordingNativeSpan:
    """Stand-in for ``_native.Span`` that records the one flush crossing."""

    def __init__(self):
        self.ended = 0
        self.errors = []
        self.async_spans = []
        self.flushed_annotations = ()
        self.flushed_span_events = ()

    def end_span_with_data(self, *args):
        self.ended += 1
        # Payload slots (see Span._end_locked): (..., error_verdicts,
        # annotations, span_events, logging).
        self.flushed_annotations = args[8]
        self.flushed_span_events = args[9]

    def set_error(self, *args):
        self.errors.append(args)

    def new_async_span(self, op, async_id, async_sequence):
        self.async_spans.append((op, async_id, async_sequence))
        return _RecordingNativeSpan()


class _BlockingNativeSpan(_RecordingNativeSpan):
    """Native stand-in that keeps a recording call in flight.

    Span events never cross the binding mid-span, so async-span creation is
    the only mid-life native crossing left — that is what blocks here."""

    def __init__(self):
        super().__init__()
        self.record_entered = threading.Event()
        self.release_record = threading.Event()
        self.end_entered = threading.Event()

    def new_async_span(self, op, async_id, async_sequence):
        self.record_entered.set()
        assert self.release_record.wait(timeout=2)
        return super().new_async_span(op, async_id, async_sequence)

    def end_span_with_data(self, *args):
        self.end_entered.set()
        super().end_span_with_data(*args)


def _make_event():
    native = _RecordingNativeSpan()
    span = Span(native)
    ev = span.new_span_event("q")
    return ev, span, native


# ---------------------------------------------------------------------------
# SpanEvent: end() buffers the completed record exactly once
# ---------------------------------------------------------------------------

def test_end_buffers_record_and_detaches():
    ev, span, _native = _make_event()
    ev.end()
    assert ev._ended is True
    assert ev._span is None
    assert span._active_events == []
    assert len(span._finished_events) == 1


def test_double_end_buffers_record_only_once():
    ev, span, _native = _make_event()
    ev.end()
    ev.end()
    ev.end()
    assert len(span._finished_events) == 1


def test_concurrent_end_buffers_record_only_once():
    ev, span, _native = _make_event()
    barrier = threading.Barrier(8)

    def ender():
        barrier.wait()
        ev.end()

    threads = [threading.Thread(target=ender) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(span._finished_events) == 1


def test_event_end_after_span_end_is_dropped():
    """A task-lifetime timer (or SIGTERM drain) may end the span while user
    code still holds an open event: the span's drain already flushed it, so
    the late end() must not buffer a second record anywhere."""
    ev, span, native = _make_event()
    span.end()
    assert len(native.flushed_span_events) == 1  # drained implicitly
    ev.end()
    assert span._finished_events == []


# ---------------------------------------------------------------------------
# SpanEvent: recording methods are safe no-ops after end()
# ---------------------------------------------------------------------------

def test_set_error_after_end_is_dropped():
    ev, span, _native = _make_event()
    ev.end()
    result = ev.set_error("BoomError", "late callback")
    assert result is ev
    assert ev._annotations is None
    assert span._finished_events[0][_REC_ANNOTATIONS] == ()


def test_set_error_with_exception_after_end_is_dropped():
    ev, span, _native = _make_event()
    ev.end()
    ev.set_error(ValueError("late"))
    assert span._finished_events[0][_REC_ANNOTATIONS] == ()


def test_set_sql_query_after_end_is_dropped():
    ev, span, _native = _make_event()
    ev.end()
    result = ev.set_sql_query("SELECT 1", "")
    assert result is ev
    assert span._finished_events[0][_REC_ANNOTATIONS] == ()


def test_recorders_buffer_and_flush_at_span_end():
    """The normal in-scope path: errors and SQL are buffered Python-side, not
    recorded per call, and the record flushes in the span-end batch."""
    ev, _span, native = _make_event()
    ev.set_error("Boom", "msg")
    ev.set_sql_query("SELECT 1")
    ev.end()
    assert native.ended == 0  # nothing crosses at event end
    _span.end()
    assert native.ended == 1
    (record,) = native.flushed_span_events
    assert record[_REC_ANNOTATIONS] == [
        (5, "Boom", "msg"),
        (4, "SELECT 1", ""),
    ]


def test_set_sql_query_buffers_typed_bind_values_until_end():
    ev, span, native = _make_event()
    bind_values = [None, True, -7, 2 ** 40, 1.5, "hello"]

    ev.set_sql_query("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)", bind_values)
    # Snapshotted at call time: mutating the caller's list afterwards must
    # not change what the flush records.
    bind_values.append("late-mutation")
    ev.end()
    span.end()

    (record,) = native.flushed_span_events
    assert record[_REC_ANNOTATIONS] == [(
        4, "INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)",
        (None, True, -7, 2 ** 40, 1.5, "hello"),
    )]


def test_set_sql_query_freezes_mutable_binds_at_call_time():
    """The flush moved ``str(value)`` from the call to span end, so anything
    mutable would report the state it has *then*. A dict of named params
    reused across a request's queries is the case that bites: without a
    call-time snapshot every query reports the last one's values."""
    ev, span, native = _make_event()
    params = {"id": 1}

    ev.set_sql_query("SELECT * FROM t WHERE id = %(id)s", params)
    params["id"] = 999                      # the caller reuses the dict
    ev.end()
    span.end()

    (record,) = native.flushed_span_events
    assert record[_REC_ANNOTATIONS] == [
        (4, "SELECT * FROM t WHERE id = %(id)s", "{'id': 1}"),
    ]


def test_set_sql_query_freezes_mutable_bind_inside_a_sequence():
    """Same for a non-scalar *element*: ``tuple(args)`` snapshots the list's
    shape, not the objects in it."""
    class _Param:
        def __init__(self):
            self.value = "first"

        def __str__(self):
            return self.value

    ev, span, native = _make_event()
    param = _Param()

    ev.set_sql_query("SELECT ?, ?", [param, 7])
    param.value = "second"
    ev.end()
    span.end()

    (record,) = native.flushed_span_events
    # The object became its text; the int stayed typed for the native side.
    assert record[_REC_ANNOTATIONS] == [(4, "SELECT ?, ?", ("first", 7))]


def test_set_sql_query_bind_with_broken_str_degrades():
    """``str()`` now runs on the query path, so a value whose ``__str__``
    raises must degrade exactly as the native fallback did, not surface in the
    caller's ``execute()``."""
    class _BrokenStr:
        def __str__(self):
            raise RuntimeError("no text form")

    ev, span, native = _make_event()
    ev.set_sql_query("SELECT ?", [_BrokenStr()])
    ev.end()
    span.end()

    (record,) = native.flushed_span_events
    assert record[_REC_ANNOTATIONS] == [(4, "SELECT ?", ("<unrepresentable>",))]


def test_set_sql_query_keeps_large_literal_until_native_normalization():
    ev, span, native = _make_event()
    sql = "SELECT '" + "x" * (70 * 1024) + "' AS value"
    ev.set_sql_query(sql)
    ev.end()
    span.end()
    (record,) = native.flushed_span_events
    assert record[_REC_ANNOTATIONS] == [(4, sql, "")]


@pytest.mark.parametrize("sql", ["x" * (1024 * 1024 + 1), "한" * 350000])
def test_set_sql_query_drops_over_one_mib_in_utf8(sql):
    ev, span, native = _make_event()
    ev.set_sql_query(sql)
    ev.end()
    span.end()
    (record,) = native.flushed_span_events
    assert not record[_REC_ANNOTATIONS]


# ---------------------------------------------------------------------------
# http_helper header recording buffers on the wrapper and stops after end()
# ---------------------------------------------------------------------------

def test_client_header_recording_buffers_and_stops_after_end(monkeypatch):
    ev, span, native = _make_event()
    # Force the config gate open so recording would otherwise fire.
    monkeypatch.setattr(
        hh, "_header_recording_config",
        lambda _attr, _target=None: (("content-type",), False))

    # Before end: headers are extracted into buffered two-string annotations.
    hh.trace_http_client_response(ev, None, {"content-type": "text/html"})

    ev.end()

    flushed = [a for a in span._finished_events[0][_REC_ANNOTATIONS]
               if a[0] == 2]
    assert flushed == [(2, 55, "content-type", "text/html")]

    # After end: the _ended probe short-circuits the helper, so nothing more
    # is buffered anywhere.
    hh.trace_http_client_response(ev, None, {"content-type": "text/html"})
    assert [a for a in span._finished_events[0][_REC_ANNOTATIONS]
            if a[0] == 2] == flushed


# ---------------------------------------------------------------------------
# Span: concurrent end() finalizes exactly once
# ---------------------------------------------------------------------------

def test_span_concurrent_end_finalizes_once():
    native = _RecordingNativeSpan()
    span = Span(native)
    barrier = threading.Barrier(8)

    def ender():
        barrier.wait()
        span.end()

    threads = [threading.Thread(target=ender) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert span._ended is True
    assert native.ended == 1


def test_span_end_waits_for_inflight_native_recording():
    native = _BlockingNativeSpan()
    span = Span(native)
    span.new_span_event("handoff")

    recorder = threading.Thread(
        target=lambda: span.new_async_span("inflight-op"),
    )
    recorder.start()
    assert native.record_entered.wait(timeout=2)

    ender = threading.Thread(target=span.end)
    ender.start()
    # The shared wrapper lock prevents native finalization from racing a
    # native call that began on another Python thread after the GIL was
    # released by the extension. Asserted on the lock rather than on
    # "end_entered stayed unset for 50 ms": that window only proved the ender
    # had not been scheduled yet, so it passed against a broken lock too.
    held = not span._native_lock.acquire(blocking=False)
    if not held:
        span._native_lock.release()
    assert held, "in-flight native recording must hold the wrapper lock"

    native.release_record.set()
    recorder.join(timeout=2)
    ender.join(timeout=2)

    assert recorder.is_alive() is False
    assert ender.is_alive() is False
    assert [entry[0] for entry in native.async_spans] == ["inflight-op"]
    assert native.end_entered.is_set() is True   # got through, once released
    assert native.ended == 1


# ---------------------------------------------------------------------------
# Span: native-touching methods are safe no-ops after end()
#
# The scenario: an implicit asyncio-task lifetime timer (or SIGTERM drain)
# force-ends the span while a fire-and-forget task keeps running with a
# captured reference. Public API paths (pinpoint.trace(), @pinpoint.spanevent,
# propagator.inject) reach the span without safe_wrapper protection, so every
# native crossing must degrade to a no-op instead of raising into user code
# (or dereferencing the drained native span).
# ---------------------------------------------------------------------------

def test_span_end_releases_native_handle():
    native = _RecordingNativeSpan()
    span = Span(native)
    span.end()
    assert span._ended is True
    assert span._native is None
    assert native.ended == 1


def test_span_set_error_after_end_does_not_touch_native():
    native = _RecordingNativeSpan()
    span = Span(native)
    span.end()
    result = span.set_error(ValueError("late"))
    assert result is span
    assert native.errors == []


def test_span_new_span_event_after_end_returns_noop_event():
    from pinpoint.agent import _NULL_SPAN_EVENT

    native = _RecordingNativeSpan()
    span = Span(native)
    span.end()

    ev = span.new_span_event("late.operation")
    assert ev is _NULL_SPAN_EVENT

    # The documented `with span.new_span_event(...)` pattern must keep working, and
    # nothing may be buffered on the drained span.
    with span.new_span_event("late.trace") as scope:
        scope.set_sql_query("SELECT 1")
    assert span._finished_events == []
    assert span._active_events == []


def test_span_metadata_survives_end():
    native = _RecordingNativeSpan()
    span = Span(native, trace_id="trace-id", span_id=42)
    assert span.trace_id == "trace-id"
    assert span.span_id == 42
    span.end()
    # Logging integrations may stamp ids from a captured span after the
    # request finished; cached values stay readable without a native crossing.
    assert span.trace_id == "trace-id"
    assert span.span_id == 42


def test_span_new_async_span_after_end_returns_null_span():
    native = _RecordingNativeSpan()
    span = Span(native)
    span.end()
    child = span.new_async_span("late.async")
    assert native.async_spans == []
    assert child.sampled is False
    # The hand-off protocol (`with async_span:`) must not raise.
    with child:
        pass


def test_propagator_inject_after_end_returns_no_headers():
    from pinpoint.propagator import inject_items

    native = _RecordingNativeSpan()
    span = Span(native)
    event = span.new_span_event("client-op")
    assert dict(inject_items(span))  # live span with an open event injects
    event.end()
    span.end()
    assert tuple(inject_items(span)) == ()  # ended span propagates nothing
