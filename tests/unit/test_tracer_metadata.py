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

"""Span metadata caching and the single end-of-span flush."""

import _fakes
from _fakes import (_REC_ANNOTATIONS, _REC_DESTINATION, _REC_END_POINT,
                    _REC_OPERATION, _REC_SERVICE_TYPE)

from pinpoint.annotation import ANNOTATION_API
from pinpoint.tracer import _MAX_ANNOTATION_CHARS, _MAX_ANNOTATIONS, Span

# Positional slots of the end_span_with_data payload (see Span._end_locked):
# (..., error_verdicts, annotations, span_events, logging).
_PAYLOAD_ANNOTATIONS = 8
_PAYLOAD_SPAN_EVENTS = 9


class _FakeNativeSpan(_fakes.FakeNativeSpan):
    """Shared fake plus per-getter call counters and a pinned span id."""

    # These tests assert the record batch handed to the one
    # ``end_span_with_data`` flush. Like the real binding, expose no
    # ``new_span_event`` so the conftest eager-replay fixture leaves the
    # finished-event records in the batch. (A getterless property makes
    # ``hasattr`` False.)
    new_span_event = property()

    def __init__(self):
        super().__init__()
        self.sampled_calls = 0

    def is_sampled(self):
        self.sampled_calls += 1
        return True


def test_span_identity_comes_from_construction():
    """The binding has no identity getters: ``Agent.new_span`` passes the
    creation call's trace/span id in, and a span built without them reports
    the empty identity rather than touching native."""
    span = Span(_FakeNativeSpan(), trace_id="trace-id", span_id=42)
    assert (span.trace_id, span.span_id, span.span_id_str) == ("trace-id", 42, "42")

    bare = Span(_FakeNativeSpan())
    assert (bare.trace_id, bare.span_id) == ("", 0)


def test_span_sampled_is_constant_true_without_native_call():
    """``Agent.new_span()`` only hands out a real ``Span`` for sampled
    transactions, so the ``sampled`` accessor must be a constant and must
    not cross the pybind11 boundary."""
    native = _FakeNativeSpan()
    span = Span(native)

    assert span.sampled is True
    assert span.sampled is True
    assert native.sampled_calls == 0


def test_span_set_logging_is_deferred_to_end_and_chainable():
    native = _FakeNativeSpan()
    span = Span(native, trace_id="t", span_id=1)
    assert span.set_logging() is span
    assert native.logging is False
    span.end()
    assert native.logging is True


def test_span_set_url_stat_respects_collect_url_stat_flag():
    native = _FakeNativeSpan()
    span = Span(native, collect_url_stat=False)

    span.set_url_stat("/items/{id}", "GET", 200)

    assert native.url_stats == []


def test_span_set_url_stat_defers_native_call_until_end_when_enabled():
    native = _FakeNativeSpan()
    span = Span(native, collect_url_stat=True)

    span.set_url_stat("/items/{id}", "GET", 200)

    assert native.url_stats == []
    span.end()

    assert native.url_stats == [("/items/{id}", "GET", 200)]
    assert native.end_called is True


def test_span_end_uses_helper_when_all_helper_data_is_cached(monkeypatch):
    native = _FakeNativeSpan()
    span = Span(native, collect_url_stat=True)
    calls = []

    def end_span_with_data(*args):
        calls.append(args)

    monkeypatch.setattr(native, "end_span_with_data", end_span_with_data)

    span.set_service_type(1700)
    span.set_remote_address("10.0.0.7")
    span.set_end_point("app.test")
    span.set_acceptor_host("app.test:8080")
    span.set_status_code(201)
    span.set_url_stat("/items/{id}", "POST", 201)
    span.end()

    assert calls == [(
        1700,
        "10.0.0.7",
        "app.test",
        "app.test:8080",
        201,
        "/items/{id}",
        "POST",
        (),
        (),
        [],
        False,
    )]
    assert native.end_called is False
    assert native.url_stats == []


def test_span_end_uses_helper_without_value_checks(monkeypatch):
    native = _FakeNativeSpan()
    span = Span(native, collect_url_stat=True)
    calls = []

    def end_span_with_data(*args):
        calls.append(args)

    monkeypatch.setattr(native, "end_span_with_data", end_span_with_data)

    span.end()

    assert calls == [(0, "", "", "", 0, "", "", (), (), [], False)]
    assert native.end_called is False


def test_span_event_record_carries_cached_metadata(monkeypatch):
    native = _FakeNativeSpan()
    span = Span(native)
    calls = []
    monkeypatch.setattr(native, "end_span_with_data",
                        lambda *args: calls.append(args), raising=False)

    event = span.new_span_event("db.query", 2501)
    event.set_destination("postgres")
    event.set_end_point("db:5432")
    event.end()
    span.end()

    (record,) = calls[0][_PAYLOAD_SPAN_EVENTS]
    assert record[_REC_SERVICE_TYPE] == 2501
    assert record[_REC_OPERATION] == "db.query"
    assert record[_REC_DESTINATION] == "postgres"
    assert record[_REC_END_POINT] == "db:5432"
    assert record[_REC_ANNOTATIONS] == ()


def test_span_end_swallows_helper_failure(monkeypatch):
    """The one native flush runs inside end()'s one-shot section; a failing
    helper must neither raise into user code nor allow a second flush."""
    native = _FakeNativeSpan()
    span = Span(native)
    calls = []

    def fail_end_span_with_data(*args):
        calls.append(args)
        raise RuntimeError("boom")

    monkeypatch.setattr(native, "end_span_with_data", fail_end_span_with_data)

    span.end()
    span.end()

    assert len(calls) == 1
    assert span._ended is True


def test_flush_failure_warns_once_then_falls_back_to_debug(monkeypatch):
    """The flush carries the whole transaction and the buffers are cleared
    before it runs, so a persistent failure loses every trace — and at DEBUG
    that is invisible by default. Warn on the first, then stay quiet: the
    cause is a broken binding, not a per-request event."""
    import pinpoint.tracer as tracer_mod
    monkeypatch.setattr(tracer_mod, "_flush_failure_warned", False)

    # The logger is intercepted rather than read through caplog: _log.configure
    # sets propagate=False on the pinpoint logger, so whether caplog sees these
    # records at all would depend on which tests ran first.
    logged = []

    class _Recorder:
        def warning(self, msg, *args, **_kw):
            logged.append(("WARNING", msg))

        def debug(self, msg, *args, **_kw):
            logged.append(("DEBUG", msg))

    monkeypatch.setattr(tracer_mod, "_log", _Recorder())

    def fail(*_args):
        raise RuntimeError("native gone")

    for _ in range(3):
        native = _FakeNativeSpan()
        monkeypatch.setattr(native, "end_span_with_data", fail)
        Span(native).end()

    levels = [level for level, msg in logged if "end_span_with_data" in msg]
    assert levels == ["WARNING", "DEBUG", "DEBUG"], logged


def test_span_event_end_is_idempotent(monkeypatch):
    """A manual ``end()`` followed by ``with``-exit (or a done-callback race)
    must buffer the event's record exactly once."""
    native = _FakeNativeSpan()
    span = Span(native)
    calls = []
    monkeypatch.setattr(native, "end_span_with_data",
                        lambda *args: calls.append(args), raising=False)

    event = span.new_span_event("op", 1701)
    event.end()
    event.end()
    span.end()

    assert len(calls[0][_PAYLOAD_SPAN_EVENTS]) == 1


def test_span_event_exit_survives_broken_str_exception(monkeypatch):
    """``__exit__`` must neither mask the user's in-flight exception nor skip
    ``end()`` when ``set_error`` blows up — e.g. a user exception whose
    ``__str__`` raises."""
    native = _FakeNativeSpan()
    span = Span(native)
    calls = []
    monkeypatch.setattr(native, "end_span_with_data",
                        lambda *args: calls.append(args), raising=False)

    event = span.new_span_event("op", 1701)

    class _BrokenStrError(Exception):
        def __str__(self):
            raise RuntimeError("broken __str__")

    import pytest

    with pytest.raises(_BrokenStrError):
        with event:
            raise _BrokenStrError()

    span.end()
    # The event still ended exactly once despite set_error failing.
    assert len(calls[0][_PAYLOAD_SPAN_EVENTS]) == 1


def test_span_annotation_buffer_is_capped(monkeypatch):
    """A long-lived span annotating per iteration must not grow its buffer
    without bound; the flushed annotations are capped with a single marker."""
    native = _FakeNativeSpan()
    span = Span(native)
    calls = []
    monkeypatch.setattr(native, "end_span_with_data",
                        lambda *args: calls.append(args), raising=False)

    for i in range(_MAX_ANNOTATIONS + 500):
        span.annotate_int(1, i)

    span.end()

    anns = calls[0][_PAYLOAD_ANNOTATIONS]
    # _MAX_ANNOTATIONS real annotations plus exactly one truncation marker.
    assert len(anns) == _MAX_ANNOTATIONS + 1
    marker = anns[-1]
    assert marker[1] == ANNOTATION_API
    assert "truncated" in marker[2]
    # The first real entry survived; overflow beyond the cap was dropped.
    assert anns[0] == (0, 1, 0)  # (_ANN_INT, key, value=0)
    assert span._annotations is None


def test_annotation_strings_are_capped(monkeypatch):
    """A buffered annotation value is pinned until the span ends, and the
    unbounded ones — outbound URLs, recorded header values — have no native
    cut to inherit. Both string overloads truncate."""
    native = _FakeNativeSpan()
    span = Span(native)
    calls = []
    monkeypatch.setattr(native, "end_span_with_data",
                        lambda *args: calls.append(args), raising=False)
    huge = "u" * (_MAX_ANNOTATION_CHARS + 5000)

    span.annotate_string(40, huge)
    span.annotate_string_string(45, huge, huge)
    span.end()

    anns = calls[0][_PAYLOAD_ANNOTATIONS]
    assert len(anns[0][2]) == _MAX_ANNOTATION_CHARS
    assert len(anns[1][2]) == len(anns[1][3]) == _MAX_ANNOTATION_CHARS


def test_annotation_strings_under_the_cap_are_untouched():
    span = Span(_FakeNativeSpan())

    span.annotate_string(40, "https://example.test/items?id=1")

    assert span._annotations[0][2] == "https://example.test/items?id=1"


def test_proxy_annotation_survives_the_flush():
    """tag 6 (_ANN_LONG_IIBBS, the Pinpoint-Proxy* payload http_helper buffers)
    is Span-only and easy to leave out of a replay — the fake native dropped it
    silently, so a test asserting one saw nothing and would read as the
    annotation never being recorded at all."""
    native = _FakeNativeSpan()
    span = Span(native)

    span.annotate_long_iibbs(300, 1700, 3, 1, 0, 0, "app")
    span.end()

    assert native.get_annotations().entries == [
        ("proxy", 300, (1700, 3, 1, 0, 0, "app")),
    ]


def test_span_event_annotation_buffer_is_capped(monkeypatch):
    native = _FakeNativeSpan()
    span = Span(native)
    calls = []
    monkeypatch.setattr(native, "end_span_with_data",
                        lambda *args: calls.append(args), raising=False)

    event = span.new_span_event("op", 1701)
    for i in range(_MAX_ANNOTATIONS + 10):
        event.annotate_string(1, f"v{i}")

    event.end()
    span.end()

    (record,) = calls[0][_PAYLOAD_SPAN_EVENTS]
    anns = record[_REC_ANNOTATIONS]
    assert len(anns) == _MAX_ANNOTATIONS + 1
    assert anns[-1][1] == ANNOTATION_API
    assert "truncated" in anns[-1][2]
    assert event._annotations is None
    assert event._span is None


def test_span_annotation_buffer_stays_lazy_without_annotations(monkeypatch):
    """No annotations means no list allocation — the flushed value is ()."""
    native = _FakeNativeSpan()
    span = Span(native)
    calls = []
    monkeypatch.setattr(native, "end_span_with_data",
                        lambda *args: calls.append(args), raising=False)

    span.end()

    assert calls[0][_PAYLOAD_ANNOTATIONS] == ()


def test_ended_wrappers_do_not_recreate_released_payload_buffers(monkeypatch):
    native = _FakeNativeSpan()
    span = Span(native)
    monkeypatch.setattr(
        native, "end_span_with_data", lambda *_args: None, raising=False,
    )
    span.annotate_string(1, "before")
    span.set_remote_address("large-before-end")

    event = span.new_span_event("op")
    event.annotate_string(1, "before")
    event.end()
    span.end()

    span.annotate_string(1, "after")
    span.set_remote_address("large-after-end")
    assert span._annotations is None
    assert span._remote_address is None
    assert span._finished_events == []

    event.annotate_string(1, "after")
    event.set_destination("large-after-end")
    assert event._annotations is None
    assert event._span is None
    assert event._destination == ""


# ---------------------------------------------------------------------------
# Endpoint ownership: one transaction, one endpoint.
# ---------------------------------------------------------------------------


def test_span_end_point_is_first_writer_wins():
    """A nested instrumentation must not relabel the transaction."""
    span = Span(_FakeNativeSpan())
    span.set_end_point("outer-transport:80")
    span.set_end_point("inner-framework:8080")
    assert span._end_point == "outer-transport:80"


def test_span_end_point_empty_value_does_not_claim_the_slot():
    """Callers pass ``endpoint or ""``; an empty first write must not block
    the real one (native drops empty endpoints anyway)."""
    span = Span(_FakeNativeSpan())
    span.set_end_point("")
    span.set_end_point("real:80")
    assert span._end_point == "real:80"


def test_span_event_end_point_stays_last_wins():
    """A span event's endpoint describes one outbound step, not the
    transaction, so it keeps the inherited setter."""
    event = Span(_FakeNativeSpan()).new_span_event("op")
    event.set_end_point("first:80").set_end_point("second:80")
    assert event._end_point == "second:80"
