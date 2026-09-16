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

"""``UnSampledSpan`` — the wrapper for a transaction native did not sample.

Contract under test: profiling detail is inherited from the pure no-op
``_NullSpan``, while error calls retain only native-policy verdict inputs. The
span still owns a native lifetime — ``end()`` must reach native exactly once,
``set_url_stat`` is cached and flushed through the URL-stat end helper,
``span_id`` is the real native id, and injection propagates the ``s0`` decision
downstream.
"""

from __future__ import annotations

import threading

import pytest

from pinpoint import propagator
from pinpoint.agent import (
    Agent,
    UnSampledSpan,
    _NULL_SPAN_EVENT,
    _NullAgent,
    _NullSpan,
)
from pinpoint.config import Config
from pinpoint.context import current_span
from pinpoint.tracer import Span


def _recording_native(span_id: int = 7777):
    """Fake native unsampled span recording every crossing.

    Deliberately hand-rolled instead of ``_fakes.FakeNativeSpan``: these
    tests assert the exact native end helper the wrapper picked, and
    ``__getattr__`` turns any crossing the wrapper is not supposed to make
    into a hard failure — the shared fake's full surface would absorb it.
    """

    class _Native:
        def __init__(self):
            self.span_id = span_id
            self.end_calls = []

        def end_span(self, url_pattern="", method="", status_code=0,
                     error_verdicts=()):
            self.end_calls.append((
                "end_span", url_pattern, method, status_code,
                tuple(error_verdicts),
            ))

        def mark_error(self, name, message):
            self.end_calls.append(("mark_error", name, message))

        def __getattr__(self, name):
            raise AssertionError(f"unexpected native span access: {name}")

    return _Native()


# ---------------------------------------------------------------------------
# Construction / dispatch
# ---------------------------------------------------------------------------

class _NativeSpan:
    """Native span double that fails any crossing the wrapper should not make."""

    def __getattr__(self, name):
        raise AssertionError(f"unexpected native call: {name}")


class _DispatchNativeAgent:
    def __init__(self, sampled, span_id=7):
        self._sampled = sampled
        self._span_id = span_id

    def get_config_snapshot(self):
        return (
            "app", 1000, "", 64, 5000,
            (), (), (), (), (), (),
            1,
            False,
        )

    def new_span(self, *_args):
        return (_NativeSpan(), self._sampled, "", self._span_id,
                1 if self._sampled else 0)


def test_agent_new_span_dispatches_on_sampling_decision():
    cfg = Config(application_name="app")
    sampled_span = Agent(_DispatchNativeAgent(True), cfg).new_span("GET /", "/")
    dropped_span = Agent(_DispatchNativeAgent(False), cfg).new_span("GET /", "/")

    assert type(sampled_span) is Span
    assert isinstance(dropped_span, UnSampledSpan)
    # The unsampled wrapper is a specialization of the pure no-op span.
    assert isinstance(dropped_span, _NullSpan)


def test_native_noop_span_becomes_a_pure_null_span():
    """Span id 0 is a native noop span, not an unsampled one.

    The native side answers that way for a url or method the HTTP filters
    excluded, a disabled agent, or a failed admission. That span owns no
    native lifetime and propagates nothing, so the wrapper must be the pure
    no-op — keeping the handle would cost an ``end_span()`` crossing per
    filtered request and claim a lifetime there is none of.
    """
    agent = Agent(_DispatchNativeAgent(False, span_id=0),
                  Config(application_name="app"))
    span = agent.new_span("GET /health", "/health", method="GET")

    assert type(span) is _NullSpan
    assert not isinstance(span, UnSampledSpan)
    assert span.sampled is False
    assert span.span_id == 0
    # No native handle: _NativeSpan raises on any attribute the wrapper touches.
    assert span._native is None
    # A genuinely unsampled span tells the downstream agent to drop its
    # transaction too; a filtered one must stay silent.
    assert tuple(span.inject_context_items()) == ()
    span.set_url_stat("/health", "GET", 200)
    span.end()


def test_null_agent_hands_out_pure_null_spans():
    span = _NullAgent(Config(application_name="app")).new_span("GET /", "/")

    assert type(span) is _NullSpan
    assert not isinstance(span, UnSampledSpan)


# ---------------------------------------------------------------------------
# Identity metadata
# ---------------------------------------------------------------------------

def test_span_id_is_real_native_id_and_cached():
    native = _recording_native(span_id=4242)
    span = UnSampledSpan(native, span_id=native.span_id)

    assert span.sampled is False
    assert span.trace_id == ""
    # The real native id, captured at creation.
    assert span.span_id == 4242
    assert span.span_id_str == "4242"


def test_span_id_survives_end():
    span = UnSampledSpan(_recording_native(span_id=4242), span_id=4242)
    span.end()

    # Identity was captured at creation, so a late reader (a logging
    # integration stamping a captured span) still sees it — and without a
    # native crossing, which the fake's __getattr__ would turn into a failure.
    assert span.span_id == 4242
    assert span.span_id_str == "4242"


# ---------------------------------------------------------------------------
# Profiling data is a no-op; error verdicts ride the sole end crossing
# ---------------------------------------------------------------------------

def test_recording_calls_drop_detail_but_batch_error_verdict_at_end():
    native = _recording_native()
    span = UnSampledSpan(native, span_id=native.span_id)

    assert span.set_service_type(1810) is span
    assert span.set_remote_address("1.2.3.4") is span
    assert span.set_end_point("api:80") is span
    assert span.set_acceptor_host("upstream") is span
    assert span.set_status_code(500) is span
    assert span.set_error(ValueError("boom")) is span
    assert span.annotate_int(1, 2) is span
    assert span.annotate_long(1, 2 ** 40) is span
    assert span.annotate_string(1, "x") is span
    event = span.new_span_event("op")
    # The event is the shared null event: an error recorded on it is dropped,
    # only the span's own verdict rides the end crossing.
    assert event is _NULL_SPAN_EVENT
    assert span.new_span_event("op") is event
    event.set_error("EventError", "event boom")

    span.end()
    assert native.end_calls == [(
        "end_span", "", "", 0, (("ValueError", "boom"),),
    )]


def test_end_is_idempotent():
    native = _recording_native()
    span = UnSampledSpan(native, span_id=native.span_id)

    span.end()
    span.end()

    assert native.end_calls == [("end_span", "", "", 0, ())]


def test_racing_ends_finalize_native_exactly_once():
    native = _recording_native()
    span = UnSampledSpan(native, span_id=native.span_id)
    barrier = threading.Barrier(4)

    def _end():
        barrier.wait()
        span.end()

    threads = [threading.Thread(target=_end) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert native.end_calls == [("end_span", "", "", 0, ())]


# ---------------------------------------------------------------------------
# URL stat
# ---------------------------------------------------------------------------

def test_url_stat_is_cached_and_flushed_through_url_stat_end_helper():
    native = _recording_native()
    span = UnSampledSpan(native, span_id=native.span_id, collect_url_stat=True)

    assert span.set_url_stat("/users/{id}", "GET", 200) is span
    assert native.end_calls == []  # cached on the wrapper, not forwarded

    span.end()
    assert native.end_calls == [
        ("end_span", "/users/{id}", "GET", 200, ())]


def test_url_stat_collection_disabled_ends_with_plain_end_span():
    native = _recording_native()
    span = UnSampledSpan(native, span_id=native.span_id, collect_url_stat=False)

    span.set_url_stat("/users/{id}", "GET", 200)
    span.end()

    assert native.end_calls == [("end_span", "", "", 0, ())]


def test_set_url_stat_after_end_is_noop():
    native = _recording_native()
    span = UnSampledSpan(native, span_id=native.span_id, collect_url_stat=True)
    span.end()

    assert span.set_url_stat("/users/{id}", "GET", 200) is span
    span.end()

    assert native.end_calls == [("end_span", "", "", 0, ())]


# ---------------------------------------------------------------------------
# Context propagation
# ---------------------------------------------------------------------------

def test_inject_items_writes_s0_marker_until_end():
    native = _recording_native()
    span = UnSampledSpan(native, span_id=native.span_id)

    # An unsampled span still hands out the s0 drop marker for downstream
    # services — built wrapper-side; the span id that distinguishes it from a
    # pure noop span was captured at creation, so nothing reaches native.
    assert dict(propagator.inject_items(span)) == {
        propagator.HEADER_SAMPLED: "s0"}

    span.end()
    assert propagator.inject_items(span) == ()


# ---------------------------------------------------------------------------
# Scope / children
# ---------------------------------------------------------------------------

def test_with_block_makes_span_current_and_ends_native():
    native = _recording_native()
    span = UnSampledSpan(native, span_id=native.span_id)

    with span as entered:
        assert entered is span
        assert current_span() is span

    assert current_span() is not span
    assert native.end_calls == [("end_span", "", "", 0, ())]


def test_with_block_swallows_no_exception_and_still_ends():
    native = _recording_native()
    span = UnSampledSpan(native, span_id=native.span_id)

    with pytest.raises(RuntimeError, match="boom"):
        with span:
            raise RuntimeError("boom")

    # No span/error metadata is recorded, but the exception verdict rides the
    # same final crossing used for the unsampled lifetime.
    assert native.end_calls == [(
        "end_span", "", "", 0, (("RuntimeError", "boom"),))]


def test_async_children_and_detached_views_are_pure_null_spans():
    native = _recording_native()
    span = UnSampledSpan(native, span_id=native.span_id)

    child = span.new_async_span("bg")
    detached = span._detached_context_span()

    assert type(child) is _NullSpan
    assert type(detached) is _NullSpan
    assert detached is not span
    # Sharing self keeps the per-task fork allocation-free (there is no
    # native event stack to protect on an unsampled span).
    assert span._fork_for_async_task(task=object()) is span

    child.end()
    detached.end()
    assert native.end_calls == []  # children never touch the parent native

    span.end()
    assert native.end_calls == [("end_span", "", "", 0, ())]


def test_unsampled_event_errors_are_dropped_and_collect_no_callstack(
    monkeypatch,
):
    """Every unsampled span hands out the one shared null event, so an error
    recorded on an event is dropped outright."""
    native1 = _recording_native(1)
    native2 = _recording_native(2)
    span1 = UnSampledSpan(native1, span_id=1)
    span2 = UnSampledSpan(native2, span_id=2)

    monkeypatch.setattr(
        "pinpoint.callstack.frames_for",
        lambda *_: pytest.fail("unsampled error must not collect frames"),
    )
    event1 = span1.new_span_event("db")
    event2 = span2.new_span_event("db")
    assert event1 is span1.new_span_event("other")
    assert event1 is event2 is _NULL_SPAN_EVENT

    event1.set_error(ValueError("one"))
    event1.set_error("one-arg-message")
    event2.set_error("LookupError", "two")
    span1.end()
    span2.end()

    assert native1.end_calls == [("end_span", "", "", 0, ())]
    assert native2.end_calls == [("end_span", "", "", 0, ())]


def test_unsampled_late_error_and_event_calls_are_noops():
    native = _recording_native()
    span = UnSampledSpan(native, span_id=native.span_id)
    event = span.new_span_event("db")
    event.set_error("First", "before end")  # dropped: event-level
    event.end()
    event.end()
    span.end()

    span.set_error("Late", "span ended")
    event.set_error("Late", "after end")
    span.new_span_event("late").set_error("Late", "null event")
    span.end()

    assert native.end_calls == [("end_span", "", "", 0, ())]


def test_unsampled_span_verdicts_do_not_mix_between_threads():
    """The shared null event is stateless, so concurrent requests cannot leak
    into each other; each span's own verdict stays on its own span."""
    natives = [_recording_native(101), _recording_native(202)]
    spans = [UnSampledSpan(native, span_id=native.span_id)
             for native in natives]
    barrier = threading.Barrier(2)

    def _record(index):
        assert spans[index].new_span_event("db") is _NULL_SPAN_EVENT
        barrier.wait()
        spans[index].set_error(f"Error{index}", f"message{index}")
        spans[index].end()

    threads = [threading.Thread(target=_record, args=(i,)) for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert natives[0].end_calls[-1][-1] == (("Error0", "message0"),)
    assert natives[1].end_calls[-1][-1] == (("Error1", "message1"),)


def test_pure_null_span_keeps_shared_allocation_free_event():
    span = _NullSpan()

    assert span.new_span_event("one") is _NULL_SPAN_EVENT
    assert span.new_span_event("two") is _NULL_SPAN_EVENT
    assert span.set_error(RuntimeError("ignored")) is span
    span.end()


def test_normal_unsampled_path_allocates_no_event_or_error_buffer():
    native = _recording_native()
    span = UnSampledSpan(native, span_id=native.span_id)

    assert span._error_verdicts is None
    span.end()

    assert native.end_calls == [("end_span", "", "", 0, ())]


def test_unsampled_mark_error_false_records_nothing():
    """An unsampled span keeps only the verdict, so mark_error=False leaves
    it with nothing to do — no crossing into native at end."""
    native = _recording_native()
    span = UnSampledSpan(native, span_id=native.span_id)
    span.set_error(ValueError("handled"), mark_error=False)
    span.new_span_event("op").set_error("Handled", "x", mark_error=False)
    span.end()
    assert native.end_calls == [("end_span", "", "", 0, ())]
