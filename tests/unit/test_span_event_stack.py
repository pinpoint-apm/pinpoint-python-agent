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

"""Python-managed span event stack semantics.

The Span wrapper owns event creation entirely: it assigns sequence/depth,
keeps the LIFO stack, unwinds out-of-order ends, drains leftovers at span end,
hands out overflow placeholders past the configured limits, and assigns the
async link ids.
These tests pin that contract against the flushed record batch.
"""

import pytest

from _fakes import (_REC_ASYNC_ID, _REC_DEPTH, _REC_END, _REC_OPERATION,
                    _REC_SEQUENCE, _REC_START)

from pinpoint.tracer import Span


class _FakeNativeSpan:
    def __init__(self):
        self.flush_calls = []
        self.async_spans = []
        self.error_verdicts = []

    def end_span_with_data(self, *args):
        self.flush_calls.append(args)

    def new_async_span(self, op, async_id, async_sequence):
        self.async_spans.append((op, async_id, async_sequence))
        return _FakeNativeSpan()

    def mark_error(self, name, message):
        self.error_verdicts.append((name, message))


# Positional slots of the end_span_with_data payload (see Span._end_locked):
# (..., error_verdicts, annotations, span_events, logging).
def _flushed(native):
    (call,) = native.flush_calls
    return call[9]


def _flushed_error_verdicts(native):
    (call,) = native.flush_calls
    return call[7]


def test_nested_events_get_monotonic_sequence_and_nested_depth():
    native = _FakeNativeSpan()
    span = Span(native)

    e0 = span.new_span_event("root-op")       # seq 0, depth 1
    e1 = span.new_span_event("child-op")      # seq 1, depth 2
    e1.end()
    e2 = span.new_span_event("sibling-op")    # seq 2, depth 2 again
    e2.end()
    e0.end()
    span.end()

    records = _flushed(native)
    assert [(r[_REC_SEQUENCE], r[_REC_DEPTH], r[_REC_OPERATION])
            for r in records] == [
        (0, 1, "root-op"),
        (1, 2, "child-op"),
        (2, 2, "sibling-op"),
    ]


def test_flush_is_sequence_ordered_despite_lifo_finish():
    """Nested events finish innermost-first; the batch must still arrive
    sequence-ascending, which is the order native expects."""
    native = _FakeNativeSpan()
    span = Span(native)

    e0 = span.new_span_event("outer")
    e1 = span.new_span_event("inner")
    e1.end()
    e0.end()
    span.end()

    records = _flushed(native)
    assert [r[_REC_SEQUENCE] for r in records] == [0, 1]
    for record in records:
        assert record[_REC_START] <= record[_REC_END]


def test_out_of_order_end_unwinds_nested_events():
    """Ending a parent before its child implicitly finishes the child, and
    the child's later end() is a no-op."""
    native = _FakeNativeSpan()
    span = Span(native)

    outer = span.new_span_event("outer")
    inner = span.new_span_event("inner")
    outer.end()

    assert inner._ended is True
    assert span._active_events == []
    inner.end()  # no-op, must not buffer a second record

    span.end()
    assert sorted(r[_REC_SEQUENCE] for r in _flushed(native)) == [0, 1]


def test_span_end_drains_open_events():
    native = _FakeNativeSpan()
    span = Span(native)

    span.new_span_event("never-ended-outer")
    span.new_span_event("never-ended-inner")
    span.end()

    records = _flushed(native)
    assert [r[_REC_OPERATION] for r in records] == [
        "never-ended-outer", "never-ended-inner"]


def test_depth_overflow_hands_out_placeholder():
    native = _FakeNativeSpan()
    span = Span(native, max_event_depth=2)

    e0 = span.new_span_event("kept")          # depth 1
    span.new_span_event("kept-2")            # depth 2
    span.new_span_event("kept-3")            # max + 1 is allowed
    e1 = span.new_span_event("discarded")     # depth 4 -> placeholder
    assert e1._sequence is None
    # The placeholder still nests and still carries outbound propagation.
    assert span._active_events[-1] is e1
    e1.set_destination("downstream")
    assert dict(span.inject_context_items())
    e1.end()
    # The placeholder consumed no depth: a sibling at depth 4 is still
    # rejected, while depth-1 recording resumes once e0 ends.
    assert span.new_span_event("still-discarded")._sequence is None
    e0.end()
    assert span.new_span_event("recorded-again")._sequence == 3
    span.end()

    records = _flushed(native)
    assert [r[_REC_OPERATION] for r in records] == ["kept", "kept-2", "kept-3", "recorded-again"]


def test_overflow_placeholder_discards_detail_but_batches_error_verdict(
    monkeypatch,
):
    """A placeholder's record is dropped at flush, so buffering its
    annotations (full SQL text included) would only pin them until the
    event is popped."""
    native = _FakeNativeSpan()
    span = Span(native, max_event_depth=1)
    span.new_span_event("kept-1")
    span.new_span_event("kept-2")

    placeholder = span.new_span_event("discarded")
    assert placeholder._sequence is None
    placeholder.annotate_string(1, "x")
    placeholder.set_sql_query("SELECT 1", ())
    monkeypatch.setattr(
        "pinpoint.callstack.frames_for",
        lambda *_: pytest.fail("overflow error must not collect frames"),
    )
    placeholder.set_error(ValueError("msg"))
    assert list(placeholder._annotations) == []
    placeholder.end()
    span.end()

    assert _flushed_error_verdicts(native) == [("ValueError", "msg")]
    assert [r[_REC_OPERATION] for r in _flushed(native)] == ["kept-1", "kept-2"]
    assert all(not record[-1] for record in _flushed(native))


def test_sequence_overflow_discards_further_events():
    native = _FakeNativeSpan()
    span = Span(native, max_event_sequence=2)

    span.new_span_event("first").end()
    span.new_span_event("second").end()
    overflow = span.new_span_event("third")
    assert overflow._sequence is None
    overflow.end()
    span.end()

    records = _flushed(native)
    assert [r[_REC_OPERATION] for r in records] == ["first", "second"]


def test_sequence_overflow_preserves_repeated_and_multiple_error_verdicts():
    native = _FakeNativeSpan()
    span = Span(native, max_event_sequence=1)
    span.new_span_event("kept").end()

    first = span.new_span_event("overflow-1")
    first.set_error("one-arg")
    first.set_error("Named", "two-arg")
    first.end()
    second = span.new_span_event("overflow-2")
    second.set_error(RuntimeError("three"))
    second.end()
    span.end()

    assert _flushed_error_verdicts(native) == [
        ("Error", "one-arg"),
        ("Named", "two-arg"),
        ("RuntimeError", "three"),
    ]
    assert [r[_REC_OPERATION] for r in _flushed(native)] == ["kept"]


def test_overflow_error_after_event_or_span_end_is_noop():
    native = _FakeNativeSpan()
    span = Span(native, max_event_sequence=0)
    event = span.new_span_event("overflow")
    event.set_error("Before", "kept")
    event.end()
    event.set_error("Late", "event ended")
    event.end()
    span.end()
    event.set_error("Later", "span ended")
    span.end()

    assert _flushed_error_verdicts(native) == [("Before", "kept")]


def test_async_overflow_error_is_applied_at_set_error_time():
    native = _FakeNativeSpan()
    parent = Span(native, max_event_sequence=1)
    with parent.new_span_event("handoff"):
        child = parent.new_async_span("background")

    # Async native already owns sequence 0, so the wrapper's first event is
    # over the max sequence and must not wait for child.end(): root can end
    # independently after SetError, matching native DisabledSpanEvent.
    overflow = child.new_span_event("overflow")
    child_native = child._native
    overflow.set_error("AsyncFailure", "boom")
    assert child_native.error_verdicts == [("AsyncFailure", "boom")]
    assert child._error_verdicts is None

    parent.end()
    overflow.end()
    child.end()


def test_async_link_ids_are_assigned_and_flushed():
    native = _FakeNativeSpan()
    span = Span(native)

    event = span.new_span_event("handoff")
    child_a = span.new_async_span("background")
    child_b = span.new_async_span("background")
    event.end()
    span.end()

    (op_a, id_a, seq_a), (op_b, id_b, seq_b) = native.async_spans
    assert op_a == op_b == "background"
    assert id_a == id_b != 0     # one async id per parent event...
    assert (seq_a, seq_b) == (1, 2)  # ...with an incrementing sequence

    (record,) = _flushed(native)
    assert record[_REC_ASYNC_ID] == id_a

    # The async child's native side holds its root event at seq 0 / depth 1,
    # so the wrapper's counters start past it.
    assert isinstance(child_a, Span)
    assert (child_a._event_sequence, child_a._event_depth) == (1, 2)
    assert isinstance(child_b, Span)


def test_new_async_span_without_open_event_returns_null_span():
    native = _FakeNativeSpan()
    span = Span(native)

    child = span.new_async_span("background")

    assert native.async_spans == []
    assert child.sampled is False


def test_new_async_span_on_overflow_event_returns_null_span():
    native = _FakeNativeSpan()
    span = Span(native, max_event_depth=1)
    span.new_span_event("kept-1")
    span.new_span_event("kept-2")

    overflow = span.new_span_event("discarded")
    assert overflow._sequence is None
    child = span.new_async_span("background")

    assert native.async_spans == []
    assert child.sampled is False


def test_event_stack_is_bounded_when_events_leak():
    """Overflow placeholders normally live briefly on the stack, but a caller
    that opens events and never ends them (trace() without ``with``) must not
    grow the stack one wrapper per call forever: past
    max_event_sequence + max_event_depth an owner-bound overflow placeholder
    is reused instead."""

    native = _FakeNativeSpan()
    span = Span(native, max_event_depth=2, max_event_sequence=2)
    cap = 2 + 2

    leaked = [span.new_span_event(f"leak-{i}") for i in range(cap + 5)]
    assert len(span._active_events) == cap
    # Everything past the cap reuses one of this span's placeholders, retaining
    # set_error without allocating or putting an owner on the global noop.
    assert all(event is leaked[cap - 1] for event in leaked[cap:])
    leaked[-1].set_error("CappedOverflow", "still marks the root")
    # A reused hand-out is still safely endable.
    leaked[-1].end()
    span.end()

    assert _flushed_error_verdicts(native) == [
        ("CappedOverflow", "still marks the root")]


# ---------------------------------------------------------------------------
# Mid-span flush: finished events stream to native once event_flush_size
# accumulate, instead of pinning every event until end().
# ---------------------------------------------------------------------------


class _ChunkingNativeSpan(_FakeNativeSpan):
    def __init__(self):
        super().__init__()
        self.partial_calls = []

    def record_span_events(self, events):
        self.partial_calls.append(list(events))


def test_finished_events_flush_mid_span_at_chunk_size():
    native = _ChunkingNativeSpan()
    span = Span(native, event_flush_size=2)
    for op in ("a", "b", "c"):
        span.new_span_event(op).end()
    assert len(native.partial_calls) == 1
    assert [r[_REC_OPERATION] for r in native.partial_calls[0]] == ["a", "b"]
    assert [r[_REC_SEQUENCE] for r in native.partial_calls[0]] == [0, 1]
    span.end()
    assert [r[_REC_OPERATION] for r in _flushed(native)] == ["c"]


def test_nested_children_flush_before_open_parent():
    native = _ChunkingNativeSpan()
    span = Span(native, event_flush_size=2)
    parent = span.new_span_event("parent")
    span.new_span_event("c1").end()
    span.new_span_event("c2").end()
    # Children ship first, sequence-sorted; the parent lands in the end flush.
    assert [r[_REC_SEQUENCE] for r in native.partial_calls[0]] == [1, 2]
    parent.end()
    span.end()
    assert [r[_REC_OPERATION] for r in _flushed(native)] == ["parent"]


def test_flush_disabled_and_legacy_native_keep_single_end_replay():
    native = _FakeNativeSpan()  # no record_span_events
    span = Span(native, event_flush_size=1)
    span.new_span_event("a").end()
    span.new_span_event("b").end()
    span.end()
    assert [r[_REC_OPERATION] for r in _flushed(native)] == ["a", "b"]

    native = _ChunkingNativeSpan()
    span = Span(native, event_flush_size=0)
    span.new_span_event("a").end()
    span.end()
    assert native.partial_calls == []
    assert [r[_REC_OPERATION] for r in _flushed(native)] == ["a"]


def test_partial_flush_failure_drops_only_that_batch():
    class _Broken(_ChunkingNativeSpan):
        def record_span_events(self, events):
            raise RuntimeError("boom")

    native = _Broken()
    span = Span(native, event_flush_size=1)
    span.new_span_event("lost").end()
    span.new_span_event("kept").end()
    span._event_flush_size = 0  # the second batch rides the end flush
    span.new_span_event("also_kept").end()
    span.end()
    assert [r[_REC_OPERATION] for r in _flushed(native)] == ["also_kept"]
