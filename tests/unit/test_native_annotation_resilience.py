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

"""Native finalize must survive malformed/out-of-range annotation data.

Exercises the *real* ``_native`` binding (not a fake): a recoverable
annotation-data error — an int64 value cast to int32, a bad string cast, or a
non-tuple buffer item — must not escape marshalling and abort the whole
``EndSpan`` sequence, which would leak the span and let the active-transaction
stat grow without bound. ``end_span_with_data`` always runs finalize; the bad
annotation (or the whole malformed span-event record) is skipped, everything
else is applied.

Span events reach native only as the completed records batched into
``end_span_with_data``'s ``span_events`` argument (the wrapper's Python-side
event stack flushes them at ``Span.end()``), so the event-side annotation
resilience is exercised through that replay path.

The native agent admits spans only after AgentInfo registration, so the fixture
starts the agent against the in-process mock collector before exercising the
real annotation/finalize path.
"""

import datetime
import decimal
import sys
import time
import uuid

import pytest

from pinpoint.tracer import _snapshot_sql_binds
from tests.integration._collector import start_native_agent

# An int64 value that overflows the int32 annotation slot — the concrete Kafka
# offset trigger from the fix report.
_OVER_INT32 = 2 ** 40


@pytest.fixture(scope="module")
def native_agent():
    with start_native_agent("unit-native-annotation") as agent:
        yield agent


def _new_span(agent):
    span, _sampled, _trace_id, _span_id, _revision = agent.new_span("op", "/rpc", {}, "")
    assert span is not None
    return span


def _end_span(span, annotations, span_events=(), error_verdicts=()):
    # Same argument shape the tracer.Span wrapper uses at end().
    span.end_span_with_data(
        1700, "10.0.0.1", "endpoint", "acceptor", 200, "/p", "GET",
        error_verdicts, annotations, span_events, False,
    )


def _event_record(annotations, sequence=0, depth=1, service_type=1400,
                  operation="child-op", destination="", endpoint="",
                  next_span_id=0, async_id=0):
    # Same record shape SpanEvent._finalize buffers (see tracer.py).
    now = int(time.time() * 1000)
    return (sequence, depth, now - 5, now, service_type, operation,
            destination, endpoint, next_span_id, async_id, annotations)


def test_out_of_range_int_annotation_does_not_abort_finalize(native_agent):
    span = _new_span(native_agent)
    # tag 0 == _ANN_INT: a value overflowing int32 must not abort finalize.
    _end_span(span, [(0, 40, _OVER_INT32)])


def test_long_annotation_flushes_at_finalize(native_agent):
    span = _new_span(native_agent)
    # tag 3 == _ANN_LONG: an int64 offset flushes cleanly instead of overflowing.
    _end_span(span, [(3, 40, _OVER_INT32)])


def test_string_string_annotation_flushes_on_span(native_agent):
    span = _new_span(native_agent)
    # tag 2 == _ANN_STRING_STRING on a span — the shape Python-side header
    # recording buffers. A malformed key is skipped and finalize always runs.
    _end_span(span, [(2, 47, "X-Capture", "v"), (2, "bad-key", "a", "b")])


def test_non_tuple_annotation_item_does_not_abort_finalize(native_agent):
    span = _new_span(native_agent)
    # A list (not a tuple) must be skipped, with finalize still running.
    _end_span(span, [[0, 40, 1]])


def test_bad_value_cast_does_not_abort_finalize(native_agent):
    span = _new_span(native_agent)
    # tag 0 with a non-int value: the int32 cast fails but finalize continues.
    _end_span(span, [(0, 40, "not-an-int")])


def test_mixed_good_and_bad_annotations_all_finalize(native_agent):
    span = _new_span(native_agent)
    # A batch mixing every failure mode with a couple of valid annotations:
    # the whole call must complete, applying the good ones and skipping the bad.
    _end_span(span, [
        (0, 40, _OVER_INT32),      # int32 overflow -> skipped
        (3, 41, _OVER_INT32),      # valid int64 -> applied
        ("x", 1, 2),               # bad tag cast -> skipped
        [2, 1],                    # non-tuple -> skipped
        (1, 5, "ok"),              # valid string -> applied
    ])


def test_malformed_error_verdict_does_not_abort_finalize(native_agent):
    span = _new_span(native_agent)
    reports = []
    previous = sys.unraisablehook
    sys.unraisablehook = lambda hook: reports.append(str(hook.object))
    try:
        _end_span(
            span,
            [(1, 5, "annotation after malformed verdict")],
            error_verdicts=[
                ("GoodError", "kept"),
                ["not", "a tuple"],
                ("short",),
                (123, "bad name"),
                ("AlsoGood", "kept too"),
            ],
        )
    finally:
        sys.unraisablehook = previous

    assert any("skipped malformed error verdict" in report
               for report in reports)


def test_span_event_finalize_survives_bad_annotation(native_agent):
    span = _new_span(native_agent)
    # A nonzero next_span_id also exercises the SetNextSpanId replay.
    _end_span(span, [], [
        _event_record([(0, 40, _OVER_INT32), (1, 5, "ok")],
                      destination="destination", endpoint="endpoint",
                      next_span_id=987654321),
    ])


def test_malformed_span_event_record_does_not_abort_finalize(native_agent):
    """A malformed event record — wrong item type, wrong field type, short
    tuple — is dropped whole while the rest of the batch and the span
    finalize still run."""
    span = _new_span(native_agent)
    _end_span(span, [], [
        "not-a-tuple",                                   # skipped
        (0, 1),                                          # short tuple -> skipped
        _event_record([(1, 5, "ok")], sequence="bad"),   # bad cast -> skipped
        _event_record([(1, 5, "ok")], sequence=1),       # valid -> applied
    ])


def test_unencodable_event_field_costs_only_that_field(native_agent):
    """A lone surrogate — what ``surrogateescape`` yields for a non-UTF-8
    filename, so a real operation name can carry one — fails
    ``PyUnicode_AsUTF8AndSize``. Dropping the whole record over it would lose
    that event's SQL, error and annotations while the request itself
    succeeded, so the field is replaced and the event kept; the unraisable
    context distinguishes the two outcomes."""
    lone_surrogate = "orders/\udcff"
    reports = []
    previous = sys.unraisablehook
    # pybind11's discard_as_unraisable passes its context as the *object*
    # PyErr_WriteUnraisable reports, so err_msg stays None.
    sys.unraisablehook = lambda hook: reports.append(str(hook.object))
    try:
        span = _new_span(native_agent)
        _end_span(span, [], [
            _event_record([(1, 5, "kept"), (4, "SELECT 1", "")],
                          operation=lone_surrogate,
                          destination=lone_surrogate,
                          endpoint=lone_surrogate),
            _event_record([(1, 5, "ok")], sequence=1),   # unaffected neighbour
        ])
    finally:
        sys.unraisablehook = previous

    # One report per unencodable field, and none of them a dropped event.
    assert len(reports) == 3, reports
    assert all("not encodable to UTF-8" in r for r in reports), reports
    assert not any("skipped malformed span event" in r for r in reports), reports


def test_non_str_event_field_still_drops_the_record(native_agent):
    """The other half of the contract: only the tracer writes these fields, so
    a non-str is a malformed record rather than application data, and the whole
    event is still dropped."""
    span = _new_span(native_agent)
    _end_span(span, [], [
        _event_record([(1, 5, "ok")], operation=42),   # not a str -> skipped
        _event_record([(1, 5, "ok")], sequence=1),     # valid -> applied
    ])


def test_span_event_replay_respects_native_limits(native_agent):
    """The native RecordSpanEvent backstop drops records past the configured
    max depth/sequence (returning the shared no-op event) without aborting
    the batch or the finalize."""
    span = _new_span(native_agent)
    _end_span(span, [], [
        _event_record([(1, 5, "ok")], sequence=10 ** 6),  # past max sequence
        _event_record([(1, 5, "ok")], depth=10 ** 6),     # past max depth
        _event_record([(1, 5, "ok")]),                    # valid -> applied
    ])


def test_span_event_buffered_sql_flushes_at_finalize(native_agent):
    """tag 4 == _ANN_SQL: the tracer buffers set_sql_query() Python-side and
    flushes it with the event record at Span.end(). A malformed item (non-str
    SQL) must be skipped without aborting the finalize."""
    span = _new_span(native_agent)
    _end_span(span, [], [
        _event_record(
            [
                (4, "SELECT * FROM t WHERE id = ?", ("42",)),  # typed binds
                (4, "SELECT 1", ""),                           # legacy no-args
                (4, 123, ()),                                  # bad sql type -> skipped
                (1, 5, "ok"),                                  # plain annotation still applied
            ],
            service_type=2501, operation="buffered-sql",
            destination="db", endpoint="db:3306",
        ),
    ])


def test_buffered_error_flushes_at_finalize(native_agent):
    """tag 5 == _ANN_ERROR: set_error is buffered Python-side and flushed at
    end(). All arities must apply — including the dumped-frames form on a
    span event — a malformed item must be skipped, and finalize always runs."""
    span = _new_span(native_agent)
    frames = [("mod", "fn", "file.py", 1)]
    # The span-side flush takes the 1- and 2-arg forms only.
    _end_span(span, [(5, "SpanError", "boom"), (5, "message-only")], [
        _event_record(
            [
                (5, "message-only"),
                (5, "Name", "message"),
                (5, "Name", "message", frames),
                (5, "Name", "message", [("mod", 1)]),  # bad frame shape -> skipped
                (5, 123),        # bad message type -> skipped
                (1, 5, "ok"),    # plain annotation still applied
            ],
            operation="buffered-error",
        ),
    ])


def test_buffered_proxy_annotation_flushes_at_finalize(native_agent):
    """tag 6 == _ANN_LONG_IIBBS: the composite proxy-header annotation is
    buffered Python-side (http_helper's setProxyHeader port) and flushed at
    end(). A malformed item is skipped and finalize always runs."""
    span = _new_span(native_agent)
    _end_span(span, [
        (6, 300, 1755678900123, 2, 150, 10, 20, "backend"),
        (6, 300, "bad-long", 2, 150, 10, 20, "x"),  # bad cast -> skipped
        (1, 5, "ok"),                               # plain annotation applied
    ])


def _sql_event(sql_items, sequence=0, operation="sql"):
    # The wrapper freezes bind values at set_sql_query time (str() for anything
    # the marshaller cannot keep typed); feed the marshaller those same shapes.
    frozen = [(tag, sql, _snapshot_sql_binds(args)) for tag, sql, args in sql_items]
    return _event_record(frozen, sequence=sequence, service_type=2501,
                         operation=operation)


def test_span_event_accepts_typed_and_legacy_sql_bind_values(native_agent):
    span = _new_span(native_agent)
    _end_span(span, [], [
        _sql_event([(4, "INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?)",
                     [None, True, -7, 2 ** 32, 2 ** 63, 1.5, "hello"])],
                   operation="typed-sql"),
        # Keep source compatibility with the former single-string Python API.
        _sql_event([(4, "SELECT * FROM t WHERE id = ?", "42")],
                   sequence=1, operation="legacy-sql"),
    ])


def test_span_event_sql_bind_values_never_raise(native_agent):
    """Bind conversion runs inside the traced query path, so values a driver
    can legitimately bind — datetime, Decimal, UUID, ints beyond int64/uint64 —
    must degrade to their str() form instead of raising into the app."""
    span = _new_span(native_agent)

    class _BrokenStr:
        def __str__(self):
            raise RuntimeError("no text form")

    _end_span(span, [], [
        _sql_event([(4, "INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)",
                     [
                         datetime.datetime(2026, 7, 16, 12, 0, 0),
                         decimal.Decimal("19.99"),
                         uuid.UUID("12345678-1234-5678-1234-567812345678"),
                         -(2 ** 63) - 1,   # below int64 min -> exact digits via str()
                         2 ** 64,          # above uint64 max -> exact digits via str()
                         _BrokenStr(),     # failing __str__ -> placeholder, still no raise
                     ])], operation="fallback-sql"),
    ])


def test_span_event_sql_args_object_records_whole_as_str(native_agent):
    """Only list/tuple expand element-wise into bind values. Any other args
    object — a dict of named params, bytes, a generator — must be recorded
    whole via str(), not iterated into keys or individual bytes."""
    span = _new_span(native_agent)
    _end_span(span, [], [
        _sql_event([
            (4, "SELECT * FROM t WHERE name = %(name)s", {"name": "Alice"}),
            (4, "SELECT * FROM t WHERE id = ?", b"42"),
            (4, "SELECT * FROM t WHERE id = ?", (v for v in (1, 2))),
        ], operation="object-args-sql"),
    ])
