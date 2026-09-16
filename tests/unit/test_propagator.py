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

"""Tests for pinpoint.propagator — distributed-tracing header propagation."""

from __future__ import annotations

import _thread

from _fakes import FakeNativeSpan
from pinpoint.propagator import (
    HEADER_FLAG,
    HEADER_HOST,
    HEADER_PARENT_APP_NAME,
    HEADER_PARENT_APP_NAMESPACE,
    HEADER_PARENT_APP_TYPE,
    HEADER_PARENT_SERVICE_NAME,
    HEADER_PARENT_SPAN_ID,
    HEADER_SAMPLED,
    HEADER_SPAN_ID,
    HEADER_TRACE_ID,
    extract_pinpoint_headers,
    inject_items,
)
from pinpoint.agent import UnSampledSpan, _NullSpan
from pinpoint.tracer import Span, SpanEvent


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


_TRACE_ID = "agent^100^7"


_INJECT_BASE = (
    (HEADER_PARENT_APP_NAME, "unit-app"),
    (HEADER_PARENT_APP_TYPE, "1700"),
    (HEADER_PARENT_APP_NAMESPACE, ""),
)


def _span_with_open_event(destination: str = ""):
    span = Span(FakeNativeSpan(), trace_id=_TRACE_ID, span_id=42,
                flags=0, inject_base=_INJECT_BASE)
    event = span.new_span_event("client-op")
    if destination:
        event.set_destination(destination)
    return span, event


# ---------------------------------------------------------------------------
# extract_pinpoint_headers
# ---------------------------------------------------------------------------


def test_extract_from_mapping_is_case_insensitive_and_canonicalizes():
    # HTTP/2 and message-queue carriers lowercase header names; extraction
    # must still find them and key the result by the canonical names.
    extracted = extract_pinpoint_headers({
        "pinpoint-traceid": "abc",
        "PINPOINT-SPANID": "7",
        "X-Custom": "val",
    })
    assert extracted == {HEADER_TRACE_ID: "abc", HEADER_SPAN_ID: "7"}


def test_extract_from_mapping_skips_none_values_and_coerces_str():
    extracted = extract_pinpoint_headers({
        "Pinpoint-TraceID": None,
        "Pinpoint-SpanID": 7,
    })
    assert extracted == {HEADER_SPAN_ID: "7"}


def test_extract_from_empty_mapping():
    assert extract_pinpoint_headers({}) == {}


def test_extract_from_reader_probes_canonical_names():
    class _Reader:
        def __init__(self):
            self.keys = []

        def get(self, key):
            self.keys.append(key)
            return "sampled" if key == HEADER_SAMPLED else None

    reader = _Reader()
    extracted = extract_pinpoint_headers(reader)
    assert extracted == {HEADER_SAMPLED: "sampled"}
    # Only the Pinpoint propagation names are ever probed — lazy readers
    # never have to materialize the full header set.
    assert HEADER_TRACE_ID in reader.keys
    assert all(k.startswith("Pinpoint-") for k in reader.keys)


# ---------------------------------------------------------------------------
# inject_items
# ---------------------------------------------------------------------------


def test_inject_items_builds_full_header_set_in_native_order():
    span, event = _span_with_open_event(destination="api.internal:8080")
    pairs = list(inject_items(span))

    keys = [k for k, _ in pairs]
    assert keys == [
        HEADER_TRACE_ID, HEADER_SPAN_ID, HEADER_PARENT_SPAN_ID, HEADER_FLAG,
        HEADER_PARENT_APP_NAME, HEADER_PARENT_APP_TYPE,
        HEADER_HOST,
    ]
    out = dict(pairs)
    assert out[HEADER_TRACE_ID] == "agent^100^7"
    assert out[HEADER_PARENT_SPAN_ID] == "42"
    assert out[HEADER_FLAG] == "0"
    assert out[HEADER_PARENT_APP_NAME] == "unit-app"
    assert out[HEADER_PARENT_APP_TYPE] == "1700"
    assert HEADER_PARENT_APP_NAMESPACE not in out
    assert out[HEADER_HOST] == "api.internal:8080"
    # A sampled transaction never writes the drop marker.
    assert HEADER_SAMPLED not in out
    # The generated child span id is buffered on the event so the finalize
    # flushes it as the native nextSpanId.
    next_span_id = int(out[HEADER_SPAN_ID])
    assert next_span_id != 0
    assert event._next_span_id == next_span_id


def test_inject_items_generates_a_fresh_child_span_id_per_call():
    span, _event = _span_with_open_event()
    first = dict(inject_items(span))[HEADER_SPAN_ID]
    second = dict(inject_items(span))[HEADER_SPAN_ID]
    assert first != second


def test_inject_items_without_open_event_returns_nothing():
    # An event-less span has nothing to inject against.
    span = Span(FakeNativeSpan(), trace_id=_TRACE_ID, span_id=42,
                inject_base=_INJECT_BASE)
    assert tuple(inject_items(span)) == ()


def test_inject_items_writes_span_headers():
    span, _event = _span_with_open_event()
    out = dict(inject_items(span))
    assert out[HEADER_TRACE_ID] == "agent^100^7"


def test_inject_items_none_span_is_empty():
    assert tuple(inject_items(None)) == ()  # type: ignore[arg-type]


def test_unsampled_span_injects_only_the_drop_marker():
    span = UnSampledSpan(FakeNativeSpan(), span_id=42)
    assert tuple(inject_items(span)) == ((HEADER_SAMPLED, "s0"),)


def test_noop_native_span_injects_nothing():
    # A native *noop* span (disabled agent, malformed inbound trace id) is
    # also wrapped as UnSampledSpan but carries span id 0 — it must not tell
    # the downstream agent to drop its own transaction.
    span = UnSampledSpan(FakeNativeSpan(), span_id=0)
    assert tuple(inject_items(span)) == ()


def test_null_span_injects_nothing():
    assert tuple(inject_items(_NullSpan())) == ()


# ---------------------------------------------------------------------------
# Header name constants
# ---------------------------------------------------------------------------


def test_header_constants_are_strings():
    for name, val in [
        ("HEADER_TRACE_ID", HEADER_TRACE_ID),
        ("HEADER_SPAN_ID", HEADER_SPAN_ID),
        ("HEADER_PARENT_SPAN_ID", HEADER_PARENT_SPAN_ID),
        ("HEADER_SAMPLED", HEADER_SAMPLED),
        ("HEADER_FLAG", HEADER_FLAG),
        ("HEADER_PARENT_APP_NAME", HEADER_PARENT_APP_NAME),
        ("HEADER_PARENT_APP_TYPE", HEADER_PARENT_APP_TYPE),
        ("HEADER_PARENT_APP_NAMESPACE", HEADER_PARENT_APP_NAMESPACE),
        ("HEADER_PARENT_SERVICE_NAME", HEADER_PARENT_SERVICE_NAME),
        ("HEADER_HOST", HEADER_HOST),
    ]:
        assert isinstance(val, str), f"{name} is not a str"


def test_header_constants_start_with_pinpoint():
    for val in (
        HEADER_TRACE_ID,
        HEADER_SPAN_ID,
        HEADER_PARENT_SPAN_ID,
        HEADER_SAMPLED,
        HEADER_FLAG,
        HEADER_PARENT_APP_NAME,
        HEADER_PARENT_APP_TYPE,
        HEADER_PARENT_APP_NAMESPACE,
        HEADER_PARENT_SERVICE_NAME,
        HEADER_HOST,
    ):
        assert val.startswith("Pinpoint-"), f"{val!r} missing Pinpoint- prefix"


def test_header_constants_are_unique():
    vals = [
        HEADER_TRACE_ID,
        HEADER_SPAN_ID,
        HEADER_PARENT_SPAN_ID,
        HEADER_SAMPLED,
        HEADER_FLAG,
        HEADER_PARENT_APP_NAME,
        HEADER_PARENT_APP_TYPE,
        HEADER_PARENT_APP_NAMESPACE,
        HEADER_PARENT_SERVICE_NAME,
        HEADER_HOST,
    ]
    assert len(vals) == len(set(vals)), "duplicate header name constants"


# ---------------------------------------------------------------------------
# inject_items — locking invariant
# ---------------------------------------------------------------------------


class _DepthTrackingLock:
    """RLock stand-in that exposes the current nesting depth."""

    def __init__(self):
        self._lock = _thread.RLock()
        self.depth = 0

    def __enter__(self):
        self._lock.acquire()
        self.depth += 1
        return self

    def __exit__(self, *_exc):
        self.depth -= 1
        self._lock.release()
        return False


class _DepthWatchingEvent(SpanEvent):
    """Span event that notes the lock depth in effect when inject stamped its
    ``_next_span_id``."""

    def __init__(self, span, lock):
        super().__init__(span, operation="client-op")
        self._watch_lock = lock
        self.stamped_at_depth = None

    def __setattr__(self, name, value):
        if name == "_next_span_id" and getattr(self, "_watch_lock", None):
            object.__setattr__(self, "stamped_at_depth", self._watch_lock.depth)
        object.__setattr__(self, name, value)


def test_inject_stamps_next_span_id_under_the_span_lock():
    """The child span id must be written to the event while the span lock is
    still held.

    Dropping the lock between the liveness check and the write lets a racing
    ``end()`` finalize the event in between: the id then goes out on the wire
    but is never recorded as the event's ``nextSpanId``, so the downstream span
    dangles with no parent to attach to.
    """
    span = Span(FakeNativeSpan(), trace_id=_TRACE_ID, span_id=42,
                flags=0, inject_base=_INJECT_BASE)
    lock = _DepthTrackingLock()
    span._native_lock = lock
    event = _DepthWatchingEvent(span, lock)  # pushes itself onto the stack

    out = dict(inject_items(span))

    assert event.stamped_at_depth == 1, (
        "next_span_id was stamped outside the span lock")
    assert int(out[HEADER_SPAN_ID]) == event._next_span_id
    assert lock.depth == 0


def test_empty_optional_headers_are_omitted():
    span, _ = _span_with_open_event()
    out = dict(inject_items(span))
    assert HEADER_HOST not in out
    assert HEADER_PARENT_APP_NAMESPACE not in out
    assert HEADER_PARENT_SERVICE_NAME not in out


def test_generated_child_id_excludes_sentinels_and_context_ids(monkeypatch):
    from pinpoint import tracer
    draws = iter([0, 2 ** 64 - 1, 42, 41, 99, 99, 100])
    monkeypatch.setattr(tracer._span_id_random, "getrandbits", lambda _: next(draws))
    span = Span(FakeNativeSpan(), trace_id=_TRACE_ID, span_id=42, parent_span_id=41)
    span.new_span_event("client")
    assert dict(inject_items(span))[HEADER_SPAN_ID] == "99"
    assert dict(inject_items(span))[HEADER_SPAN_ID] == "100"
