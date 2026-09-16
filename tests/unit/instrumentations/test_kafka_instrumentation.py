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

"""kafka-python producer + consumer instrumentation.

Drives the (synchronous) wrappers against in-memory fakes — no real Kafka.
Validates trace-header injection on send, span-event lifecycle, the consumer
delivery span, and the no-upstream-context fast path (reader-less new_span).
"""

from __future__ import annotations

import pytest

from _fakes import (FakeAgent as _FakeAgent, FakeNativeSpan as _FakeNativeSpan, stub_inject)

import pinpoint
from pinpoint import context as ppctx
from pinpoint.annotation import ANNOTATION_KAFKA_BATCH, ANNOTATION_KAFKA_HEADER
from pinpoint.instrumentations import _kafka as kafka_common
from pinpoint.instrumentations import kafka as kafka_instr


@pytest.fixture(autouse=True)
def _stub_inject(monkeypatch):
    from pinpoint.instrumentations import _kafka

    stub_inject(monkeypatch, _kafka)


class _FakeClient:
    def __init__(self, broker="broker:9092"):
        self.config = {"bootstrap_servers": broker}


class _Record:
    def __init__(self, topic="orders", partition=0, offset=42, headers=None):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.headers = headers or []


def test_consumer_registration_cannot_precheck_away_scope_cleanup(monkeypatch):
    """The installed wrapper, not just its callback in isolation, must run
    after an agent is disabled so it can close the previous record scope."""
    registrations = {}

    def capture_wrap(module, target, wrapper, precheck=None):
        registrations[target] = (module, wrapper, precheck)
        return True

    monkeypatch.setattr(kafka_instr, "wrap", capture_wrap)
    kafka_instr.KafkaInstrumentor()._instrument()

    assert registrations["KafkaConsumer.__next__"][2] is None
    assert registrations["KafkaConsumer.poll"][2] is None


# ---------------------------------------------------------------------------
# Shared consumer-header bounds
# ---------------------------------------------------------------------------

def test_kafka_header_context_reuses_list_and_extracts_only_trace_headers():
    class _MustNotStringify:
        def __str__(self):
            raise AssertionError("non-Pinpoint value must not be decoded")

    pairs = [
        ("x-custom", _MustNotStringify()),
        ("Pinpoint-TraceID", b"upstream"),
    ]
    reusable, headers = kafka_common.kafka_header_context(pairs)

    assert reusable is pairs
    assert headers == {"Pinpoint-TraceID": "upstream"}


def test_kafka_header_context_bounds_one_shot_iterables():
    consumed = 0

    def generate_headers():
        nonlocal consumed
        while True:
            consumed += 1
            yield (f"x-{consumed}", b"v")

    reusable, headers = kafka_common.kafka_header_context(generate_headers())

    assert headers is None
    assert len(reusable) == kafka_common._KAFKA_HEADER_MAX_SCAN + 1
    assert consumed == kafka_common._KAFKA_HEADER_MAX_SCAN + 1


def test_kafka_header_context_snapshots_one_shot_items_result():
    class _Headers:
        def items(self):
            yield "Pinpoint-TraceID", b"upstream"
            yield "x-custom", b"value"

    reusable, headers = kafka_common.kafka_header_context(_Headers())

    assert reusable == (
        ("Pinpoint-TraceID", b"upstream"),
        ("x-custom", b"value"),
    )
    assert headers == {"Pinpoint-TraceID": "upstream"}

    class _Target:
        def __init__(self):
            self.entries = []

        def annotate_string(self, key, value):
            self.entries.append(("str", key, value))

    target = _Target()
    kafka_common.annotate_kafka_headers(target, reusable)
    assert target.entries == [
        ("str", ANNOTATION_KAFKA_HEADER, "x-custom=value"),
    ]


def test_kafka_header_annotations_enforce_count_and_size_budgets():
    class _Target:
        def __init__(self):
            self.values = []

        def annotate_string(self, key, value):
            assert key == ANNOTATION_KAFKA_HEADER
            self.values.append(value)

    target = _Target()
    headers = [
        (f"x-{index}", b"v" * (kafka_common._KAFKA_HEADER_MAX_VALUE_BYTES * 2))
        for index in range(kafka_common._KAFKA_HEADER_MAX_SCAN + 10)
    ]

    kafka_common.annotate_kafka_headers(target, headers)

    assert target.values[-1] == kafka_common._KAFKA_HEADER_TRUNCATION_MARKER
    recorded = target.values[:-1]
    assert len(recorded) <= kafka_common._KAFKA_HEADER_MAX_ANNOTATIONS
    assert sum(map(len, recorded)) <= kafka_common._KAFKA_HEADER_MAX_TOTAL_CHARS
    assert all(
        len(value)
        <= (
            kafka_common._KAFKA_HEADER_MAX_KEY_BYTES
            + 1
            + kafka_common._KAFKA_HEADER_MAX_VALUE_BYTES
            + len(kafka_common._KAFKA_HEADER_TRUNCATION_SUFFIX)
        )
        for value in recorded
    )


# ---------------------------------------------------------------------------
# Producer: send wrapper
# ---------------------------------------------------------------------------

def test_producer_send_emits_event_and_injects_headers(push_span):
    _, rec = push_span
    captured = {}

    def wrapped(*args, **kwargs):
        captured["kwargs"] = kwargs
        return "future"

    out = kafka_instr._producer_send_wrapper(
        wrapped, instance=_FakeClient(),
        args=("billing",), kwargs={"value": b"payload"},
    )
    assert out == "future"
    operation = "kafka.producer.kafka.KafkaProducer.send"
    assert ("event_start", "root", operation) in rec.events
    assert ("event_end", "root", operation) in rec.events
    headers = captured["kwargs"].get("headers")
    assert headers is not None
    keys = {k for k, _ in headers}
    assert "Pinpoint-TraceID" in keys


def test_producer_send_preserves_existing_headers(push_span):
    span, _rec = push_span
    captured = {}

    def wrapped(*_a, **kw):
        captured["kwargs"] = kw

    kafka_instr._producer_send_wrapper(
        wrapped, instance=_FakeClient(),
        args=("orders",),
        kwargs={"headers": [("x-custom", b"v")]},
    )
    headers = dict(captured["kwargs"]["headers"])
    assert headers["x-custom"] == b"v"
    assert headers["Pinpoint-TraceID"] == b"trace-id"
    ev = span._native.all_events[-1]
    assert ("str", ANNOTATION_KAFKA_HEADER, "x-custom=v") in ev.annotations.entries
    assert not any(
        isinstance(entry[2], str) and "Pinpoint-" in entry[2]
        for entry in ev.annotations.entries
    )


def test_producer_send_ends_event_when_annotation_fails_no_leak(push_span, monkeypatch):
    """Regression (native span-event leak): a failure in the sampled-path
    annotation must end the already-opened span event and fall back to a single
    untraced send — not leak the event."""
    _, rec = push_span
    calls = {"n": 0, "boom": 0}

    def _boom(*_a, **_kw):
        calls["boom"] += 1
        raise RuntimeError("annotate failed")

    monkeypatch.setattr(kafka_common, "annotate_producer", _boom)

    def wrapped(*_a, **_kw):
        calls["n"] += 1
        return "future"

    out = kafka_instr._producer_send_wrapper(
        wrapped, instance=_FakeClient(),
        args=("billing",), kwargs={"value": b"payload"},
    )

    # The send still ran, exactly once, and the failure path really was taken.
    assert out == "future"
    assert calls["n"] == 1
    assert calls["boom"] == 1
    # Event opened, then ended on failure — no leaked span event.
    operation = "kafka.producer.kafka.KafkaProducer.send"
    assert ("event_start", "root", operation) in rec.events
    assert ("event_end", "root", operation) in rec.events


def test_producer_send_positional_headers_no_collision(push_span):
    """``KafkaProducer.send(topic, value, key, headers)`` passes ``headers``
    positionally. The wrapper must merge into that positional slot instead of
    also setting ``kwargs['headers']`` — otherwise ``wrapped(*args, **kwargs)``
    raises ``TypeError: got multiple values for 'headers'``."""
    span, _rec = push_span
    captured = {}

    def wrapped(*args, **kwargs):
        # Mirror the real signature so a double-passed ``headers`` would
        # actually raise, not silently pass.
        captured["args"] = args
        captured["kwargs"] = kwargs
        return "future"

    out = kafka_instr._producer_send_wrapper(
        wrapped, instance=_FakeClient(),
        # topic, value, key, headers — all positional.
        args=("orders", b"payload", b"k", [("x-custom", b"v")]),
        kwargs={},
    )
    assert out == "future"
    # No stray ``headers`` keyword was introduced.
    assert "headers" not in captured["kwargs"]
    # Injected + existing headers live in the positional slot (index 3).
    merged = dict(captured["args"][3])
    assert merged["x-custom"] == b"v"
    assert merged["Pinpoint-TraceID"] == b"trace-id"
    ev = span._native.all_events[-1]
    assert ("str", ANNOTATION_KAFKA_HEADER, "x-custom=v") in ev.annotations.entries


def test_producer_send_does_not_mutate_original_args(push_span):
    """The wrapper works on copies so a ``safe_wrapper`` fallback retry sees
    the caller's original, uninjected arguments."""
    original_headers = [("x-custom", b"v")]
    args = ("orders", b"payload", b"k", original_headers)

    def wrapped(*_a, **_kw):
        return "future"

    kafka_instr._producer_send_wrapper(
        wrapped, instance=_FakeClient(), args=args, kwargs={},
    )
    # Original tuple element untouched — no Pinpoint headers leaked in.
    assert original_headers == [("x-custom", b"v")]


def test_producer_send_records_exception(push_span):
    _, rec = push_span

    def wrapped(*_a, **_kw):
        raise RuntimeError("kaput")

    with pytest.raises(RuntimeError, match="kaput"):
        kafka_instr._producer_send_wrapper(
            wrapped, instance=_FakeClient(),
            args=("orders",), kwargs={},
        )
    assert any(e[0] == "event_error" for e in rec.events)


def test_producer_send_no_parent_passes_through():
    seen = {}

    def wrapped(*_a, **kw):
        seen["kwargs"] = kw
        return "future"

    out = kafka_instr._producer_send_wrapper(
        wrapped, instance=_FakeClient(),
        args=("orders",), kwargs={},
    )
    assert out == "future"
    # No active span -> no header injection.
    assert "headers" not in seen["kwargs"]


# ---------------------------------------------------------------------------
# Consumer: __next__ wrapper
# ---------------------------------------------------------------------------

def test_consumer_next_holds_the_span_across_the_caller_body(fake_agent):
    """The span stays open — and current — after ``__next__`` returns, so the
    caller's handling of the record nests inside it. It closes when the caller
    comes back for the next record."""
    record = _Record(topic="orders", partition=0, offset=99,
                     headers=[("Pinpoint-TraceID", b"abc")])
    consumer = _FakeClient()

    def wrapped(*_a, **_kw):
        return record

    out = kafka_instr._consumer_next_wrapper(
        wrapped, instance=consumer, args=(), kwargs={},
    )
    assert out is record
    rpc = "kafka://topic=orders?partition=0&offset=99"
    assert ("span_start", "Kafka Consumer Invocation", rpc) in fake_agent.events
    # Still open, and the caller's work would land on it.
    assert ("span_end", "Kafka Consumer Invocation") not in fake_agent.events
    assert ppctx.current_span() is not None

    kafka_instr._consumer_next_wrapper(
        wrapped, instance=consumer, args=(), kwargs={},
    )
    assert ("span_end", "Kafka Consumer Invocation") in fake_agent.events
    kafka_instr._consumer_close_wrapper(
        lambda *_a, **_kw: None, instance=consumer, args=(), kwargs={},
    )
    assert ppctx.current_span() is None
    assert not any(
        isinstance(entry[2], str) and "Pinpoint-" in entry[2]
        for entry in fake_agent.last_native.annotations.entries
    )
    # Upstream Pinpoint header present -> reader-backed native call.
    assert fake_agent.last_native.headers is not None


def test_consumer_next_skips_reader_when_no_upstream_context(fake_agent):
    """A record without a Pinpoint header opens a new trace via the
    reader-less new_span; the delivery span still opens and closes."""
    record = _Record(topic="orders", partition=0, offset=99,
                     headers=[("x-custom", b"v")])
    consumer = _FakeClient()

    def wrapped(*_a, **_kw):
        return record

    out = kafka_instr._consumer_next_wrapper(
        wrapped, instance=consumer, args=(), kwargs={},
    )
    kafka_instr._consumer_close_wrapper(
        lambda *_a, **_kw: None, instance=consumer, args=(), kwargs={},
    )
    assert out is record
    assert fake_agent.last_native.headers is None
    rpc = "kafka://topic=orders?partition=0&offset=99"
    assert ("span_start", "Kafka Consumer Invocation", rpc) in fake_agent.events
    assert ("span_end", "Kafka Consumer Invocation") in fake_agent.events
    assert (
        "str",
        ANNOTATION_KAFKA_HEADER,
        "x-custom=v",
    ) in fake_agent.last_native.annotations.entries


def test_consumer_next_unsampled_skips_annotation_prep(monkeypatch):
    """On an unsampled span the annotation block is gated out: no broker
    lookup and no per-header decode/format — only the delivery span
    open+close. Sampled coverage lives in the tests above."""
    from pinpoint.agent import UnSampledSpan

    agent = _FakeAgent()

    def _unsampled_new_span(operation, rpc_point, headers=None):
        native = _FakeNativeSpan(operation, rpc_point, recorder=agent,
                                 headers=headers)
        agent.last_native = native
        return UnSampledSpan(native)

    agent.new_span = _unsampled_new_span
    monkeypatch.setattr(pinpoint.agent, "_instance", agent)

    broker_calls = []
    monkeypatch.setattr(
        kafka_instr, "_bootstrap_server",
        lambda c: broker_calls.append(c) or "broker:9092",
    )

    record = _Record(topic="orders", partition=0, offset=99,
                     headers=[("Pinpoint-TraceID", b"abc"), ("x-custom", b"v")])

    def wrapped(*_a, **_kw):
        return record

    consumer = _FakeClient()
    out = kafka_instr._consumer_next_wrapper(
        wrapped, instance=consumer, args=(), kwargs={},
    )
    kafka_instr._consumer_close_wrapper(
        lambda *_a, **_kw: None, instance=consumer, args=(), kwargs={},
    )
    assert out is record
    rpc = "kafka://topic=orders?partition=0&offset=99"
    assert ("span_start", "Kafka Consumer Invocation", rpc) in agent.events
    assert ("span_end", "Kafka Consumer Invocation") in agent.events
    # Gate held: no broker lookup, no annotation entries.
    assert broker_calls == []
    assert agent.last_native.annotations.entries == []


def test_consumer_next_no_op_when_disabled(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))
    record = _Record()

    def wrapped(*_a, **_kw):
        return record

    out = kafka_instr._consumer_next_wrapper(
        wrapped, instance=_FakeClient(), args=(), kwargs={},
    )
    assert out is record


# ---------------------------------------------------------------------------
# Consumer: poll wrapper (batch interface)
# ---------------------------------------------------------------------------

def test_consumer_poll_opens_span_per_record(fake_agent, monkeypatch):
    """Regression: ``poll`` returns ``{TopicPartition: [records]}`` and never
    flows through ``__next__``. Open one delivery span per record, annotate the
    batch size for multi-record partitions, and preserve each record's own
    upstream context (mirrors aiokafka getmany / confluent consume)."""
    records = {
        "orders-0": [
            _Record(topic="orders", partition=0, offset=1),
            _Record(topic="orders", partition=0, offset=2),
        ],
        "billing-1": [
            _Record(topic="billing", partition=1, offset=7,
                    headers=[("Pinpoint-TraceID", b"abc"), ("x-custom", b"v")]),
        ],
    }

    def wrapped(*_a, **_kw):
        return records

    broker_calls = []
    monkeypatch.setattr(
        kafka_instr,
        "_bootstrap_server",
        lambda consumer: broker_calls.append(consumer) or "broker:9092",
    )

    out = kafka_instr._consumer_poll_wrapper(
        wrapped, instance=_FakeClient(), args=(500,), kwargs={},
    )
    assert out is records

    span_starts = [e for e in fake_agent.events if e[0] == "span_start"]
    assert [e[2] for e in span_starts] == [
        "kafka://topic=orders?partition=0&offset=1",
        "kafka://topic=orders?partition=0&offset=2",
        "kafka://topic=billing?partition=1&offset=7",
    ]
    assert len([e for e in fake_agent.events if e[0] == "span_end"]) == 3
    assert len(broker_calls) == 1

    # The 2-record partition annotates batch size 2 on each of its spans; the
    # single-record partition annotates no batch size at all.
    batch_entries = [
        entry
        for native in fake_agent.native_spans
        for entry in native.annotations.entries
        if entry[1] == ANNOTATION_KAFKA_BATCH
    ]
    assert batch_entries == [
        ("int", ANNOTATION_KAFKA_BATCH, 2),
        ("int", ANNOTATION_KAFKA_BATCH, 2),
    ]

    # Per-record upstream context is independent: only the billing record
    # carried a Pinpoint header, so only its span got a reader-backed new_span.
    assert fake_agent.native_spans[0].headers is None
    assert fake_agent.native_spans[1].headers is None
    assert fake_agent.native_spans[2].headers is not None
    assert (
        "str",
        ANNOTATION_KAFKA_HEADER,
        "x-custom=v",
    ) in fake_agent.native_spans[2].annotations.entries


def test_consumer_poll_skips_iterator_internal_call(fake_agent):
    """``__next__`` drives ``poll(update_offsets=False)`` internally and
    instruments each yielded record itself. The poll wrapper must skip that
    internal call so records are not double-spanned (and so records the iterator
    has not yet handed to the caller are not spanned early)."""
    records = {"orders-0": [_Record(topic="orders", partition=0, offset=1)]}

    def wrapped(*_a, **_kw):
        return records

    # Exactly how kafka.consumer.group._message_generator_v2 calls poll.
    out = kafka_instr._consumer_poll_wrapper(
        wrapped, instance=_FakeClient(),
        args=(), kwargs={"timeout_ms": 1000, "update_offsets": False},
    )
    assert out is records
    assert not any(e[0] == "span_start" for e in fake_agent.events)


def test_consumer_poll_positional_update_offsets_false_skips(fake_agent):
    """The internal flag can also arrive positionally as
    ``poll(timeout_ms, max_records, update_offsets)`` — index 2 == False must be
    skipped too."""
    records = {"orders-0": [_Record()]}

    def wrapped(*_a, **_kw):
        return records

    out = kafka_instr._consumer_poll_wrapper(
        wrapped, instance=_FakeClient(), args=(1000, 10, False), kwargs={},
    )
    assert out is records
    assert not any(e[0] == "span_start" for e in fake_agent.events)


def test_consumer_poll_empty_batch_no_span(fake_agent):
    """An empty poll returns ``{}`` — no spans opened."""
    def wrapped(*_a, **_kw):
        return {}

    out = kafka_instr._consumer_poll_wrapper(
        wrapped, instance=_FakeClient(), args=(500,), kwargs={},
    )
    assert out == {}
    assert not any(e[0] == "span_start" for e in fake_agent.events)


def test_consumer_poll_no_op_when_disabled(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))
    records = {"orders-0": [_Record()]}

    def wrapped(*_a, **_kw):
        return records

    out = kafka_instr._consumer_poll_wrapper(
        wrapped, instance=_FakeClient(), args=(500,), kwargs={},
    )
    assert out is records


def test_disabled_mid_run_releases_the_held_record_scope(fake_agent, monkeypatch):
    """``pinpoint.shutdown()`` while a record scope is held: the next fetch
    must still release it. Returning early on the agent check left that span
    open until the consumer was closed — which a loop that simply stops
    polling never does — and left its span current on the thread."""
    consumer = _FakeClient()
    kafka_instr._consumer_next_wrapper(
        lambda *_a, **_kw: _Record(offset=1), instance=consumer,
        args=(), kwargs={},
    )
    assert ppctx.current_span() is not None          # scope held

    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))
    record = kafka_instr._consumer_next_wrapper(
        lambda *_a, **_kw: _Record(offset=2), instance=consumer,
        args=(), kwargs={},
    )

    assert record.offset == 2                        # caller still served
    assert ppctx.current_span() is None              # scope released
    assert getattr(consumer, kafka_common._SCOPE_ATTR, None) is None
    assert all(s.end_called for s in fake_agent.native_spans)


def test_disabled_mid_run_releases_the_held_scope_on_batch_poll(fake_agent, monkeypatch):
    """Same for the batch surface: a user-facing poll releases the scope the
    iterator left open even when the agent went away in between."""
    consumer = _FakeClient()
    kafka_instr._consumer_next_wrapper(
        lambda *_a, **_kw: _Record(offset=1), instance=consumer,
        args=(), kwargs={},
    )
    assert ppctx.current_span() is not None

    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))
    records = {"orders-0": [_Record(offset=2)]}
    out = kafka_instr._consumer_poll_wrapper(
        lambda *_a, **_kw: records, instance=consumer, args=(500,), kwargs={},
    )

    assert out is records
    assert ppctx.current_span() is None


def test_iterator_internal_poll_keeps_the_scope_when_disabled(fake_agent, monkeypatch):
    """The iterator-internal ``poll(update_offsets=False)`` must stay hands-off
    even on the disabled path: ``__next__`` owns that scope, and closing it
    here would end the record span mid-handling."""
    consumer = _FakeClient()
    kafka_instr._consumer_next_wrapper(
        lambda *_a, **_kw: _Record(offset=1), instance=consumer,
        args=(), kwargs={},
    )
    held = ppctx.current_span()
    assert held is not None

    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))
    kafka_instr._consumer_poll_wrapper(
        lambda *_a, **_kw: {}, instance=consumer,
        args=(500, None, False), kwargs={},
    )

    assert ppctx.current_span() is held
    kafka_instr._consumer_close_wrapper(
        lambda *_a, **_kw: None, instance=consumer, args=(), kwargs={},
    )
    assert ppctx.current_span() is None


def test_client_broker_memoizes_resolved_broker_on_the_client():
    """The bootstrap list is fixed for a client's life: resolve once, stash,
    and never re-run the resolver for that client."""
    class _Client:
        pass

    client = _Client()
    calls = []

    def resolver(c):
        calls.append(c)
        return "kafka.internal:9092"

    assert kafka_common.client_broker(client, resolver) == "kafka.internal:9092"
    assert kafka_common.client_broker(client, resolver) == "kafka.internal:9092"
    assert len(calls) == 1


def test_client_broker_does_not_cache_a_failed_resolve():
    """An empty resolve returns the Unknown placeholder but must not stick —
    a client populated late still gets picked up."""
    class _Client:
        pass

    client = _Client()
    answers = ["", "kafka.internal:9092"]

    def resolver(c):
        return answers.pop(0)

    assert kafka_common.client_broker(client, resolver) == "Unknown"
    assert kafka_common.client_broker(client, resolver) == "kafka.internal:9092"


# ---------------------------------------------------------------------------
# Held record scope: the span lasts the caller's handling
# ---------------------------------------------------------------------------

def test_caller_work_lands_on_the_held_record_span(fake_agent):
    """The point of holding the span: a child event created in the loop body
    belongs to that record's span, not to a later one."""
    consumer = _FakeClient()
    records = iter([_Record(offset=1), _Record(offset=2)])

    def wrapped(*_a, **_kw):
        return next(records)

    for _ in range(2):
        record = kafka_instr._consumer_next_wrapper(
            wrapped, instance=consumer, args=(), kwargs={},
        )
        ppctx.current_span().new_span_event(f"handle-{record.offset}").end()
    kafka_instr._consumer_close_wrapper(
        lambda *_a, **_kw: None, instance=consumer, args=(), kwargs={},
    )

    # Completion order, not creation order: the nested handle-N event finishes
    # before the kafka.consume event the scope closes. What matters is which
    # span each landed on.
    per_span = [{e.op for e in s.all_events} for s in fake_agent.native_spans]
    assert per_span == [{"kafka.consume", "handle-1"},
                        {"kafka.consume", "handle-2"}]
    assert all(s.end_called for s in fake_agent.native_spans)


def test_batch_poll_closes_a_held_scope_first(fake_agent, monkeypatch):
    """A held record span must not become the parent of the batch's delivery
    spans — the batch is a separate set of transactions."""
    monkeypatch.setattr(kafka_instr, "_bootstrap_server",
                        lambda _c: "broker:9092")
    consumer = _FakeClient()
    kafka_instr._consumer_next_wrapper(
        lambda *_a, **_kw: _Record(offset=1), instance=consumer,
        args=(), kwargs={},
    )
    assert ppctx.current_span() is not None

    kafka_instr._consumer_poll_wrapper(
        lambda *_a, **_kw: {"tp": [_Record(offset=2)]},
        instance=consumer, args=(), kwargs={},
    )
    # The held scope was released before the batch ran, and the batch's own
    # delivery spans close themselves.
    assert ppctx.current_span() is None
    assert all(s.end_called for s in fake_agent.native_spans)


def test_fetch_inside_an_active_span_leaves_it_current(fake_agent):
    """A fetch from inside someone else's trace (``poll()`` in a request
    handler) must not hold a record span: that span would become the parent of
    the rest of the request, and only this consumer's next fetch would close
    it. The record still reaches the caller — untraced beats corrupted."""
    consumer = _FakeClient()
    outer = fake_agent.new_span("outer", "/orders")
    token = ppctx.set_current_span(outer)
    try:
        record = kafka_instr._consumer_next_wrapper(
            lambda *_a, **_kw: _Record(offset=1), instance=consumer,
            args=(), kwargs={},
        )
        assert record.offset == 1
        assert ppctx.current_span() is outer
        assert getattr(consumer, kafka_common._SCOPE_ATTR, None) is None
        # No consume span was opened at all — only the caller's own.
        assert len(fake_agent.native_spans) == 1
    finally:
        ppctx.reset_current_span(token)


def test_scope_that_cannot_be_stored_degrades_to_delivery_only(fake_agent):
    """A consumer with no attribute slot (a C type) must not leak an open
    span — the scope closes immediately instead."""
    class _NoAttrs:
        __slots__ = ()

    kafka_common.advance_consume_scope(
        _NoAttrs(), fake_agent, _Record(offset=7), "broker:9092",
    )
    assert ppctx.current_span() is None
    assert fake_agent.native_spans and fake_agent.native_spans[0].end_called


def test_producer_send_replaces_stale_pinpoint_headers(push_span):
    """A relayed record carries the previous hop's Pinpoint-* headers; ours
    must replace them while user headers survive."""
    span, _rec = push_span
    captured = {}

    def wrapped(*_a, **kw):
        captured["kwargs"] = kw

    kafka_instr._producer_send_wrapper(
        wrapped, instance=_FakeClient(),
        args=("orders",),
        kwargs={"headers": [("pinpoint-traceid", b"old"),
                            ("Pinpoint-SpanID", b"old"),
                            ("Pinpoint-Sampled", b"s0"),
                            ("x-custom", b"v")]},
    )
    headers = captured["kwargs"]["headers"]
    keys = [k for k, _ in headers]
    assert keys.count("Pinpoint-TraceID") == 1 and "pinpoint-traceid" not in keys
    assert "Pinpoint-Sampled" not in keys  # sampled hop: no stale s0 marker
    assert dict(headers)["Pinpoint-TraceID"] == b"trace-id"
    assert dict(headers)["x-custom"] == b"v"


def test_dropped_consumer_ends_its_held_scope(fake_agent):
    """A worker that fetches one record and never fetches again or calls
    close() would otherwise leave the record span open and current on its
    thread for good. Collecting the consumer must end it and release the
    thread's context."""
    import gc

    consumer = _FakeClient()
    kafka_instr._consumer_next_wrapper(
        lambda *_a, **_kw: _Record(offset=1), instance=consumer,
        args=(), kwargs={},
    )
    assert ppctx.current_span() is not None          # scope held
    assert not all(s.end_called for s in fake_agent.native_spans)

    del consumer
    gc.collect()

    assert all(s.end_called for s in fake_agent.native_spans)
    assert ppctx.current_span() is None


def test_normal_close_detaches_the_gc_safety_net(fake_agent):
    """The finalizer must not keep an already-closed scope (and its span)
    alive for the life of a long-running consumer."""
    consumer = _FakeClient()
    kafka_instr._consumer_next_wrapper(
        lambda *_a, **_kw: _Record(offset=1), instance=consumer,
        args=(), kwargs={},
    )
    _scope, finalizer = getattr(consumer, kafka_common._SCOPE_ATTR)
    assert finalizer.alive

    kafka_common.close_consumer_scope(consumer)

    # Detached, not fired: the scope was closed by the consumer itself, and
    # the finalizer no longer holds it (peek() is None once detached).
    assert not finalizer.alive
    assert finalizer.peek() is None
    assert getattr(consumer, kafka_common._SCOPE_ATTR, None) is None
    assert ppctx.current_span() is None
    assert all(s.end_called for s in fake_agent.native_spans)
