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

"""aiokafka producer + consumer instrumentation.

Drives the wrappers against in-memory fakes — no real Kafka. Validates
trace header injection on send, span event lifecycle, getone delivery
span, and getmany batch handling.
"""

from __future__ import annotations

import asyncio

import pytest

from _fakes import (FakeAgent as _FakeAgent, FakeNativeSpan as _FakeNativeSpan,
                    UnsampledNative, stub_inject)

import pinpoint
from pinpoint import context as ppctx
from pinpoint.annotation import ANNOTATION_KAFKA_BATCH, ANNOTATION_KAFKA_HEADER
from pinpoint.instrumentations import aiokafka as aiokafka_instr
from pinpoint.propagator import HEADER_SAMPLED


@pytest.fixture(autouse=True)
def _stub_inject(monkeypatch):
    from pinpoint.instrumentations import _kafka

    stub_inject(monkeypatch, _kafka)


class _Record:
    def __init__(self, topic="orders", partition=0, offset=42, headers=None):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.headers = headers or []


# ---------------------------------------------------------------------------
# Producer: send wrapper
# ---------------------------------------------------------------------------

def test_producer_send_emits_event_and_injects_headers(push_span):
    _, rec = push_span
    captured = {}

    async def wrapped(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return None

    asyncio.run(aiokafka_instr._producer_send_wrapper(
        wrapped, instance=None,
        args=("billing", b"payload"), kwargs={"key": b"k"},
    ))
    operation = "aiokafka.producer.producer.AIOKafkaProducer.send"
    assert ("event_start", "root", operation) in rec.events
    assert ("event_end", "root", operation) in rec.events
    headers = captured["kwargs"].get("headers")
    assert headers is not None
    keys = {k for k, _ in headers}
    assert "Pinpoint-TraceID" in keys


def test_producer_send_unsampled_inject_writes_s0_marker():
    """An unsampled parent must still propagate ``Pinpoint-Sampled: s0`` so
    a downstream consumer short-circuits its own sampling decision."""
    from pinpoint.agent import UnSampledSpan  # type: ignore[attr-defined]

    sp = UnSampledSpan(UnsampledNative())
    token = ppctx.set_current_span(sp)
    captured: dict = {}
    try:
        async def wrapped(*_a, **kw):
            captured["kwargs"] = kw
            return None

        asyncio.run(aiokafka_instr._producer_send_wrapper(
            wrapped, instance=None,
            args=("topic", b"payload"), kwargs={},
        ))
        headers = dict(captured["kwargs"]["headers"])
        assert headers.get(HEADER_SAMPLED) == b"s0"
    finally:
        ppctx.reset_current_span(token)


def test_producer_send_preserves_existing_headers(push_span):
    span, _rec = push_span
    captured = {}

    async def wrapped(*_a, **kw):
        captured["kwargs"] = kw

    asyncio.run(aiokafka_instr._producer_send_wrapper(
        wrapped, instance=None,
        args=("topic",),
        kwargs={"headers": [("x-custom", b"v")]},
    ))
    headers = dict(captured["kwargs"]["headers"])
    assert headers["x-custom"] == b"v"
    assert headers["Pinpoint-TraceID"] == b"trace-id"
    ev = span._native.all_events[-1]
    assert ("str", ANNOTATION_KAFKA_HEADER, "x-custom=v") in ev.annotations.entries


def test_producer_send_handles_positional_headers(push_span):
    """``send_and_wait`` forwards every arg positionally to ``send``:

        await self.send(topic, value, key, partition, timestamp_ms, headers)

    Older wrapper code injected ``headers`` into kwargs while leaving the
    positional copy in args, which crashed ``send`` with
    ``TypeError: got multiple values for 'headers'``. This guards the fix.
    """
    captured = {}

    async def wrapped(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs

    asyncio.run(aiokafka_instr._producer_send_wrapper(
        wrapped, instance=None,
        # topic, value, key, partition, timestamp_ms, headers — all positional
        args=("topic", b"payload", None, None, None, [("x-custom", b"v")]),
        kwargs={},
    ))
    # Wrapper must not produce both positional + kw `headers`
    assert "headers" not in captured["kwargs"]
    args = captured["args"]
    assert len(args) == 6
    merged = dict(args[5])
    assert merged["x-custom"] == b"v"
    assert merged["Pinpoint-TraceID"] == b"trace-id"


def test_producer_send_records_exception(push_span):
    _, rec = push_span

    async def boom(*_a, **_kw):
        raise RuntimeError("kaput")

    with pytest.raises(RuntimeError, match="kaput"):
        asyncio.run(aiokafka_instr._producer_send_wrapper(
            boom, instance=None, args=("topic",), kwargs={},
        ))
    assert any(e[0] == "event_error" for e in rec.events)


def test_producer_send_setup_failure_still_sends_no_leak(push_span, monkeypatch):
    """A native/parse error in the async pre-await instrumentation must be
    contained: the send still goes out (with the *original* args) and any span
    event opened before the failure is ended, not leaked."""
    _, rec = push_span
    captured = {}

    # Force a failure *after* the span event is opened (span_is_sampled runs
    # once the event exists) — this exercises the event.end() cleanup path.
    def _boom(_span):
        raise RuntimeError("native sampled check failed")

    from pinpoint.instrumentations import _kafka

    monkeypatch.setattr(_kafka, "span_is_sampled", _boom)

    async def wrapped(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return "sent"

    out = asyncio.run(aiokafka_instr._producer_send_wrapper(
        wrapped, instance=None, args=("topic", b"payload"), kwargs={},
    ))
    # The user send still ran, once, with the untouched original args — no
    # injected ``headers`` kwarg, which is what proves the fallback was taken.
    assert out == "sent"
    assert captured["args"] == ("topic", b"payload")
    assert captured["kwargs"] == {}
    operation = "aiokafka.producer.producer.AIOKafkaProducer.send"
    # Event was opened, then ended on failure — no leaked span event.
    assert ("event_start", "root", operation) in rec.events
    assert ("event_end", "root", operation) in rec.events


def test_producer_send_no_parent_passes_through_safely():
    """No parent span → no event but call still succeeds."""
    captured = {}

    async def wrapped(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return None

    asyncio.run(aiokafka_instr._producer_send_wrapper(
        wrapped, instance=None, args=("topic",), kwargs={},
    ))
    # Without a parent span there's nothing to inject; the kwargs may still
    # be the same as we received.
    assert captured["args"] == ("topic",)


# ---------------------------------------------------------------------------
# Consumer: getone wrapper
# ---------------------------------------------------------------------------

def test_getone_wrapper_opens_delivery_span(fake_agent):
    record = _Record(topic="orders", partition=0, offset=99,
                     headers=[("Pinpoint-TraceID", b"abc")])

    async def wrapped(*_a, **_kw):
        return record

    out = asyncio.run(aiokafka_instr._consumer_getone_wrapper(
        wrapped, instance=None, args=(), kwargs={},
    ))
    assert out is record
    rpc = "kafka://topic=orders?partition=0&offset=99"
    assert ("span_start", "Kafka Consumer Invocation", rpc) in fake_agent.events
    assert ("span_end", "Kafka Consumer Invocation") in fake_agent.events
    # Upstream Pinpoint header present -> reader-backed native call (continuation).
    assert fake_agent.last_native.headers is not None


def test_getone_wrapper_skips_reader_when_no_upstream_context(fake_agent):
    """A record without a Pinpoint header opens a new trace via the reader-less
    new_span; the delivery span still opens and closes."""
    record = _Record(topic="orders", partition=0, offset=99,
                     headers=[("x-custom", b"v")])

    async def wrapped(*_a, **_kw):
        return record

    out = asyncio.run(aiokafka_instr._consumer_getone_wrapper(
        wrapped, instance=None, args=(), kwargs={},
    ))
    assert out is record
    assert fake_agent.last_native.headers is None
    rpc = "kafka://topic=orders?partition=0&offset=99"
    assert ("span_start", "Kafka Consumer Invocation", rpc) in fake_agent.events
    assert ("span_end", "Kafka Consumer Invocation") in fake_agent.events


def test_getone_wrapper_unsampled_skips_annotation_prep(monkeypatch):
    """On an unsampled span the annotation block is gated out: no broker
    lookup and no per-header decode/format — only the delivery span
    open+close."""
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
        aiokafka_instr, "_bootstrap_server",
        lambda c: broker_calls.append(c) or "broker:9092",
    )

    record = _Record(topic="orders", partition=0, offset=99,
                     headers=[("Pinpoint-TraceID", b"abc"), ("x-custom", b"v")])

    async def wrapped(*_a, **_kw):
        return record

    out = asyncio.run(aiokafka_instr._consumer_getone_wrapper(
        wrapped, instance=None, args=(), kwargs={},
    ))
    assert out is record
    rpc = "kafka://topic=orders?partition=0&offset=99"
    assert ("span_start", "Kafka Consumer Invocation", rpc) in agent.events
    assert ("span_end", "Kafka Consumer Invocation") in agent.events
    assert broker_calls == []
    assert agent.last_native.annotations.entries == []


def test_getone_wrapper_no_op_when_disabled(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))
    record = _Record()

    async def wrapped(*_a, **_kw):
        return record

    out = asyncio.run(aiokafka_instr._consumer_getone_wrapper(
        wrapped, instance=None, args=(), kwargs={},
    ))
    assert out is record


# ---------------------------------------------------------------------------
# Consumer: getmany wrapper
# ---------------------------------------------------------------------------

def test_getmany_wrapper_opens_span_per_record(fake_agent, monkeypatch):
    """Every record needs a span because each may have a distinct producer
    trace context."""
    fetched = {
        "orders-0": [_Record(topic="orders", offset=1),
                     _Record(topic="orders", offset=2)],
        "billing-0": [_Record(topic="billing", offset=10)],
    }

    async def wrapped(*_a, **_kw):
        return fetched

    broker_calls = []
    monkeypatch.setattr(
        aiokafka_instr,
        "_bootstrap_server",
        lambda consumer: broker_calls.append(consumer) or "broker:9092",
    )

    out = asyncio.run(aiokafka_instr._consumer_getmany_wrapper(
        wrapped, instance=None, args=(), kwargs={},
    ))
    assert out is fetched
    span_starts = [e for e in fake_agent.events if e[0] == "span_start"]
    assert [entry[2] for entry in span_starts] == [
        "kafka://topic=orders?partition=0&offset=1",
        "kafka://topic=orders?partition=0&offset=2",
        "kafka://topic=billing?partition=0&offset=10",
    ]
    assert len(broker_calls) == 1
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


def test_getmany_preserves_each_records_propagation_context(fake_agent):
    fetched = {
        "orders-0": [
            _Record(topic="orders", offset=1, headers=[("x-custom", b"v")]),
            _Record(
                topic="orders",
                offset=2,
                headers=[("Pinpoint-TraceID", b"upstream")],
            ),
        ],
    }

    async def wrapped(*_a, **_kw):
        return fetched

    asyncio.run(aiokafka_instr._consumer_getmany_wrapper(
        wrapped, instance=None, args=(), kwargs={},
    ))

    assert len(fake_agent.native_spans) == 2
    assert fake_agent.native_spans[0].headers is None
    assert fake_agent.native_spans[1].headers["Pinpoint-TraceID"] == "upstream"


def test_getmany_wrapper_handles_empty_dict(fake_agent):
    async def wrapped(*_a, **_kw):
        return {}

    asyncio.run(aiokafka_instr._consumer_getmany_wrapper(
        wrapped, instance=None, args=(), kwargs={},
    ))
    assert not any(e[0] == "span_start" for e in fake_agent.events)
