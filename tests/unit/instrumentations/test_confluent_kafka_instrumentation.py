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

"""confluent-kafka producer + consumer instrumentation.

Drives the wrappers against in-memory fakes — no real Kafka. Validates
trace header injection on produce, span event lifecycle, poll delivery
span (with error filtering), and consume batch handling.
"""

from __future__ import annotations

import sys
import types

import pytest

from _fakes import (FakeAgent as _FakeAgent, FakeNativeSpan as _FakeNativeSpan,
                    UnsampledNative, stub_inject)

import pinpoint
from pinpoint import context as ppctx
from pinpoint.annotation import ANNOTATION_KAFKA_BATCH, ANNOTATION_KAFKA_HEADER
from pinpoint.instrumentations import _kafka
from pinpoint.instrumentations import confluent_kafka as ck_instr
from pinpoint.propagator import HEADER_SAMPLED


@pytest.fixture(autouse=True)
def _stub_inject(monkeypatch):
    stub_inject(monkeypatch, _kafka)


class _Producer:
    """Stand-in for confluent_kafka.Producer."""
    def __init__(self, bootstrap="broker.test:9092"):
        self._config = {"bootstrap.servers": bootstrap}


class _Consumer:
    def __init__(self, bootstrap="broker.test:9092"):
        self._config = {"bootstrap.servers": bootstrap}


class _Message:
    """Confluent-kafka Message exposes everything as methods. Test-double
    matches that shape so the safe-call helper exercises the method path."""
    def __init__(self, topic="orders", partition=0, offset=42,
                 headers=None, error=None):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._headers = headers or []
        self._error = error

    def topic(self): return self._topic
    def partition(self): return self._partition
    def offset(self): return self._offset
    def headers(self): return self._headers
    def error(self): return self._error


def test_consumer_registration_cannot_precheck_away_scope_cleanup(monkeypatch):
    """Consumer fetch methods must always reach their wrappers so an agent
    disabled between records cannot strand the previously held scope."""
    module = types.SimpleNamespace(__name__="confluent_kafka")
    patches = {}

    def capture_patch(_module, attr, methods):
        patches[attr] = methods
        return None

    monkeypatch.setitem(sys.modules, "confluent_kafka", module)
    monkeypatch.setattr(ck_instr, "_patch_class", capture_patch)
    ck_instr.ConfluentKafkaInstrumentor()._instrument()

    assert patches["Consumer"]["poll"][1] is None
    assert patches["Consumer"]["consume"][1] is None
    assert patches["DeserializingConsumer"]["poll"][1] is None


# ---------------------------------------------------------------------------
# Producer: produce wrapper
# ---------------------------------------------------------------------------

def test_produce_emits_event_and_injects_headers(push_span):
    _, rec = push_span
    captured = {}

    def wrapped(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs

    ck_instr._producer_produce_wrapper(
        wrapped, instance=_Producer(),
        args=("orders", b"payload"), kwargs={},
    )
    operation = "confluent_kafka.Producer.produce"
    assert ("event_start", "root", operation) in rec.events
    assert ("event_end", "root", operation) in rec.events
    headers = dict(captured["kwargs"]["headers"])
    assert headers["Pinpoint-TraceID"] == b"trace-id"


def test_produce_unsampled_inject_writes_s0_marker():
    """An unsampled parent must still propagate ``Pinpoint-Sampled: s0`` so
    a downstream consumer short-circuits its own sampling decision."""
    from pinpoint.agent import UnSampledSpan  # type: ignore[attr-defined]

    sp = UnSampledSpan(UnsampledNative())
    token = ppctx.set_current_span(sp)
    captured: dict = {}
    try:
        def wrapped(*_a, **kw):
            captured["kwargs"] = kw

        ck_instr._producer_produce_wrapper(
            wrapped, instance=_Producer(),
            args=("topic", b"payload"), kwargs={},
        )
        headers = dict(captured["kwargs"]["headers"])
        assert headers.get(HEADER_SAMPLED) == b"s0"
    finally:
        ppctx.reset_current_span(token)


def test_produce_preserves_dict_headers(push_span):
    """Some users pass headers as dict[str,bytes]; preserve that format."""
    span, _rec = push_span
    captured = {}

    def wrapped(*_a, **kw):
        captured["kwargs"] = kw

    ck_instr._producer_produce_wrapper(
        wrapped, instance=_Producer(),
        args=("topic",),
        kwargs={"headers": {"x-custom": b"v"}},
    )
    assert isinstance(captured["kwargs"]["headers"], dict)
    assert captured["kwargs"]["headers"]["x-custom"] == b"v"
    assert captured["kwargs"]["headers"]["Pinpoint-TraceID"] == b"trace-id"
    ev = span._native.all_events[-1]
    assert ("str", ANNOTATION_KAFKA_HEADER, "x-custom=v") in ev.annotations.entries


def test_produce_headers_passed_positionally(push_span):
    """``produce(topic, value, key, partition, callback, on_delivery,
    timestamp, headers)`` accepts ``headers`` positionally at index 7 —
    librdkafka's C signature exposes ``callback`` and ``on_delivery`` as
    separate slots, so a positional ``headers`` needs all 8 args. Injecting a
    ``headers`` kwarg on top would crash the C client with ``TypeError: got
    multiple values for argument 'headers'`` — inject into the positional
    slot instead."""
    _, rec = push_span
    captured = {}

    def wrapped(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs

    # topic, value, key, partition, callback, on_delivery, timestamp, headers
    positional = ("orders", b"payload", b"key", 0, None, None, 0,
                  [("x-custom", b"v")])
    ck_instr._producer_produce_wrapper(
        wrapped, instance=_Producer(),
        args=positional, kwargs={},
    )
    # No colliding kwarg was added.
    assert "headers" not in captured["kwargs"]
    injected = dict(captured["args"][ck_instr._PRODUCE_HEADERS_POS])
    assert injected["x-custom"] == b"v"
    assert injected["Pinpoint-TraceID"] == b"trace-id"


def test_produce_timestamp_positional_not_treated_as_headers(push_span):
    """Regression for the off-by-one: ``produce(topic, value, key, partition,
    callback, on_delivery, timestamp)`` passes ``timestamp`` positionally at
    index 6 with NO positional headers. The wrapper must inject headers as a
    kwarg and leave the timestamp int intact — mistaking index 6 for the
    headers slot would overwrite the timestamp with a header list (and crash
    librdkafka with ``TypeError: 'list' object cannot be interpreted as an
    integer``)."""
    _, rec = push_span
    captured = {}

    def wrapped(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs

    # 7 positional args: timestamp=1234 at index 6; headers (index 7) absent.
    ck_instr._producer_produce_wrapper(
        wrapped, instance=_Producer(),
        args=("orders", b"payload", b"key", 0, None, None, 1234), kwargs={},
    )
    # Timestamp int preserved at its slot — args untouched.
    assert captured["args"] == ("orders", b"payload", b"key", 0, None, None, 1234)
    # Headers injected as a kwarg (no positional collision).
    headers = dict(captured["kwargs"]["headers"])
    assert headers["Pinpoint-TraceID"] == b"trace-id"


def test_produce_positional_dict_headers_merged_in_positional_slot(push_span):
    """``headers`` supplied POSITIONALLY at index 7 as a ``dict`` (not a list):
    the ``isinstance(existing, dict)`` branch must merge the trace pairs and
    write the result back to the POSITIONAL slot (``new_args[7]``), never to
    ``kwargs['headers']`` — a kwarg on top of the positional arg would crash the
    C client with ``got multiple values for argument 'headers'``. Existing tests
    cover positional-list and kwarg-dict, but not this combination."""
    _, rec = push_span
    captured = {}

    def wrapped(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs

    # 8 positional args; headers (index 7) is a DICT.
    positional = ("orders", b"payload", b"key", 0, None, None, 0,
                  {"x-custom": b"v"})
    ck_instr._producer_produce_wrapper(
        wrapped, instance=_Producer(), args=positional, kwargs={},
    )
    # No colliding kwarg was added; merged dict written to the positional slot.
    assert "headers" not in captured["kwargs"]
    merged = captured["args"][ck_instr._PRODUCE_HEADERS_POS]
    assert isinstance(merged, dict)
    assert merged["x-custom"] == b"v"
    assert merged["Pinpoint-TraceID"] == b"trace-id"


def test_produce_headers_passed_as_list_kwarg(push_span):
    """``headers=`` kwarg as a list is preserved and trace pairs appended."""
    _, rec = push_span
    captured = {}

    def wrapped(*_a, **kw):
        captured["kwargs"] = kw

    ck_instr._producer_produce_wrapper(
        wrapped, instance=_Producer(),
        args=("orders", b"payload"),
        kwargs={"headers": [("x-custom", b"v")]},
    )
    headers = dict(captured["kwargs"]["headers"])
    assert headers["x-custom"] == b"v"
    assert headers["Pinpoint-TraceID"] == b"trace-id"


def test_produce_no_headers_appends_fresh_list(push_span):
    """Caller passed no headers at all: inject a fresh headers list as a
    kwarg with correctly-encoded bytes values."""
    _, rec = push_span
    captured = {}

    def wrapped(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs

    ck_instr._producer_produce_wrapper(
        wrapped, instance=_Producer(),
        args=("orders", b"payload"), kwargs={},
    )
    headers = captured["kwargs"]["headers"]
    assert isinstance(headers, list)
    assert all(isinstance(v, bytes) for _k, v in headers)
    assert dict(headers)["Pinpoint-TraceID"] == b"trace-id"


def test_produce_records_exception(push_span):
    _, rec = push_span

    def boom(*_a, **_kw):
        raise RuntimeError("kaput")

    with pytest.raises(RuntimeError, match="kaput"):
        ck_instr._producer_produce_wrapper(
            boom, instance=_Producer(),
            args=("topic",), kwargs={},
        )
    assert any(e[0] == "event_error" for e in rec.events)


def test_produce_no_parent_passes_through():
    """No span → still call wrapped, no headers injected."""
    captured = {}

    def wrapped(*_a, **kw):
        captured["kwargs"] = kw

    ck_instr._producer_produce_wrapper(
        wrapped, instance=_Producer(),
        args=("topic",), kwargs={},
    )
    headers = captured["kwargs"].get("headers") or []
    keys = {k for k, _ in headers} if isinstance(headers, list) else set(headers)
    assert "Pinpoint-TraceID" not in keys


# ---------------------------------------------------------------------------
# Consumer: poll wrapper
# ---------------------------------------------------------------------------

def test_poll_wrapper_holds_the_span_until_the_next_poll(fake_agent):
    """``Consumer.poll`` hands back one message, so the span stays open — and
    current — while the caller handles it, and closes when they poll again."""
    msg = _Message(headers=[("Pinpoint-TraceID", b"abc"), ("x-custom", b"v")])
    consumer = _Consumer()

    def wrapped(*_a, **_kw):
        return msg

    out = ck_instr._consumer_poll_wrapper(
        wrapped, instance=consumer, args=(), kwargs={},
    )
    assert out is msg
    rpc = "kafka://topic=orders?partition=0&offset=42"
    assert ("span_start", "Kafka Consumer Invocation", rpc) in fake_agent.events
    assert ("span_end", "Kafka Consumer Invocation") not in fake_agent.events
    assert ppctx.current_span() is not None

    # A poll that times out (None) still closes the held scope.
    ck_instr._consumer_poll_wrapper(
        lambda *_a, **_kw: None, instance=consumer, args=(), kwargs={},
    )
    assert ("span_end", "Kafka Consumer Invocation") in fake_agent.events
    assert ppctx.current_span() is None
    assert (
        "str",
        ANNOTATION_KAFKA_HEADER,
        "x-custom=v",
    ) in fake_agent.last_native.annotations.entries


def test_poll_wrapper_unsampled_skips_annotation_prep(monkeypatch):
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
        ck_instr, "_bootstrap_server",
        lambda c: broker_calls.append(c) or "broker:9092",
    )

    msg = _Message(headers=[("Pinpoint-TraceID", b"abc"), ("x-custom", b"v")])

    def wrapped(*_a, **_kw):
        return msg

    consumer = _Consumer()
    out = ck_instr._consumer_poll_wrapper(
        wrapped, instance=consumer, args=(), kwargs={},
    )
    ck_instr._consumer_close_wrapper(
        lambda *_a, **_kw: None, instance=consumer, args=(), kwargs={},
    )
    assert out is msg
    rpc = "kafka://topic=orders?partition=0&offset=42"
    assert ("span_start", "Kafka Consumer Invocation", rpc) in agent.events
    assert ("span_end", "Kafka Consumer Invocation") in agent.events
    assert broker_calls == []
    assert agent.last_native.annotations.entries == []


def test_poll_wrapper_skips_messages_with_error(fake_agent):
    """Partition EOF / errors arrive as Messages with .error() set —
    these should NOT open spans (they're not real deliveries)."""
    err_msg = _Message(error="PartitionEOF")

    def wrapped(*_a, **_kw):
        return err_msg

    out = ck_instr._consumer_poll_wrapper(
        wrapped, instance=_Consumer(),
        args=(), kwargs={},
    )
    assert out is err_msg
    assert not any(e[0] == "span_start" for e in fake_agent.events)


def test_poll_wrapper_handles_no_message(fake_agent):
    """Empty poll returns None."""
    def wrapped(*_a, **_kw):
        return None

    out = ck_instr._consumer_poll_wrapper(
        wrapped, instance=_Consumer(),
        args=(1.0,), kwargs={},
    )
    assert out is None
    assert not any(e[0] == "span_start" for e in fake_agent.events)


# ---------------------------------------------------------------------------
# Consumer: consume wrapper
# ---------------------------------------------------------------------------

def test_consume_wrapper_opens_span_per_message(fake_agent, monkeypatch):
    messages = [
        _Message(topic="orders", offset=1),
        _Message(topic="orders", offset=2),
        _Message(error="EOF"),  # filtered out
        _Message(topic="billing", offset=10),
    ]

    def wrapped(*_a, **_kw):
        return messages

    broker_calls = []
    monkeypatch.setattr(
        ck_instr,
        "_bootstrap_server",
        lambda consumer: broker_calls.append(consumer) or "broker:9092",
    )

    out = ck_instr._consumer_consume_wrapper(
        wrapped, instance=_Consumer(),
        args=(), kwargs={"num_messages": 4},
    )
    assert out is messages
    span_starts = [e for e in fake_agent.events if e[0] == "span_start"]
    # The error message is filtered out.
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
        ("int", ANNOTATION_KAFKA_BATCH, 3),
        ("int", ANNOTATION_KAFKA_BATCH, 3),
        ("int", ANNOTATION_KAFKA_BATCH, 3),
    ]


def test_consume_preserves_each_messages_propagation_context(fake_agent):
    messages = [
        _Message(offset=1, headers=[("x-custom", b"v")]),
        _Message(offset=2, headers=[("Pinpoint-TraceID", b"upstream")]),
    ]

    def wrapped(*_a, **_kw):
        return messages

    ck_instr._consumer_consume_wrapper(
        wrapped, instance=_Consumer(), args=(), kwargs={"num_messages": 2},
    )

    assert len(fake_agent.native_spans) == 2
    assert fake_agent.native_spans[0].headers is None
    assert fake_agent.native_spans[1].headers["Pinpoint-TraceID"] == "upstream"


def test_consume_all_error_batch_creates_no_span(fake_agent):
    messages = [_Message(error="EOF"), _Message(error="transport")]

    def wrapped(*_a, **_kw):
        return messages

    out = ck_instr._consumer_consume_wrapper(
        wrapped, instance=_Consumer(), args=(), kwargs={"num_messages": 2},
    )

    assert out is messages
    assert fake_agent.native_spans == []


# ---------------------------------------------------------------------------
# SerializingProducer: produce wrapper (headers at index 6)
# ---------------------------------------------------------------------------

def test_serializing_producer_headers_positional_index_6(push_span):
    """``SerializingProducer.produce(topic, key, value,
    partition, on_delivery, timestamp, headers)`` puts ``headers`` at index 6 —
    one slot earlier than the C base (it drops the ``callback`` slot and swaps
    key/value). The wrapper must merge into slot 6, not add a colliding
    ``headers`` kwarg. ``wrapped`` mirrors the real signature so a double-passed
    ``headers`` would raise ``TypeError: got multiple values for argument
    'headers'`` — the exact crash the index-6 handling prevents."""
    _, rec = push_span
    captured = {}

    def wrapped(topic, key=None, value=None, partition=-1, on_delivery=None,
                timestamp=0, headers=None):
        captured["topic"] = topic
        captured["headers"] = headers

    # topic, key, value, partition, on_delivery, timestamp, headers (index 6).
    positional = ("orders", b"key", b"payload", -1, None, 0, [("x-custom", b"v")])
    ck_instr._serializing_producer_produce_wrapper(
        wrapped, instance=_Producer(), args=positional, kwargs={},
    )
    operation = "confluent_kafka.Producer.produce"
    assert ("event_start", "root", operation) in rec.events
    assert ("event_end", "root", operation) in rec.events
    # Merged into the positional slot (no kwarg collision — the mirrored-signature
    # call above did not raise).
    merged = dict(captured["headers"])
    assert merged["x-custom"] == b"v"
    assert merged["Pinpoint-TraceID"] == b"trace-id"


def test_serializing_producer_headers_kwarg(push_span):
    """Headers passed as a kwarg are preserved and trace pairs appended; topic
    (index 0, unchanged vs the C base) is still annotated."""
    span, _rec = push_span
    captured = {}

    def wrapped(*_a, **kw):
        captured["kwargs"] = kw

    ck_instr._serializing_producer_produce_wrapper(
        wrapped, instance=_Producer(),
        args=("orders", b"key", b"payload"),
        kwargs={"headers": [("x-custom", b"v")]},
    )
    headers = dict(captured["kwargs"]["headers"])
    assert headers["x-custom"] == b"v"
    assert headers["Pinpoint-TraceID"] == b"trace-id"
    ev = span._native.all_events[-1]
    assert ("str", ANNOTATION_KAFKA_HEADER, "x-custom=v") in ev.annotations.entries


def test_serializing_producer_no_parent_passes_through():
    """No active span → still call produce, no headers injected."""
    captured = {}

    def wrapped(*_a, **kw):
        captured["kwargs"] = kw

    ck_instr._serializing_producer_produce_wrapper(
        wrapped, instance=_Producer(),
        args=("orders", b"key", b"payload"), kwargs={},
    )
    headers = captured["kwargs"].get("headers") or []
    keys = {k for k, _ in headers} if isinstance(headers, list) else set(headers)
    assert "Pinpoint-TraceID" not in keys


# ---------------------------------------------------------------------------
# C-extension class patching
# ---------------------------------------------------------------------------

class _FakeKafkaModule:
    """A standalone module-like namespace for ``_patch_class`` to chew on,
    so the test doesn't mutate the real ``confluent_kafka`` package."""

    __name__ = "confluent_kafka_fake"


def test_patch_class_subclasses_immutable_type_and_funnels_through_wrapper():
    """``Producer`` and ``Consumer`` are immutable C-extension types, so
    we install instrumentation by *subclassing* and swapping the module
    attribute. The subclass's overridden methods must drive the supplied
    wrapper, end up isinstance-compatible with the original, and refuse
    to double-patch."""
    seen = []

    class OrigImmutable:
        def produce(self, topic, value=None):
            seen.append(("orig.produce", topic, value))
            return ("ok", topic, value)

    def my_wrapper(wrapped, instance, args, kwargs):
        seen.append(("wrap_in", args, kwargs))
        out = wrapped(*args, **kwargs)
        seen.append(("wrap_out", out))
        return out

    mod = _FakeKafkaModule()
    mod.Producer = OrigImmutable
    # precheck returns False -> always take the traced (wrapper) path.
    ck_instr._patch_class(mod, "Producer", {"produce": (my_wrapper, lambda: False)})

    # Class identity changed but isinstance against the original still works.
    new_cls = mod.Producer
    assert new_cls is not OrigImmutable
    assert issubclass(new_cls, OrigImmutable)
    inst = new_cls()
    assert isinstance(inst, OrigImmutable)

    # Calls are funneled through ``my_wrapper`` before reaching ``OrigImmutable.produce``.
    inst.produce("billing", value=b"payload")
    assert seen == [
        ("wrap_in", ("billing",), {"value": b"payload"}),
        ("orig.produce", "billing", b"payload"),
        ("wrap_out", ("ok", "billing", b"payload")),
    ]

    # Idempotent: re-patching keeps the existing subclass.
    cached = mod.Producer
    ck_instr._patch_class(mod, "Producer", {"produce": (my_wrapper, lambda: False)})
    assert mod.Producer is cached


def test_patch_class_precheck_skips_wrapper_when_tracing_off():
    """When ``precheck()`` reports tracing is off, the method calls the raw
    C method directly and never touches the wrapper (nor the safe_wrapper
    machinery) — the hot fire-and-forget / tight-loop path."""
    seen = []

    class OrigImmutable:
        def produce(self, topic, value=None):
            seen.append(("orig.produce", topic, value))
            return ("ok", topic, value)

    def my_wrapper(wrapped, instance, args, kwargs):
        seen.append(("wrap_in", args, kwargs))
        return wrapped(*args, **kwargs)

    mod = _FakeKafkaModule()
    mod.Producer = OrigImmutable
    # precheck returns True -> tracing off -> raw method, wrapper skipped.
    ck_instr._patch_class(mod, "Producer", {"produce": (my_wrapper, lambda: True)})

    inst = mod.Producer()
    out = inst.produce("billing", value=b"payload")
    assert out == ("ok", "billing", b"payload")
    assert seen == [("orig.produce", "billing", b"payload")]


def test_patch_class_falls_back_when_subclass_creation_fails():
    """Truly immutable types that disallow subclassing (eg. ``type``)
    leave the module attribute alone instead of crashing autoload."""
    mod = _FakeKafkaModule()

    class ReallyFinal:
        def __init_subclass__(cls, **_kw):
            raise TypeError("subclassing forbidden")

    mod.Bad = ReallyFinal

    ck_instr._patch_class(
        mod, "Bad", {"x": (lambda w, i, a, k: w(*a, **k), lambda: False)})
    # On failure we leave the original attribute in place rather than
    # raising — autoload must never crash the host process.
    assert mod.Bad is ReallyFinal


def test_uninstrument_restores_patched_extension_class():
    class OrigImmutable:
        def produce(self, topic):
            return topic

    mod = _FakeKafkaModule()
    mod.Producer = OrigImmutable
    patch = ck_instr._patch_class(
        mod,
        "Producer",
        {"produce": (lambda w, i, a, k: w(*a, **k), lambda: False)},
    )
    assert patch is not None
    inst = ck_instr.ConfluentKafkaInstrumentor()
    inst._class_patches.append(patch)

    inst._uninstrument()

    assert mod.Producer is OrigImmutable
    assert inst._class_patches == []


def test_uninstrument_does_not_overwrite_later_extension_class_patch():
    class OrigImmutable:
        def produce(self, topic):
            return topic

    mod = _FakeKafkaModule()
    mod.Producer = OrigImmutable
    patch = ck_instr._patch_class(
        mod,
        "Producer",
        {"produce": (lambda w, i, a, k: w(*a, **k), lambda: False)},
    )
    assert patch is not None
    pinpoint_class = mod.Producer
    foreign_class = type("ForeignProducer", (pinpoint_class,), {})
    mod.Producer = foreign_class
    inst = ck_instr.ConfluentKafkaInstrumentor()
    inst._class_patches.append(patch)

    inst._uninstrument()

    assert mod.Producer is foreign_class


def test_instrument_patches_serializing_and_deserializing_subclasses():
    """``SerializingProducer`` / ``DeserializingConsumer`` subclass the C
    ``cimpl.*`` types directly, so rebinding ``confluent_kafka.Producer`` /
    ``.Consumer`` alone never reaches them and their ``produce`` / ``poll``
    would run uninstrumented — no producer spans, no header injection, no
    downstream stitching. ``_instrument`` must patch the concrete
    classes too — without wrapping the shared C base twice — and
    ``_uninstrument`` must restore every rebound name."""
    confluent_kafka = pytest.importorskip("confluent_kafka")
    from confluent_kafka import cimpl

    orig_sp = confluent_kafka.SerializingProducer
    orig_dc = confluent_kafka.DeserializingConsumer
    orig_prod = confluent_kafka.Producer
    orig_cons = confluent_kafka.Consumer
    orig_cimpl_produce = cimpl.Producer.produce
    orig_cimpl_poll = cimpl.Consumer.poll

    inst = ck_instr.ConfluentKafkaInstrumentor()
    try:
        inst._instrument()

        sp = confluent_kafka.SerializingProducer
        dc = confluent_kafka.DeserializingConsumer
        # Rebound to a pinpoint subclass, still isinstance-compatible with the
        # documented base classes.
        assert sp is not orig_sp and issubclass(sp, orig_sp)
        assert dc is not orig_dc and issubclass(dc, orig_dc)
        assert getattr(sp, "_pinpoint_patched", False) is True
        assert getattr(dc, "_pinpoint_patched", False) is True
        # The pinpoint subclass owns produce/poll...
        assert "produce" in sp.__dict__
        assert "poll" in dc.__dict__
        # ...but the shared C base stays untouched, so the override's
        # ``super().produce()`` / ``super().poll()`` cannot hit a 2nd wrapper.
        assert cimpl.Producer.produce is orig_cimpl_produce
        assert cimpl.Consumer.poll is orig_cimpl_poll
        # Producer, Consumer, SerializingProducer, DeserializingConsumer.
        assert len(inst._class_patches) == 4
    finally:
        inst._uninstrument()

    assert confluent_kafka.SerializingProducer is orig_sp
    assert confluent_kafka.DeserializingConsumer is orig_dc
    assert confluent_kafka.Producer is orig_prod
    assert confluent_kafka.Consumer is orig_cons


def test_patched_init_stashes_only_the_broker_keys():
    """A librdkafka config routinely carries ``sasl.password`` /
    ``ssl.key.password``. The subclass ``__init__`` must keep only what
    ``_bootstrap_server`` actually reads, rather than pinning the whole config
    — secrets included — on the client for the life of the process."""
    class OrigProducer:
        def __init__(self, conf=None):
            self.conf = conf

    mod = _FakeKafkaModule()
    mod.Producer = OrigProducer
    ck_instr._patch_class(mod, "Producer", {})

    client = mod.Producer({
        "bootstrap.servers": "broker-a:9092,broker-b:9092",
        "sasl.password": "hunter2",
        "ssl.key.password": "s3cret",
        "sasl.oauthbearer.client.secret": "oauth-secret",
    })

    assert client._pinpoint_config == {
        "bootstrap.servers": "broker-a:9092,broker-b:9092",
    }
    # The stash is the only thing pinpoint retains — no credential reaches it.
    for secret in ("hunter2", "s3cret", "oauth-secret"):
        assert secret not in repr(client._pinpoint_config)
    # The driver still gets the full config, and the endpoint label still resolves.
    assert client.conf["sasl.password"] == "hunter2"
    assert ck_instr._bootstrap_server(client) == "broker-a:9092"


def test_patched_init_stashes_the_legacy_broker_key():
    """``metadata.broker.list`` is the older librdkafka spelling and is the
    second key ``_bootstrap_server`` falls back to."""
    class OrigConsumer:
        def __init__(self, conf=None):
            self.conf = conf

    mod = _FakeKafkaModule()
    mod.Consumer = OrigConsumer
    ck_instr._patch_class(mod, "Consumer", {})

    client = mod.Consumer({"metadata.broker.list": "legacy:9092",
                           "sasl.password": "hunter2"})

    assert client._pinpoint_config == {"metadata.broker.list": "legacy:9092"}
    assert ck_instr._bootstrap_server(client) == "legacy:9092"


def test_produce_replaces_stale_pinpoint_headers_in_dict(push_span):
    span, _rec = push_span
    captured = {}

    def wrapped(*_a, **kw):
        captured["kwargs"] = kw

    ck_instr._producer_produce_wrapper(
        wrapped, instance=_Producer(),
        args=("topic",),
        kwargs={"headers": {"pinpoint-traceid": b"old", "x-custom": b"v"}},
    )
    headers = captured["kwargs"]["headers"]
    assert "pinpoint-traceid" not in headers
    assert headers["Pinpoint-TraceID"] == b"trace-id"
    assert headers["x-custom"] == b"v"
