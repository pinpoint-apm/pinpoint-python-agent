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

"""aio-pika producer + consumer instrumentation.

Drives the wrappers directly against in-memory fakes — no real RabbitMQ
or aio-pika import. Validates trace header injection on Message.headers,
async callback wrapping, and the QueueIterator delivery-only span.
"""

from __future__ import annotations

import asyncio
from typing import List, Tuple

import pytest

from _fakes import (FakeAgent as _FakeAgent, FakeNativeSpan as _FakeNativeSpan,
                    SampledClientSpan, UnsampledNative, stub_inject)

import pinpoint
from pinpoint import context as ppctx
from pinpoint.instrumentations import aio_pika as aio_pika_instr
from pinpoint.propagator import HEADER_SAMPLED


@pytest.fixture(autouse=True)
def _stub_inject(monkeypatch):
    stub_inject(monkeypatch, aio_pika_instr)


# Stand-ins for aio_pika types — use plain attribute objects.
class _Message:
    def __init__(self, headers=None, exchange="", routing_key=""):
        self.headers = headers
        self.exchange = exchange
        self.routing_key = routing_key


class _Exchange:
    def __init__(self, name="my_exchange"):
        self.name = name


class _Queue:
    def __init__(self, name="orders"):
        self.name = name


# ---------------------------------------------------------------------------
# Producer: Exchange.publish wrapper
# ---------------------------------------------------------------------------

def test_exchange_publish_emits_event_and_injects_headers(push_span):
    _, rec = push_span
    captured = {}

    async def wrapped(*args, **kwargs):
        # Capture the headers the transport actually sees during publish.
        captured["headers"] = dict(args[0].headers or {})
        return None

    original_headers = {"x-custom": "v"}
    msg = _Message(headers=original_headers)
    asyncio.run(aio_pika_instr._exchange_publish_wrapper(
        wrapped, instance=_Exchange("billing"),
        args=(msg, "billing.charge"), kwargs={},
    ))
    operation = "aio_pika.exchange.Exchange.publish"
    assert ("event_start", "root", operation) in rec.events
    assert ("event_end", "root", operation) in rec.events
    # During the publish the transport saw user + trace headers merged.
    assert captured["headers"]["x-custom"] == "v"
    assert captured["headers"]["Pinpoint-TraceID"] == "trace-id"
    # After the publish the user-owned Message is restored: reusing it for a
    # later publish outside a span must not leak this trace's context.
    assert msg.headers is original_headers
    assert "Pinpoint-TraceID" not in msg.headers


def test_message_copy_reports_failure_instead_of_faking_success():
    """When ``message.headers = ...`` raises on the copy, the helper must return
    None — not write to ``message.properties`` (which aio-pika rebuilds fresh
    on every access, so the write is discarded) and hand back a copy that
    silently dropped the trace headers."""
    class _LockedMessage:
        # A message whose headers can't be set directly.
        @property
        def headers(self):
            return {}

        @headers.setter
        def headers(self, value):
            raise AttributeError("read-only")

        @property
        def properties(self):
            # Rebuilt fresh each access — writing here is a no-op on the wire.
            return type("Props", (), {"headers": None})()

    assert aio_pika_instr._message_with_trace_headers(
        _LockedMessage(), SampledClientSpan()) is None


def test_exchange_publish_message_as_keyword_substitutes_copy(push_span):
    """``Exchange.publish(message=msg, routing_key=...)`` passes the message by
    keyword. The publish wrapper must replace it in the KEYWORD slot with a
    per-publish copy carrying the trace headers, leaving the user's original
    ``Message`` untouched. The positional-args tests never reach this branch."""
    _, rec = push_span
    captured = {}

    async def wrapped(*args, **kwargs):
        m = args[0] if args else kwargs.get("message")
        captured["msg_obj"] = m
        captured["headers"] = dict(m.headers or {})
        return None

    original_headers = {"x-custom": "v"}
    msg = _Message(headers=original_headers)
    asyncio.run(aio_pika_instr._exchange_publish_wrapper(
        wrapped, instance=_Exchange("billing"),
        args=(), kwargs={"message": msg, "routing_key": "billing.charge"},
    ))
    # The transport saw a *copy* of the message (not the user's own object) ...
    assert captured["msg_obj"] is not msg
    # ... carrying both the user headers and the injected trace context.
    assert captured["headers"]["x-custom"] == "v"
    assert captured["headers"]["Pinpoint-TraceID"] == "trace-id"
    # The user-owned Message keeps its own headers, uncontaminated.
    assert msg.headers is original_headers
    assert "Pinpoint-TraceID" not in msg.headers


def test_exchange_publish_unsampled_inject_writes_s0_marker():
    """An unsampled parent must still propagate ``Pinpoint-Sampled: s0`` so
    a downstream consumer short-circuits its own sampling decision."""
    from pinpoint.agent import UnSampledSpan  # type: ignore[attr-defined]

    sp = UnSampledSpan(UnsampledNative())
    token = ppctx.set_current_span(sp)
    try:
        captured = {}

        async def wrapped(*args, **_kw):
            captured["headers"] = dict(args[0].headers or {})
            return None

        msg = _Message(headers={"x-custom": "v"})
        asyncio.run(aio_pika_instr._exchange_publish_wrapper(
            wrapped, instance=_Exchange("billing"),
            args=(msg, "billing.charge"), kwargs={},
        ))
        # The transport saw the s0 marker alongside the user headers…
        assert captured["headers"].get(HEADER_SAMPLED) == "s0"
        assert captured["headers"]["x-custom"] == "v"
        # …and the user-owned Message is restored after the publish.
        assert HEADER_SAMPLED not in (msg.headers or {})
        assert msg.headers["x-custom"] == "v"
    finally:
        ppctx.reset_current_span(token)


def test_exchange_publish_setup_failure_still_publishes_no_leak(push_span, monkeypatch):
    """A native/parse error in the async pre-await instrumentation must be
    contained: the publish still goes out, the user-owned Message headers are
    restored, and the span event opened before the failure is ended, not
    leaked."""
    _, rec = push_span
    captured = {}

    # Force a failure *after* the span event is opened and headers injected
    # (span_is_sampled runs at that point) to exercise the event.end() +
    # header-restore cleanup path.
    def _boom(_span):
        raise RuntimeError("native sampled check failed")

    monkeypatch.setattr(aio_pika_instr, "span_is_sampled", _boom)

    async def wrapped(*args, **_kw):
        captured["headers"] = dict(args[0].headers or {})
        return "published"

    original_headers = {"x-custom": "v"}
    msg = _Message(headers=original_headers)
    out = asyncio.run(aio_pika_instr._exchange_publish_wrapper(
        wrapped, instance=_Exchange("billing"),
        args=(msg, "billing.charge"), kwargs={},
    ))
    # The publish still ran, once.
    assert out == "published"
    # The Message is restored: the failed publish must not leave trace headers
    # on the user-owned object.
    assert msg.headers is original_headers
    assert "Pinpoint-TraceID" not in msg.headers
    operation = "aio_pika.exchange.Exchange.publish"
    # Event opened, then ended on failure — no leaked span event.
    assert ("event_start", "root", operation) in rec.events
    assert ("event_end", "root", operation) in rec.events


def test_exchange_publish_records_exception_and_reraises(push_span):
    _, rec = push_span

    async def boom(*_a, **_kw):
        raise RuntimeError("kaput")

    msg = _Message()
    with pytest.raises(RuntimeError, match="kaput"):
        asyncio.run(aio_pika_instr._exchange_publish_wrapper(
            boom, instance=_Exchange("ex"),
            args=(msg, "rk"), kwargs={},
        ))
    assert any(e[0] == "event_error" for e in rec.events)


def test_exchange_publish_concurrent_shared_message_no_race(push_span):
    """Two concurrent publishes of the *same* user ``Message`` must each carry
    this trace's headers, and the user-owned ``Message`` must be left with no
    ``Pinpoint-*`` residue — no matter how the publishes interleave at the
    ``await``.

    The worst-case interleaving is forced deterministically with two events so
    the shared-state inject/restore race is exposed reproducibly, not left to
    scheduling luck:

    1. Task A injects, then blocks *inside* the transport until B has injected.
    2. Task B injects (snapshotting A's headers as its "original"), unblocks A,
       then blocks until A has fully returned (its restore has run).
    3. A publishes and restores; B then publishes — seeing whatever headers A's
       restore left behind — and restores last.

    With in-place mutate/restore this makes B publish untraced (A's restore
    stripped the headers) *and* leaves the shared ``Message`` carrying A's
    ``Pinpoint-*`` headers permanently. Injecting into a per-publish copy makes
    both invariants hold regardless of interleaving.
    """
    both_injected = asyncio.Event()
    a_restored = asyncio.Event()
    captured: List[Tuple[str, dict]] = []
    entered = {"count": 0}

    async def fake_publish(*args, **_kwargs):
        # args[0] is the Message the wrapper handed the transport; snapshot the
        # headers that would go on the wire at this await point (mirrors
        # aio_pika reading Message.properties only after the channel-lock await).
        role = entered["count"]
        entered["count"] += 1
        if role == 0:
            # Task A: publish only once B has injected, so a shared-state
            # mutation by B is in effect when A serialises.
            await both_injected.wait()
            captured.append(("A", dict(args[0].headers or {})))
            return "ok-A"
        # Task B: release A, then publish only after A has fully returned (its
        # finally/restore has run) — the moment an in-place restore corrupts
        # the shared Message.
        both_injected.set()
        await a_restored.wait()
        captured.append(("B", dict(args[0].headers or {})))
        return "ok-B"

    original_headers = {"x-custom": "v"}
    msg = _Message(headers=original_headers)
    exchange = _Exchange("billing")

    async def publish_a():
        await aio_pika_instr._exchange_publish_wrapper(
            fake_publish, instance=exchange, args=(msg, "k1"), kwargs={},
        )
        # The wrapper's finally has already run by the time this await returns.
        a_restored.set()

    async def publish_b():
        await aio_pika_instr._exchange_publish_wrapper(
            fake_publish, instance=exchange, args=(msg, "k2"), kwargs={},
        )

    async def main():
        await asyncio.gather(publish_a(), publish_b())

    asyncio.run(main())

    seen = dict(captured)
    assert set(seen) == {"A", "B"}
    # Invariant 1: every publish carried this trace's headers.
    assert seen["A"].get("Pinpoint-TraceID") == "trace-id"
    assert seen["B"].get("Pinpoint-TraceID") == "trace-id"
    # …and the user's own header on both.
    assert seen["A"]["x-custom"] == "v"
    assert seen["B"]["x-custom"] == "v"
    # Invariant 2: the user-owned Message is left exactly as created — no
    # Pinpoint-* residue that would stitch a later reuse into this (ended) trace.
    assert msg.headers is original_headers
    assert not any(str(k).startswith("Pinpoint") for k in (msg.headers or {}))


# ---------------------------------------------------------------------------
# Consumer: Queue.consume wrapper
# ---------------------------------------------------------------------------

def test_queue_consume_wraps_callback(fake_agent):
    delivered = []

    async def user_cb(message):
        delivered.append(message)

    captured = {}

    def fake_consume(*args, **kwargs):
        captured["cb"] = args[0] if args else kwargs.get("callback")
        return None

    queue = _Queue("orders")
    aio_pika_instr._queue_consume_wrapper(
        fake_consume, instance=queue,
        args=(user_cb,), kwargs={},
    )
    msg = _Message(headers={"Pinpoint-TraceID": "abc"},
                   exchange="ex", routing_key="orders.process")
    asyncio.run(captured["cb"](msg))
    outer = "RabbitMQ Consumer Invocation"
    assert ("span_start", outer, "rabbitmq://exchange=ex") in fake_agent.events
    assert ("span_end", outer) in fake_agent.events
    assert ("event_start", outer, "aio_pika.consume") in fake_agent.events
    assert delivered == [msg]


def test_queue_consume_callback_steps_aside_inside_an_active_span(fake_agent):
    """A delivery dispatched while a span is current (the consumer driven from
    inside a traced task) must leave that span in place: the held scope would
    otherwise re-parent the rest of the caller's work under this message."""
    inside = {}

    async def user_cb(message):
        inside["current"] = ppctx.current_span()

    captured = {}

    def fake_consume(*args, **kwargs):
        captured["cb"] = args[0] if args else kwargs.get("callback")
        return None

    aio_pika_instr._queue_consume_wrapper(
        fake_consume, instance=_Queue("orders"), args=(user_cb,), kwargs={},
    )

    async def main():
        outer_span = fake_agent.new_span("outer", "/orders")
        token = ppctx.set_current_span(outer_span)
        try:
            await captured["cb"](_Message(headers=None, exchange="ex",
                                          routing_key="orders.process"))
            return outer_span, ppctx.current_span()
        finally:
            ppctx.reset_current_span(token)

    outer_span, after = asyncio.run(main())
    assert inside["current"] is outer_span      # handler ran under the caller
    assert after is outer_span                  # and it was never displaced
    assert not any(e[0] == "span_start"
                   and e[1] == "RabbitMQ Consumer Invocation"
                   for e in fake_agent.events)


def test_queue_consume_skips_already_wrapped_callback(fake_agent):
    """``RobustQueue.consume`` delegates to ``Queue.consume`` and both are
    instrumented — the second wrapper must pass an already-wrapped callback
    through unchanged, or every delivery opens two nested root spans."""
    async def user_cb(message):
        pass

    captured = {}

    def fake_consume(*args, **kwargs):
        captured["cb"] = args[0] if args else kwargs.get("callback")
        return None

    queue = _Queue("orders")
    # First (robust) layer wraps the user callback ...
    aio_pika_instr._queue_consume_wrapper(
        lambda *a, **kw: None, instance=queue, args=(user_cb,), kwargs={},
    )
    wrapped_once = aio_pika_instr._wrap_consumer_callback(user_cb, queue)
    # ... and the delegated base-class layer must not wrap it again.
    aio_pika_instr._queue_consume_wrapper(
        fake_consume, instance=queue, args=(wrapped_once,), kwargs={},
    )
    assert captured["cb"] is wrapped_once

    msg = _Message(headers={}, exchange="ex", routing_key="orders.process")
    asyncio.run(captured["cb"](msg))
    outer = "RabbitMQ Consumer Invocation"
    starts = [e for e in fake_agent.events
              if e[0] == "span_start" and e[1] == outer]
    assert len(starts) == 1


def test_queue_consume_skips_iterator_internal_callback(fake_agent):
    """``queue.iterator()`` registers its internal buffering callback
    (``QueueIterator.on_message`` — a bare ``queue.put``) through the same
    ``Queue.consume`` we instrument. It must pass through unwrapped: the
    ``__anext__`` wrapper owns the iterator path's delivery span, so wrapping
    the buffer callback too would emit two consumer root spans per message."""
    QueueIterator = pytest.importorskip("aio_pika.queue").QueueIterator
    RobustQueueIterator = pytest.importorskip(
        "aio_pika.robust_queue").RobustQueueIterator

    for iterator_cls in (QueueIterator, RobustQueueIterator):
        # __new__ only: the identity (isinstance) is what the guard keys on,
        # and __init__ needs a live queue we don't have in a unit test.
        iterator = iterator_cls.__new__(iterator_cls)
        internal_cb = iterator.on_message

        captured = {}

        def fake_consume(*args, **kwargs):
            captured["cb"] = args[0] if args else kwargs.get("callback")
            return None

        aio_pika_instr._queue_consume_wrapper(
            fake_consume, instance=_Queue("orders"),
            args=(internal_cb,), kwargs={},
        )
        assert captured["cb"] is internal_cb, (
            f"{iterator_cls.__name__}.on_message must not be wrapped"
        )
        assert not getattr(captured["cb"], "_pinpoint_consumer_wrapped", False)


def test_queue_consume_still_wraps_user_bound_methods(fake_agent):
    """The iterator-callback guard keys on the callback owner's type, not on
    it merely being a bound method — a user's bound handler still traces."""
    delivered = []

    class _Handler:
        async def on_message(self, message):
            delivered.append(message)

    captured = {}

    def fake_consume(*args, **kwargs):
        captured["cb"] = args[0] if args else kwargs.get("callback")
        return None

    aio_pika_instr._queue_consume_wrapper(
        fake_consume, instance=_Queue("orders"),
        args=(_Handler().on_message,), kwargs={},
    )
    msg = _Message(headers={}, exchange="ex", routing_key="orders.process")
    asyncio.run(captured["cb"](msg))
    outer = "RabbitMQ Consumer Invocation"
    starts = [e for e in fake_agent.events
              if e[0] == "span_start" and e[1] == outer]
    assert len(starts) == 1
    assert delivered == [msg]


def test_queue_consume_unsampled_skips_annotation_prep(monkeypatch):
    """On an unsampled span ``annotate_consume`` returns before touching the
    connection-endpoint getattr chain / string building; the delivery span
    and its span event still open+close so trace propagation is intact."""
    from pinpoint.agent import UnSampledSpan

    agent = _FakeAgent()

    def _unsampled_new_span(operation, rpc_point, headers=None):
        native = _FakeNativeSpan(operation, rpc_point, recorder=agent,
                                 headers=headers)
        agent.last_native = native
        return UnSampledSpan(native)

    agent.new_span = _unsampled_new_span
    monkeypatch.setattr(pinpoint.agent, "_instance", agent)

    endpoint_calls = []
    monkeypatch.setattr(
        aio_pika_instr, "_connection_endpoint",
        lambda q: endpoint_calls.append(q) or "broker:5672",
    )

    captured = {}

    def fake_consume(*args, **kwargs):
        captured["cb"] = args[0] if args else kwargs.get("callback")

    aio_pika_instr._queue_consume_wrapper(
        fake_consume, instance=_Queue("orders"),
        args=(lambda m: None,), kwargs={},
    )
    msg = _Message(headers={"Pinpoint-TraceID": "abc"},
                   exchange="ex", routing_key="orders.process")
    asyncio.run(captured["cb"](msg))
    outer = "RabbitMQ Consumer Invocation"
    assert ("span_start", outer, "rabbitmq://exchange=ex") in agent.events
    assert ("span_end", outer) in agent.events
    assert endpoint_calls == []
    assert agent.last_native.annotations.entries == []


def test_queue_consume_records_exception(fake_agent):
    async def boom(message):
        raise ValueError("bad")

    captured = {}
    aio_pika_instr._queue_consume_wrapper(
        lambda *a, **kw: captured.setdefault("cb", a[0]),
        instance=_Queue("q"),
        args=(boom,), kwargs={},
    )
    with pytest.raises(ValueError, match="bad"):
        asyncio.run(captured["cb"](_Message(routing_key="q")))
    assert any(e[0] == "span_error" for e in fake_agent.events)


# ---------------------------------------------------------------------------
# QueueIterator.__anext__ wrapper
# ---------------------------------------------------------------------------

def test_queue_iterator_anext_opens_delivery_only_span(fake_agent):
    msg = _Message(headers={"Pinpoint-TraceID": "abc"},
                   exchange="", routing_key="orders")

    async def wrapped(*_a, **_kw):
        return msg

    class _Iter:
        _queue = _Queue("orders")

    out = asyncio.run(aio_pika_instr._queue_iterator_anext_wrapper(
        wrapped, instance=_Iter(), args=(), kwargs={},
    ))
    assert out is msg
    # Span opens and closes synchronously after the message is fetched.
    outer = "RabbitMQ Consumer Invocation"
    assert any(
        e[0] == "span_start" and e[1] == outer for e in fake_agent.events
    )
    assert any(
        e[0] == "event_start" and e[2] == "aio_pika.consume.iterator"
        for e in fake_agent.events
    )
    assert any(e[0] == "span_end" for e in fake_agent.events)


def test_queue_iterator_anext_passes_through_when_disabled(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))
    msg = _Message()

    async def wrapped(*_a, **_kw):
        return msg

    class _Iter:
        _queue = _Queue("q")

    out = asyncio.run(aio_pika_instr._queue_iterator_anext_wrapper(
        wrapped, instance=_Iter(), args=(), kwargs={},
    ))
    assert out is msg


def test_queue_consume_supports_sync_callback(fake_agent):
    """aio-pika 9.x still accepts deprecated plain (sync) consume callbacks
    via ensure_awaitable — our wrapper must not blindly await their return
    value (that would TypeError after the handler already ran)."""
    delivered = []

    def sync_user_cb(message):  # note: NOT async
        delivered.append(message)
        return None

    captured = {}
    aio_pika_instr._queue_consume_wrapper(
        lambda *a, **kw: captured.setdefault("cb", a[0]),
        instance=_Queue("orders"),
        args=(sync_user_cb,), kwargs={},
    )
    msg = _Message(exchange="ex", routing_key="orders.process")
    asyncio.run(captured["cb"](msg))
    assert delivered == [msg]
    assert ("span_end", "RabbitMQ Consumer Invocation") in fake_agent.events
