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

"""pika producer + consumer instrumentation.

Drives the wrappers directly against in-memory fakes — no real RabbitMQ.
Validates trace header injection on publish, span event lifecycle,
consumer-callback wrapping, and the basic_get sync poll path.
"""

from __future__ import annotations

import pytest

from _fakes import (FakeAgent as _FakeAgent, FakeNativeSpan as _FakeNativeSpan,
                    UnsampledNative, stub_inject)

import pinpoint
from pinpoint import context as ppctx
from pinpoint.instrumentations import pika as pika_instr
from pinpoint.propagator import HEADER_SAMPLED


@pytest.fixture(autouse=True)
def _stub_inject(monkeypatch):
    """Return deterministic trace headers from fake native spans."""
    stub_inject(monkeypatch, pika_instr)
    yield
    # Producer tests replace ``pika.spec.BasicProperties`` with a fake. The
    # instrumentation caches that class, so reset the cache after every test;
    # otherwise a later real-Pika integration test in the same pytest process
    # receives the fake properties object.
    pika_instr._basic_properties_cls.cache_clear()


# ---------------------------------------------------------------------------
# Producer: basic_publish wrapper
# ---------------------------------------------------------------------------

class _FakeProperties:
    """Stand-in for pika.spec.BasicProperties."""
    def __init__(self, headers=None):
        self.headers = headers


class _FakeChannel:
    class _Conn:
        class _Params:
            host = "broker.test"
            port = 5672
        params = _Params()
    connection = _Conn()


def test_basic_publish_emits_event_and_injects_headers(push_span, monkeypatch):
    # Stub pika.spec to avoid the real import requirement.
    import sys
    pika_spec = type(sys)("pika_spec")
    pika_spec.BasicProperties = _FakeProperties
    monkeypatch.setitem(sys.modules, "pika.spec", pika_spec)

    _, rec = push_span
    channel = _FakeChannel()
    captured = {}

    def _wrapped(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return None

    pika_instr._basic_publish_wrapper(
        _wrapped, channel,
        ("my_exchange", "billing.charge", b"payload"), {},
    )
    operation = "pika.channel.Channel.basic_publish"
    assert ("event_start", "root", operation) in rec.events
    assert ("event_end", "root", operation) in rec.events
    # Headers were injected — check on properties (kwargs["properties"]).
    props = captured["kwargs"].get("properties")
    assert props is not None
    assert props.headers["Pinpoint-TraceID"] == "trace-id"


def test_basic_publish_unsampled_inject_writes_s0_marker(monkeypatch):
    """An unsampled parent must still propagate ``Pinpoint-Sampled: s0`` so
    a downstream consumer short-circuits its own sampling decision."""
    import sys
    pika_spec = type(sys)("pika_spec")
    pika_spec.BasicProperties = _FakeProperties
    monkeypatch.setitem(sys.modules, "pika.spec", pika_spec)

    from pinpoint.agent import UnSampledSpan  # type: ignore[attr-defined]

    sp = UnSampledSpan(UnsampledNative())
    token = ppctx.set_current_span(sp)
    captured: dict = {}
    try:
        def _wrapped(*_a, **kw):
            captured["kwargs"] = kw
            return None

        pika_instr._basic_publish_wrapper(
            _wrapped, _FakeChannel(),
            ("my_exchange", "billing.charge", b"payload"), {},
        )
        props = captured["kwargs"].get("properties")
        assert props is not None
        assert props.headers.get(HEADER_SAMPLED) == "s0"
    finally:
        ppctx.reset_current_span(token)


def test_basic_publish_preserves_existing_headers(push_span, monkeypatch):
    import sys
    pika_spec = type(sys)("pika_spec")
    pika_spec.BasicProperties = _FakeProperties
    monkeypatch.setitem(sys.modules, "pika.spec", pika_spec)

    captured = {}

    def _wrapped(*args, **kwargs):
        captured["kwargs"] = kwargs
        return None

    existing = _FakeProperties(headers={"x-custom": "v"})
    pika_instr._basic_publish_wrapper(
        _wrapped, _FakeChannel(),
        (), {"exchange": "ex", "routing_key": "rk", "body": b"x", "properties": existing},
    )
    props = captured["kwargs"]["properties"]
    assert props.headers["x-custom"] == "v"
    assert props.headers["Pinpoint-TraceID"] == "trace-id"


def test_basic_publish_records_exception_and_reraises(push_span, monkeypatch):
    import sys
    pika_spec = type(sys)("pika_spec")
    pika_spec.BasicProperties = _FakeProperties
    monkeypatch.setitem(sys.modules, "pika.spec", pika_spec)

    _, rec = push_span

    def _boom(*_a, **_kw):
        raise RuntimeError("kaput")

    with pytest.raises(RuntimeError, match="kaput"):
        pika_instr._basic_publish_wrapper(
            _boom, _FakeChannel(), ("ex", "rk", b"x"), {},
        )
    assert any(e[0] == "event_error" for e in rec.events)
    assert ("event_end", "root", "pika.channel.Channel.basic_publish") in rec.events


def test_basic_publish_no_parent_span_still_injects(monkeypatch):
    """No active span → no event, but pass-through must succeed."""
    import sys
    pika_spec = type(sys)("pika_spec")
    pika_spec.BasicProperties = _FakeProperties
    monkeypatch.setitem(sys.modules, "pika.spec", pika_spec)

    captured = {}

    def _wrapped(*args, **kwargs):
        captured["kwargs"] = kwargs
        return None

    pika_instr._basic_publish_wrapper(
        _wrapped, _FakeChannel(), ("ex", "rk", b"x"), {},
    )
    # No span → no headers injected, but call still succeeded.
    props = captured["kwargs"].get("properties")
    if props is not None and props.headers:
        assert "Pinpoint-TraceID" not in props.headers


# ---------------------------------------------------------------------------
# Consumer: basic_consume wrapper
# ---------------------------------------------------------------------------

class _FakeMethod:
    def __init__(self, exchange="ex", routing_key="orders.process"):
        self.exchange = exchange
        self.routing_key = routing_key


def test_basic_consume_wrapper_wraps_callback(fake_agent):
    delivered = []

    def user_cb(channel, method, properties, body):
        delivered.append((channel, method, properties, body))

    captured = {}

    def fake_basic_consume(*args, **kwargs):
        captured["cb"] = kwargs.get("on_message_callback") or args[1]
        return None

    pika_instr._basic_consume_wrapper(
        fake_basic_consume, instance=None,
        args=("queue.name", user_cb), kwargs={},
    )
    assert "cb" in captured
    # The wrapped callback should open a span when called.
    method = _FakeMethod()
    properties = _FakeProperties(headers={"Pinpoint-TraceID": "abc"})
    captured["cb"](_FakeChannel(), method, properties, b"hello")
    outer = "RabbitMQ Consumer Invocation"
    assert ("span_start", outer, "rabbitmq://exchange=ex") in fake_agent.events
    assert ("span_end", outer) in fake_agent.events
    assert ("event_start", outer, "pika.consume") in fake_agent.events
    assert delivered  # user callback got the message
    # Upstream Pinpoint header present -> reader-backed native call.
    assert fake_agent.last_native.headers is not None


def test_basic_consume_extracts_callback_pika_1x_signature(fake_agent):
    """pika ≥1.0: ``basic_consume(queue, on_message_callback, ...)`` — the
    callback is positional index 1 and must be wrapped there, leaving the
    queue string at index 0 untouched."""
    delivered = []

    def user_cb(channel, method, properties, body):
        delivered.append(body)

    captured = {}

    def fake_basic_consume(queue, on_message_callback, auto_ack=False,
                           **kwargs):
        captured["queue"] = queue
        captured["cb"] = on_message_callback

    pika_instr._basic_consume_wrapper(
        fake_basic_consume, instance=None,
        args=("queue.name", user_cb), kwargs={},
    )
    # Queue string preserved, callback swapped for our wrapper.
    assert captured["queue"] == "queue.name"
    assert captured["cb"] is not user_cb
    captured["cb"](_FakeChannel(), _FakeMethod(), _FakeProperties(), b"hi")
    assert delivered == [b"hi"]
    assert ("span_start", "RabbitMQ Consumer Invocation",
            "rabbitmq://exchange=ex") in fake_agent.events


def test_basic_consume_extracts_callback_pika_0x_signature(fake_agent):
    """pika 0.x: ``basic_consume(consumer_callback, queue, ...)`` — the
    callback is positional index 0. Assuming ``args[1]`` would grab the queue
    string here, so signature resolution must wrap index 0 and leave the queue
    string (index 1) intact."""
    delivered = []

    def user_cb(channel, method, properties, body):
        delivered.append(body)

    captured = {}

    def fake_basic_consume(consumer_callback, queue="", no_ack=False,
                           **kwargs):
        captured["queue"] = queue
        captured["cb"] = consumer_callback

    pika_instr._basic_consume_wrapper(
        fake_basic_consume, instance=None,
        args=(user_cb, "queue.name"), kwargs={},
    )
    # Queue string at index 1 untouched; the callback (index 0) was wrapped.
    assert captured["queue"] == "queue.name"
    assert captured["cb"] is not user_cb
    assert callable(captured["cb"])
    captured["cb"](_FakeChannel(), _FakeMethod(), _FakeProperties(), b"hi")
    assert delivered == [b"hi"]
    assert ("span_start", "RabbitMQ Consumer Invocation",
            "rabbitmq://exchange=ex") in fake_agent.events


def test_basic_consume_extracts_keyword_callback_pika_1x(fake_agent):
    """pika ≥1.0 with the callback passed by keyword:
    ``basic_consume(queue, on_message_callback=cb)``. Exercises the KEYWORD
    branch of both ``_extract_callback`` and ``replace_arg`` — every other
    positive test passes the callback positionally, so this branch was
    uncovered."""
    delivered = []

    def user_cb(channel, method, properties, body):
        delivered.append(body)

    captured = {}

    def fake_basic_consume(queue, on_message_callback=None, auto_ack=False,
                           **kwargs):
        captured["queue"] = queue
        captured["cb"] = on_message_callback

    pika_instr._basic_consume_wrapper(
        fake_basic_consume, instance=None,
        args=("queue.name",), kwargs={"on_message_callback": user_cb},
    )
    # Queue positional untouched; callback re-injected as the keyword arg.
    assert captured["queue"] == "queue.name"
    assert captured["cb"] is not user_cb
    assert callable(captured["cb"])
    captured["cb"](_FakeChannel(), _FakeMethod(), _FakeProperties(), b"hi")
    assert delivered == [b"hi"]
    assert ("span_start", "RabbitMQ Consumer Invocation",
            "rabbitmq://exchange=ex") in fake_agent.events


def test_basic_consume_extracts_keyword_callback_pika_0x(fake_agent):
    """pika 0.x with the callback passed by keyword:
    ``basic_consume(consumer_callback=cb, queue=...)``. The wrapper must
    re-inject under the SAME resolved name (``consumer_callback``), never a
    hardcoded ``on_message_callback`` — the pre-fix code did the latter, which
    left the 0.x handler untraced AND crashed the real ``basic_consume`` with an
    unexpected ``on_message_callback`` kwarg."""
    delivered = []

    def user_cb(channel, method, properties, body):
        delivered.append(body)

    captured = {}

    def fake_basic_consume(consumer_callback=None, queue="", no_ack=False,
                           **kwargs):
        captured["queue"] = queue
        captured["cb"] = consumer_callback
        captured["extra_kwargs"] = kwargs

    pika_instr._basic_consume_wrapper(
        fake_basic_consume, instance=None,
        args=(), kwargs={"consumer_callback": user_cb, "queue": "q.name"},
    )
    assert captured["queue"] == "q.name"
    assert captured["cb"] is not user_cb
    assert callable(captured["cb"])
    # No spurious on_message_callback kwarg was injected alongside.
    assert "on_message_callback" not in captured["extra_kwargs"]
    captured["cb"](_FakeChannel(), _FakeMethod(), _FakeProperties(), b"hi")
    assert delivered == [b"hi"]


def test_basic_consume_resolves_through_safe_wrapper_sentinel():
    """Regression: in production ``_basic_consume_wrapper`` is reached through
    ``safe_wrapper``, which hands it a per-call ``_SafeSentinel`` (not the real
    ``basic_consume``) as ``wrapped``. Signature resolution must see through the
    sentinel — otherwise it always falls back to the ≥1.0 index and pika 0.x
    consumers are silently untraced."""
    from pinpoint.instrumentations._util import _SafeSentinel

    def user_cb(channel, method, properties, body):
        pass

    # pika 0.x shape: callback at index 0, queue at index 1.
    def fake_basic_consume(consumer_callback, queue="", no_ack=False, **kwargs):
        fake_basic_consume.captured = (consumer_callback, queue)

    pika_instr._basic_consume_wrapper(
        _SafeSentinel(fake_basic_consume), instance=None,
        args=(user_cb, "queue.name"), kwargs={},
    )

    cb, queue = fake_basic_consume.captured
    # 0.x callback (index 0) wrapped; queue (index 1) untouched. The bug made
    # this fall back to index 1 and wrap the queue string instead.
    assert queue == "queue.name"
    assert cb is not user_cb and callable(cb)


def test_basic_consume_passes_through_when_callback_not_callable(fake_agent):
    """If the resolved position doesn't hold a callable (signature mismatch we
    couldn't reconcile), pass the call through untouched rather than wrapping
    a non-callable — no ``functools.wraps(str)`` corruption."""
    captured = {}

    def fake_basic_consume(queue, on_message_callback=None, **kwargs):
        captured["cb"] = on_message_callback

    # Only the queue positional supplied; callback slot absent.
    pika_instr._basic_consume_wrapper(
        fake_basic_consume, instance=None,
        args=(), kwargs={"queue": "queue.name"},
    )
    assert captured["cb"] is None
    assert not any(e[0] == "span_start" for e in fake_agent.events)


def test_basic_consume_skips_reader_when_no_upstream_context(fake_agent):
    """A delivery whose AMQP properties carry no Pinpoint header opens a new
    trace via the reader-less new_span."""
    captured = {}

    def fake_basic_consume(*args, **kwargs):
        captured["cb"] = kwargs.get("on_message_callback") or args[1]

    pika_instr._basic_consume_wrapper(
        fake_basic_consume, instance=None,
        args=("queue.name", lambda *a: None), kwargs={},
    )
    properties = _FakeProperties(headers={"x-custom": "v"})
    captured["cb"](_FakeChannel(), _FakeMethod(), properties, b"hello")
    assert fake_agent.last_native.headers is None
    assert ("span_start", "RabbitMQ Consumer Invocation",
            "rabbitmq://exchange=ex") in fake_agent.events


def test_basic_consume_unsampled_skips_annotation_prep(monkeypatch):
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
        pika_instr, "_connection_endpoint",
        lambda ch: endpoint_calls.append(ch) or "broker:5672",
    )

    captured = {}

    def fake_basic_consume(*args, **kwargs):
        captured["cb"] = kwargs.get("on_message_callback") or args[1]

    pika_instr._basic_consume_wrapper(
        fake_basic_consume, instance=None,
        args=("queue.name", lambda *a: None), kwargs={},
    )
    properties = _FakeProperties(headers={"Pinpoint-TraceID": "abc"})
    captured["cb"](_FakeChannel(), _FakeMethod(), properties, b"hello")
    outer = "RabbitMQ Consumer Invocation"
    assert ("span_start", outer, "rabbitmq://exchange=ex") in agent.events
    assert ("span_end", outer) in agent.events
    # Gate held: annotation prep skipped entirely.
    assert endpoint_calls == []
    assert agent.last_native.annotations.entries == []


def test_basic_consume_callback_records_user_exception(fake_agent):
    def boom(channel, method, properties, body):
        raise ValueError("bad")

    captured = {}

    def fake_basic_consume(*args, **kwargs):
        captured["cb"] = args[1]

    pika_instr._basic_consume_wrapper(
        fake_basic_consume, instance=None,
        args=("q", boom), kwargs={},
    )
    with pytest.raises(ValueError, match="bad"):
        captured["cb"](_FakeChannel(), _FakeMethod(), _FakeProperties(), b"x")
    assert any(e[0] == "span_error" for e in fake_agent.events)


def test_basic_consume_no_op_when_agent_disabled(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))
    delivered = []

    def user_cb(channel, method, properties, body):
        delivered.append(body)

    captured = {}
    pika_instr._basic_consume_wrapper(
        lambda *a, **kw: captured.setdefault("cb", a[1]),
        instance=None, args=("q", user_cb), kwargs={},
    )
    captured["cb"](_FakeChannel(), _FakeMethod(), _FakeProperties(), b"hello")
    assert delivered == [b"hello"]


# ---------------------------------------------------------------------------
# Sync-poll path: basic_get
# ---------------------------------------------------------------------------

def test_blocking_basic_get_opens_span_when_message_available(fake_agent):
    method = _FakeMethod(exchange="", routing_key="orders")
    props = _FakeProperties(headers={"Pinpoint-TraceID": "abc"})
    body = b"payload"

    def wrapped(*_a, **_kw):
        return method, props, body

    out = pika_instr._blocking_basic_get_wrapper(
        wrapped, instance=_FakeChannel(), args=("orders",), kwargs={},
    )
    assert out == (method, props, body)
    outer = "RabbitMQ Consumer Invocation"
    assert any(
        e[0] == "span_start" and e[1] == outer for e in fake_agent.events
    )
    assert any(
        e[0] == "event_start" and e[2] == "pika.consume.basic_get"
        for e in fake_agent.events
    )
    assert any(e[0] == "span_end" for e in fake_agent.events)


def test_blocking_basic_get_still_traces_inside_an_active_span(fake_agent):
    """The delivery-only paths hand their span to nobody and end it before
    returning, so an enclosing trace does not stop them: only the *held*
    paths step aside. Guards the ``held=`` split from collapsing."""
    outer_span = fake_agent.new_span("outer", "/orders")
    token = ppctx.set_current_span(outer_span)
    try:
        out = pika_instr._blocking_basic_get_wrapper(
            lambda *_a, **_kw: (_FakeMethod(), _FakeProperties(), b"payload"),
            instance=_FakeChannel(), args=("orders",), kwargs={},
        )
        assert out[2] == b"payload"
        assert any(e[0] == "event_start" and e[2] == "pika.consume.basic_get"
                   for e in fake_agent.events)
        assert ppctx.current_span() is outer_span     # and never displaced
    finally:
        ppctx.reset_current_span(token)


def test_blocking_basic_get_no_span_when_queue_empty(fake_agent):
    """``basic_get`` returns ``(None, None, None)`` when the queue is
    empty — no span should open."""
    def wrapped(*_a, **_kw):
        return None, None, None

    out = pika_instr._blocking_basic_get_wrapper(
        wrapped, instance=_FakeChannel(), args=("orders",), kwargs={},
    )
    assert out == (None, None, None)
    assert not any(e[0] == "span_start" for e in fake_agent.events)


# ---------------------------------------------------------------------------
# BlockingChannel.basic_consume — the dominant blocking API
#
# pika stores the user callback and dispatches it later (process_data_events /
# start_consuming); at Channel.basic_consume it registers only its internal
# buffering sink. We wrap the user callback at the BlockingChannel seam so the
# span wraps the actual handler and is active while it runs.
# ---------------------------------------------------------------------------

# Real pika 1.x BlockingChannel.basic_consume signature shape — the callback is
# ``on_message_callback`` at positional index 1, resolved from the signature.
def _blocking_basic_consume(queue, on_message_callback, auto_ack=False,
                            exclusive=False, consumer_tag=None,
                            arguments=None):
    _blocking_basic_consume.captured = {
        "queue": queue, "cb": on_message_callback,
    }


def test_blocking_channel_basic_consume_span_active_during_handler(fake_agent):
    """Regression for the core bug: on the BlockingChannel path the span must
    wrap the *user handler* and be active while it runs — not an internal buffer
    append that ends before the handler is dispatched.

    Assert the wrapped callback opens a span that (a) is the current span while
    the handler runs, so a nested ``trace()`` stitches onto it, and (b) starts
    before and ends after the handler body."""
    inside = {}

    def user_cb(channel, method, properties, body):
        span = ppctx.current_span()
        inside["current"] = span
        # A downstream DB/HTTP/producer call resolves its parent via
        # current_span(); opening one here must attach to the consumer span.
        if span is not None:
            span.new_span_event("db.query").end()

    pika_instr._basic_consume_wrapper(
        _blocking_basic_consume, instance=None,
        args=("orders.q", user_cb), kwargs={},
    )
    captured = _blocking_basic_consume.captured
    assert captured["queue"] == "orders.q"       # queue arg untouched
    assert captured["cb"] is not user_cb         # callback wrapped

    # Registration alone must open no span — only pika's later dispatch does.
    assert not any(e[0] == "span_start" for e in fake_agent.events)

    # Simulate pika dispatching the stored callback from process_data_events.
    captured["cb"](_FakeChannel(), _FakeMethod(), _FakeProperties(), b"hi")

    outer = "RabbitMQ Consumer Invocation"
    assert inside["current"] is not None                       # active in handler
    events = fake_agent.events
    assert ("event_start", outer, "db.query") in events        # nested stitched
    # The span spans the handler: opened before the nested call, closed after.
    i_start = events.index(("span_start", outer, "rabbitmq://exchange=ex"))
    i_child = events.index(("event_start", outer, "db.query"))
    i_end = events.index(("span_end", outer))
    assert i_start < i_child < i_end


def test_blocking_channel_basic_consume_records_handler_exception(fake_agent):
    """A handler raising must be recorded on the consumer span and re-raised
    (never swallowed), and the span still closed."""
    def boom(channel, method, properties, body):
        raise ValueError("handler failed")

    pika_instr._basic_consume_wrapper(
        _blocking_basic_consume, instance=None,
        args=("orders.q", boom), kwargs={},
    )
    cb = _blocking_basic_consume.captured["cb"]
    with pytest.raises(ValueError, match="handler failed"):
        cb(_FakeChannel(), _FakeMethod(), _FakeProperties(), b"x")
    outer = "RabbitMQ Consumer Invocation"
    assert any(e[0] == "span_error" for e in fake_agent.events)
    assert ("span_end", outer) in fake_agent.events


def test_channel_seam_skips_blocking_internal_dispatcher(fake_agent):
    """BlockingChannel hands pika's internal buffering sink
    (``_on_consumer_message_delivery``) down to ``Channel.basic_consume``.
    Wrapping it would span the enqueue (the bug) and double-count against the
    handler wrapped at the BlockingChannel seam — so the Channel seam must pass
    it through untouched."""
    BlockingChannel = pytest.importorskip(
        "pika.adapters.blocking_connection").BlockingChannel

    # A real BlockingChannel without running __init__ — enough for the
    # bound-method isinstance check the guard performs.
    bc = object.__new__(BlockingChannel)
    dispatcher = bc._on_consumer_message_delivery

    captured = {}

    def channel_basic_consume(queue, on_message_callback, auto_ack=False,
                              exclusive=False, consumer_tag=None,
                              arguments=None, callback=None):
        captured["cb"] = on_message_callback

    pika_instr._basic_consume_wrapper(
        channel_basic_consume, instance=None,
        args=("orders.q", dispatcher), kwargs={},
    )
    # Passed straight through — NOT wrapped.
    assert captured["cb"] is dispatcher
    assert not getattr(captured["cb"], "_pinpoint_consumer_wrapped", False)


def test_basic_consume_does_not_double_wrap_already_wrapped_callback(fake_agent):
    """Delegation guard (mirrors aio_pika RobustQueue→Queue): a callback already
    carrying the ``_pinpoint_consumer_wrapped`` marker is passed straight
    through, so a second registration seam can't stack two root spans per
    delivery."""
    def user_cb(channel, method, properties, body):
        pass

    first = pika_instr._wrap_consumer_callback(user_cb)
    assert first._pinpoint_consumer_wrapped is True

    captured = {}

    def basic_consume(queue, on_message_callback, auto_ack=False, **kwargs):
        captured["cb"] = on_message_callback

    pika_instr._basic_consume_wrapper(
        basic_consume, instance=None,
        args=("q", first), kwargs={},
    )
    # Same wrapper object handed down — not wrapped a second time.
    assert captured["cb"] is first


# ---------------------------------------------------------------------------
# BlockingChannel.consume() — the callback-less generator form
#
# Yields (method, properties, body) per delivery. We open a root span per real
# delivery and keep it active across the yield so the loop body's child calls
# stitch onto it; the idle wait between messages stays untraced.
# ---------------------------------------------------------------------------

def test_consumer_callback_steps_aside_inside_an_active_span(fake_agent):
    """A dispatch loop pumped from inside a trace (``process_data_events()``
    in a request handler) must not have its handler re-parented under a
    delivery span: the held scope steps aside and the handler runs untraced."""
    inside = {}

    def user_cb(channel, method, properties, body):
        inside["current"] = ppctx.current_span()

    pika_instr._basic_consume_wrapper(
        _blocking_basic_consume, instance=None,
        args=("orders.q", user_cb), kwargs={},
    )
    wrapped_cb = _blocking_basic_consume.captured["cb"]

    outer_span = fake_agent.new_span("outer", "/orders")
    token = ppctx.set_current_span(outer_span)
    try:
        wrapped_cb(_FakeChannel(), _FakeMethod(), _FakeProperties(), b"hi")
    finally:
        ppctx.reset_current_span(token)

    # The handler ran, still under the caller's span, and no delivery span
    # was opened to re-parent it.
    assert inside["current"] is outer_span
    assert not any(e[0] == "span_start"
                   and e[1] == "RabbitMQ Consumer Invocation"
                   for e in fake_agent.events)


def test_blocking_consume_generator_steps_aside_inside_an_active_span(
        fake_agent):
    """``for ... in channel.consume(...)`` inside a traced request: the held
    span would outlive the loop body and only close on the next delivery, so
    it steps aside and the caller's span stays current."""
    def fake_consume(*_a, **_kw):
        yield (_FakeMethod(routing_key="orders.a"), _FakeProperties(), b"one")

    gen = pika_instr._blocking_consume_wrapper(
        fake_consume, instance=_FakeChannel(), args=("orders",), kwargs={},
    )
    outer_span = fake_agent.new_span("outer", "/orders")
    token = ppctx.set_current_span(outer_span)
    try:
        during = [ppctx.current_span() for _ in gen]
    finally:
        ppctx.reset_current_span(token)

    assert during == [outer_span]
    assert not any(e[0] == "span_start"
                   and e[1] == "RabbitMQ Consumer Invocation"
                   for e in fake_agent.events)


def test_blocking_consume_generator_spans_each_delivery_with_active_span(
        fake_agent):
    m1 = _FakeMethod(routing_key="orders.a")
    m2 = _FakeMethod(routing_key="orders.b")
    up = _FakeProperties(headers={"Pinpoint-TraceID": "abc"})
    plain = _FakeProperties(headers=None)
    deliveries = [
        (m1, up, b"one"),
        (None, None, None),      # inactivity-timeout tick — no message
        (m2, plain, b"two"),
    ]

    def fake_consume(*_a, **_kw):
        yield from deliveries

    gen = pika_instr._blocking_consume_wrapper(
        fake_consume, instance=_FakeChannel(), args=("orders",), kwargs={},
    )

    seen = []
    during = []
    for method, properties, body in gen:
        during.append(ppctx.current_span())
        if method is not None:
            # nested downstream call attaches to the consumer span
            cur = ppctx.current_span()
            assert cur is not None
            cur.new_span_event("db.query").end()
        seen.append(body)

    # Every delivery (including the inactivity tick) is passed through verbatim.
    assert seen == [b"one", None, b"two"]
    # Span active during real deliveries, absent during the inactivity tick.
    assert during[0] is not None and during[2] is not None
    assert during[1] is None
    assert during[0] is not during[2]        # a distinct span per delivery
    # Contextvar cleared once the generator is exhausted.
    assert ppctx.current_span() is None

    outer = "RabbitMQ Consumer Invocation"
    events = fake_agent.events
    assert sum(1 for e in events
               if e[0] == "span_start" and e[1] == outer) == 2
    assert sum(1 for e in events
               if e[0] == "span_end" and e[1] == outer) == 2
    # Nested db.query events stitched onto the consumer span, one per delivery.
    assert sum(1 for e in events
               if e == ("event_start", outer, "db.query")) == 2


def test_blocking_consume_generator_reads_upstream_headers(fake_agent):
    """An upstream Pinpoint header in the delivery's properties seeds a
    reader-backed span; its absence opens a fresh local trace."""
    up = _FakeProperties(headers={"Pinpoint-TraceID": "abc"})

    def with_upstream(*_a, **_kw):
        yield (_FakeMethod(), up, b"one")

    gen = pika_instr._blocking_consume_wrapper(
        with_upstream, instance=_FakeChannel(), args=("q",), kwargs={},
    )
    list(gen)
    assert fake_agent.last_native.headers is not None

    plain = _FakeProperties(headers={"x-other": "v"})

    def no_upstream(*_a, **_kw):
        yield (_FakeMethod(), plain, b"two")

    gen = pika_instr._blocking_consume_wrapper(
        no_upstream, instance=_FakeChannel(), args=("q",), kwargs={},
    )
    list(gen)
    assert fake_agent.last_native.headers is None


def test_blocking_consume_generator_survives_span_setup_failure(
        fake_agent, monkeypatch):
    """A tracing failure must never break iteration or withhold a delivery from
    the caller — the generator degrades to untraced delivery."""
    def _boom(*_a, **_kw):
        raise RuntimeError("native down")

    monkeypatch.setattr(fake_agent, "new_span", _boom)

    def fake_consume(*_a, **_kw):
        yield (_FakeMethod(), _FakeProperties(), b"one")
        yield (_FakeMethod(), _FakeProperties(), b"two")

    gen = pika_instr._blocking_consume_wrapper(
        fake_consume, instance=_FakeChannel(), args=("q",), kwargs={},
    )
    seen = [body for _m, _p, body in gen]
    assert seen == [b"one", b"two"]          # deliveries never dropped
    assert ppctx.current_span() is None      # no dangling context


def test_blocking_consume_generator_ends_span_when_generator_closed(fake_agent):
    """Breaking out of the ``for`` loop closes the generator; the open delivery
    span must be ended and the contextvar detached (no span leak)."""
    def fake_consume(*_a, **_kw):
        yield (_FakeMethod(), _FakeProperties(), b"one")
        yield (_FakeMethod(), _FakeProperties(), b"two")

    gen = pika_instr._blocking_consume_wrapper(
        fake_consume, instance=_FakeChannel(), args=("q",), kwargs={},
    )
    next(gen)                                    # take the first delivery
    assert ppctx.current_span() is not None      # span active mid-processing
    gen.close()                                  # caller broke out of the loop
    assert ppctx.current_span() is None          # span detached
    outer = "RabbitMQ Consumer Invocation"
    assert ("span_end", outer) in fake_agent.events


def test_blocking_consume_generator_passthrough_when_agent_disabled(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))

    def fake_consume(*_a, **_kw):
        yield (_FakeMethod(), _FakeProperties(), b"one")
        yield (_FakeMethod(), _FakeProperties(), b"two")

    gen = pika_instr._blocking_consume_wrapper(
        fake_consume, instance=_FakeChannel(), args=("q",), kwargs={},
    )
    seen = [body for _m, _p, body in gen]
    assert seen == [b"one", b"two"]
    assert ppctx.current_span() is None


def test_blocking_consume_generator_tolerates_unexpected_yield_shapes(fake_agent):
    """``consume()`` deliveries are unpacked inline (no per-field helpers), so
    a shape we don't recognise must degrade to "untraced" rather than raise a
    ValueError/TypeError into the caller's ``for`` loop and drop the item."""
    m = _FakeMethod()
    odd = [
        (m,),                     # short tuple — no properties
        (m, _FakeProperties()),   # 2-tuple
        (),                       # empty
        None,                     # not a sequence at all
        "not-a-tuple",
        (m, _FakeProperties(), b"body", "extra"),   # longer than expected
    ]

    def fake_consume(*_a, **_kw):
        yield from odd

    gen = pika_instr._blocking_consume_wrapper(
        fake_consume, instance=_FakeChannel(), args=("q",), kwargs={},
    )
    assert list(gen) == odd                  # every item delivered verbatim
    assert ppctx.current_span() is None      # no dangling context
    outer = "RabbitMQ Consumer Invocation"
    # Traced only the shapes that actually carry a delivery method.
    assert sum(1 for e in fake_agent.events
               if e[0] == "span_start" and e[1] == outer) == 3


def test_broker_endpoint_resolves_through_a_blocking_connection_facade():
    """``BlockingConnection`` keeps its parameters on ``_impl``; reading only
    ``params``/``_params`` yielded nothing (and a falsy result is never
    memoized, so it was recomputed per delivery) (regression)."""
    class _Params:
        host = "mq.local"
        port = 5672

    class _Impl:
        params = _Params()

    class _Blocking:
        _impl = _Impl()

    class _Channel:
        connection = _Blocking()

    assert pika_instr._connection_endpoint(_Channel()) == "mq.local:5672"
