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

"""Celery producer + worker instrumentation.

Drives the ``apply_async`` wrapper and the signal receivers directly against
in-memory fakes — no broker, no Celery. The last section runs the same flow
through the real library on its in-process ``memory://`` transport and is
skipped when Celery is not installed.
"""

from __future__ import annotations

import types

import pytest

from _fakes import (FakeNativeSpan as _FakeNativeSpan, UnsampledAgent,
                    UnsampledNative, stub_inject)

from pinpoint import context as ppctx
from pinpoint.agent import UnSampledSpan
from pinpoint.instrumentations import celery as celery_instr
from pinpoint.propagator import (HEADER_SAMPLED, HEADER_SPAN_ID,
                                 HEADER_TRACE_ID, extract_pinpoint_headers)
from pinpoint.service_type import (SERVICE_TYPE_CELERY,
                                   SERVICE_TYPE_CELERY_WORKER)

_OP_PUBLISH = "celery.app.task.Task.apply_async"
_OP_WORKER = "Celery Worker Invocation"
_BROKER = "amqp://guest:guest@broker.test:5672//"


@pytest.fixture(autouse=True)
def _stub_inject(monkeypatch):
    """Deterministic trace headers from fake native spans."""
    stub_inject(monkeypatch, celery_instr)


class _Request:
    """Shaped after ``celery.app.task.Context``: message headers become
    attributes, ``get`` is ``getattr``."""

    def __init__(self, **attrs):
        self.__dict__.update(attrs)

    def get(self, key, default=None):
        return getattr(self, key, default)


def _task(name="orders.charge", queue=None, request=None, broker=_BROKER):
    conf = types.SimpleNamespace(broker_url=broker, task_default_queue="celery")
    return types.SimpleNamespace(
        name=name, queue=queue, request=request if request is not None else _Request(),
        app=types.SimpleNamespace(conf=conf),
    )


def _publishing(headers, routing_key="orders", exchange=""):
    """A ``wrapped`` that does what Celery does inside apply_async: fire
    ``before_task_publish`` with the final message headers."""
    def _wrapped(*_args, **_kwargs):
        celery_instr._on_before_publish(
            headers=headers, routing_key=routing_key, exchange=exchange)
        return "async-result"
    return _wrapped


# ---------------------------------------------------------------------------
# Producer: apply_async wrapper + before_task_publish receiver
# ---------------------------------------------------------------------------

def test_apply_async_emits_event_and_injects_routed_headers(push_span):
    sp, rec = push_span
    headers = {"id": "task-id", "task": "orders.charge"}

    result = celery_instr._apply_async_wrapper(
        _publishing(headers), _task(), ((1, 2),), {})

    assert result == "async-result"
    assert ("event_start", "root", _OP_PUBLISH) in rec.events
    assert ("event_end", "root", _OP_PUBLISH) in rec.events
    event = sp._native.all_events[0]
    assert event.service_type == SERVICE_TYPE_CELERY
    # The receiver overrides the static queue with the routed key.
    assert event.destination == "orders"
    assert event.endpoint == "broker.test:5672"
    assert headers[HEADER_TRACE_ID] == "trace-id"
    assert headers["id"] == "task-id"


def test_apply_async_eager_keeps_the_static_queue(push_span):
    """``task_always_eager`` publishes nothing, so the receiver never fires;
    the task's own queue (or the app default) remains the destination."""
    sp, _ = push_span

    celery_instr._apply_async_wrapper(
        lambda *a, **k: None, _task(queue="billing"), (), {})
    celery_instr._apply_async_wrapper(
        lambda *a, **k: None, _task(), (), {})

    assert [e.destination for e in sp._native.all_events] == ["billing", "celery"]


def test_apply_async_records_exception_and_ends_event(push_span):
    sp, rec = push_span

    def _boom(*_a, **_k):
        raise ConnectionError("broker down")

    with pytest.raises(ConnectionError):
        celery_instr._apply_async_wrapper(_boom, _task(), (), {})

    assert ("event_error", _OP_PUBLISH, ("ConnectionError", "broker down")) in rec.events
    assert ("event_end", "root", _OP_PUBLISH) in rec.events
    assert celery_instr._publish_event.get() is None


def test_apply_async_no_parent_passes_through():
    headers = {}
    calls = []
    result = celery_instr._apply_async_wrapper(
        lambda *a, **k: calls.append((a, k)) or "r", _task(), ((1,),), {"countdown": 5})
    celery_instr._on_before_publish(headers=headers, routing_key="orders")

    assert result == "r"
    assert calls == [(((1,),), {"countdown": 5})]
    assert headers == {}


def test_before_publish_outside_apply_async_still_injects(push_span):
    """``app.send_task(name)`` never passes through ``Task.apply_async``; the
    receiver still stitches the message onto the current span."""
    sp, _ = push_span
    headers = {}
    with sp.new_span_event("caller"):
        celery_instr._on_before_publish(headers=headers, routing_key="orders")
    assert headers[HEADER_TRACE_ID] == "trace-id"


def test_before_publish_unsampled_writes_s0_marker():
    """An unsampled parent must still propagate ``Pinpoint-Sampled: s0`` so
    the worker short-circuits its own sampling decision."""
    sp = UnSampledSpan(UnsampledNative())
    token = ppctx.set_current_span(sp)
    headers = {}
    try:
        celery_instr._apply_async_wrapper(
            _publishing(headers), _task(), (), {})
    finally:
        ppctx.reset_current_span(token)
    assert headers.get(HEADER_SAMPLED) == "s0"


def test_before_publish_tolerates_missing_headers(push_span):
    celery_instr._on_before_publish(headers=None, routing_key="orders")


@pytest.mark.parametrize("url, expected", [
    ("amqp://guest:guest@broker.test:5672//", "broker.test:5672"),
    ("redis://:secret@cache.test:6379/0", "cache.test:6379"),
    ("amqp://", "localhost"),
    (["amqp://a.test:5672//", "amqp://b.test:5672//"], "a.test:5672"),
    ("", ""),
])
def test_broker_endpoint_drops_credentials(url, expected):
    app = types.SimpleNamespace(conf=types.SimpleNamespace(broker_url=url))
    assert celery_instr._broker_endpoint(app) == expected


# ---------------------------------------------------------------------------
# Worker: task_prerun / task_postrun receivers
# ---------------------------------------------------------------------------

def _delivered(trace_id="app^1^1", span_id="42", sampled="s1", **extra):
    attrs = {
        HEADER_TRACE_ID: trace_id, HEADER_SPAN_ID: span_id,
        HEADER_SAMPLED: sampled,
        "delivery_info": {"exchange": "", "routing_key": "orders"},
    }
    attrs.update(extra)
    return _Request(**attrs)


def test_prerun_opens_root_span_and_postrun_ends_it_once(fake_agent):
    task = _task(request=_delivered())

    celery_instr._on_prerun(task=task, task_id="t1")
    assert ppctx.current_span() is not None
    native = fake_agent.last_native
    assert ("span_start", _OP_WORKER, "orders.charge") in fake_agent.events
    assert extract_pinpoint_headers(native.headers)[HEADER_SPAN_ID] == "42"

    celery_instr._on_postrun(task=task, task_id="t1", retval=3, state="SUCCESS")
    celery_instr._on_postrun(task=task, task_id="t1", retval=3, state="SUCCESS")

    assert ppctx.current_span() is None
    assert ("event_start", _OP_WORKER, "celery.run") in fake_agent.events
    assert fake_agent.events.count(("span_end", _OP_WORKER)) == 1
    assert not hasattr(task.request, "_pinpoint_scope")
    # Span attributes are buffered Python-side and land on the native span at end.
    assert native.service_type == SERVICE_TYPE_CELERY_WORKER
    assert native.acceptor_host == "orders"
    assert native.remote_address == "broker.test:5672"
    assert native.endpoint == "broker.test:5672"


def test_request_headers_reader_falls_back_to_nested_mapping():
    nested = _Request(headers={HEADER_TRACE_ID: "nested^1^1"})
    assert extract_pinpoint_headers(
        celery_instr._RequestHeaders(nested)) == {HEADER_TRACE_ID: "nested^1^1"}
    flat = _Request(**{HEADER_TRACE_ID: "flat^1^1"}, headers=None)
    assert extract_pinpoint_headers(
        celery_instr._RequestHeaders(flat)) == {HEADER_TRACE_ID: "flat^1^1"}
    assert extract_pinpoint_headers(celery_instr._RequestHeaders(_Request())) == {}


def test_postrun_failure_marks_the_span_error(fake_agent):
    task = _task(request=_delivered())
    celery_instr._on_prerun(task=task)
    celery_instr._on_postrun(task=task, retval=ValueError("unlucky"), state="FAILURE")

    assert ("span_error", _OP_WORKER, ("ValueError", "unlucky")) in fake_agent.events
    assert ("span_end", _OP_WORKER) in fake_agent.events
    assert ppctx.current_span() is None


@pytest.mark.parametrize("state", ["RETRY", "IGNORED", "REJECTED"])
def test_postrun_control_flow_states_are_not_errors(fake_agent, state):
    task = _task(request=_delivered())
    celery_instr._on_prerun(task=task)
    celery_instr._on_postrun(task=task, retval=RuntimeError("retry later"), state=state)

    assert not any(e[0] == "span_error" for e in fake_agent.events)
    assert ("span_end", _OP_WORKER) in fake_agent.events


def test_prerun_inside_an_active_span_leaves_it_current(fake_agent, push_span):
    """Inline execution — ``apply()`` or ``task_always_eager`` from a traced
    request — belongs to the caller's trace, not to a new root span."""
    sp, _ = push_span
    task = _task(request=_delivered())
    celery_instr._on_prerun(task=task)
    assert ppctx.current_span() is sp
    assert fake_agent.events == []
    celery_instr._on_postrun(task=task, retval=None, state="SUCCESS")
    assert ppctx.current_span() is sp


def test_prerun_no_op_when_agent_disabled(monkeypatch):
    import _fakes
    import pinpoint.agent
    agent = _fakes.FakeAgent(enabled=False)
    monkeypatch.setattr(pinpoint.agent, "_instance", agent)
    task = _task(request=_delivered())
    celery_instr._on_prerun(task=task)
    assert agent.events == []
    assert ppctx.current_span() is None
    celery_instr._on_postrun(task=task, retval=None, state="SUCCESS")


def test_postrun_without_prerun_is_noop(fake_agent):
    celery_instr._on_postrun(task=_task(), retval=None, state="SUCCESS")
    assert fake_agent.events == []


def test_unsampled_upstream_propagates_s0_from_the_worker(monkeypatch):
    """A ``Pinpoint-Sampled: s0`` delivery yields an unsampled worker span:
    nothing is annotated, and a task that enqueues further work passes the
    drop marker on."""
    import pinpoint.agent
    native = _FakeNativeSpan(_OP_WORKER, "orders.charge")
    agent = UnsampledAgent(native)
    # A real unsampled span has a native span id; without one the wrapper
    # reads as a noop span and propagates nothing.
    agent.new_span = lambda *a, **k: UnSampledSpan(native, span_id=7)
    monkeypatch.setattr(pinpoint.agent, "_instance", agent)
    # The real injector: an UnSampledSpan builds the s0 marker itself.
    monkeypatch.setattr(celery_instr, "inject_items",
                        lambda span: span.inject_context_items())
    task = _task(request=_delivered(sampled="s0"))

    celery_instr._on_prerun(task=task)
    try:
        assert isinstance(ppctx.current_span(), UnSampledSpan)
        assert native.service_type is None
        assert native.acceptor_host is None
        headers = {}
        celery_instr._apply_async_wrapper(_publishing(headers), _task(), (), {})
        assert headers == {HEADER_SAMPLED: "s0"}
    finally:
        celery_instr._on_postrun(task=task, retval=None, state="SUCCESS")
    assert ppctx.current_span() is None
    assert native.end_called


def test_prerun_setup_failure_ends_the_span(fake_agent, monkeypatch):
    """If making the span current fails after it was opened, the native span
    must still be ended — an open root span would leak for the process life."""
    monkeypatch.setattr(celery_instr, "set_current_span",
                        lambda *_: (_ for _ in ()).throw(RuntimeError("ctx")))
    task = _task(request=_delivered())
    celery_instr._on_prerun(task=task)
    assert ("span_end", _OP_WORKER) in fake_agent.events
    assert ppctx.current_span() is None


# ---------------------------------------------------------------------------
# Real library on the in-process memory transport (skipped without Celery)
# ---------------------------------------------------------------------------

@pytest.fixture
def celery_app():
    celery = pytest.importorskip("celery")
    from celery import signals

    celery_instr.instrument()
    app = celery.Celery("pp-test", broker="memory://", backend="cache+memory://")
    app.conf.task_routes = {"pp.add": {"queue": "math"}}
    app.conf.broker_connection_retry_on_startup = False
    try:
        yield app, signals
    finally:
        celery_instr.CeleryInstrumentor().uninstrument()
    assert not signals.before_task_publish.receivers
    assert not signals.task_prerun.receivers
    assert not signals.task_postrun.receivers


def test_real_celery_producer_to_worker_round_trip(celery_app, fake_agent, push_span):
    app, signals = celery_app
    sp, rec = push_span
    published = {}

    @app.task(name="pp.add")
    def add(a, b):
        if a == 13:
            raise ValueError("unlucky")
        return a + b

    def _capture(headers=None, routing_key=None, **_):
        published.update(headers=headers, routing_key=routing_key)
    signals.after_task_publish.connect(_capture, weak=False)
    try:
        add.apply_async((1, 2))
    finally:
        signals.after_task_publish.disconnect(_capture)

    # Producer: event on the caller's span, routed queue as destination, trace
    # context on the wire.
    assert published["routing_key"] == "math"
    assert published["headers"][HEADER_TRACE_ID] == "trace-id"
    assert sp._native.all_events[0].destination == "math"
    assert ("event_end", "root", _OP_PUBLISH) in rec.events

    # Worker: ``apply()`` runs the real trace_task (prerun/postrun) inline;
    # Celery parks ``headers=`` under ``request.headers``.
    token = ppctx.set_current_span(None)
    try:
        on_wire = {k: v for k, v in published["headers"].items() if k.startswith("Pinpoint-")}
        result = add.apply((2, 3), headers=on_wire)
        assert result.get() == 5
        native = fake_agent.last_native
        assert native.rpc == "pp.add"
        assert native.service_type == SERVICE_TYPE_CELERY_WORKER
        assert extract_pinpoint_headers(native.headers)[HEADER_TRACE_ID] == "trace-id"
        assert ("span_end", _OP_WORKER) in fake_agent.events

        failed = add.apply((13, 0))
        assert failed.failed()
        assert ("span_error", _OP_WORKER, ("ValueError", "unlucky")) in fake_agent.events
        assert fake_agent.events.count(("span_end", _OP_WORKER)) == 2
    finally:
        ppctx.reset_current_span(token)
