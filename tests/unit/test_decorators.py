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

"""@span / @spanevent decorators — the manual tracing entrypoints.

The decorators run on top of the public ``trace`` context manager and
``agent.new_span`` API, so we exercise them against an in-memory agent +
span recorder rather than the native module.
"""

from __future__ import annotations

import asyncio

import pytest

import _fakes

import pinpoint
from pinpoint import context as ppctx
from pinpoint.tracer import Span


# The in-memory span / agent doubles and the `push_span` / `fake_agent`
# fixtures come from the shared _fakes module / tests/conftest.py.


# ---------------------------------------------------------------------------
# @spanevent — sync + async + decoration shapes
# ---------------------------------------------------------------------------

def test_spanevent_no_parens_uses_function_qualname(push_span):
    _, rec = push_span

    @pinpoint.spanevent
    def compute(x):
        return x + 1

    assert compute(5) == 6
    starts = [e for e in rec.events if e[0] == "event_start"]
    assert starts and starts[0][2].endswith("compute")


def test_spanevent_with_explicit_name(push_span):
    _, rec = push_span

    @pinpoint.spanevent("billing.charge")
    def charge(amount):
        return amount

    charge(42)
    assert ("event_start", "root", "billing.charge") in rec.events
    assert ("event_end", "root", "billing.charge") in rec.events


def test_spanevent_no_current_span_runs_untraced():
    # No push_span fixture — current_span is None.
    @pinpoint.spanevent("orphan")
    def f():
        return 7

    assert f() == 7  # function still runs


def test_spanevent_async(push_span):
    _, rec = push_span

    @pinpoint.spanevent("await_task")
    async def fetch():
        await asyncio.sleep(0)
        return "done"

    assert asyncio.run(fetch()) == "done"
    assert ("event_start", "root", "await_task") in rec.events
    assert ("event_end", "root", "await_task") in rec.events


def test_spanevent_records_error_and_reraises(push_span):
    _, rec = push_span

    @pinpoint.spanevent("boom")
    def bad():
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError, match="nope"):
        bad()

    # The trace context manager calls SpanEvent.set_error on exception.
    assert any(e[0] == "event_error" for e in rec.events)
    assert ("event_end", "root", "boom") in rec.events


def test_spanevent_async_records_error_and_reraises(push_span):
    _, rec = push_span

    @pinpoint.spanevent("aboom")
    async def bad():
        raise RuntimeError("aerr")

    with pytest.raises(RuntimeError, match="aerr"):
        asyncio.run(bad())

    assert any(e[0] == "event_error" for e in rec.events)


def test_spanevent_passes_through_args_and_return_value(push_span):

    @pinpoint.spanevent
    def add(a, b, *, c=0):
        return a + b + c

    assert add(1, 2, c=4) == 7


def test_spanevent_preserves_metadata(push_span):

    @pinpoint.spanevent("op")
    def documented():
        """My docstring."""
        return 1

    assert documented.__name__ == "documented"
    assert documented.__doc__ == "My docstring."


# ---------------------------------------------------------------------------
# pinpoint.trace context-manager contract (now a plain function, not a
# @contextmanager generator — the yielded value and exception propagation
# must be unchanged).
# ---------------------------------------------------------------------------

def test_trace_yields_span_event_when_span_current(push_span):
    from pinpoint.tracer import SpanEvent
    _, rec = push_span

    with pinpoint.trace("compute") as ev:
        assert isinstance(ev, SpanEvent)

    assert ("event_start", "root", "compute") in rec.events
    assert ("event_end", "root", "compute") in rec.events


def test_trace_yields_none_without_span():
    # No current span -> shared no-op CM whose __enter__ yields None.
    with pinpoint.trace("orphan") as ev:
        assert ev is None


def test_trace_reuses_null_event_on_unsampled_span():
    """An ``UnSampledSpan`` is current on unsampled requests; its ``trace``
    returns the shared no-op event, so ``pinpoint.trace`` allocates nothing and
    still yields a usable context manager."""
    from pinpoint.agent import UnSampledSpan, _NULL_SPAN_EVENT
    token = ppctx.set_current_span(UnSampledSpan(object()))
    try:
        with pinpoint.trace("x") as ev:
            assert ev is _NULL_SPAN_EVENT
    finally:
        ppctx.reset_current_span(token)


def test_trace_records_error_and_reraises(push_span):
    _, rec = push_span

    with pytest.raises(RuntimeError, match="nope"):
        with pinpoint.trace("boom"):
            raise RuntimeError("nope")

    # SpanEvent.__exit__ records the error on the event, then ends it.
    assert any(e[0] == "event_error" for e in rec.events)
    assert ("event_end", "root", "boom") in rec.events


def test_trace_no_span_propagates_exception():
    with pytest.raises(ValueError, match="boom"):
        with pinpoint.trace("orphan"):
            raise ValueError("boom")


# ---------------------------------------------------------------------------
# @span — sync + async + decoration shapes
# ---------------------------------------------------------------------------

def test_span_no_parens_creates_root_span(fake_agent):
    @pinpoint.span
    def worker(x):
        # Inside the function, `current_span` resolves to our fresh span.
        assert ppctx.current_span() is not None
        return x * 2

    assert worker(21) == 42
    starts = [e for e in fake_agent.events if e[0] == "span_start"]
    assert len(starts) == 1
    assert starts[0][1].endswith("worker")
    assert ("span_end", starts[0][1]) in fake_agent.events


def test_span_with_explicit_name_and_rpc_point(fake_agent):
    @pinpoint.span("nightly_billing", rpc_point="/cron/billing")
    def job():
        pass

    job()
    assert ("span_start", "nightly_billing", "/cron/billing") in fake_agent.events
    assert ("span_end", "nightly_billing") in fake_agent.events


def test_span_rpc_point_defaults_to_operation(fake_agent):
    @pinpoint.span("op_only")
    def f():
        pass

    f()
    assert ("span_start", "op_only", "op_only") in fake_agent.events


def test_span_async(fake_agent):
    @pinpoint.span("async_job")
    async def job():
        await asyncio.sleep(0)
        return "ok"

    assert asyncio.run(job()) == "ok"
    assert ("span_start", "async_job", "async_job") in fake_agent.events
    assert ("span_end", "async_job") in fake_agent.events


def test_span_records_error_and_reraises(fake_agent):
    @pinpoint.span("boom")
    def bad():
        raise RuntimeError("kaboom")

    with pytest.raises(RuntimeError, match="kaboom"):
        bad()

    assert any(e[0] == "span_error" for e in fake_agent.events)
    assert ("span_end", "boom") in fake_agent.events


def test_span_async_records_error_and_reraises(fake_agent):
    @pinpoint.span("aboom")
    async def bad():
        raise RuntimeError("akaboom")

    with pytest.raises(RuntimeError, match="akaboom"):
        asyncio.run(bad())

    assert any(e[0] == "span_error" for e in fake_agent.events)


def test_span_no_agent_runs_untraced(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", None)

    @pinpoint.span("orphan")
    def f():
        return 1

    assert f() == 1


def test_span_disabled_agent_runs_untraced(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _fakes.FakeAgent(enabled=False))

    @pinpoint.span("disabled")
    def f():
        return 1

    assert f() == 1


def test_span_makes_span_current_inside_body(fake_agent):
    seen = {}

    @pinpoint.span("op")
    def f():
        seen["span"] = ppctx.current_span()

    f()
    assert isinstance(seen["span"], Span)
    # After the call, the span has been popped.
    assert ppctx.current_span() is None


# ---------------------------------------------------------------------------
# Composition: @span outer + @spanevent inner
# ---------------------------------------------------------------------------

def test_span_with_nested_spanevent_links_correctly(fake_agent):
    @pinpoint.spanevent("inner")
    def inner():
        return 1

    @pinpoint.span("outer", rpc_point="/cron/outer")
    def outer():
        return inner()

    outer()

    # Outer span starts → inner event opens/closes under it → outer span ends.
    kinds = [(e[0], e[1] if len(e) > 1 else None) for e in fake_agent.events]
    assert ("span_start", "outer") in kinds
    assert ("event_start", "outer") in kinds
    assert ("event_end", "outer") in kinds
    assert ("span_end", "outer") in kinds
