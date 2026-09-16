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

"""Shared pytest config — makes ``pinpoint`` importable without installing,
and ``_fakes`` (tests/unit/_fakes.py) importable from every test dir."""

import os
import sys

_here = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(_here)
for _p in (_root, os.path.join(_here, "unit")):
    if _p not in sys.path:
        sys.path.insert(0, _p)


import pytest


def _patch_bind_values(monkeypatch, value):
    import importlib
    for name in ("dbapi", "asyncpg", "cassandra", "pymongo", "sqlalchemy"):
        mod = sys.modules.get(f"pinpoint.instrumentations.{name}")
        if mod is None:
            try:
                mod = importlib.import_module(f"pinpoint.instrumentations.{name}")
            except Exception:  # noqa: BLE001
                continue
        if hasattr(mod, "sql_bind_values_enabled"):
            monkeypatch.setattr(mod, "sql_bind_values_enabled",
                                lambda *_: value)


@pytest.fixture
def sql_bind_values_on(monkeypatch):
    """Enable DB bound-value capture (``Config.sql_trace_bind_values``, default
    OFF) for tests that assert rendered SQL/params or the Mongo JSON payload.

    The gate lives in ``_util.sql_bind_values_enabled``; each DB instrumentation
    imports it by name, so patch the reference bound into every DB module (only
    those actually imported are patched)."""
    _patch_bind_values(monkeypatch, True)


@pytest.fixture
def sql_bind_values_off(monkeypatch):
    """Force DB bound-value capture OFF deterministically (independent of any
    ambient agent / cached flag), for tests asserting the secure default —
    that each capture call site skips values when the gate is off."""
    _patch_bind_values(monkeypatch, False)


@pytest.fixture
def push_span():
    """A real ``Span`` over a shared fake native span, set as the current
    span. Yields ``(span, recorder)``."""
    import _fakes
    from pinpoint import context as ppctx
    from pinpoint.tracer import Span
    rec = _fakes.Recorder()
    sp = Span(_fakes.FakeNativeSpan("root", recorder=rec))
    token = ppctx.set_current_span(sp)
    yield sp, rec
    ppctx.reset_current_span(token)


@pytest.fixture
def sql_push_span():
    """``push_span`` over the SQL-mirroring fakes: set_sql_query /
    set_destination / set_end_point also land in ``rec.events``."""
    import _fakes
    from pinpoint import context as ppctx
    from pinpoint.tracer import Span
    rec = _fakes.Recorder()
    sp = Span(_fakes.SqlFakeNativeSpan("root", recorder=rec))
    token = ppctx.set_current_span(sp)
    yield sp, rec
    ppctx.reset_current_span(token)


@pytest.fixture
def fake_agent(monkeypatch):
    """Install a shared ``FakeAgent`` as the process-wide agent instance."""
    import _fakes
    import pinpoint.agent
    agent = _fakes.FakeAgent()
    monkeypatch.setattr(pinpoint.agent, "_instance", agent)
    return agent


@pytest.fixture(autouse=True)
def _no_leaked_current_span():
    """Fail the test that leaks a current span instead of the 60 after it.

    The Kafka consumers now hold their record span open (and current) until the
    next fetch or ``close()``, so a test that consumes without closing would
    otherwise leave that span current for the rest of the session."""
    yield
    from pinpoint import context as ppctx
    leaked = ppctx.current_span()
    if leaked is not None:
        ppctx.set_current_span(None)
        raise AssertionError(
            f"test left a current span behind: {leaked!r} — close the consumer "
            "or reset the contextvar")


@pytest.fixture(autouse=True)
def _replay_events_eagerly(monkeypatch):
    """Replay each finished span event onto fake native spans immediately.

    Production buffers the finished-event record until the span-end batch
    flush (``end_span_with_data``), but the unit tests assert on the fake
    native span right after each event ends. Replay the record at finalize
    time and pop it from the batch, so the span-end flush doesn't replay it
    twice. On the real binding the native span has no ``new_span_event``
    (events are Python-only), so the record stays buffered for the
    production flush path."""
    import _fakes
    from pinpoint.tracer import SpanEvent as _SpanEvent
    _orig_finalize = _SpanEvent._finalize

    def _finalize_replaying(self, span, end_time):
        finished = getattr(span, "_finished_events", None) \
            if span is not None else None
        before = len(finished) if finished is not None else None
        _orig_finalize(self, span, end_time)
        if before is None or len(finished) <= before:
            return  # dropped record (overflow placeholder / detached parent)
        native_span = getattr(span, "_native", None)
        if native_span is None or not hasattr(native_span, "new_span_event"):
            return
        _fakes.replay_span_event(native_span, finished.pop())

    monkeypatch.setattr(_SpanEvent, "_finalize", _finalize_replaying)
