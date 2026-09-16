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

"""Generic ASGI middleware.

Drives ``PinpointASGIMiddleware`` against a tiny ASGI app and a fake agent —
no real ASGI server required. Validates root-span lifecycle, status capture
on http.response.start, header decoding, scope passthrough for non-HTTP
types, and exception propagation.
"""

from __future__ import annotations

import asyncio
from typing import List

import pytest

from _fakes import FakeAgent as _FakeAgent, UnsampledAgent, http_scope

import pinpoint
from pinpoint.http_helper import ASGIHeadersReader
from pinpoint.instrumentations.asgi import PinpointASGIMiddleware


# The native-span fakes and the `fake_agent` fixture come from the shared
# _fakes module / tests/conftest.py.


def _http_scope(**overrides):
    return http_scope(trace_id="T-1", **overrides)


# ---------------------------------------------------------------------------
# PinpointASGIMiddleware — happy path & lifecycle
# ---------------------------------------------------------------------------

def test_entry_event_false_falls_back_when_nothing_was_recorded(fake_agent):
    """``entry_event=False`` trusts the framework to name the handler that ran.
    A request matching no route reaches none — Starlette answers a 404 from
    ``Router.default`` and a redirect_slashes hit from the router itself — so
    the transaction would report an empty call tree, the very thing every root
    span is supposed to avoid."""
    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 404, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    mw = PinpointASGIMiddleware(app, framework_name="Starlette",
                                entry_event=False)
    asyncio.run(mw(_http_scope(), receive, lambda _m: _noop()))

    assert [e.op for e in fake_agent.last_native.all_events] == [
        "starlette.no_route"]
    assert fake_agent.last_native.status_code == 404


def test_entry_event_false_stays_out_of_the_way_when_a_handler_ran(fake_agent):
    """The fallback must not add a second row to a normally traced request:
    that outer event is exactly the noise ``entry_event=False`` avoids."""
    async def app(scope, receive, send):
        from pinpoint.context import current_span
        current_span().new_span_event("my_view").end()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    mw = PinpointASGIMiddleware(app, framework_name="Starlette",
                                entry_event=False)
    asyncio.run(mw(_http_scope(), receive, lambda _m: _noop()))

    assert [e.op for e in fake_agent.last_native.all_events] == ["my_view"]


async def _noop():
    return None


def test_middleware_opens_span_and_captures_status(fake_agent):
    sent: List[dict] = []

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"hi"})

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(msg):
        sent.append(msg)

    mw = PinpointASGIMiddleware(app)
    asyncio.run(mw(_http_scope(), receive, send))

    assert any(m.get("type") == "http.response.start" for m in sent)
    assert ("span_start", "ASGI HTTP Server", "/items/42") in fake_agent.events
    assert ("span_end", "ASGI HTTP Server") in fake_agent.events
    assert fake_agent.last_native.status_code == 200
    assert fake_agent.last_native.url_stats == [("/items/42", "GET", 200)]
    # Reaches the native Http.Server.ExcludeMethod filter, which is skipped
    # for an empty method.
    assert fake_agent.last_method == "GET"
    assert isinstance(fake_agent.last_native.headers, ASGIHeadersReader)
    assert fake_agent.last_native.headers.get("host") == "example.test"


def test_middleware_skips_reader_when_no_upstream_context(fake_agent):
    """Without a Pinpoint header, new_span is called without a reader, yet the
    sampled request is still annotated and recorded."""
    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async def receive():
        return {"type": "http.request"}

    async def send(_msg):
        pass

    scope = _http_scope(headers=[(b"host", b"example.test"), (b"x-trace", b"abc")])
    mw = PinpointASGIMiddleware(app)
    asyncio.run(mw(scope, receive, send))

    # No reader was passed into new_span (cheap no-context native path) …
    assert fake_agent.last_native.headers is None
    # … but the span still opened, recorded url_stat, and closed.
    assert ("span_start", "ASGI HTTP Server", "/items/42") in fake_agent.events
    assert fake_agent.last_native.url_stats == [("/items/42", "GET", 200)]
    assert ("span_end", "ASGI HTTP Server") in fake_agent.events


def test_middleware_records_url_pattern_when_set_by_inner(fake_agent):
    async def app(scope, receive, send):
        # Simulates Starlette/FastAPI router stashing the matched template.
        scope["pinpoint.url_pattern"] = "/items/{id}"
        await send({"type": "http.response.start", "status": 201, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async def receive():
        return {"type": "http.request"}

    async def send(_msg):
        pass

    mw = PinpointASGIMiddleware(app)
    asyncio.run(mw(_http_scope(), receive, send))
    assert fake_agent.last_native.url_stats == [("/items/{id}", "GET", 201)]


def test_middleware_records_route_path_when_route_in_scope(fake_agent):
    class _FakeRoute:
        path = "/items/{id}"

    async def app(scope, receive, send):
        scope["route"] = _FakeRoute()
        await send({"type": "http.response.start", "status": 202, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async def receive():
        return {"type": "http.request"}

    async def send(_msg):
        pass

    mw = PinpointASGIMiddleware(app)
    asyncio.run(mw(_http_scope(), receive, send))
    assert fake_agent.last_native.url_stats == [("/items/{id}", "GET", 202)]


class _FakeNullNative:
    """Native span backing an unsampled ``UnSampledSpan``.

    Deliberately minimal (see ``_replay_events_eagerly`` in tests/conftest.py
    for the sampled-path machinery): it only needs the finalizers
    ``UnSampledSpan.end`` calls on the native span."""

    def __init__(self):
        self.url_stats: List[tuple] = []
        self.ended = False

    def set_url_stat(self, *args):
        self.url_stats.append(args)

    def end_span(self, url_pattern="", method="", status_code=0,
                 error_verdicts=()):
        if url_pattern:
            self.set_url_stat(url_pattern, method, status_code)
        self.ended = True


def _run_unsampled(monkeypatch, native, collect_url_stat):
    monkeypatch.setattr(pinpoint.agent, "_instance",
                        UnsampledAgent(native, collect_url_stat))

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async def receive():
        return {"type": "http.request"}

    async def send(_msg):
        pass

    mw = PinpointASGIMiddleware(app)
    asyncio.run(mw(_http_scope(), receive, send))


def test_middleware_unsampled_captures_status_when_url_stat_enabled(monkeypatch):
    """Unsampled + URL-stat collection on: the send wrapper is still installed
    so the status reaches ``set_url_stat`` and the span ends via the URL-stat
    helper."""
    native = _FakeNullNative()
    _run_unsampled(monkeypatch, native, collect_url_stat=True)
    assert native.url_stats == [("/items/42", "GET", 200)]


def test_middleware_unsampled_skips_send_wrapper_without_url_stat(monkeypatch):
    """Unsampled + URL-stat collection off (the default): no send wrapper, so
    no status is captured and the span ends via the plain path."""
    native = _FakeNullNative()
    _run_unsampled(monkeypatch, native, collect_url_stat=False)
    assert native.url_stats == []
    assert native.ended is True


def test_middleware_records_exception_and_reraises(fake_agent):
    async def app(scope, receive, send):
        raise RuntimeError("kaput")

    async def receive():
        return {}

    async def send(_msg):
        pass

    mw = PinpointASGIMiddleware(app)
    with pytest.raises(RuntimeError, match="kaput"):
        asyncio.run(mw(_http_scope(), receive, send))

    assert any(e[0] == "span_error" for e in fake_agent.events)
    assert ("span_end", "ASGI HTTP Server") in fake_agent.events
    # No http.response.start reached the middleware, but the server answers
    # 500 — so that is what the span and the URL stat report.
    assert fake_agent.last_native.status_code == 500
    assert fake_agent.last_native.url_stats == [("/items/42", "GET", 500)]


def test_middleware_passes_through_lifespan_scope(fake_agent):
    """Lifespan scopes shouldn't open a span."""
    seen = {}

    async def app(scope, receive, send):
        seen["scope_type"] = scope["type"]

    mw = PinpointASGIMiddleware(app)
    asyncio.run(mw({"type": "lifespan"}, lambda: None, lambda m: None))
    assert seen["scope_type"] == "lifespan"
    assert fake_agent.last_native is None  # no span opened


def test_middleware_passes_through_when_agent_disabled(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))

    seen = {}

    async def app(scope, receive, send):
        seen["called"] = True
        await send({"type": "http.response.start", "status": 200, "headers": []})

    async def receive():
        return {}

    async def send(_msg):
        pass

    mw = PinpointASGIMiddleware(app)
    asyncio.run(mw(_http_scope(), receive, send))
    assert seen["called"] is True
