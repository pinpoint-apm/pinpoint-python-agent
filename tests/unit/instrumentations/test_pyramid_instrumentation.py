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

"""Pyramid instrumentation.

Drives the wrappers directly: ``_router_call_wrapper`` (root WSGI span) and
``_call_view_wrapper`` (per-view span event) against fakes — no Pyramid
dependency. Validates the route template lift, view-name extraction, and
exception propagation.
"""

from __future__ import annotations

import pytest
from _fakes import (FakeAgent as _FakeAgent,
                    FakeNativeSpan as _FakeNativeSpan, wsgi_environ)

from pinpoint import context as ppctx
from pinpoint.instrumentations import pyramid as pyramid_instr
from pinpoint.instrumentations import wsgi as wsgi_instr
from pinpoint.tracer import Span


def _environ(**overrides):
    return wsgi_environ(method="POST", **overrides)


# ---------------------------------------------------------------------------
# _router_call_wrapper
# ---------------------------------------------------------------------------

def test_router_call_wrapper_opens_span_and_records_status(fake_agent):
    captured = {}

    def app(environ, start_response):
        captured["environ"] = environ
        start_response("201 Created", [])
        return [b"ok"]

    # Drain the returned body so the span finalizes (it now ends when the
    # response iterable is consumed, not when __call__ returns).
    out = list(pyramid_instr._router_call_wrapper(
        app, instance=None,
        args=(_environ(), lambda *a, **k: None),
        kwargs={},
    ))
    assert out == [b"ok"]
    assert ("span_start", "Pyramid HTTP Server", "/items/42") in fake_agent.events
    assert ("span_end", "Pyramid HTTP Server") in fake_agent.events
    assert fake_agent.last_native.status_code == 201


def test_router_call_wrapper_records_exception(fake_agent):
    def boom(environ, start_response):
        raise RuntimeError("kaput")

    with pytest.raises(RuntimeError, match="kaput"):
        pyramid_instr._router_call_wrapper(
            boom, instance=None,
            args=(_environ(), lambda *a, **k: None),
            kwargs={},
        )
    assert any(e[0] == "span_error" for e in fake_agent.events)
    assert ("span_end", "Pyramid HTTP Server") in fake_agent.events


def test_router_call_wrapper_streaming_body_ends_response_child(fake_agent):
    """A streamed body runs under a linked response child, not the root."""
    seen = []

    def app(environ, start_response):
        start_response("200 OK", [])

        def gen():
            seen.append(ppctx.current_span())
            yield b"a"
            yield b"b"

        return gen()

    body = pyramid_instr._router_call_wrapper(
        app, instance=None,
        args=(_environ(), lambda *a, **k: None), kwargs={},
    )
    root = fake_agent.last_native
    child = fake_agent.last_async_native
    assert root.end_called is True
    assert child.end_called is False
    assert ppctx.current_span() is None

    assert list(body) == [b"a", b"b"]
    assert seen and seen[0] is not None and seen[0].sampled
    assert child.end_called is True


def test_router_call_wrapper_ends_span_when_setup_fails_after_creation(fake_agent, monkeypatch):
    """Regression (native span leak): if span setup raises *after* new_span
    created the native span (e.g. set_current_span), the request must still run
    untraced AND the already-created native span must be ended — not leaked."""
    def _boom(_span):
        raise RuntimeError("setup boom")

    monkeypatch.setattr(wsgi_instr, "set_current_span", _boom)

    calls = []

    def app(environ, start_response):
        calls.append(1)
        start_response("200 OK", [])
        return [b"ok"]

    body = list(pyramid_instr._router_call_wrapper(
        app, instance=None,
        args=(_environ(), lambda *a, **k: None), kwargs={},
    ))

    # Request still completes, untraced, exactly once.
    assert body == [b"ok"]
    assert calls == [1]
    # The created native span was ended — no leak.
    assert fake_agent.last_native.end_called is True
    assert ("span_end", "Pyramid HTTP Server") in fake_agent.events


def test_router_call_wrapper_uses_url_pattern_set_by_view_layer(fake_agent):
    def app(environ, start_response):
        environ["pinpoint.url_pattern"] = "/items/{id}"
        start_response("200 OK", [])
        return [b""]

    # Drain the returned body so the span finalizes and records url_stat.
    list(pyramid_instr._router_call_wrapper(
        app, instance=None,
        args=(_environ(), lambda *a, **k: None),
        kwargs={},
    ))
    assert fake_agent.last_native.url_stats == [("/items/{id}", "POST", 200)]


# ---------------------------------------------------------------------------
# _call_view_wrapper
# ---------------------------------------------------------------------------

class _FakeRoute:
    name = "items.detail"
    pattern = "/items/{id}"


class _FakeRequest:
    def __init__(self, view=None, environ=None):
        self.matched_route = _FakeRoute()
        self.environ = environ if environ is not None else {}
        if view is not None:
            self._view_callable_ = view  # exposed by Pyramid 2.x


def test_call_view_wrapper_emits_event_and_lifts_url_pattern():
    rec = _FakeAgent()
    sp = Span(_FakeNativeSpan("root", "/", rec))
    token = ppctx.set_current_span(sp)
    try:
        def real_view(request):
            return None

        request = _FakeRequest(view=real_view)

        def wrapped(*args, **kwargs):
            return "rendered"

        out = pyramid_instr._call_view_wrapper(
            wrapped, instance=None,
            args=(request,), kwargs={},
        )
        assert out == "rendered"
        # Inner functions get a `<locals>.` prefix in __qualname__, so match on suffix.
        assert any(
            kind == "event_start" and op == "root" and name.endswith(".real_view")
            for kind, op, name in rec.events
        )
        assert any(
            kind == "event_end" and op == "root" and name.endswith(".real_view")
            for kind, op, name in rec.events
        )
        assert request.environ.get("pinpoint.url_pattern") == "/items/{id}"
    finally:
        ppctx.reset_current_span(token)


def test_call_view_wrapper_falls_back_to_route_name_when_view_unset():
    """No view callable on the request → use the route's logical name."""
    rec = _FakeAgent()
    sp = Span(_FakeNativeSpan("root", "/", rec))
    token = ppctx.set_current_span(sp)
    try:
        request = _FakeRequest()

        def wrapped(*args, **kwargs): return None

        pyramid_instr._call_view_wrapper(
            wrapped, instance=None, args=(request,), kwargs={},
        )
        assert ("event_start", "root", "items.detail") in rec.events
    finally:
        ppctx.reset_current_span(token)


def test_call_view_wrapper_no_active_span_passes_through():
    """No current span → just delegate, no event."""
    request = _FakeRequest(view=lambda req: None)
    captured = {}

    def wrapped(*args, **kwargs):
        captured["called"] = True
        return None

    pyramid_instr._call_view_wrapper(
        wrapped, instance=None, args=(request,), kwargs={},
    )
    assert captured["called"] is True


# ---------------------------------------------------------------------------
# Binding coverage — Router.handle_request resolves ``_call_view`` against
# ``pyramid.router``'s module globals, NOT ``pyramid.view``. Wrapping only
# the latter (the historical hook) leaves the routing path uninstrumented,
# so verify both bindings end up as wrapt proxies.
# ---------------------------------------------------------------------------

def test_instrument_wraps_call_view_on_both_pyramid_router_and_view():
    pytest.importorskip("pyramid.router")
    pytest.importorskip("pyramid.view")
    import pyramid.router
    import pyramid.view

    # A post-import hook may already have wrapped ``_call_view`` earlier in this
    # process. Install is now idempotent (it won't stack a second wrapper), so
    # strip any existing pinpoint layer to a clean baseline before asserting
    # that ``_instrument`` wraps exactly once.
    def _unwrap(fn):
        while getattr(
            getattr(fn, "_self_wrapper", None), "__pinpoint_wrapper__", False,
        ) is True:
            fn = fn.__wrapped__
        return fn

    pyramid.router._call_view = _unwrap(pyramid.router._call_view)
    pyramid.view._call_view = _unwrap(pyramid.view._call_view)

    router_orig = pyramid.router._call_view
    view_orig = pyramid.view._call_view

    instr = pyramid_instr.PyramidInstrumentor()
    instr._instrument()
    try:
        # wrapt's FunctionWrapper exposes ``__wrapped__`` — both bindings
        # should now be proxies, otherwise Router.handle_request would
        # bypass instrumentation entirely.
        assert hasattr(pyramid.router._call_view, "__wrapped__")
        assert hasattr(pyramid.view._call_view, "__wrapped__")
        assert pyramid.router._call_view.__wrapped__ is router_orig
        assert pyramid.view._call_view.__wrapped__ is view_orig
    finally:
        pyramid.router._call_view = router_orig
        pyramid.view._call_view = view_orig


# ---------------------------------------------------------------------------
# _call_view_wrapper — view span EVENT control-flow filtering
# (mirrors aiohttp's test_traced_handler_does_not_record_control_flow_*).
# ``raise pyramid.httpexceptions.HTTPNotFound()`` (and friends) doubles as a WSGI
# response, so a sub-500 one is control flow — the view span event must NOT be
# flagged as an error.
# ---------------------------------------------------------------------------

class _FakeHTTPException(Exception):
    """Mimics ``pyramid.httpexceptions.HTTPException`` — the numeric status
    lives on ``.code``."""
    code = -1


class _FakeHTTPNotFound(_FakeHTTPException):
    code = 404


class _FakeHTTPServerError(_FakeHTTPException):
    code = 500


# Match pyramid's real class identity — name *and* defining module — so the
# module-gated classifier fires for these fakes exactly as for the genuine type.
_FakeHTTPException.__name__ = "HTTPException"
_FakeHTTPException.__module__ = "pyramid.httpexceptions"


def _run_call_view_raising(exc):
    """Drive ``_call_view_wrapper`` under a real root span with a view that
    raises ``exc``; return the recorder for event assertions."""
    rec = _FakeAgent()
    sp = Span(_FakeNativeSpan("root", "/", rec))
    token = ppctx.set_current_span(sp)
    try:
        def real_view(request):
            return None

        request = _FakeRequest(view=real_view)

        def wrapped(*args, **kwargs):
            raise exc

        with pytest.raises(type(exc)):
            pyramid_instr._call_view_wrapper(
                wrapped, instance=None, args=(request,), kwargs={},
            )
        return rec
    finally:
        ppctx.reset_current_span(token)


def test_call_view_wrapper_does_not_record_control_flow_http_exception():
    """A view raising ``HTTPNotFound()`` is normal control flow — the view span
    event is created but NOT marked as an error."""
    rec = _run_call_view_raising(_FakeHTTPNotFound())
    assert any(e[0] == "event_start" for e in rec.events)
    assert not any(e[0] == "event_error" for e in rec.events)


def test_call_view_wrapper_records_5xx_http_exception():
    """A 5xx ``HTTPException`` is a genuine server error and must be recorded."""
    rec = _run_call_view_raising(_FakeHTTPServerError())
    assert any(e[0] == "event_error" for e in rec.events)


def test_call_view_wrapper_records_generic_exception():
    """A non-HTTP exception is always a real failure."""
    class Boom(Exception):
        pass

    rec = _run_call_view_raising(Boom("kaput"))
    assert any(e[0] == "event_error" for e in rec.events)


def test_is_control_flow_exception_classifies_pyramid_by_status():
    assert pyramid_instr._is_control_flow_exception(_FakeHTTPNotFound()) is True
    assert pyramid_instr._is_control_flow_exception(_FakeHTTPServerError()) is False
    assert pyramid_instr._is_control_flow_exception(RuntimeError()) is False

    class HTTPException(Exception):  # not pyramid's — must not match
        code = 404

    assert pyramid_instr._is_control_flow_exception(HTTPException()) is False
