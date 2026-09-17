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

"""aiohttp server + client instrumentation.

Drives the wrappers directly against fakes — no aiohttp dependency. The
real integration paths are exercised by ``_handle_request_wrapper`` which
opens a span, runs an async callable, and ends the span; by
``_resolve_wrapper`` which lifts the matched resource pattern onto the
request mapping; and by ``_request_wrapper`` (the outbound
``ClientSession._request`` hook) which opens an HTTP-client span event,
injects trace headers into a per-request header copy, and ends the event.
"""

from __future__ import annotations

import asyncio
import pytest

from _fakes import (FakeAgent as _FakeAgent,
                    FakeNativeSpan as _FakeNativeSpan, SampledClientSpan,
                    UnsampledClientSpan, pinpoint_keys)

import pinpoint
from pinpoint.context import reset_current_span, set_current_span
from pinpoint.instrumentations import aiohttp as aiohttp_instr
from pinpoint.instrumentations._util import suppress_http_client_instrumentation
from pinpoint.propagator import HEADER_SAMPLED, HEADER_SPAN_ID, HEADER_TRACE_ID
from pinpoint.tracer import Span


# ---------------------------------------------------------------------------
# Fakes (native layer comes from the shared _fakes module; the `fake_agent`
# fixture comes from tests/conftest.py)
# ---------------------------------------------------------------------------

class _FakeRequest(dict):
    """aiohttp.web.Request is dict-like (supports __setitem__/get for app
    state). We mimic just the surface our wrapper touches."""
    def __init__(self, method="GET", path="/items/42", host="example.test",
                 remote="10.0.0.1", url="http://example.test/items/42",
                 headers=None):
        super().__init__()
        self.method = method
        self.path = path
        self.host = host
        self.remote = remote
        self.url = url
        self.headers = headers or {"X-Trace": "abc"}


class _FakeResponse:
    def __init__(self, status=200):
        self.status = status


class _FakeHTTPException(Exception):  # mimics aiohttp.web.HTTPException
    status_code = -1


class _FakeHTTPNotFound(_FakeHTTPException):
    status_code = 404


class _FakeHTTPServerError(_FakeHTTPException):
    status_code = 503


# Match aiohttp's real base-class name and module so the name+module MRO
# classification in http_exception_status fires.
_FakeHTTPException.__name__ = "HTTPException"
_FakeHTTPException.__module__ = "aiohttp.web_exceptions"


class _FakeRoute:
    def __init__(self, handler):
        self._handler = handler


class _FakeSystemRoute(_FakeRoute):  # mimics aiohttp's throwaway 404/405 route
    pass


_FakeSystemRoute.__name__ = "SystemRoute"


class _FakeMatchInfo:
    def __init__(self, route):
        self.route = route


# ---------------------------------------------------------------------------
# _handle_request_wrapper
# ---------------------------------------------------------------------------

def test_handle_request_wrapper_opens_root_span_and_records_status(fake_agent):
    request = _FakeRequest()

    async def wrapped(req, start_time, handler):
        # Real aiohttp returns (response, reset). Mimic that shape.
        return _FakeResponse(200), False

    out = asyncio.run(aiohttp_instr._handle_request_wrapper(
        wrapped, instance=None, args=(request, 0.0, None), kwargs={},
    ))
    assert isinstance(out, tuple)
    assert ("span_start", "aiohttp HTTP Server", "/items/42") in fake_agent.events
    assert ("span_end", "aiohttp HTTP Server") in fake_agent.events
    assert fake_agent.last_native.status_code == 200
    assert fake_agent.last_native.url_stats == [("/items/42", "GET", 200)]


def test_handle_request_wrapper_records_exception(fake_agent):
    request = _FakeRequest()

    async def boom(*_a, **_kw):
        raise RuntimeError("kaput")

    with pytest.raises(RuntimeError, match="kaput"):
        asyncio.run(aiohttp_instr._handle_request_wrapper(
            boom, instance=None, args=(request, 0.0, None), kwargs={},
        ))
    assert any(e[0] == "span_error" for e in fake_agent.events)
    assert ("span_end", "aiohttp HTTP Server") in fake_agent.events


def test_handle_request_wrapper_uses_url_pattern_when_resolver_set_one(fake_agent):
    request = _FakeRequest()
    request["pinpoint.url_pattern"] = "/items/{id}"

    async def wrapped(*_a, **_kw):
        return _FakeResponse(204), False

    asyncio.run(aiohttp_instr._handle_request_wrapper(
        wrapped, instance=None, args=(request, 0.0, None), kwargs={},
    ))
    assert fake_agent.last_native.url_stats == [("/items/{id}", "GET", 204)]


def test_handle_request_wrapper_bypasses_websocket_upgrade(fake_agent):
    """A websocket upgrade turns the handler into a long-lived receive loop;
    _handle_request only returns on disconnect. Opening a root span would hold
    one unbounded span open for the connection, so tracing is bypassed."""
    import pinpoint.context as ppctx

    request = _FakeRequest(headers={"Upgrade": "websocket",
                                    "Connection": "Upgrade"})

    async def receive_loop(*_a, **_kw):
        # Long-lived message loop: no root span means no accumulation.
        for _ in range(1000):
            assert ppctx.current_span() is None
        return _FakeResponse(101), False

    out = asyncio.run(aiohttp_instr._handle_request_wrapper(
        receive_loop, instance=None, args=(request, 0.0, None), kwargs={},
    ))
    assert isinstance(out, tuple)
    assert fake_agent.events == []
    assert fake_agent.last_native is None


def test_handle_request_wrapper_traces_request_with_stray_upgrade_header(fake_agent):
    """A plain HTTP request that merely carries ``Upgrade: websocket`` but no
    ``Connection: upgrade`` (a client quirk or a misbehaving proxy) is NOT a
    real handshake — aiohttp would reject the upgrade with 400 and the handler
    returns promptly. It must keep its root span; otherwise a single stray
    request header would silently drop the request (and all its child calls,
    since ``current_span()`` would be ``None``) from APM."""
    import pinpoint.context as ppctx

    request = _FakeRequest(headers={"Upgrade": "websocket"})

    async def wrapped(req, start_time, handler):
        # A real root span must be active for the whole of a normal handler.
        assert ppctx.current_span() is not None
        return _FakeResponse(200), False

    out = asyncio.run(aiohttp_instr._handle_request_wrapper(
        wrapped, instance=None, args=(request, 0.0, None), kwargs={},
    ))
    assert isinstance(out, tuple)
    assert ("span_start", "aiohttp HTTP Server", "/items/42") in fake_agent.events
    assert ("span_end", "aiohttp HTTP Server") in fake_agent.events
    assert fake_agent.last_native is not None
    assert fake_agent.last_native.status_code == 200


@pytest.mark.parametrize(
    "headers, expected",
    [
        # Genuine handshake: both headers present -> bypass.
        ({"Upgrade": "websocket", "Connection": "Upgrade"}, True),
        # Connection as a token list (as browsers send it) -> bypass.
        ({"Upgrade": "websocket", "Connection": "keep-alive, Upgrade"}, True),
        # Case-insensitive / whitespace, mirroring aiohttp's own gate -> bypass.
        ({"Upgrade": " WebSocket ", "Connection": "upgrade"}, True),
        # Stray Upgrade header with no Connection upgrade -> NOT a handshake.
        ({"Upgrade": "websocket"}, False),
        ({"Upgrade": "websocket", "Connection": "keep-alive"}, False),
        # Non-websocket upgrade (e.g. h2c) -> NOT a websocket handshake.
        ({"Upgrade": "h2c", "Connection": "Upgrade"}, False),
        # Plain request.
        ({"X-Trace": "abc"}, False),
    ],
)
def test_is_websocket_request_requires_upgrade_and_connection(headers, expected):
    """Detection must match aiohttp's ``_handshake`` gate: both ``Upgrade:
    websocket`` and a ``Connection`` header carrying the ``upgrade`` token."""
    request = _FakeRequest(headers=headers)
    assert aiohttp_instr._is_websocket_request(request) is expected


def test_handle_request_wrapper_passes_through_on_true_reentrancy_marker(fake_agent):
    """Defense against a stacked wrapper left by a re-instrument cycle: the
    outer wrapper stamps a per-request marker on the request and opens the root
    span, so the inner (duplicate) wrapper re-entering ``_handle_request`` for
    the same request must see that marker and pass through rather than open a
    second root span."""
    import pinpoint.context as ppctx

    request = _FakeRequest()
    # Reproduce the state an outer wrapper leaves for THIS request: our
    # per-request marker on the request plus the root span current in context.
    request[aiohttp_instr._ROOT_SPAN_ACTIVE_KEY] = True
    rec = _FakeAgent()
    sp = Span(_FakeNativeSpan("root", "/", rec))
    token = ppctx.set_current_span(sp)
    try:
        called = {"flag": False}

        async def wrapped(*_a, **_kw):
            called["flag"] = True
            return _FakeResponse(200), False

        out = asyncio.run(aiohttp_instr._handle_request_wrapper(
            wrapped, instance=None, args=(request, 0.0, None), kwargs={},
        ))
        assert called["flag"] is True
        assert isinstance(out, tuple)
        # The inner (duplicate) wrapper opened no second root span.
        assert fake_agent.events == []
        assert fake_agent.last_native is None
    finally:
        ppctx.reset_current_span(token)


def test_handle_request_wrapper_nested_reentry_opens_single_root_span(fake_agent):
    """The headline reentrancy fix drives the *stamp* side: the outer
    wrapper must mark the per-request marker before dispatch so a stacked inner
    wrapper re-entering ``_handle_request`` for the same request passes through.
    A genuine nested re-entry must yield exactly one root span — dropping the
    ``_mark_root_span_active`` call would open a second here."""
    request = _FakeRequest()

    async def inner(*_a, **_kw):
        # Re-entry with no root span of its own — the outer's marker shields it.
        return _FakeResponse(200), False

    async def outer(*_a, **_kw):
        return await aiohttp_instr._handle_request_wrapper(
            inner, instance=None, args=(request, 0.0, None), kwargs={},
        )

    asyncio.run(aiohttp_instr._handle_request_wrapper(
        outer, instance=None, args=(request, 0.0, None), kwargs={},
    ))

    starts = [e for e in fake_agent.events if e[0] == "span_start"]
    ends = [e for e in fake_agent.events if e[0] == "span_end"]
    assert len(starts) == 1
    assert len(ends) == 1
    # The marker was actually stamped on the request during the outer dispatch.
    assert request.get(aiohttp_instr._ROOT_SPAN_ACTIVE_KEY) is True


def test_handle_request_wrapper_opens_root_span_despite_ambient_leaked_span(fake_agent):
    """An ambient/leaked span inherited into the request task's context must
    NOT suppress tracing.

    asyncio copies the current contextvars context into every request task, so a
    span left current when the server was set up inside a traced context, or by
    an instrumentation that leaked ``set_current_span`` without a reset, makes
    ``current_span()`` non-None for every request. Keying the nesting guard on
    ``current_span()`` would therefore pass *all* of them through untraced. A
    fresh request (no per-request marker) must still open its own root span, and
    the ambient span must be left intact afterwards."""
    import pinpoint.context as ppctx

    request = _FakeRequest()

    # Simulate the leak: an unrelated span is current in the ambient context,
    # but our wrapper never opened it for THIS request (no marker stamped).
    leak_rec = _FakeAgent()
    ambient = Span(_FakeNativeSpan("ambient-leak", "/", leak_rec))
    token = ppctx.set_current_span(ambient)
    try:
        async def wrapped(*_a, **_kw):
            # Our own fresh root span — not the ambient one — must be current.
            assert ppctx.current_span() is not None
            assert ppctx.current_span() is not ambient
            return _FakeResponse(200), False

        out = asyncio.run(aiohttp_instr._handle_request_wrapper(
            wrapped, instance=None, args=(request, 0.0, None), kwargs={},
        ))
        assert isinstance(out, tuple)
        # A brand-new root span was opened for this request ...
        assert ("span_start", "aiohttp HTTP Server", "/items/42") in fake_agent.events
        assert ("span_end", "aiohttp HTTP Server") in fake_agent.events
        assert fake_agent.last_native is not None
        assert fake_agent.last_native.status_code == 200
        # ... without disturbing the ambient span (its recorder is untouched).
        assert leak_rec.events == [("span_start", "ambient-leak", "/")]
        # The ambient span is restored once our request completes.
        assert ppctx.current_span() is ambient
    finally:
        ppctx.reset_current_span(token)


def test_handle_request_wrapper_passes_through_when_disabled(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))
    request = _FakeRequest()

    async def wrapped(*_a, **_kw):
        return _FakeResponse(200), False

    asyncio.run(aiohttp_instr._handle_request_wrapper(
        wrapped, instance=None, args=(request, 0.0, None), kwargs={},
    ))


# ---------------------------------------------------------------------------
# _resolve_wrapper
# ---------------------------------------------------------------------------

def test_resolve_wrapper_lifts_canonical_pattern_onto_request():
    """The matched Resource's ``canonical`` should land on the request as
    ``pinpoint.url_pattern``. resolve() runs inside the root-span scope in
    real dispatch, so a span must be current for the stash to happen."""
    import pinpoint.context as ppctx

    request = _FakeRequest(path="/items/42")

    class _FakeResource:
        canonical = "/items/{id}"

    class _FakeRoute:
        resource = _FakeResource()

    class _FakeMatchInfo:
        route = _FakeRoute()

    async def wrapped(req):
        return _FakeMatchInfo()

    span = Span(_FakeNativeSpan("root", "/items/42", _FakeAgent()))
    token = ppctx.set_current_span(span)
    try:
        out = asyncio.run(aiohttp_instr._resolve_wrapper(
            wrapped, instance=None, args=(request,), kwargs={},
        ))
    finally:
        ppctx.reset_current_span(token)
    assert isinstance(out, _FakeMatchInfo)
    assert request.get("pinpoint.url_pattern") == "/items/{id}"


def test_resolve_wrapper_skips_work_without_current_span():
    """No root span (agent down/disabled) -> neither the stash nor the
    handler wrap would be consumed, so the wrapper skips both."""
    request = _FakeRequest(path="/items/42")

    class _FakeResource:
        canonical = "/items/{id}"

    class _FakeRoute:
        resource = _FakeResource()

    class _FakeMatchInfo:
        route = _FakeRoute()

    async def wrapped(req):
        return _FakeMatchInfo()

    out = asyncio.run(aiohttp_instr._resolve_wrapper(
        wrapped, instance=None, args=(request,), kwargs={},
    ))
    assert isinstance(out, _FakeMatchInfo)
    assert "pinpoint.url_pattern" not in request


def test_resolve_wrapper_handles_no_route_match():
    """When resolve() returns a 404-style match info with no resource, the
    wrapper must not blow up."""
    request = _FakeRequest()

    class _NoRouteMatchInfo:
        route = None

    async def wrapped(req):
        return _NoRouteMatchInfo()

    asyncio.run(aiohttp_instr._resolve_wrapper(
        wrapped, instance=None, args=(request,), kwargs={},
    ))
    assert "pinpoint.url_pattern" not in request


# ---------------------------------------------------------------------------
# _instrument_matched_handler — control-flow / SystemRoute handling
# ---------------------------------------------------------------------------

async def _plain_handler(*_a, **_kw):
    return _FakeResponse(200)


def test_instrument_matched_handler_skips_system_route():
    """A ``MatchInfoError``'s throwaway ``SystemRoute`` (used for every 404/405)
    must not be wrapped — otherwise each unmatched request re-wraps a fresh
    handler and records the control-flow ``HTTPNotFound`` as a span event."""
    route = _FakeSystemRoute(_plain_handler)
    aiohttp_instr._instrument_matched_handler(_FakeMatchInfo(route))
    # Handler left untouched: no traced wrapper installed.
    assert route._handler is _plain_handler
    assert not getattr(route._handler, "_pinpoint_traced", False)


def test_instrument_matched_handler_wraps_normal_route():
    route = _FakeRoute(_plain_handler)
    aiohttp_instr._instrument_matched_handler(_FakeMatchInfo(route))
    assert route._handler is not _plain_handler
    assert getattr(route._handler, "_pinpoint_traced", False) is True


def test_instrument_matched_handler_is_idempotent():
    """``resolve()`` runs on every request, so ``_instrument_matched_handler`` is
    invoked repeatedly for the same route. The second call must be a no-op
    (guarded by the handler's ``_pinpoint_traced`` marker): re-wrapping an
    already-traced handler would nest traced closures and emit duplicate
    ``PythonMethod`` span events per single handler invocation."""
    route = _FakeRoute(_plain_handler)
    aiohttp_instr._instrument_matched_handler(_FakeMatchInfo(route))
    first_wrap = route._handler
    assert first_wrap is not _plain_handler

    # Second resolve() of the same route: the traced marker must short-circuit.
    aiohttp_instr._instrument_matched_handler(_FakeMatchInfo(route))
    assert route._handler is first_wrap  # not re-wrapped
    # And the underlying user handler is reachable exactly once in the chain.
    assert getattr(first_wrap, "__wrapped__", None) is _plain_handler


def test_uninstrument_restores_wrapped_route_handler():
    """Uninstrument must restore every mutated route's original handler so the
    user's route table isn't left permanently mutated after shutdown."""
    route = _FakeRoute(_plain_handler)
    aiohttp_instr._instrument_matched_handler(_FakeMatchInfo(route))
    assert route._handler is not _plain_handler  # confirm it was swapped

    aiohttp_instr.AiohttpServerInstrumentor()._uninstrument()

    assert route._handler is _plain_handler
    assert not getattr(route._handler, "_pinpoint_traced", False)
    # The route is dropped from the tracking set once restored.
    assert route not in aiohttp_instr._traced_routes


def test_transport_instrumentors_wrap_only_their_own_targets(monkeypatch):
    """Independent import hooks must not consume one global guard or install a
    transport that the user explicitly disabled."""
    calls = []
    monkeypatch.setattr(
        aiohttp_instr,
        "wrap",
        lambda module, target, wrapper: calls.append((module, target)),
    )

    aiohttp_instr.AiohttpServerInstrumentor()._instrument()
    assert calls == [
        ("aiohttp.web_protocol", "RequestHandler._handle_request"),
        ("aiohttp.web_urldispatcher", "UrlDispatcher.resolve"),
    ]

    calls.clear()
    aiohttp_instr.AiohttpClientInstrumentor()._instrument()
    assert calls == [
        ("aiohttp.client", "ClientSession._request"),
    ]


def test_transport_entry_points_use_independent_instrumentors(monkeypatch):
    calls = []
    monkeypatch.setattr(
        aiohttp_instr.AiohttpServerInstrumentor,
        "instrument",
        lambda self: calls.append("server"),
    )
    monkeypatch.setattr(
        aiohttp_instr.AiohttpClientInstrumentor,
        "instrument",
        lambda self: calls.append("client"),
    )

    aiohttp_instr.instrument_client()
    aiohttp_instr.instrument_server()

    assert calls == ["client", "server"]


def test_restore_route_handler_leaves_untraced_route_untouched():
    """A route we never wrapped must not be touched by restore."""
    route = _FakeRoute(_plain_handler)
    aiohttp_instr._restore_route_handler(route)
    assert route._handler is _plain_handler


def _run_traced_handler(handler, fake_agent):
    """Wrap ``handler`` via _instrument_matched_handler, then invoke the traced
    wrapper under an active root span; return (result_or_exc, agent)."""
    import pinpoint.context as ppctx

    route = _FakeRoute(handler)
    aiohttp_instr._instrument_matched_handler(_FakeMatchInfo(route))
    traced = route._handler

    span = fake_agent.new_span("root", "/x")
    token = ppctx.set_current_span(span)
    try:
        return asyncio.run(traced())
    finally:
        ppctx.reset_current_span(token)


def test_traced_handler_does_not_record_control_flow_http_exception(fake_agent):
    """A handler raising ``web.HTTPNotFound()`` (404) is normal control flow —
    the span event must be created but NOT marked as an error."""
    async def handler(*_a, **_kw):
        raise _FakeHTTPNotFound()

    with pytest.raises(_FakeHTTPNotFound):
        _run_traced_handler(handler, fake_agent)
    assert any(e[0] == "event_start" for e in fake_agent.events)
    assert not any(e[0] == "event_error" for e in fake_agent.events)


def test_traced_handler_records_5xx_http_exception(fake_agent):
    """A 5xx ``HTTPException`` is a genuine server error and must be recorded."""
    async def handler(*_a, **_kw):
        raise _FakeHTTPServerError()

    with pytest.raises(_FakeHTTPServerError):
        _run_traced_handler(handler, fake_agent)
    assert any(e[0] == "event_error" for e in fake_agent.events)


def test_traced_handler_records_generic_exception(fake_agent):
    """A non-HTTP exception is always a real failure."""
    async def handler(*_a, **_kw):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        _run_traced_handler(handler, fake_agent)
    assert any(e[0] == "event_error" for e in fake_agent.events)


# ---------------------------------------------------------------------------
# client-side: ClientSession._request wrapper
#
# Driven httpx-style against lightweight fakes (the closest analogue). The
# wrapper is a coroutine, so every case runs under ``asyncio.run``; the fake
# ``_request`` coroutine captures the outbound ``headers`` kwarg so we can
# assert trace-header injection directly against what would go on the wire.
# ``trace_http_client_{request,response}`` early-return on an event without an
# ``_ev`` slot, so these fakes stay minimal and never touch the native layer.
# ---------------------------------------------------------------------------

def _run_client_request(wrapped, args, kwargs, span, *, suppressed=False):
    """Drive ``_request_wrapper`` under ``span`` as the current span.

    ``asyncio.run`` copies the calling context into the task, so both the
    current span and (optionally) the suppression scope propagate into the
    wrapper exactly as they do in production."""
    token = set_current_span(span)
    try:
        if suppressed:
            with suppress_http_client_instrumentation():
                return asyncio.run(aiohttp_instr._request_wrapper(
                    wrapped, instance=None, args=args, kwargs=kwargs,
                ))
        return asyncio.run(aiohttp_instr._request_wrapper(
            wrapped, instance=None, args=args, kwargs=kwargs,
        ))
    finally:
        reset_current_span(token)


def test_client_request_injects_headers_and_ends_event():
    """Success path: the outbound request carries this span's trace context and
    the HTTP-client span event is opened and ended exactly once."""
    span = SampledClientSpan()
    captured: dict = {}

    async def _request(method, str_or_url, **kwargs):
        captured["method"] = method
        captured["url"] = str_or_url
        captured["headers"] = kwargs.get("headers")
        return _FakeResponse(200)

    resp = _run_client_request(
        _request, ("GET", "http://svc.test/items/1"), {}, span,
    )

    assert isinstance(resp, _FakeResponse)
    assert captured["headers"][HEADER_TRACE_ID] == "app^1700000000000^1"
    assert captured["headers"][HEADER_SPAN_ID] == "42"
    assert len(span.events) == 1
    assert span.events[0].ended is True
    assert span.events[0].errors == []


def test_client_request_copies_user_headers_without_mutating():
    """A caller-supplied headers dict is copied, not mutated in place: the
    outgoing copy carries both the user's header and the trace context, while
    the original dict stays free of any Pinpoint-* keys."""
    span = SampledClientSpan()
    user_headers = {"X-Custom": "keep"}
    captured: dict = {}

    async def _request(method, str_or_url, **kwargs):
        captured["headers"] = kwargs.get("headers")
        return _FakeResponse(200)

    _run_client_request(
        _request, ("GET", "http://svc.test/x"),
        {"headers": user_headers}, span,
    )

    assert captured["headers"]["X-Custom"] == "keep"
    assert captured["headers"][HEADER_TRACE_ID] == "app^1700000000000^1"
    # Injection happened on a copy — the user's dict is untouched.
    assert captured["headers"] is not user_headers
    assert user_headers == {"X-Custom": "keep"}
    assert pinpoint_keys(user_headers) == set()


def test_client_request_preserves_duplicate_pair_headers():
    """aiohttp LooseHeaders accepts a pair sequence where repeated names are
    meaningful; trace injection must not collapse those values through dict()."""
    span = SampledClientSpan()
    user_headers = [
        ("Warning", "199 first"),
        ("Warning", "299 second"),
    ]
    captured: dict = {}

    async def _request(method, str_or_url, **kwargs):
        captured["headers"] = kwargs.get("headers")
        return _FakeResponse(200)

    _run_client_request(
        _request,
        ("GET", "http://svc.test/x"),
        {"headers": user_headers},
        span,
    )

    outgoing = captured["headers"]
    assert [value for key, value in outgoing if key == "Warning"] == [
        "199 first",
        "299 second",
    ]
    assert dict(outgoing)[HEADER_TRACE_ID] == "app^1700000000000^1"
    assert outgoing is not user_headers
    assert user_headers == [
        ("Warning", "199 first"),
        ("Warning", "299 second"),
    ]


def test_client_request_records_error_and_reraises():
    """A network error must be recorded on the event, the event ended, and the
    exception re-raised unchanged into the user's await."""
    span = SampledClientSpan()

    async def _request(method, str_or_url, **kwargs):
        raise ConnectionError("connection refused")

    with pytest.raises(ConnectionError, match="connection refused"):
        _run_client_request(_request, ("GET", "http://svc.test/x"), {}, span)

    assert len(span.events) == 1
    assert span.events[0].errors  # error recorded via span_event_scope
    assert span.events[0].ended is True  # event still ended


def test_client_request_suppressed_short_circuits():
    """Under a higher-level client wrapper (suppression active) the wrapper is a
    pass-through: no event is opened and no trace headers are injected."""
    span = SampledClientSpan()
    captured: dict = {}

    async def _request(method, str_or_url, **kwargs):
        captured["headers"] = kwargs.get("headers")
        return _FakeResponse(200)

    resp = _run_client_request(
        _request, ("GET", "http://svc.test/x"), {}, span, suppressed=True,
    )

    assert isinstance(resp, _FakeResponse)
    assert span.events == []
    assert captured["headers"] is None


def test_client_request_unsampled_still_injects_headers():
    """Unsampled: headers are still injected (the ``s0`` marker rides the
    request so the downstream server short-circuits its sampling), but the
    sampled-only annotation / event scope is skipped."""
    span = UnsampledClientSpan()
    captured: dict = {}

    async def _request(method, str_or_url, **kwargs):
        captured["headers"] = kwargs.get("headers")
        return _FakeResponse(200)

    resp = _run_client_request(_request, ("GET", "http://svc.test/x"), {}, span)

    assert isinstance(resp, _FakeResponse)
    assert captured["headers"][HEADER_SAMPLED] == "s0"
    # Event opened (so injected context has the right depth/sequence) but the
    # unsampled fast path does not run it through span_event_scope.
    assert len(span.events) == 1
    assert span.events[0].ended is False


def test_client_request_setup_failure_still_sends_no_leak(monkeypatch):
    """An agent-side error in the pre-await setup must never escape into the
    user's await: the request still goes out exactly once and the event opened
    before the failure is ended, not leaked."""
    span = SampledClientSpan()
    calls = {"n": 0}

    def _boom(_span):
        raise RuntimeError("native sampled check failed")

    # Fail *after* the event is opened + headers injected, exercising the
    # except-clause event.end() cleanup and the untraced fallback send.
    monkeypatch.setattr(aiohttp_instr, "span_is_sampled", _boom)

    async def _request(method, str_or_url, **kwargs):
        calls["n"] += 1
        return _FakeResponse(200)

    resp = _run_client_request(_request, ("GET", "http://svc.test/x"), {}, span)

    assert isinstance(resp, _FakeResponse)
    assert calls["n"] == 1
    assert len(span.events) == 1
    assert span.events[0].ended is True
