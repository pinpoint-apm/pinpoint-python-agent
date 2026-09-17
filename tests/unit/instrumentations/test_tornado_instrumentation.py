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

"""Tornado instrumentation.

Drives ``_execute_wrapper`` and ``_log_exception_wrapper`` against an
in-memory fake handler/request — no Tornado dependency. Validates root-
span lifecycle, status code capture, handler-method span event, and the
log_exception → set_error path.
"""

from __future__ import annotations

import asyncio

import pytest
from _fakes import FakeAgent as _FakeAgent, FakeNativeSpan as _FakeNativeSpan

import pinpoint
from pinpoint import context as ppctx
from pinpoint.instrumentations import tornado as tornado_instr
from pinpoint.tracer import Span


class _FakeRequest:
    def __init__(self, method="GET", path="/users/42", host="example.test",
                 uri="/users/42", remote_ip="10.0.0.1",
                 headers=None):
        self.method = method
        self.path = path
        self.host = host
        self.uri = uri
        self.remote_ip = remote_ip
        self.headers = headers or {"X-Trace": "abc"}

    def full_url(self):
        return f"http://{self.host}{self.uri}"


class _FakeHandler:
    def __init__(self, request, status=200):
        self.request = request
        self._status = status

    def get_status(self):
        return self._status


class _FakeHTTPError(Exception):  # mimics tornado.web.HTTPError
    def __init__(self, status_code):
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code


class _FakeFinish(Exception):  # mimics tornado.web.Finish (control-flow signal)
    pass


# Mimic tornado's real class identity — name *and* defining module — so the
# control-flow classifier (which requires ``__module__ == "tornado.web"``) fires
# for these fakes exactly as it would for the genuine exceptions.
_FakeHTTPError.__name__ = "HTTPError"
_FakeHTTPError.__module__ = "tornado.web"
_FakeFinish.__name__ = "Finish"
_FakeFinish.__module__ = "tornado.web"


class UserHandler(_FakeHandler):  # used to test handler-method op name
    pass


class WebSocketHandler(_FakeHandler):  # mimics tornado.websocket.WebSocketHandler
    pass


class _ChatSocket(WebSocketHandler):  # a user subclass of WebSocketHandler
    pass


# ---------------------------------------------------------------------------
# _execute_wrapper
# ---------------------------------------------------------------------------

def test_execute_wrapper_opens_root_span_with_handler_event(fake_agent):
    request = _FakeRequest()
    handler = UserHandler(request, status=200)

    async def wrapped(*_a, **_kw):
        return None

    asyncio.run(tornado_instr._execute_wrapper(
        wrapped, instance=handler, args=(), kwargs={},
    ))
    assert ("span_start", "Tornado HTTP Server", "/users/42") in fake_agent.events
    assert ("event_start", "Tornado HTTP Server", "UserHandler.get") in fake_agent.events
    assert ("event_end", "Tornado HTTP Server", "UserHandler.get") in fake_agent.events
    assert ("span_end", "Tornado HTTP Server") in fake_agent.events
    assert fake_agent.last_native.status_code == 200
    assert fake_agent.last_native.url_stats == [("/users/42", "GET", 200)]


def test_execute_wrapper_records_handler_exception(fake_agent):
    request = _FakeRequest()
    handler = UserHandler(request, status=500)

    async def boom(*_a, **_kw):
        raise RuntimeError("kaput")

    with pytest.raises(RuntimeError, match="kaput"):
        asyncio.run(tornado_instr._execute_wrapper(
            boom, instance=handler, args=(), kwargs={},
        ))
    assert any(e[0] == "span_error" for e in fake_agent.events)
    assert any(e[0] == "event_error" for e in fake_agent.events)
    assert ("span_end", "Tornado HTTP Server") in fake_agent.events


def test_execute_wrapper_bypasses_websocket_handler(fake_agent):
    """A long-lived WebSocketHandler must not get a root span: _execute stays
    pending for the connection lifetime, so any events made from on_message
    would accumulate on one unbounded span. Bypass tracing entirely."""
    request = _FakeRequest(headers={"Upgrade": "websocket"})
    handler = _ChatSocket(request, status=101)

    # Simulate a long connection issuing many instrumented calls: no root span
    # exists, so nothing accumulates.
    async def receive_loop(*_a, **_kw):
        for _ in range(1000):
            assert ppctx.current_span() is None
        return None

    asyncio.run(tornado_instr._execute_wrapper(
        receive_loop, instance=handler, args=(), kwargs={},
    ))
    assert fake_agent.events == []
    assert fake_agent.last_native is None


def test_execute_wrapper_bypasses_websocket_upgrade_header(fake_agent):
    """A plain RequestHandler that speaks the websocket handshake (both
    ``Upgrade: websocket`` and ``Connection: Upgrade``) is also bypassed, even
    without subclassing WebSocketHandler."""
    request = _FakeRequest(headers={"Upgrade": "websocket",
                                    "Connection": "Upgrade"})
    handler = UserHandler(request, status=101)

    async def wrapped(*_a, **_kw):
        return None

    asyncio.run(tornado_instr._execute_wrapper(
        wrapped, instance=handler, args=(), kwargs={},
    ))
    assert fake_agent.events == []


def test_execute_wrapper_traces_request_with_stray_upgrade_header(fake_agent):
    """A plain handler that merely carries ``Upgrade: websocket`` but no
    ``Connection: upgrade`` (a client quirk or a misbehaving proxy) is NOT a
    real handshake — the handler never upgrades and returns promptly. It must
    keep its root span; otherwise a single stray request header would silently
    drop the request (and all its child calls, since ``current_span()`` would be
    ``None``) from APM. Mirrors aiohttp's
    ``test_handle_request_wrapper_traces_request_with_stray_upgrade_header``."""
    request = _FakeRequest(headers={"Upgrade": "websocket"})
    handler = UserHandler(request, status=200)

    async def wrapped(*_a, **_kw):
        # A real root span must be active for the whole of a normal handler.
        assert ppctx.current_span() is not None
        return None

    asyncio.run(tornado_instr._execute_wrapper(
        wrapped, instance=handler, args=(), kwargs={},
    ))
    assert ("span_start", "Tornado HTTP Server", "/users/42") in fake_agent.events
    assert ("span_end", "Tornado HTTP Server") in fake_agent.events
    assert fake_agent.last_native is not None
    assert fake_agent.last_native.status_code == 200


def test_execute_wrapper_passes_through_on_true_reentrancy_marker(fake_agent):
    """Defense against a stacked wrapper left by a re-instrument cycle: the
    outer wrapper stamps a per-request marker on the handler and opens the root
    span, so the inner (duplicate) wrapper re-entering ``_execute`` for the same
    handler must see that marker and pass through rather than open a second root
    span."""
    request = _FakeRequest()
    handler = UserHandler(request, status=200)

    # Reproduce the state an outer wrapper leaves for THIS request: our
    # per-request marker on the handler plus the root span current in context.
    setattr(handler, tornado_instr._ROOT_SPAN_ACTIVE_ATTR, True)
    rec = _FakeAgent()
    sp = Span(_FakeNativeSpan("root", "/", rec))
    token = ppctx.set_current_span(sp)
    try:
        called = {"flag": False}

        async def wrapped(*_a, **_kw):
            called["flag"] = True
            return None

        asyncio.run(tornado_instr._execute_wrapper(
            wrapped, instance=handler, args=(), kwargs={},
        ))
        assert called["flag"] is True
        # The inner (duplicate) wrapper opened no second root span.
        assert fake_agent.events == []
        assert fake_agent.last_native is None
    finally:
        ppctx.reset_current_span(token)


def test_execute_wrapper_opens_root_span_despite_ambient_leaked_span(fake_agent):
    """An ambient/leaked span inherited into the request task's context must
    NOT suppress tracing.

    asyncio copies the current contextvars context into every request task, so a
    span left current by startup-in-a-traced-context, or by an instrumentation
    that leaked ``set_current_span`` without a reset, makes ``current_span()``
    non-None for every request. Keying the nesting guard on ``current_span()``
    would therefore pass *all* of them through untraced. A fresh request (no
    per-request marker) must still open its own root span, and the ambient span
    must be left intact afterwards."""
    request = _FakeRequest()
    handler = UserHandler(request, status=200)

    # Simulate the leak: an unrelated span is current in the ambient context,
    # but our wrapper never opened it for THIS handler (no marker stamped).
    leak_rec = _FakeAgent()
    ambient = Span(_FakeNativeSpan("ambient-leak", "/", leak_rec))
    token = ppctx.set_current_span(ambient)
    try:
        async def wrapped(*_a, **_kw):
            # Our own fresh root span — not the ambient one — must be current.
            assert ppctx.current_span() is not None
            assert ppctx.current_span() is not ambient
            return None

        asyncio.run(tornado_instr._execute_wrapper(
            wrapped, instance=handler, args=(), kwargs={},
        ))
        # A brand-new root span was opened for this request ...
        assert ("span_start", "Tornado HTTP Server", "/users/42") in fake_agent.events
        assert ("span_end", "Tornado HTTP Server") in fake_agent.events
        assert fake_agent.last_native is not None
        assert fake_agent.last_native.status_code == 200
        # ... without disturbing the ambient span (its recorder is untouched).
        assert leak_rec.events == [("span_start", "ambient-leak", "/")]
        # The ambient span is restored once our request completes.
        assert ppctx.current_span() is ambient
    finally:
        ppctx.reset_current_span(token)


def test_execute_wrapper_passes_through_when_disabled(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))
    request = _FakeRequest()
    handler = UserHandler(request)

    seen = {}

    async def wrapped(*_a, **_kw):
        seen["called"] = True

    asyncio.run(tornado_instr._execute_wrapper(
        wrapped, instance=handler, args=(), kwargs={},
    ))
    assert seen["called"] is True


# ---------------------------------------------------------------------------
# _is_websocket_handler — header fallback must require BOTH Upgrade: websocket
# and a Connection header carrying the ``upgrade`` token (mirrors aiohttp's
# _is_websocket_request; see test_is_websocket_request_requires_upgrade_and_connection).
# The MRO WebSocketHandler class check remains the primary path.
# ---------------------------------------------------------------------------

def test_is_websocket_handler_matches_subclass_without_headers():
    """The MRO ``WebSocketHandler`` check is the primary path and fires even
    when the request carries no upgrade headers at all — the handshake headers
    are only a fallback for handlers that don't subclass WebSocketHandler."""
    request = _FakeRequest(headers={"X-Trace": "abc"})
    handler = _ChatSocket(request, status=101)
    assert tornado_instr._is_websocket_handler(handler, request) is True


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
def test_is_websocket_handler_header_fallback_requires_upgrade_and_connection(
        headers, expected):
    """For a handler that does NOT subclass ``WebSocketHandler``, the header
    fallback must match aiohttp's ``_handshake`` gate: both ``Upgrade:
    websocket`` and a ``Connection`` header carrying the ``upgrade`` token."""
    request = _FakeRequest(headers=headers)
    # UserHandler is a plain RequestHandler -> exercises the header fallback.
    handler = UserHandler(request)
    assert tornado_instr._is_websocket_handler(handler, request) is expected


# ---------------------------------------------------------------------------
# _log_exception_wrapper
# ---------------------------------------------------------------------------

def test_log_exception_wrapper_records_error_on_active_span(fake_agent):
    """When Tornado calls ``log_exception(typ, value, tb)`` the wrapper
    should mirror the exception onto the currently-active span."""
    rec = _FakeAgent()
    sp = Span(_FakeNativeSpan("root", "/", rec))
    token = ppctx.set_current_span(sp)
    try:
        called = {"flag": False}

        def wrapped(typ, value, tb):
            called["flag"] = True

        exc = ValueError("oops")
        tornado_instr._log_exception_wrapper(
            wrapped, instance=None,
            args=(type(exc), exc, None), kwargs={},
        )
        assert called["flag"] is True
        sp.end()  # errors are buffered and flush at end()
        assert any(e[0] == "span_error" for e in rec.events)
    finally:
        ppctx.reset_current_span(token)


def test_log_exception_wrapper_ignores_http_error_4xx(fake_agent):
    """A handler that ``raise HTTPError(404)`` produces a normal error
    response, not a transaction failure — it must not be recorded as a span
    error (which would inflate the error rate)."""
    rec = _FakeAgent()
    sp = Span(_FakeNativeSpan("root", "/", rec))
    token = ppctx.set_current_span(sp)
    try:
        exc = _FakeHTTPError(404)
        tornado_instr._log_exception_wrapper(
            lambda *_a, **_kw: None, instance=None,
            args=(type(exc), exc, None), kwargs={},
        )
        sp.end()  # flush any buffered error before asserting absence
        assert not any(e[0] == "span_error" for e in rec.events)
    finally:
        ppctx.reset_current_span(token)


def test_log_exception_wrapper_ignores_finish(fake_agent):
    """``raise Finish()`` is a control-flow signal, never an error."""
    rec = _FakeAgent()
    sp = Span(_FakeNativeSpan("root", "/", rec))
    token = ppctx.set_current_span(sp)
    try:
        exc = _FakeFinish()
        tornado_instr._log_exception_wrapper(
            lambda *_a, **_kw: None, instance=None,
            args=(type(exc), exc, None), kwargs={},
        )
        sp.end()  # flush any buffered error before asserting absence
        assert not any(e[0] == "span_error" for e in rec.events)
    finally:
        ppctx.reset_current_span(token)


def test_log_exception_wrapper_records_http_error_5xx(fake_agent):
    """A 5xx HTTPError is a genuine server failure and must still be recorded."""
    rec = _FakeAgent()
    sp = Span(_FakeNativeSpan("root", "/", rec))
    token = ppctx.set_current_span(sp)
    try:
        exc = _FakeHTTPError(503)
        tornado_instr._log_exception_wrapper(
            lambda *_a, **_kw: None, instance=None,
            args=(type(exc), exc, None), kwargs={},
        )
        sp.end()  # errors are buffered and flush at end()
        assert any(e[0] == "span_error" for e in rec.events)
    finally:
        ppctx.reset_current_span(token)


def test_log_exception_wrapper_no_op_without_active_span():
    """No span set → still call wrapped, don't try to record."""
    called = {"flag": False}

    def wrapped(typ, value, tb):
        called["flag"] = True

    tornado_instr._log_exception_wrapper(
        wrapped, instance=None,
        args=(ValueError, ValueError("x"), None), kwargs={},
    )
    assert called["flag"] is True


# ---------------------------------------------------------------------------
# _is_control_flow_exception — must match ONLY tornado.web's own classes
# ---------------------------------------------------------------------------
# Matching ``__mro__`` on class *name* alone would let any unrelated
# ``HTTPError``/``Finish`` (``requests.exceptions.HTTPError``) pass as tornado
# control flow. Those carry no tornado ``status_code``, so they classify as
# "< 500" and a genuine downstream 500 vanishes from the trace. The matched
# class' ``__module__`` must be ``"tornado.web"`` too.

def test_control_flow_tornado_http_error_classified_by_status():
    tornado_web = pytest.importorskip("tornado.web")
    assert tornado_instr._is_control_flow_exception(tornado_web.HTTPError(404)) is True
    assert tornado_instr._is_control_flow_exception(tornado_web.HTTPError(500)) is False


def test_control_flow_tornado_finish_is_control_flow():
    tornado_web = pytest.importorskip("tornado.web")
    assert tornado_instr._is_control_flow_exception(tornado_web.Finish()) is True


def test_control_flow_user_subclass_of_tornado_http_error():
    """A user subclass of the genuine ``tornado.web.HTTPError`` is still control
    flow — its MRO includes the tornado base (``__module__ == "tornado.web"``)."""
    tornado_web = pytest.importorskip("tornado.web")

    class MyHTTPError(tornado_web.HTTPError):
        pass

    assert tornado_instr._is_control_flow_exception(MyHTTPError(404)) is True
    assert tornado_instr._is_control_flow_exception(MyHTTPError(503)) is False


def test_control_flow_ignores_foreign_http_error():
    """A class merely *named* ``HTTPError`` from another library is a real error,
    not tornado control flow — it must NOT be swallowed."""
    import urllib.error
    requests_exceptions = pytest.importorskip("requests.exceptions")

    u = urllib.error.HTTPError("http://downstream", 500, "boom", {}, None)
    assert tornado_instr._is_control_flow_exception(u) is False
    assert tornado_instr._is_control_flow_exception(
        requests_exceptions.HTTPError()) is False


def test_control_flow_ignores_foreign_finish():
    """A class named ``Finish`` defined outside ``tornado.web`` is not tornado's
    control-flow signal and must not be swallowed."""

    class Finish(Exception):  # noqa: N801 - deliberately shares tornado's name
        pass

    assert Finish.__module__ != "tornado.web"
    assert tornado_instr._is_control_flow_exception(Finish()) is False


def test_log_exception_wrapper_records_foreign_http_error_500(fake_agent):
    """End-to-end regression: a downstream failure raised inside a handler as
    ``urllib.error.HTTPError`` (name ``HTTPError`` but not tornado's) is a
    genuine error and MUST be recorded on the active span via log_exception."""
    import urllib.error

    rec = _FakeAgent()
    sp = Span(_FakeNativeSpan("root", "/", rec))
    token = ppctx.set_current_span(sp)
    try:
        exc = urllib.error.HTTPError("http://downstream", 500, "boom", {}, None)
        tornado_instr._log_exception_wrapper(
            lambda *_a, **_kw: None, instance=None,
            args=(type(exc), exc, None), kwargs={},
        )
        sp.end()  # errors are buffered and flush at end()
        assert any(e[0] == "span_error" for e in rec.events)
    finally:
        ppctx.reset_current_span(token)


def test_control_flow_ignores_tornado_httpclient_http_error():
    """The canonical case the module restriction was written for:
    ``tornado.httpclient.HTTPError`` (an outbound AsyncHTTPClient failure) lives
    in module ``tornado.httpclient`` — NOT ``tornado.web`` — and carries ``.code``
    rather than ``status_code``. It must be treated as a genuine error, not
    swallowed as control flow. Guards specifically against loosening the module
    check to ``startswith("tornado.")``, which the foreign urllib/requests cases
    would not catch."""
    tornado_httpclient = pytest.importorskip("tornado.httpclient")

    err = tornado_httpclient.HTTPError(502)
    assert not hasattr(err, "status_code")  # only .code — would read as 0
    assert tornado_instr._is_control_flow_exception(err) is False


# ---------------------------------------------------------------------------
# Reentrancy marker — the marker must actually be *stamped* by the
# wrapper, not just read. These drive a genuine nested re-entry rather than
# pre-seeding the marker by hand.
# ---------------------------------------------------------------------------

def test_execute_wrapper_nested_reentry_opens_single_root_span(fake_agent):
    """A stacked duplicate wrapper (left by a re-instrument cycle) re-enters
    ``_execute`` for the SAME in-flight request. Because the outer wrapper stamps
    the per-request marker before dispatch, the inner one passes through and
    exactly one root span is opened. Deleting the ``_mark_root_span_active`` call
    would open a second root span here."""
    request = _FakeRequest()
    handler = UserHandler(request, status=200)

    async def inner(*_a, **_kw):
        # Re-entry happens with no root span of its own — the outer's marker
        # must shield it.
        return None

    async def outer(*_a, **_kw):
        return await tornado_instr._execute_wrapper(
            inner, instance=handler, args=(), kwargs={},
        )

    asyncio.run(tornado_instr._execute_wrapper(
        outer, instance=handler, args=(), kwargs={},
    ))

    starts = [e for e in fake_agent.events if e[0] == "span_start"]
    ends = [e for e in fake_agent.events if e[0] == "span_end"]
    assert len(starts) == 1
    assert len(ends) == 1
    # The marker was actually stamped on the handler during the outer dispatch.
    assert getattr(handler, tornado_instr._ROOT_SPAN_ACTIVE_ATTR, False) is True


def test_execute_wrapper_does_not_record_control_flow_span_error(fake_agent):
    """``_execute_wrapper`` has its OWN root-span control-flow filter (line
    ``if sampled and not _is_control_flow_exception(exc)``), separate from
    ``_log_exception_wrapper``. An ``HTTPError(404)`` that escapes the dispatch
    must not be recorded as a *span* (transaction) error, yet the span is still
    ended and the exception re-raised. Reverting that guard to a bare
    ``if sampled:`` would flag the transaction as failed on a normal 404.

    (The handler-method span *event* is filtered by the same classifier via
    ``span_event_scope(event, _is_control_flow_exception)`` — see
    ``test_execute_wrapper_does_not_record_control_flow_event_error``.)"""
    request = _FakeRequest()
    handler = UserHandler(request, status=404)

    async def boom(*_a, **_kw):
        raise _FakeHTTPError(404)

    with pytest.raises(_FakeHTTPError):
        asyncio.run(tornado_instr._execute_wrapper(
            boom, instance=handler, args=(), kwargs={},
        ))

    assert not any(e[0] == "span_error" for e in fake_agent.events)
    # The span still closed cleanly despite the control-flow exception.
    assert ("span_end", "Tornado HTTP Server") in fake_agent.events


def test_execute_wrapper_records_5xx_http_error_as_span_error(fake_agent):
    """The other side of the line-190 filter: an ``HTTPError(503)`` escaping the
    dispatch IS a genuine server error and must be recorded on the span."""
    request = _FakeRequest()
    handler = UserHandler(request, status=503)

    async def boom(*_a, **_kw):
        raise _FakeHTTPError(503)

    with pytest.raises(_FakeHTTPError):
        asyncio.run(tornado_instr._execute_wrapper(
            boom, instance=handler, args=(), kwargs={},
        ))

    assert any(e[0] == "span_error" for e in fake_agent.events)
    assert ("span_end", "Tornado HTTP Server") in fake_agent.events


# ---------------------------------------------------------------------------
# _execute_wrapper — handler-method span EVENT control-flow filtering
# (mirrors aiohttp's test_traced_handler_does_not_record_control_flow_*).
# The dispatch runs inside ``span_event_scope(event, _is_control_flow_exception)``;
# a sub-500 HTTPError / Finish must NOT flag the event as an error.
# ---------------------------------------------------------------------------

def test_execute_wrapper_does_not_record_control_flow_event_error(fake_agent):
    """``raise HTTPError(404)`` is normal control flow: the handler-method span
    EVENT must be opened but NOT flagged as an error. Before the fix,
    ``span_event_scope`` recorded it unconditionally — telemetry noise on every
    idiomatic 4xx."""
    request = _FakeRequest()
    handler = UserHandler(request, status=404)

    async def boom(*_a, **_kw):
        raise _FakeHTTPError(404)

    with pytest.raises(_FakeHTTPError):
        asyncio.run(tornado_instr._execute_wrapper(
            boom, instance=handler, args=(), kwargs={},
        ))

    assert any(e[0] == "event_start" for e in fake_agent.events)
    assert not any(e[0] == "event_error" for e in fake_agent.events)
    # And still not on the span (the existing line-190 guard).
    assert not any(e[0] == "span_error" for e in fake_agent.events)


def test_execute_wrapper_finish_is_not_event_error(fake_agent):
    """``raise Finish()`` is a pure control-flow signal — never an event error."""
    request = _FakeRequest()
    handler = UserHandler(request, status=200)

    async def finish(*_a, **_kw):
        raise _FakeFinish()

    with pytest.raises(_FakeFinish):
        asyncio.run(tornado_instr._execute_wrapper(
            finish, instance=handler, args=(), kwargs={},
        ))

    assert any(e[0] == "event_start" for e in fake_agent.events)
    assert not any(e[0] == "event_error" for e in fake_agent.events)


def test_execute_wrapper_records_5xx_http_error_as_event_error(fake_agent):
    """A 5xx ``HTTPError`` IS a genuine server failure and must still flag the
    handler-method event — the control-flow filter is status-scoped, not a
    blanket suppression of every ``HTTPError``."""
    request = _FakeRequest()
    handler = UserHandler(request, status=503)

    async def boom(*_a, **_kw):
        raise _FakeHTTPError(503)

    with pytest.raises(_FakeHTTPError):
        asyncio.run(tornado_instr._execute_wrapper(
            boom, instance=handler, args=(), kwargs={},
        ))

    assert any(e[0] == "event_error" for e in fake_agent.events)
