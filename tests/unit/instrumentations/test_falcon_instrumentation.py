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

"""Falcon instrumentation tests (WSGI + ASGI).

Three layers:

1. ``_get_responder_wrapper`` against fakes — covers URL-template stashing,
   responder wrapping (sync + async), exception propagation, and the
   "unexpected return shape" graceful path.
2. ``_wsgi_call_wrapper`` against a tiny in-process WSGI app — root span
   lifecycle, status capture, exception path, agent-disabled bypass.
3. End-to-end: drive a real ``falcon.App`` and assert exactly one root
   span plus a handler span event named after the responder qualname.

ASGI per-request behavior is covered by the existing ASGI tests — the
falcon ASGI wrapper just delegates to ``PinpointASGIMiddleware``.
"""

from __future__ import annotations

import asyncio
import io
import sys
import pytest
from _fakes import FakeAgent as _FakeAgent, wsgi_environ

import pinpoint
from pinpoint import context as ppctx
from pinpoint.instrumentations import falcon as falcon_instr
from pinpoint.instrumentations import wsgi as wsgi_instr


class _FakeReq:
    """Stands in for falcon's Request — only the env/scope dict matters
    for url_pattern stashing."""
    def __init__(self, env=None, scope=None):
        if env is not None:
            self.env = env
        if scope is not None:
            self.scope = scope


# ---------------------------------------------------------------------------
# _get_responder_wrapper — URL pattern stashing
# ---------------------------------------------------------------------------

def test_get_responder_stashes_uri_template_into_env():
    env: dict = {}

    class Res:
        def on_get(self, req, resp): pass

    res = Res()

    def wrapped(*_a, **_kw):
        return (res.on_get, {}, res, "/users/{id}")

    falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(env=env),), kwargs={},
    )
    assert env["pinpoint.url_pattern"] == "/users/{id}"


def test_get_responder_stashes_uri_template_into_scope():
    scope: dict = {}

    class Res:
        async def on_get(self, req, resp): pass

    res = Res()

    def wrapped(*_a, **_kw):
        return (res.on_get, {}, res, "/api/v1/{x}")

    falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(scope=scope),), kwargs={},
    )
    assert scope["pinpoint.url_pattern"] == "/api/v1/{x}"


def test_get_responder_no_uri_template_is_safe():
    """Older falcon routers may return None for the template slot — don't
    blow up, just leave the env untouched."""
    env: dict = {}

    def responder(req, resp): return "ok"

    def wrapped(*_a, **_kw):
        return (responder, {}, object(), None)

    falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(env=env),), kwargs={},
    )
    assert "pinpoint.url_pattern" not in env


def test_get_responder_passes_through_unexpected_shape():
    """Non-tuple / short-tuple returns must propagate unchanged."""
    def wrapped(*_a, **_kw):
        return None

    out = falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(env={}),), kwargs={},
    )
    assert out is None


def test_wrap_responder_preserves_responder_metadata():
    """The traced responder carries the original's identity/metadata via
    functools.wraps, so middleware / falcon.hooks introspecting the resolved
    responder see the real one, not an anonymous ``_traced(*a, **kw)``."""
    class UserResource:
        def on_get(self, req, resp):
            return "ok"

    responder = UserResource().on_get
    traced = falcon_instr._wrap_responder(responder)
    assert traced is not None
    assert getattr(traced, "__wrapped__", None) is responder
    assert traced.__name__ == "on_get"
    assert "UserResource.on_get" in traced.__qualname__


# ---------------------------------------------------------------------------
# _get_responder_wrapper — responder wrapping (sync)
# ---------------------------------------------------------------------------

def test_get_responder_sync_emits_span_event(push_span):
    sp, rec = push_span

    class UserResource:
        def on_get(self, req, resp): return "got"

    res = UserResource()

    def wrapped(*_a, **_kw):
        return (res.on_get, {}, res, "/users")

    result = falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(env={}),), kwargs={},
    )
    traced = result[0]
    assert traced(None, None) == "got"
    # Inner-class qualnames carry a ``<locals>`` prefix — match the suffix.
    assert any(
        kind == "event_start" and op == "root" and name.endswith("UserResource.on_get")
        for kind, op, name in rec.events
    )
    assert any(
        kind == "event_end" and op == "root" and name.endswith("UserResource.on_get")
        for kind, op, name in rec.events
    )


def test_get_responder_sync_records_exception(push_span):
    sp, rec = push_span

    class Boom:
        def on_get(self, req, resp): raise RuntimeError("boom")

    res = Boom()

    def wrapped(*_a, **_kw):
        return (res.on_get, {}, res, "/x")

    result = falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(env={}),), kwargs={},
    )
    with pytest.raises(RuntimeError, match="boom"):
        result[0](None, None)

    assert any(e[0] == "event_error" for e in rec.events)
    # Event must still close in the finally.
    assert any(
        kind == "event_end" and op == "root" and name.endswith("Boom.on_get")
        for kind, op, name in rec.events
    )


def test_get_responder_no_active_span_delegates_transparently():
    """No span on the contextvar → wrapper passes through to the inner
    responder without opening a span event."""
    def responder(req, resp): return "ok"

    def wrapped(*_a, **_kw):
        return (responder, {}, object(), "/x")

    result = falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(env={}),), kwargs={},
    )
    assert result[0](None, None) == "ok"


# ---------------------------------------------------------------------------
# _get_responder_wrapper — responder wrapping (async)
# ---------------------------------------------------------------------------

def test_get_responder_async_emits_span_event(push_span):
    import inspect as _inspect

    sp, rec = push_span

    class AsyncUserResource:
        async def on_get(self, req, resp): return "got-async"

    res = AsyncUserResource()

    def wrapped(*_a, **_kw):
        return (res.on_get, {}, res, "/users")

    result = falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(scope={}),), kwargs={},
    )
    traced = result[0]
    assert _inspect.iscoroutinefunction(traced)
    out = asyncio.run(traced(None, None))
    assert out == "got-async"
    assert any(
        kind == "event_start" and op == "root" and name.endswith("AsyncUserResource.on_get")
        for kind, op, name in rec.events
    )
    assert any(
        kind == "event_end" and op == "root" and name.endswith("AsyncUserResource.on_get")
        for kind, op, name in rec.events
    )


def test_get_responder_async_records_exception(push_span):
    sp, rec = push_span

    class AsyncBoom:
        async def on_get(self, req, resp): raise RuntimeError("async-boom")

    res = AsyncBoom()

    def wrapped(*_a, **_kw):
        return (res.on_get, {}, res, "/y")

    result = falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(scope={}),), kwargs={},
    )
    with pytest.raises(RuntimeError, match="async-boom"):
        asyncio.run(result[0](None, None))

    assert any(e[0] == "event_error" for e in rec.events)
    assert any(
        kind == "event_end" and op == "root" and name.endswith("AsyncBoom.on_get")
        for kind, op, name in rec.events
    )


def test_get_responder_async_callable_object_takes_async_path(push_span):
    """A responder that is a *callable object* whose ``__call__`` is async
    must take the async trace path — awaited by us, with a real (non
    zero-duration) span event — not the sync path where the coroutine is
    returned un-awaited and the event closes immediately."""
    import inspect as _inspect

    sp, rec = push_span

    class AsyncResponder:
        async def __call__(self, req, resp):
            return "got-callable"

    responder = AsyncResponder()

    def wrapped(*_a, **_kw):
        return (responder, {}, object(), "/users")

    result = falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(scope={}),), kwargs={},
    )
    traced = result[0]
    # Took the async branch: the wrapper is itself a coroutine function.
    assert _inspect.iscoroutinefunction(traced)
    assert asyncio.run(traced(None, None)) == "got-callable"
    # Event opened and closed around the awaited call (non-zero duration).
    assert any(kind == "event_start" and op == "root" for kind, op, _ in rec.events)
    assert any(kind == "event_end" and op == "root" for kind, op, _ in rec.events)


def test_get_responder_async_callable_object_records_exception(push_span):
    """Exceptions from an async callable-object responder are recorded on the
    span event — the failure the sync path silently dropped."""
    sp, rec = push_span

    class AsyncBoomResponder:
        async def __call__(self, req, resp):
            raise RuntimeError("callable-boom")

    responder = AsyncBoomResponder()

    def wrapped(*_a, **_kw):
        return (responder, {}, object(), "/x")

    result = falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(scope={}),), kwargs={},
    )
    with pytest.raises(RuntimeError, match="callable-boom"):
        asyncio.run(result[0](None, None))
    assert any(e[0] == "event_error" for e in rec.events)
    assert any(kind == "event_end" and op == "root" for kind, op, _ in rec.events)


def test_get_responder_partial_of_coroutine_takes_async_path(push_span):
    """A ``functools.partial`` wrapping a coroutine function must also take
    the async path."""
    import functools
    import inspect as _inspect

    sp, rec = push_span

    async def handler(req, resp, tenant):
        return f"got-{tenant}"

    responder = functools.partial(handler, tenant="acme")

    def wrapped(*_a, **_kw):
        return (responder, {}, object(), "/x")

    result = falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(scope={}),), kwargs={},
    )
    traced = result[0]
    assert _inspect.iscoroutinefunction(traced)
    assert asyncio.run(traced(None, None)) == "got-acme"
    assert any(kind == "event_start" and op == "root" for kind, op, _ in rec.events)


# ---------------------------------------------------------------------------
# _wsgi_call_wrapper
# ---------------------------------------------------------------------------

def _make_environ(**overrides):
    return wsgi_environ(path="/users/42", trace_id="T-1", **overrides)


def test_wsgi_call_creates_root_span_and_captures_status(fake_agent):
    def app(env, start_response):
        start_response("200 OK", [])
        return [b"ok"]

    out = falcon_instr._wsgi_call_wrapper(
        app, instance=None,
        args=(_make_environ(), lambda *a, **k: None), kwargs={},
    )
    assert list(out) == [b"ok"]
    assert ("span_start", "Falcon HTTP Server", "/users/42") in fake_agent.events
    assert ("span_end", "Falcon HTTP Server") in fake_agent.events
    assert fake_agent.last_native.status_code == 200


def test_wsgi_call_uses_url_pattern_when_stashed(fake_agent):
    """A downstream layer (_get_responder_wrapper) stashes
    ``pinpoint.url_pattern`` on env — url_stat should record against
    the template, not the concrete path."""
    def app(env, start_response):
        env["pinpoint.url_pattern"] = "/users/{id}"
        start_response("200 OK", [])
        return [b""]

    # Drain the returned body so the span finalizes (it now ends when the
    # response iterable is consumed, not when __call__ returns).
    list(falcon_instr._wsgi_call_wrapper(
        app, instance=None,
        args=(_make_environ(), lambda *a, **k: None), kwargs={},
    ))
    assert fake_agent.last_native.url_stats == [("/users/{id}", "GET", 200)]


def test_wsgi_call_records_exception_and_reraises(fake_agent):
    def app(env, start_response):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        falcon_instr._wsgi_call_wrapper(
            app, instance=None,
            args=(_make_environ(), lambda *a, **k: None), kwargs={},
        )
    assert any(e[0] == "span_error" for e in fake_agent.events)
    assert ("span_end", "Falcon HTTP Server") in fake_agent.events


def test_wsgi_call_streaming_body_ends_response_child_after_drain(fake_agent):
    """Streaming uses a linked response child and never moves the root."""
    seen = []

    def app(env, start_response):
        start_response("200 OK", [])

        def gen():
            seen.append(ppctx.current_span())
            yield b"a"
            yield b"b"

        return gen()

    body = falcon_instr._wsgi_call_wrapper(
        app, instance=None,
        args=(_make_environ(), lambda *a, **k: None), kwargs={},
    )
    root = fake_agent.last_native
    child = fake_agent.last_async_native
    assert root.end_called is True
    assert child.end_called is False
    assert ppctx.current_span() is None

    assert list(body) == [b"a", b"b"]
    assert seen and seen[0] is not None and seen[0].sampled
    assert child.end_called is True


def test_wsgi_call_ends_span_when_setup_fails_after_creation(fake_agent, monkeypatch):
    """Regression (native span leak): if span setup raises *after* new_span
    created the native span (e.g. set_current_span), the request must still run
    untraced AND the already-created native span must be ended — not leaked."""
    def _boom(_span):
        raise RuntimeError("setup boom")

    monkeypatch.setattr(wsgi_instr, "set_current_span", _boom)

    calls = []

    def app(env, start_response):
        calls.append(1)
        start_response("200 OK", [])
        return [b"ok"]

    body = list(falcon_instr._wsgi_call_wrapper(
        app, instance=None,
        args=(_make_environ(), lambda *a, **k: None), kwargs={},
    ))

    # Request still completes, untraced, exactly once.
    assert body == [b"ok"]
    assert calls == [1]
    # The created native span was ended — no leak.
    assert fake_agent.last_native.end_called is True
    assert ("span_end", "Falcon HTTP Server") in fake_agent.events


def test_wsgi_call_propagates_distributed_headers(fake_agent):
    """``HTTP_PINPOINT_*`` env vars must surface as headers on new_span()
    so upstream trace IDs link in."""
    def app(env, start_response):
        start_response("200 OK", [])
        return [b""]

    falcon_instr._wsgi_call_wrapper(
        app, instance=None,
        args=(_make_environ(), lambda *a, **k: None), kwargs={},
    )
    assert fake_agent.last_native.headers.get("PINPOINT-TRACEID") == "T-1"


def test_wsgi_call_passes_through_when_agent_disabled(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))

    seen = {}

    def app(env, start_response):
        seen["called"] = True
        start_response("200 OK", [])
        return [b""]

    out = falcon_instr._wsgi_call_wrapper(
        app, instance=None,
        args=(_make_environ(), lambda *a, **k: None), kwargs={},
    )
    assert list(out) == [b""]
    assert seen["called"]


# ---------------------------------------------------------------------------
# _asgi_call_wrapper — lifecycle of non-http scopes
# ---------------------------------------------------------------------------

def test_asgi_call_non_http_scope_passes_through():
    """Lifespan / websocket scopes must not start a span."""
    called: list = []

    async def app(scope, receive, send):
        called.append(scope["type"])

    asyncio.run(falcon_instr._asgi_call_wrapper(
        app, instance=None,
        args=({"type": "lifespan"}, None, None), kwargs={},
    ))
    assert called == ["lifespan"]


# ---------------------------------------------------------------------------
# End-to-end: drive a real falcon.App through its WSGI entry point.
#
# Establishes that wrapping the real ``App.__call__`` and the real
# ``_get_responder`` together produces exactly one root span and one
# handler-named span event per request.
# ---------------------------------------------------------------------------

def test_falcon_real_wsgi_app_emits_root_and_handler_event(fake_agent):
    pytest.importorskip("falcon")
    import falcon

    events = fake_agent.events

    # Install just the falcon wraps directly — calling ``autoload()`` would
    # re-fire post-import hooks for every already-loaded framework
    # (fastapi, starlette, …) and ``wrap_function_wrapper`` does not dedupe,
    # so a sibling end-to-end test would see double-wrapped behavior.
    falcon_instr.FalconInstrumentor().instrument()

    class UserResource:
        def on_get(self, req, resp, user_id):
            resp.text = f"id={user_id}"
            resp.status = "200 OK"

    app = falcon.App()
    app.add_route("/users/{user_id:int}", UserResource())

    env = {
        "REQUEST_METHOD": "GET",
        "PATH_INFO": "/users/42",
        "wsgi.url_scheme": "http",
        "wsgi.input": io.BytesIO(),
        "wsgi.errors": sys.stderr,
        "HTTP_HOST": "example.test",
        "SERVER_NAME": "example.test",
        "SERVER_PORT": "80",
        "SERVER_PROTOCOL": "HTTP/1.1",
    }
    captured = {}

    def start_response(status, headers, exc_info=None):
        captured["status"] = status

    body = b"".join(app(env, start_response))
    assert captured["status"].startswith("200")
    assert b"id=42" in body

    new_spans = [e for e in events if e[0] == "span_start"]
    root_ends = [e for e in events
                 if e == ("span_end", "Falcon HTTP Server")]
    async_ends = [e for e in events
                  if e == ("span_end", "async:wsgi.response.iteration")]
    sp_events = [e for e in events if e[0] == "event_start"]
    assert len(new_spans) == 1, f"expected one root span, got: {events}"
    assert len(root_ends) == 1, f"root span must end, got: {events}"
    assert async_ends == [], f"eager response must not create a child, got: {events}"
    assert any(name.endswith(".on_get") for _, _, name in sp_events), (
        f"handler span event missing: {events}"
    )


# ---------------------------------------------------------------------------
# _wrap_responder — responder span EVENT control-flow filtering
# (mirrors aiohttp's test_traced_handler_does_not_record_control_flow_*).
# ``raise falcon.HTTPError(falcon.HTTP_400)`` (and HTTPNotFound, ...) is the
# normal way to produce a non-2xx response; the responder span event must NOT
# be flagged as an error for a sub-500 status.
# ---------------------------------------------------------------------------

class _FakeHTTPError(Exception):
    """Mimics ``falcon.HTTPError`` — ``.status`` is a ``"NNN Reason"`` string
    (the ``falcon.HTTP_404`` constant form)."""
    def __init__(self, status):
        super().__init__(status)
        self.status = status


# Match falcon's real class identity — name *and* defining module — so the
# module-gated classifier fires for the fake exactly as for the genuine type.
_FakeHTTPError.__name__ = "HTTPError"
_FakeHTTPError.__module__ = "falcon.errors"


def test_get_responder_sync_does_not_record_control_flow_http_error(push_span):
    sp, rec = push_span

    class Res:
        def on_get(self, req, resp):
            raise _FakeHTTPError("404 Not Found")

    res = Res()

    def wrapped(*_a, **_kw):
        return (res.on_get, {}, res, "/x")

    result = falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(env={}),), kwargs={},
    )
    with pytest.raises(_FakeHTTPError):
        result[0](None, None)

    assert any(e[0] == "event_start" for e in rec.events)
    assert not any(e[0] == "event_error" for e in rec.events)
    # The event must still close in the finally.
    assert any(kind == "event_end" for kind, *_ in rec.events)


def test_get_responder_sync_records_5xx_http_error(push_span):
    sp, rec = push_span

    class Res:
        def on_get(self, req, resp):
            raise _FakeHTTPError("503 Service Unavailable")

    res = Res()

    def wrapped(*_a, **_kw):
        return (res.on_get, {}, res, "/x")

    result = falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(env={}),), kwargs={},
    )
    with pytest.raises(_FakeHTTPError):
        result[0](None, None)

    assert any(e[0] == "event_error" for e in rec.events)


def test_get_responder_async_does_not_record_control_flow_http_error(push_span):
    sp, rec = push_span

    class AsyncRes:
        async def on_get(self, req, resp):
            raise _FakeHTTPError("400 Bad Request")

    res = AsyncRes()

    def wrapped(*_a, **_kw):
        return (res.on_get, {}, res, "/x")

    result = falcon_instr._get_responder_wrapper(
        wrapped, instance=None, args=(_FakeReq(scope={}),), kwargs={},
    )
    with pytest.raises(_FakeHTTPError):
        asyncio.run(result[0](None, None))

    assert any(e[0] == "event_start" for e in rec.events)
    assert not any(e[0] == "event_error" for e in rec.events)


def test_is_control_flow_exception_classifies_falcon_http_error():
    assert falcon_instr._is_control_flow_exception(
        _FakeHTTPError("404 Not Found")) is True
    assert falcon_instr._is_control_flow_exception(
        _FakeHTTPError("503 Service Unavailable")) is False
    assert falcon_instr._is_control_flow_exception(RuntimeError()) is False

    class HTTPError(Exception):  # not falcon's — must not match
        status = "404 Not Found"

    assert falcon_instr._is_control_flow_exception(HTTPError()) is False
