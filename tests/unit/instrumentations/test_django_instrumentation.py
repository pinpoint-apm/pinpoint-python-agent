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

"""Django WSGI/ASGI root-span and resolved-view instrumentation."""

from __future__ import annotations

import asyncio
import gc
import weakref

import pytest
from _fakes import FakeAgent as _FakeAgent, http_scope, wsgi_environ

import pinpoint
from pinpoint import context as ppctx
from pinpoint.instrumentations import django as django_instr
from pinpoint.instrumentations import wsgi as wsgi_instr


class _FakeRequest:
    def __init__(self, method="GET", path="/items/42", with_pinpoint=False):
        self.method = method
        self.path = path
        self.scope = {}
        self.META = {
            "REQUEST_METHOD": method,
            "REMOTE_ADDR": "10.0.0.1",
            "HTTP_HOST": "example.test",
            "HTTP_X_TRACE": "abc",
        }
        if with_pinpoint:
            self.META["HTTP_PINPOINT_TRACEID"] = "T-1"


class _FakeResponse:
    def __init__(self, status_code=200):
        self.status_code = status_code
        self.headers = {"content-type": "text/plain"}


# ---------------------------------------------------------------------------
# WSGIHandler.__call__ root span
# ---------------------------------------------------------------------------


def _make_environ(with_pinpoint=True, **overrides):
    return wsgi_environ(trace_id="T-1" if with_pinpoint else None, **overrides)


def test_wsgi_handler_eager_body_ends_root_and_captures_status(fake_agent):
    expected_body = [b"ok"]

    def wrapped(env, start_response):
        start_response("200 OK", [("content-type", "text/plain")])
        return expected_body

    body = django_instr._wsgi_handler_wrapper(
        wrapped, instance=object(),
        args=(_make_environ(), lambda *_a, **_kw: None), kwargs={},
    )

    root = fake_agent.last_native
    assert root.end_called is True
    assert body is expected_body
    assert fake_agent.last_async_native is None
    assert ppctx.current_span() is None
    assert list(body) == [b"ok"]
    assert ("span_start", "Django HTTP Server", "/items/42") in fake_agent.events
    assert ("span_end", "Django HTTP Server") in fake_agent.events
    assert root.status_code == 200
    assert ("/items/42", "GET", 200) in root.url_stats
    assert root.headers is not None


def test_wsgi_handler_streaming_body_runs_under_span(fake_agent):
    seen = []

    def wrapped(env, start_response):
        start_response("200 OK", [])

        def stream():
            seen.append(ppctx.current_span())
            yield b"a"
            seen.append(ppctx.current_span())
            yield b"b"

        return stream()

    body = django_instr._wsgi_handler_wrapper(
        wrapped, instance=object(),
        args=(_make_environ(), lambda *_a, **_kw: None), kwargs={},
    )

    root = fake_agent.last_native
    child = fake_agent.last_async_native
    assert root.end_called is True
    assert child.end_called is False
    assert list(body) == [b"a", b"b"]
    assert all(span is not None and span.sampled for span in seen)
    assert child.end_called is True


def test_wsgi_handler_skips_reader_without_upstream_context(fake_agent):
    def wrapped(env, start_response):
        start_response("204 No Content", [])
        return [b""]

    body = django_instr._wsgi_handler_wrapper(
        wrapped, instance=object(),
        args=(_make_environ(with_pinpoint=False), lambda *_a, **_kw: None),
        kwargs={},
    )
    list(body)

    assert fake_agent.last_native.headers is None
    assert fake_agent.last_native.status_code == 204
    assert ("span_end", "Django HTTP Server") in fake_agent.events


def test_wsgi_handler_records_exception_and_reraises(fake_agent):
    def wrapped(env, start_response):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        django_instr._wsgi_handler_wrapper(
            wrapped, instance=object(),
            args=(_make_environ(), lambda *_a, **_kw: None), kwargs={},
        )

    assert any(e[0] == "span_error" for e in fake_agent.events)
    assert ("span_end", "Django HTTP Server") in fake_agent.events


def test_wsgi_handler_ends_span_when_setup_fails_after_creation(
        fake_agent, monkeypatch):
    def _boom(_span):
        raise RuntimeError("setup boom")

    monkeypatch.setattr(wsgi_instr, "set_current_span", _boom)
    calls = []

    def wrapped(env, start_response):
        calls.append(1)
        start_response("200 OK", [])
        return [b"ok"]

    body = django_instr._wsgi_handler_wrapper(
        wrapped, instance=object(),
        args=(_make_environ(), lambda *_a, **_kw: None), kwargs={},
    )

    assert list(body) == [b"ok"]
    assert calls == [1]
    assert fake_agent.last_native.end_called is True


def test_wsgi_handler_passes_through_when_agent_disabled(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))
    seen = {}

    def wrapped(env, start_response):
        seen["called"] = True
        return [b"ok"]

    body = django_instr._wsgi_handler_wrapper(
        wrapped, instance=object(),
        args=(_make_environ(), lambda *_a, **_kw: None), kwargs={},
    )
    assert list(body) == [b"ok"]
    assert seen["called"] is True


# ---------------------------------------------------------------------------
# ASGIHandler.__call__ root span
# ---------------------------------------------------------------------------


def test_transport_hook_installs_the_asgi_instrumentor_only_while_installed(monkeypatch):
    """The explicit instrumentor defers each transport to its own instrumentor
    when that handler module loads — and, since wrapt import hooks can never be
    unregistered, the hook must do nothing once the instrumentor is gone."""
    calls = []
    monkeypatch.setattr(
        django_instr, "wrap",
        lambda module, target, wrapper: calls.append((module, target, wrapper)),
    )
    instrumentor = django_instr.DjangoInstrumentor()
    hook = instrumentor._transport_hook(django_instr.DjangoASGIInstrumentor)
    try:
        hook(None)  # not installed: inert
        assert calls == []

        instrumentor._installing = True
        hook(None)
        assert [target for _module, target, _wrapper in calls] == [
            "BaseHandler._get_response_async",
            "ASGIHandler.__call__",
            "ASGIHandler.run_get_response",
        ]
    finally:
        django_instr.DjangoASGIInstrumentor().uninstrument()


def test_transport_autoload_instrumentors_wrap_only_their_transport(monkeypatch):
    calls = []
    monkeypatch.setattr(
        django_instr, "wrap",
        lambda module, target, wrapper: calls.append((module, target, wrapper)),
    )

    django_instr.DjangoWSGIInstrumentor()._instrument()
    assert calls == [
        (
            "django.core.handlers.base",
            "BaseHandler._get_response",
            django_instr._handler_wrapper,
        ),
        (
            "django.core.handlers.wsgi",
            "WSGIHandler.__call__",
            django_instr._wsgi_handler_wrapper,
        ),
    ]

    calls.clear()
    django_instr.DjangoASGIInstrumentor()._instrument()
    assert calls == [
        (
            "django.core.handlers.base",
            "BaseHandler._get_response_async",
            django_instr._async_handler_wrapper,
        ),
        (
            "django.core.handlers.asgi",
            "ASGIHandler.__call__",
            django_instr._asgi_handler_wrapper,
        ),
        (
            "django.core.handlers.asgi",
            "ASGIHandler.run_get_response",
            django_instr._asgi_run_get_response_wrapper,
        ),
    ]


def _http_scope(**overrides):
    return http_scope(trace_id="T-1", client=("10.0.0.1", 54321), **overrides)


def test_asgi_handler_traces_complete_send_lifecycle(fake_agent):
    seen = []
    sent = []

    async def app(scope, receive, send):
        seen.append(ppctx.current_span())
        scope["pinpoint.url_pattern"] = "items/<int:item_id>/"
        await send({"type": "http.response.start", "status": 200, "headers": []})
        seen.append(ppctx.current_span())
        await send({
            "type": "http.response.body", "body": b"a", "more_body": True,
        })
        seen.append(ppctx.current_span())
        await send({"type": "http.response.body", "body": b"b"})

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    coroutine = django_instr._asgi_handler_wrapper(
        app, instance=object(), args=(_http_scope(), receive, send), kwargs={},
    )
    assert fake_agent.last_native is None
    asyncio.run(coroutine)

    assert len(sent) == 3
    assert all(span is not None for span in seen)
    assert ("span_start", "Django HTTP Server", "/items/42") in fake_agent.events
    assert ("span_end", "Django HTTP Server") in fake_agent.events
    assert fake_agent.last_native.status_code == 200
    assert fake_agent.last_native.url_stats == [
        ("items/<int:item_id>/", "GET", 200),
    ]


def test_asgi_process_request_task_keeps_view_on_request_root(fake_agent):
    """Django 5+ runs request processing in an internal child Task.

    That task is a lifecycle detail, not concurrent application work: it must
    adopt the request root rather than creating a synthetic ``asyncio.task``
    span before the view wrapper and any user middleware execute.
    """
    seen = {}

    async def app(scope, receive, send):
        seen["root"] = ppctx.current_span()
        request = _FakeRequest()
        request.scope = scope

        async def get_response(req):
            seen["view"] = ppctx.current_span()
            req.resolver_match = _ResolverMatch(
                func=_sample_view, route="items/<int:pk>/",
            )
            return _FakeResponse(200)

        async def run_get_response(req):
            return await django_instr._async_handler_wrapper(
                get_response, None, (req,), {},
            )

        async def process_request():
            return await django_instr._asgi_run_get_response_wrapper(
                run_get_response, None, (request,), {},
            )

        await asyncio.create_task(process_request())
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(_message):
        pass

    asyncio.run(django_instr._asgi_handler_wrapper(
        app, instance=object(), args=(_http_scope(), receive, send), kwargs={},
    ))

    assert seen["view"] is seen["root"]
    assert fake_agent.last_async_native is None  # no async child span created
    # The view event replays to the ROOT span's native at its end (already
    # renamed from the placeholder) — not to an async child.
    assert (
        "event_start", "Django HTTP Server", "_sample_view",
    ) in fake_agent.events


def test_asgi_handler_non_http_scope_preserves_django_behavior(fake_agent):
    seen = []

    async def app(scope, receive, send):
        seen.append(scope["type"])

    asyncio.run(django_instr._asgi_handler_wrapper(
        app, instance=object(), args=({"type": "lifespan"}, None, None), kwargs={},
    ))

    assert seen == ["lifespan"]
    assert fake_agent.last_native is None


def test_asgi_handler_does_not_retain_dead_handler_instances(fake_agent):
    """App-factory handlers must be collectible instead of filling a global
    id-keyed middleware cache and forcing later handlers onto a slow path."""
    refs = []

    class Handler:
        async def __call__(self, scope, receive, send):
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": b""})

    async def exercise():
        async def receive():
            return {"type": "http.request", "body": b""}

        async def send(_message):
            pass

        for _ in range(80):
            handler = Handler()
            refs.append(weakref.ref(handler))
            await django_instr._asgi_handler_wrapper(
                handler.__call__, handler,
                (_http_scope(), receive, send), {},
            )

    asyncio.run(exercise())
    gc.collect()

    assert all(ref() is None for ref in refs)


# ---------------------------------------------------------------------------
# _handler_wrapper — view span-event naming (placeholder + rename)
# ---------------------------------------------------------------------------

class _ResolverMatch:
    """Stand-in for django's ``request.resolver_match``."""
    def __init__(self, func, route="", url_name=None):
        self.func = func
        self.route = route
        self.url_name = url_name


def _sample_view(request):  # module-level so __qualname__ is a clean name
    return _FakeResponse(200)


def test_handler_wrapper_renames_view_event_and_stashes_url_pattern(fake_agent):
    """``_handler_wrapper`` opens the view span event under a placeholder
    (``django.request``) and renames it from ``request.resolver_match`` just
    before it ends; it also stashes the matched route template for url-stat
    bucketing. Nothing covered this — the fake span had no span-event support,
    so a regression collapsing every view to ``django.request`` (or losing the
    per-route url_stat) would go unnoticed."""
    from pinpoint import context as ppctx
    from pinpoint.instrumentations import django as django_instr

    request = _FakeRequest()

    def _get_response(req):
        # Django resolves the URL during _get_response and stashes the match.
        req.resolver_match = _ResolverMatch(
            func=_sample_view, route="items/<int:pk>/")
        return _FakeResponse(200)

    sp = fake_agent.new_span("root", "/items/42")
    token = ppctx.set_current_span(sp)
    try:
        django_instr._handler_wrapper(_get_response, None, (request,), {})
    finally:
        ppctx.reset_current_span(token)

    native = fake_agent.last_native
    # The event is buffered Python-side and reaches native only at its end,
    # already renamed: the placeholder never appears, only the resolved
    # view's qualname does.
    assert ("event_start", "root", django_instr._OPERATION_VIEW_FALLBACK) \
        not in fake_agent.events
    ev = native.all_events[-1]
    assert ev.op != django_instr._OPERATION_VIEW_FALLBACK
    assert ev.op.endswith("_sample_view")
    # The matched route template is stashed for url-stat bucketing.
    assert request.META["pinpoint.url_pattern"] == "items/<int:pk>/"


def test_async_handler_wrapper_traces_view_and_stashes_scope(fake_agent):
    """Django ASGI resolves views in ``_get_response_async``; the event must
    stay open across the await and publish the route to the outer ASGI scope."""
    request = _FakeRequest()

    async def _get_response_async(req):
        await asyncio.sleep(0)
        req.resolver_match = _ResolverMatch(
            func=_sample_view, route="items/<int:pk>/",
        )
        return _FakeResponse(200)

    span = fake_agent.new_span("root", "/items/42")
    token = ppctx.set_current_span(span)
    try:
        response = asyncio.run(django_instr._async_handler_wrapper(
            _get_response_async, None, (request,), {},
        ))
    finally:
        ppctx.reset_current_span(token)

    assert response.status_code == 200
    event = fake_agent.last_native.all_events[-1]
    assert event.operation_name.endswith("_sample_view")
    assert request.scope["pinpoint.url_pattern"] == "items/<int:pk>/"
    # The event replays to native at its end, already renamed.
    ends = [e for e in fake_agent.events if e[0] == "event_end"]
    assert ends and ends[-1][2].endswith("_sample_view")


def test_handler_wrapper_keeps_placeholder_when_no_resolver_match(fake_agent):
    """When Django never resolved a route (e.g. a 404 before routing, or a raw
    ASGI request), ``resolver_match`` is absent: the view event keeps its
    placeholder name, no url pattern is stashed, and nothing raises."""
    from pinpoint import context as ppctx
    from pinpoint.instrumentations import django as django_instr

    request = _FakeRequest()

    def _get_response(req):
        return _FakeResponse(404)  # no resolver_match set

    sp = fake_agent.new_span("root", "/nope")
    token = ppctx.set_current_span(sp)
    try:
        django_instr._handler_wrapper(_get_response, None, (request,), {})
    finally:
        ppctx.reset_current_span(token)

    ev = fake_agent.last_native.all_events[-1]
    assert ev.operation_name == django_instr._OPERATION_VIEW_FALLBACK
    assert "pinpoint.url_pattern" not in request.META


# ---------------------------------------------------------------------------
# _handler_wrapper — view span EVENT control-flow filtering
# (mirrors aiohttp's test_traced_handler_does_not_record_control_flow_*).
# ``raise Http404`` is Django's idiomatic "return a 404"; it propagates out of
# ``_get_response`` and Django converts it to a 404 response, so the view span
# event must NOT be flagged as an error.
# ---------------------------------------------------------------------------

class _FakeHttp404(Exception):
    """Mimics ``django.http.Http404`` — a plain exception (no status attr) that
    Django maps to a 404 response."""


# Match django's real class identity — name *and* defining module — so the
# module-gated classifier fires for the fake exactly as for the genuine type.
_FakeHttp404.__name__ = "Http404"
_FakeHttp404.__module__ = "django.http"


def test_handler_wrapper_does_not_record_control_flow_http404(fake_agent):
    """A view raising ``Http404`` is normal control flow — the view span event
    is created but NOT marked as an error."""
    from pinpoint import context as ppctx
    from pinpoint.instrumentations import django as django_instr

    request = _FakeRequest()

    def _get_response(req):
        # Routing already ran (the match is set) before the view raises 404.
        req.resolver_match = _ResolverMatch(
            func=_sample_view, route="items/<int:pk>/")
        raise _FakeHttp404()

    sp = fake_agent.new_span("root", "/items/42")
    token = ppctx.set_current_span(sp)
    try:
        with pytest.raises(_FakeHttp404):
            django_instr._handler_wrapper(_get_response, None, (request,), {})
    finally:
        ppctx.reset_current_span(token)

    assert any(e[0] == "event_start" for e in fake_agent.events)
    assert not any(e[0] == "event_error" for e in fake_agent.events)


def test_handler_wrapper_records_generic_exception_on_event(fake_agent):
    """A non-HTTP exception is a real failure and must flag the view event."""
    from pinpoint import context as ppctx
    from pinpoint.instrumentations import django as django_instr

    request = _FakeRequest()

    def _get_response(req):
        raise RuntimeError("boom")

    sp = fake_agent.new_span("root", "/items/42")
    token = ppctx.set_current_span(sp)
    try:
        with pytest.raises(RuntimeError, match="boom"):
            django_instr._handler_wrapper(_get_response, None, (request,), {})
    finally:
        ppctx.reset_current_span(token)

    assert any(e[0] == "event_error" for e in fake_agent.events)


def test_is_control_flow_exception_classifies_http404():
    from pinpoint.instrumentations import django as django_instr

    assert django_instr._is_control_flow_exception(_FakeHttp404()) is True
    assert django_instr._is_control_flow_exception(RuntimeError()) is False

    class Http404(Exception):  # not django's — must not match
        pass

    assert django_instr._is_control_flow_exception(Http404()) is False


def test_async_view_wrapper_falls_back_when_the_event_cannot_open(monkeypatch):
    """safe_wrapper cannot guard an async body — it only guards coroutine
    creation — so a failure opening the event would escape into the handler and
    500 a request the view would have served. Every sibling async wrapper
    (asgi, starlette, fastapi, falcon) guards this; Django must too."""
    class _BrokenSpan:
        _ended = False
        sampled = True
        _collect_url_stat = False

        def new_span_event(self, *args, **kwargs):
            raise RuntimeError("native span event failed")

    served = []

    async def get_response(req):
        served.append(req)
        return _FakeResponse(200)

    request = _FakeRequest()
    token = ppctx.set_current_span(_BrokenSpan())
    try:
        response = asyncio.run(django_instr._async_handler_wrapper(
            get_response, None, (request,), {},
        ))
    finally:
        ppctx.reset_current_span(token)

    assert served == [request]
    assert response.status_code == 200
