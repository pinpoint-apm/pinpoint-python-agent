# pinpoint-python-agent
# Copyright (c) 2026-present NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Real in-process web framework requests through the public instrumentation.

These tests complement the wrapper-level unit suite: the framework owns
routing, exception conversion, response iteration, and middleware ordering.
No Docker service is needed.
"""

from __future__ import annotations

import pytest

from tests.integration._recorder import RecordingAgent

pytestmark = pytest.mark.no_docker

# Django resolves ROOT_URLCONF as this module during its in-process tests.
urlpatterns = []


def _single_span_named(recorder, operation):
    matches = [span for span in recorder.spans
               if span.operation == operation]
    assert len(matches) == 1, (
        f"expected one {operation!r} span, got "
        f"{[span.operation for span in recorder.spans]!r}"
    )
    return matches[0]


@pytest.fixture
def recording_agent(recorder, monkeypatch):
    import pinpoint.agent as agent_mod
    from types import SimpleNamespace

    agent = RecordingAgent(recorder, config=SimpleNamespace(
        http_server_record_request_header=["X-Capture"],
        http_server_record_request_cookie=["session"],
        http_server_record_response_header=["X-Response"],
    ))
    monkeypatch.setattr(agent_mod, "_instance", agent)
    return recorder


@pytest.fixture
def flask_app(recording_agent):
    flask = pytest.importorskip("flask")
    from pinpoint.instrumentations import flask as flask_instr

    flask_instr.instrument()
    app = flask.Flask(__name__)
    app.config.update(TESTING=False, PROPAGATE_EXCEPTIONS=False)

    @app.get("/items/<int:item_id>")
    def item_detail(item_id):
        return {"item_id": item_id}

    @app.get("/missing")
    def missing():
        flask.abort(404)

    @app.get("/stream")
    def stream():
        def generate():
            yield b"first"
            yield b"second"

        return flask.Response(generate(), mimetype="text/plain")

    @app.get("/headers")
    def headers():
        response = flask.Response("headers")
        response.headers["X-Response"] = "response-value"
        return response

    return app


def test_flask_real_request_records_route_and_view(flask_app, recording_agent):
    response = flask_app.test_client().get(
        "/items/42",
        headers={"Pinpoint-TraceID": "upstream-trace"},
    )
    response.get_data()

    assert response.status_code == 200
    span = _single_span_named(recording_agent, "Flask HTTP Server")
    assert span.operation == "Flask HTTP Server"
    assert span.rpc == "/items/42"
    assert span.headers is not None
    assert span.url_pattern == "/items/<int:item_id>"
    assert span.method == "GET"
    assert span.status_code == 200
    assert span.ended
    view_events = [e for e in recording_agent.events if "item_detail" in e.operation]
    assert len(view_events) == 1 and view_events[0].ended


def test_flask_real_404_is_control_flow_not_view_error(
        flask_app, recording_agent):
    response = flask_app.test_client().get("/missing")
    response.get_data()

    assert response.status_code == 404
    span = _single_span_named(recording_agent, "Flask HTTP Server")
    assert span.status_code == 404
    assert span.url_pattern == "/missing"
    view_events = [e for e in recording_agent.events if "missing" in e.operation]
    assert len(view_events) == 1
    assert view_events[0].error is None
    assert view_events[0].ended


def test_flask_stream_uses_response_child_for_body_consumption(
        flask_app, recording_agent):
    response = flask_app.test_client().get("/stream", buffered=False)
    root = _single_span_named(recording_agent, "Flask HTTP Server")
    response_span = _single_span_named(
        recording_agent, "wsgi.response.iteration",
    )

    assert root.ended
    assert not response_span.ended
    assert b"".join(response.response) == b"firstsecond"
    assert response_span.ended
    assert root.url_pattern == "/stream"


def test_flask_real_request_records_headers_cookies_and_response(
        flask_app, recording_agent):
    from pinpoint.annotation import (
        ANNOTATION_HTTP_COOKIE,
        ANNOTATION_HTTP_REQUEST_HEADER,
        ANNOTATION_HTTP_RESPONSE_HEADER,
    )

    client = flask_app.test_client()
    client.set_cookie("session", "cookie-value")
    response = client.get("/headers", headers={"X-Capture": "request-value"})
    response.get_data()

    span = _single_span_named(recording_agent, "Flask HTTP Server")
    # Header recording lands as buffered two-string annotations keyed by the
    # HeaderType's annotation key, carrying the configured name.
    by_key: dict = {}
    for item in span.annotations:
        if item[0] == "strstr":
            by_key.setdefault(item[1], {})[item[2].lower()] = item[3]
    assert by_key[ANNOTATION_HTTP_REQUEST_HEADER]["x-capture"] == "request-value"
    assert by_key[ANNOTATION_HTTP_COOKIE]["session"] == "cookie-value"
    assert by_key[ANNOTATION_HTTP_RESPONSE_HEADER]["x-response"] == "response-value"


@pytest.fixture
def pyramid_app(recording_agent):
    pytest.importorskip("pyramid")
    from pyramid.config import Configurator
    from pyramid.httpexceptions import HTTPNotFound
    from pyramid.response import Response
    from pinpoint.instrumentations import pyramid as pyramid_instr

    # A wrapper-binding unit test deliberately restores Pyramid's original
    # functions after probing them while the process-wide instrumentor guard
    # can still remember an earlier installation. Drive the idempotent hook
    # directly so this real-app test always starts from the methods that are
    # actually installed at this point in the shared pytest process.
    pyramid_instr.PyramidInstrumentor()._instrument()

    def item_detail(request):
        return Response(f"item:{request.matchdict['item_id']}")

    def missing(request):
        raise HTTPNotFound()

    with Configurator() as config:
        config.add_route("item_detail", "/items/{item_id}")
        config.add_view(item_detail, route_name="item_detail")
        config.add_route("missing", "/missing")
        config.add_view(missing, route_name="missing")
        return config.make_wsgi_app()


def _wsgi_get(app, path, headers=None):
    from wsgiref.util import setup_testing_defaults

    environ = {}
    setup_testing_defaults(environ)
    environ.update(REQUEST_METHOD="GET", PATH_INFO=path)
    for key, value in (headers or {}).items():
        environ["HTTP_" + key.upper().replace("-", "_")] = value
    state = {}

    def start_response(status, response_headers, exc_info=None):
        state["status"] = int(status.split(" ", 1)[0])
        state["headers"] = response_headers

    body_iter = app(environ, start_response)
    try:
        body = b"".join(body_iter)
    finally:
        close = getattr(body_iter, "close", None)
        if close is not None:
            close()
    return state["status"], body


def test_pyramid_real_request_records_route_and_view(
        pyramid_app, recording_agent):
    status, body = _wsgi_get(
        pyramid_app, "/items/42",
        {"Pinpoint-TraceID": "upstream-trace"},
    )

    assert status == 200 and body == b"item:42"
    span = _single_span_named(recording_agent, "Pyramid HTTP Server")
    assert span.operation == "Pyramid HTTP Server"
    assert span.headers is not None
    assert span.url_pattern == "/items/{item_id}"
    assert span.status_code == 200 and span.ended
    view_events = [e for e in recording_agent.events if "item_detail" in e.operation]
    assert len(view_events) == 1 and view_events[0].ended


def test_pyramid_real_404_is_control_flow_not_view_error(
        pyramid_app, recording_agent):
    status, _body = _wsgi_get(pyramid_app, "/missing")

    assert status == 404
    span = _single_span_named(recording_agent, "Pyramid HTTP Server")
    assert span.url_pattern == "/missing"
    assert span.status_code == 404 and span.ended
    view_events = [e for e in recording_agent.events if "missing" in e.operation]
    assert view_events
    assert all(e.error is None and e.ended for e in view_events)


@pytest.fixture
def starlette_app(recording_agent):
    pytest.importorskip("starlette")
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route
    from pinpoint.instrumentations import starlette as starlette_instr

    starlette_instr.instrument()

    async def item_detail(request):
        return JSONResponse({"item_id": request.path_params["item_id"]})

    return Starlette(routes=[Route("/items/{item_id}", item_detail)])


def test_starlette_real_request_records_route_and_endpoint(
        starlette_app, recording_agent):
    from starlette.testclient import TestClient

    with TestClient(starlette_app) as client:
        response = client.get(
            "/items/42", headers={"Pinpoint-TraceID": "upstream-trace"})

    assert response.status_code == 200
    spans = [s for s in recording_agent.spans
             if s.operation == "Starlette HTTP Server"]
    assert len(spans) == 1
    span = spans[0]
    assert span.headers is not None
    assert span.url_pattern == "/items/{item_id}"
    assert span.method == "GET"
    assert span.status_code == 200 and span.ended
    events = [e for e in recording_agent.events if "item_detail" in e.operation]
    assert len(events) == 1 and events[0].ended


@pytest.fixture
def django_app(recording_agent):
    django = pytest.importorskip("django")
    from django.conf import settings

    if not settings.configured:
        settings.configure(
            SECRET_KEY="pinpoint-integration-test",
            ROOT_URLCONF=__name__,
            ALLOWED_HOSTS=["testserver"],
            MIDDLEWARE=[],
            DEBUG=False,
        )
        django.setup()

    from django.http import Http404, HttpResponse, StreamingHttpResponse
    from django.urls import path
    from pinpoint.instrumentations import django as django_instr

    django_instr.instrument()

    def item_detail(request, item_id):
        return HttpResponse(f"item:{item_id}")

    def missing(request):
        raise Http404("missing")

    def stream(request):
        def generate():
            yield b"first"
            yield b"second"

        return StreamingHttpResponse(generate(), content_type="text/plain")

    async def async_item_detail(request, item_id):
        return HttpResponse(f"async-item:{item_id}")

    urlpatterns[:] = [
        path("items/<int:item_id>/", item_detail, name="item-detail"),
        path(
            "async-items/<int:item_id>/",
            async_item_detail,
            name="async-item-detail",
        ),
        path("missing/", missing, name="missing"),
        path("stream/", stream, name="stream"),
    ]
    from django.core.wsgi import get_wsgi_application

    return get_wsgi_application()


def test_django_real_request_records_route_and_view(
        django_app, recording_agent):
    status, body = _wsgi_get(
        django_app,
        "/items/42/",
        {"Pinpoint-TraceID": "upstream-trace"},
    )

    assert status == 200 and body == b"item:42"
    spans = [s for s in recording_agent.spans
             if s.operation == "Django HTTP Server"]
    assert len(spans) == 1
    span = spans[0]
    assert span.headers is not None
    assert span.url_pattern == "items/<int:item_id>/"
    assert span.status_code == 200 and span.ended
    events = [e for e in recording_agent.events if "item_detail" in e.operation]
    assert len(events) == 1 and events[0].ended


def test_django_real_http404_is_control_flow_not_view_error(
        django_app, recording_agent):
    status, _body = _wsgi_get(django_app, "/missing/")

    assert status == 404
    span = _single_span_named(recording_agent, "Django HTTP Server")
    assert span.url_pattern == "missing/"
    assert span.status_code == 404 and span.ended
    events = [e for e in recording_agent.events if "missing" in e.operation]
    assert len(events) == 1
    assert events[0].error is None and events[0].ended


def test_django_stream_uses_response_child_for_body_consumption(
        django_app, recording_agent):
    from wsgiref.util import setup_testing_defaults

    environ = {}
    setup_testing_defaults(environ)
    environ.update(REQUEST_METHOD="GET", PATH_INFO="/stream/")

    def start_response(_status, _headers, _exc_info=None):
        return None

    body_iter = django_app(environ, start_response)
    root = _single_span_named(recording_agent, "Django HTTP Server")
    response_span = _single_span_named(
        recording_agent, "wsgi.response.iteration",
    )
    assert root.ended
    assert not response_span.ended
    try:
        assert b"".join(body_iter) == b"firstsecond"
    finally:
        close = getattr(body_iter, "close", None)
        if close is not None:
            close()
    assert response_span.ended
    assert root.url_pattern == "stream/"


@pytest.fixture
def django_asgi_app(django_app):
    from django.core.asgi import get_asgi_application

    return get_asgi_application()


async def _asgi_get(app, path, headers=None):
    import asyncio

    receive_queue = asyncio.Queue()
    await receive_queue.put({
        "type": "http.request",
        "body": b"",
        "more_body": False,
    })
    sent = []

    async def receive():
        return await receive_queue.get()

    async def send(message):
        sent.append(message)

    raw_headers = [
        (str(key).lower().encode("latin-1"), str(value).encode("latin-1"))
        for key, value in (headers or {}).items()
    ]
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": [(b"host", b"testserver"), *raw_headers],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    await app(scope, receive, send)
    status = next(
        message["status"] for message in sent
        if message["type"] == "http.response.start"
    )
    body = b"".join(
        message.get("body", b"") for message in sent
        if message["type"] == "http.response.body"
    )
    return status, body


def test_django_real_asgi_request_records_route_and_async_view(
        django_asgi_app, recording_agent):
    import asyncio

    status, body = asyncio.run(_asgi_get(
        django_asgi_app,
        "/async-items/42/",
        {"Pinpoint-TraceID": "upstream-trace"},
    ))

    assert status == 200 and body == b"async-item:42"
    spans = [s for s in recording_agent.spans
             if s.operation == "Django HTTP Server"]
    assert len(spans) == 1
    span = spans[0]
    assert [s.operation for s in recording_agent.spans] == [
        "Django HTTP Server",
    ]
    assert span.headers is not None
    assert span.url_pattern == "async-items/<int:item_id>/"
    assert span.status_code == 200 and span.ended
    events = [
        event for event in recording_agent.events
        if "async_item_detail" in event.operation
    ]
    assert len(events) == 1 and events[0].ended
    assert events[0].span.record is span


# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------

@pytest.fixture
def fastapi_app(recording_agent):
    import threading

    fastapi = pytest.importorskip("fastapi")
    from pinpoint.context import current_span
    from pinpoint.instrumentations import fastapi as fastapi_instr

    fastapi_instr.instrument()
    app = fastapi.FastAPI()

    @app.get("/items/{item_id}")
    def item_detail(item_id: int):
        return {"item_id": item_id}

    @app.get("/missing")
    def missing():
        raise fastapi.HTTPException(status_code=404)

    @app.get("/boom")
    def boom():
        raise RuntimeError("boom-it")

    app.state.loop_thread_idents = []
    app.state.teardown_thread_idents = []

    def session_dependency():
        try:
            yield "session-value"
        finally:
            # Generator-dependency teardown: FastAPI dispatches cm.__exit__
            # straight to an anyio worker, where the wrapped
            # contextmanager_in_threadpool must have installed a linked child
            # as the current span — otherwise this trace lands on the root.
            app.state.teardown_thread_idents.append(threading.get_ident())
            with current_span().new_span_event("dependency.teardown"):
                pass

    @app.get("/with-dependency")
    async def with_dependency(
            session: str = fastapi.Depends(session_dependency)):
        app.state.loop_thread_idents.append(threading.get_ident())
        return {"session": session}

    @app.get("/with-dependency-boom")
    async def with_dependency_boom(
            _session: str = fastapi.Depends(session_dependency)):
        raise RuntimeError("boom-dep")

    return app


def test_fastapi_real_request_records_route_and_endpoint(
        fastapi_app, recording_agent):
    from fastapi.testclient import TestClient

    with TestClient(fastapi_app) as client:
        response = client.get(
            "/items/42", headers={"Pinpoint-TraceID": "upstream-trace"})

    assert response.status_code == 200
    assert response.json() == {"item_id": 42}
    spans = [s for s in recording_agent.spans
             if s.operation == "FastAPI HTTP Server"]
    assert len(spans) == 1
    span = spans[0]
    assert span.headers is not None
    assert span.url_pattern == "/items/{item_id}"
    assert span.method == "GET"
    assert span.status_code == 200 and span.ended
    events = [e for e in recording_agent.events if "item_detail" in e.operation]
    assert len(events) == 1 and events[0].ended
    assert events[0].error is None


def test_fastapi_http_exception_is_control_flow_not_endpoint_error(
        fastapi_app, recording_agent):
    from fastapi.testclient import TestClient

    with TestClient(fastapi_app) as client:
        response = client.get("/missing")

    assert response.status_code == 404
    span = _single_span_named(recording_agent, "FastAPI HTTP Server")
    assert span.url_pattern == "/missing"
    assert span.status_code == 404 and span.ended
    assert span.error is None


def test_fastapi_unhandled_exception_records_span_error(
        fastapi_app, recording_agent):
    from fastapi.testclient import TestClient

    with TestClient(fastapi_app, raise_server_exceptions=False) as client:
        response = client.get("/boom")

    assert response.status_code == 500
    span = _single_span_named(recording_agent, "FastAPI HTTP Server")
    assert span.status_code == 500 and span.ended
    assert span.error is not None
    assert span.error[0] == "RuntimeError"


def test_fastapi_generator_dependency_teardown_gets_worker_handoff(
        fastapi_app, recording_agent):
    from fastapi.testclient import TestClient

    with TestClient(fastapi_app) as client:
        response = client.get("/with-dependency")

    assert response.status_code == 200
    assert response.json() == {"session": "session-value"}
    root = _single_span_named(recording_agent, "FastAPI HTTP Server")
    assert root.status_code == 200 and root.ended

    # Dependency setup (cm.__enter__) rides FastAPI's wrapped
    # run_in_threadpool alias and gets the generic threadpool child.
    enter_spans = [s for s in recording_agent.spans
                   if s.operation == "starlette.threadpool"]
    assert enter_spans and all(s.ended for s in enter_spans)

    # Teardown (cm.__exit__) bypasses that alias, so it must receive its own
    # explicit hand-off: a link event on the root plus an async child span.
    link_events = recording_agent.events_named("fastapi.contextmanager.exit")
    assert len(link_events) == 1 and link_events[0].ended
    assert link_events[0].span.record is root
    exit_span = _single_span_named(
        recording_agent, "fastapi.contextmanager.exit")
    assert exit_span.ended

    # The teardown body ran on a worker thread with the hand-off child
    # current — its trace event attaches to the async child, not the root.
    teardown_events = recording_agent.events_named("dependency.teardown")
    assert len(teardown_events) == 1 and teardown_events[0].ended
    assert teardown_events[0].span.record is exit_span
    [loop_ident] = fastapi_app.state.loop_thread_idents
    [teardown_ident] = fastapi_app.state.teardown_thread_idents
    assert teardown_ident != loop_ident


def test_fastapi_generator_dependency_teardown_handoff_when_endpoint_raises(
        fastapi_app, recording_agent):
    from fastapi.testclient import TestClient

    with TestClient(fastapi_app, raise_server_exceptions=False) as client:
        response = client.get("/with-dependency-boom")

    # The endpoint exception is thrown into the dependency generator through
    # FastAPI's except-branch dispatch; the hand-off must survive that path.
    assert response.status_code == 500
    root = _single_span_named(recording_agent, "FastAPI HTTP Server")
    assert root.status_code == 500 and root.ended
    assert root.error is not None and root.error[0] == "RuntimeError"
    exit_span = _single_span_named(
        recording_agent, "fastapi.contextmanager.exit")
    assert exit_span.ended
    teardown_events = recording_agent.events_named("dependency.teardown")
    assert len(teardown_events) == 1 and teardown_events[0].ended
    assert teardown_events[0].span.record is exit_span


# ---------------------------------------------------------------------------
# Tornado
# ---------------------------------------------------------------------------

@pytest.fixture
def tornado_app(recording_agent):
    pytest.importorskip("tornado")
    import tornado.web as web
    from pinpoint.instrumentations import tornado as tornado_instr

    tornado_instr.instrument()

    class ItemHandler(web.RequestHandler):
        def get(self, item_id):
            self.write(f"item:{item_id}")

    class MissingHandler(web.RequestHandler):
        def get(self):
            raise web.HTTPError(404)

    class BoomHandler(web.RequestHandler):
        def get(self):
            raise RuntimeError("boom-it")

    return web.Application([
        (r"/items/([0-9]+)", ItemHandler),
        (r"/missing", MissingHandler),
        (r"/boom", BoomHandler),
    ])


def _tornado_fetch(app, path, headers=None):
    """Serve ``app`` on an unused port and fetch ``path`` once, both on a
    private asyncio loop — the real HTTP server/client round-trip without
    tornado.testing's unittest scaffolding."""
    import asyncio

    async def main():
        from tornado.httpclient import AsyncHTTPClient
        from tornado.httpserver import HTTPServer
        from tornado.testing import bind_unused_port

        sock, port = bind_unused_port()
        server = HTTPServer(app)
        server.add_sockets([sock])
        client = AsyncHTTPClient()
        try:
            response = await client.fetch(
                f"http://127.0.0.1:{port}{path}",
                headers=headers, raise_error=False)
            return response.code, response.body
        finally:
            client.close()
            server.stop()
            await server.close_all_connections()

    return asyncio.run(main())


def test_tornado_real_request_records_span_and_handler_event(
        tornado_app, recording_agent):
    status, body = _tornado_fetch(
        tornado_app, "/items/42", {"Pinpoint-TraceID": "upstream-trace"})

    assert status == 200 and body == b"item:42"
    spans = [s for s in recording_agent.spans
             if s.operation == "Tornado HTTP Server"]
    assert len(spans) == 1
    span = spans[0]
    assert span.headers is not None
    # Tornado records url_stat against the raw path (no template exposed).
    assert span.url_pattern == "/items/42"
    assert span.method == "GET"
    assert span.status_code == 200 and span.ended
    events = recording_agent.events_named("ItemHandler.get")
    assert len(events) == 1 and events[0].ended
    assert events[0].error is None


def test_tornado_http_error_is_control_flow_not_span_error(
        tornado_app, recording_agent):
    status, _body = _tornado_fetch(tornado_app, "/missing")

    assert status == 404
    span = recording_agent.spans[-1]
    assert span.status_code == 404 and span.ended
    assert span.error is None
    events = recording_agent.events_named("MissingHandler.get")
    assert len(events) == 1 and events[0].ended
    assert events[0].error is None


def test_tornado_unhandled_exception_records_span_error(
        tornado_app, recording_agent):
    status, _body = _tornado_fetch(tornado_app, "/boom")

    assert status == 500
    span = recording_agent.spans[-1]
    assert span.status_code == 500 and span.ended
    assert span.error is not None
    assert span.error[0] == "RuntimeError"
    # Tornado consumes the handler exception inside ``_execute`` (it routes it
    # through ``log_exception`` and serves the 500 itself), so the error lands
    # on the span via the log_exception hook — the handler event just ends.
    events = recording_agent.events_named("BoomHandler.get")
    assert len(events) == 1 and events[0].ended


# ---------------------------------------------------------------------------
# aiohttp server
# ---------------------------------------------------------------------------

@pytest.fixture
def aiohttp_web(recording_agent):
    pytest.importorskip("aiohttp")
    from aiohttp import web
    from pinpoint.instrumentations import aiohttp as aiohttp_instr

    # Server-side only: the outbound TestClient request in these tests runs
    # with no current span, so the client wrapper (exercised against a real
    # echo server in test_http_clients.py) would be inert here anyway.
    aiohttp_instr.instrument_server()
    return web


def _aiohttp_run(web, build_app, path, headers=None):
    """Serve the app on aiohttp's own test server and issue one real request
    over a socket; returns (status, body_text)."""
    import asyncio

    async def main():
        from aiohttp.test_utils import TestClient, TestServer

        app = build_app()
        async with TestClient(TestServer(app)) as client:
            response = await client.get(path, headers=headers)
            return response.status, await response.text()

    return asyncio.run(main())


def test_aiohttp_real_request_records_route_and_handler_event(
        aiohttp_web, recording_agent):
    web = aiohttp_web

    async def item_detail(request):
        return web.Response(text=f"item:{request.match_info['item_id']}")

    def build_app():
        app = web.Application()
        app.router.add_get("/items/{item_id}", item_detail)
        return app

    status, body = _aiohttp_run(
        web, build_app, "/items/42", {"Pinpoint-TraceID": "upstream-trace"})

    assert status == 200 and body == "item:42"
    spans = [s for s in recording_agent.spans
             if s.operation == "aiohttp HTTP Server"]
    assert len(spans) == 1
    span = spans[0]
    assert span.headers is not None
    assert span.url_pattern == "/items/{item_id}"
    assert span.method == "GET"
    assert span.status_code == 200 and span.ended
    events = [e for e in recording_agent.events
              if "item_detail" in e.operation]
    assert len(events) == 1 and events[0].ended
    assert events[0].error is None


def test_aiohttp_http_not_found_is_control_flow_not_handler_error(
        aiohttp_web, recording_agent):
    web = aiohttp_web

    async def missing(request):
        raise web.HTTPNotFound()

    def build_app():
        app = web.Application()
        app.router.add_get("/missing", missing)
        return app

    status, _body = _aiohttp_run(web, build_app, "/missing")

    assert status == 404
    span = recording_agent.spans[-1]
    assert span.status_code == 404 and span.ended
    events = [e for e in recording_agent.events if "missing" in e.operation]
    assert len(events) == 1 and events[0].ended
    assert events[0].error is None


def test_aiohttp_unhandled_exception_records_handler_error(
        aiohttp_web, recording_agent):
    web = aiohttp_web

    async def boom(request):
        raise RuntimeError("boom-it")

    def build_app():
        app = web.Application()
        app.router.add_get("/boom", boom)
        return app

    status, _body = _aiohttp_run(web, build_app, "/boom")

    assert status == 500
    span = recording_agent.spans[-1]
    assert span.status_code == 500 and span.ended
    events = [e for e in recording_agent.events if "boom" in e.operation]
    assert len(events) == 1 and events[0].ended
    assert events[0].error is not None
    assert events[0].error[0] == "RuntimeError"


# ---------------------------------------------------------------------------
# Falcon (WSGI)
# ---------------------------------------------------------------------------

@pytest.fixture
def falcon_app(recording_agent):
    falcon = pytest.importorskip("falcon")
    from pinpoint.instrumentations import falcon as falcon_instr

    falcon_instr.instrument()

    class ItemResource:
        def on_get(self, req, resp, item_id):
            resp.text = f"item:{item_id}"

    class MissingResource:
        def on_get(self, req, resp):
            raise falcon.HTTPNotFound()

    app = falcon.App()
    app.add_route("/items/{item_id}", ItemResource())
    app.add_route("/missing", MissingResource())
    return app


def test_falcon_real_request_records_route_and_responder(
        falcon_app, recording_agent):
    status, body = _wsgi_get(
        falcon_app, "/items/42", {"Pinpoint-TraceID": "upstream-trace"})

    assert status == 200 and body == b"item:42"
    spans = [s for s in recording_agent.spans
             if s.operation == "Falcon HTTP Server"]
    assert len(spans) == 1
    span = spans[0]
    assert span.headers is not None
    assert span.url_pattern == "/items/{item_id}"
    assert span.status_code == 200 and span.ended
    events = [e for e in recording_agent.events
              if "ItemResource.on_get" in e.operation]
    assert len(events) == 1 and events[0].ended
    assert events[0].error is None


def test_falcon_http_not_found_is_control_flow_not_responder_error(
        falcon_app, recording_agent):
    status, _body = _wsgi_get(falcon_app, "/missing")

    assert status == 404
    span = _single_span_named(recording_agent, "Falcon HTTP Server")
    assert span.url_pattern == "/missing"
    assert span.status_code == 404 and span.ended
    assert span.error is None
    events = [e for e in recording_agent.events
              if "MissingResource.on_get" in e.operation]
    assert len(events) == 1 and events[0].ended
    assert events[0].error is None
