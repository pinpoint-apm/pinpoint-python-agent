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

"""Every root span the agent opens ends with at least one span event.

A span event is what the UI renders as the transaction's call tree, so a root
span with none shows up as an empty transaction. The framework integrations
(Flask, Django, FastAPI, Starlette, Pyramid, Falcon) and the tornado/aiohttp
server instrumentations name the handler that ran, which covers them; this
module pins the invariant for the entry points that have no framework layer to
do it for them.

Only the *agent-owned* entry points are covered. ``@pinpoint.span`` is the
user's own API — the event there is theirs to add with ``@pinpoint.spanevent``.
"""

from __future__ import annotations

import asyncio

import pytest

from _fakes import http_scope, wsgi_environ
from pinpoint.instrumentations.asgi import PinpointASGIMiddleware
from pinpoint.instrumentations.wsgi import PinpointWSGIMiddleware


# Module-level so ``__qualname__`` is the bare name a real application has,
# not pytest's ``<test>.<locals>.application``.
def application(environ, start_response):
    start_response("200 OK", [("Content-Type", "text/plain")])
    return [b"hello"]


def failing_application(environ, start_response):
    raise RuntimeError("boom")


async def async_application(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"hello"})


async def _noop_send(message):
    return None


def _events(agent):
    assert agent.native_spans, "no root span was opened"
    return [(s.op, [e.op for e in s.all_events]) for s in agent.native_spans]


def test_bare_wsgi_app_root_span_has_an_event(fake_agent):
    """A buffered list body returns before any child work happens, so nothing
    else would put an event on the span."""
    wrapped = PinpointWSGIMiddleware(application)
    body = wrapped(wsgi_environ("/ping"),
                   lambda status, headers: None)
    list(body)

    (op, events), = _events(fake_agent)
    assert op == "WSGI HTTP Server"
    assert events == ["application"]


def test_bare_asgi_app_root_span_has_an_event(fake_agent):
    wrapped = PinpointASGIMiddleware(async_application)
    asyncio.run(wrapped(http_scope("/ping"), lambda: None, _noop_send))

    (op, events), = _events(fake_agent)
    assert op == "ASGI HTTP Server"
    assert events == ["async_application"]


def test_wsgi_entry_event_survives_an_app_that_raises(fake_agent):
    """The event has to be closed on the failure path too, or the span ends
    with an event still open on its stack."""
    wrapped = PinpointWSGIMiddleware(failing_application)
    with pytest.raises(RuntimeError):
        wrapped(wsgi_environ("/boom"),
                lambda status, headers: None)

    (op, events), = _events(fake_agent)
    assert events == ["failing_application"]
    assert fake_agent.native_spans[0].end_called


def test_grpc_server_span_has_an_event(fake_agent):
    """Go's ppgrpc opens NewSpanEvent(info.FullMethod); a handler that calls
    nothing downstream would otherwise leave the span bare."""
    grpc = pytest.importorskip("grpc")
    from collections import namedtuple

    from pinpoint.instrumentations.grpc import _PinpointServerInterceptor

    details = namedtuple("D", ["method", "invocation_metadata"])

    class _Ctx:
        def invocation_metadata(self):
            return ()

        def peer(self):
            return "ipv4:10.0.0.1:5"

    handler = _PinpointServerInterceptor().intercept_service(
        lambda _d: grpc.unary_unary_rpc_method_handler(lambda r, c: "ok"),
        details(method="/svc/Method", invocation_metadata=()),
    )
    handler.unary_unary("REQ", _Ctx())

    (op, events), = _events(fake_agent)
    assert op == "gRPC Server"
    assert events == ["/svc/Method"]


def test_kafka_delivery_span_has_an_event(fake_agent):
    """Delivery-only spans close immediately, so no child work ever lands on
    them — same one-event scope the AMQP consumer already opens."""
    from pinpoint.instrumentations._kafka import open_consume_span

    open_consume_span(fake_agent, "t", 0, 5, (), "broker:9092")

    (op, events), = _events(fake_agent)
    assert op == "Kafka Consumer Invocation"
    assert events == ["kafka.consume"]


def test_framework_root_span_keeps_exactly_one_handler_event(fake_agent):
    """The entry event is for bare apps only.

    Starlette (and FastAPI through it) builds its app *through*
    ``PinpointASGIMiddleware``, so without the ``entry_event=False`` opt-out
    every framework request would carry a second, outer event named after the
    middleware stack — noise, since the endpoint wrapper already names the
    handler that ran."""
    pytest.importorskip("starlette")
    from starlette.applications import Starlette
    from starlette.routing import Route
    from starlette.testclient import TestClient

    from pinpoint.instrumentations import starlette as starlette_instr

    starlette_instr.instrument()
    TestClient(Starlette(routes=[Route("/ping", endpoint)])).get("/ping")

    (op, events), = _events(fake_agent)
    assert op == "Starlette HTTP Server"
    assert events == ["endpoint"]


async def endpoint(request):
    from starlette.responses import JSONResponse
    return JSONResponse({"ok": True})
