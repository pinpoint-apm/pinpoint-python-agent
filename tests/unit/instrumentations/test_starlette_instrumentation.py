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

"""Starlette instrumentation.

Tests the two wrappers exposed by ``pinpoint.instrumentations.starlette``:

- ``_build_middleware_stack_wrapper`` — wraps Starlette's middleware stack so
  the Pinpoint ASGI middleware sits at the outside.
- ``_route_handle_wrapper`` — surfaces the matched Route.path into
  ``scope["pinpoint.url_pattern"]`` and emits endpoint span events.

We don't import Starlette itself; the test exercises the wrappers against
in-memory fakes so it stays fast and dep-free.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import threading
import types

import wrapt

from pinpoint import context as ppctx
from pinpoint.instrumentations import starlette as starlette_instr
from pinpoint.instrumentations._util import (
    mark_pinpoint_wrapper,
    restore_pinpoint_target,
)
from pinpoint.instrumentations.asgi import PinpointASGIMiddleware


def test_build_middleware_stack_wrapper_wraps_with_asgi_middleware():
    """build_middleware_stack returns the ASGI app; the wrapper should put
    PinpointASGIMiddleware on the outside."""

    async def inner(scope, receive, send):  # original stack
        await send({"type": "marker"})

    def fake_build_stack():
        return inner

    out = starlette_instr._build_middleware_stack_wrapper(
        fake_build_stack, instance=None, args=(), kwargs={},
    )
    assert isinstance(out, PinpointASGIMiddleware)
    assert out._operation == "Starlette HTTP Server"
    # Re-running the wrapped stack should still produce the marker.
    captured = []

    async def send(msg): captured.append(msg)

    async def receive(): return {}

    asyncio.run(out({"type": "lifespan"}, receive, send))
    assert captured == [{"type": "marker"}]


def test_route_handle_wrapper_lifts_route_path_into_scope_without_span():
    """Route.handle should stash the template even for unsampled/no-span paths."""
    class _FakeRoute:
        path = "/items/{id}"

    captured_scope = {}

    async def wrapped(scope, receive, send):
        captured_scope.update(scope)
        return "ok"

    async def receive(): return {}

    async def send(_msg): pass

    scope = {"type": "http", "path": "/items/42", "method": "GET"}
    out = asyncio.run(starlette_instr._route_handle_wrapper(
        wrapped, instance=_FakeRoute(), args=(scope, receive, send), kwargs={},
    ))
    assert out == "ok"
    assert captured_scope.get("pinpoint.url_pattern") == "/items/{id}"


def test_route_handle_wrapper_preserves_existing_url_pattern():
    class _FakeRoute:
        path = "/items/{id}"

    async def wrapped(scope, receive, send):
        return "ok"

    async def receive(): return {}

    async def send(_msg): pass

    scope = {
        "type": "http",
        "path": "/items/42",
        "method": "GET",
        "pinpoint.url_pattern": "/already/{set}",
    }
    asyncio.run(starlette_instr._route_handle_wrapper(
        wrapped, instance=_FakeRoute(), args=(scope, receive, send), kwargs={},
    ))

    assert scope.get("pinpoint.url_pattern") == "/already/{set}"


def test_route_handle_wrapper_does_not_stash_pattern_for_non_http_scope():
    """Lifespan / websocket scopes go through untouched — no scope mutation."""
    class _FakeRoute:
        path = "/items/{id}"

    seen = {}

    async def wrapped(scope, receive, send):
        seen["scope_type"] = scope["type"]
        return "ok"

    async def receive(): return {}

    async def send(_): pass

    scope = {"type": "lifespan"}
    out = asyncio.run(starlette_instr._route_handle_wrapper(
        wrapped, instance=_FakeRoute(), args=(scope, receive, send), kwargs={},
    ))
    assert out == "ok"
    assert seen["scope_type"] == "lifespan"
    assert "pinpoint.url_pattern" not in scope


def test_instrumentor_does_not_wrap_starlette_call(monkeypatch):
    calls = []

    monkeypatch.setattr(
        starlette_instr,
        "wrap",
        lambda module, target, wrapper: calls.append((module, target)),
    )

    starlette_instr.StarletteInstrumentor()._instrument()

    assert ("starlette.applications", "Starlette.__call__") not in calls
    assert ("starlette.applications", "Starlette.build_middleware_stack") in calls
    assert ("starlette.routing", "Route.handle") in calls
    assert ("starlette.concurrency", "run_in_threadpool") in calls
    assert ("starlette.routing", "run_in_threadpool") in calls
    assert ("starlette.background", "run_in_threadpool") in calls
    assert ("starlette.endpoints", "run_in_threadpool") in calls
    assert ("starlette._exception_handler", "run_in_threadpool") in calls
    assert ("starlette.middleware.errors", "run_in_threadpool") in calls
    assert ("starlette.middleware.exceptions", "run_in_threadpool") in calls


def test_run_in_threadpool_hands_distinct_child_to_worker():
    owner_ident = threading.get_ident()

    class _WorkerSpan:
        sampled = True

        def __init__(self):
            self.enter_ident = None
            self.ended = False
            self.token = None

        def __enter__(self):
            self.enter_ident = threading.get_ident()
            self.token = ppctx.set_current_span(self)
            return self

        def __exit__(self, *_args):
            ppctx.reset_current_span(self.token)
            self.ended = True

    child = _WorkerSpan()

    class _RootSpan:
        sampled = True

        def new_span_event(self, _operation, service_type=None):
            return contextlib.nullcontext()

        def new_async_span(self, _operation, _async_id=0, _async_sequence=0):
            return child

    root = _RootSpan()
    seen = []

    def endpoint(value):
        seen.append((ppctx.current_span(), threading.get_ident()))
        return value + 1

    async def fake_pool(fn, *args, **kwargs):
        # asyncio.to_thread copies the caller's ContextVars, matching anyio's
        # worker behavior used by Starlette/FastAPI.
        return await asyncio.to_thread(fn, *args, **kwargs)

    token = ppctx.set_current_span(root)  # type: ignore[arg-type]
    try:
        result = asyncio.run(starlette_instr._run_in_threadpool_wrapper(
            fake_pool, instance=None, args=(endpoint, 41), kwargs={},
        ))
    finally:
        ppctx.reset_current_span(token)

    assert result == 42
    assert seen == [(child, child.enter_ident)]
    assert child.enter_ident != owner_ident
    assert child.ended is True


def test_route_handle_wrapper_no_op_for_unsampled_span(monkeypatch):
    """Unsampled transactions should skip route classification and event setup."""
    class _UnsampledSpan:
        sampled = False

        def new_span_event(self, *_args, **_kwargs):
            raise AssertionError("unsampled span must not open route events")

    def fail_fastapi_check(_instance):
        raise AssertionError("unsampled span must not classify route type")

    monkeypatch.setattr(starlette_instr, "_is_fastapi_api_route", fail_fastapi_check)

    async def wrapped(*args, **kwargs):
        return "ok"

    token = ppctx.set_current_span(_UnsampledSpan())  # type: ignore[arg-type]
    try:
        out = asyncio.run(starlette_instr._route_handle_wrapper(
            wrapped, instance=object(), args=(), kwargs={},
        ))
    finally:
        ppctx.reset_current_span(token)

    assert out == "ok"


def test_route_handle_wrapper_composes_with_later_wrapper():
    """A wrapper another library installs on ``Route.handle`` *after* Pinpoint
    must still run for FastAPI's APIRoute.

    Pinpoint leaves ``APIRoute.handle`` alone, so APIRoute keeps resolving
    ``handle`` dynamically through the MRO and a later wrapper on the class
    attribute composes on top of Pinpoint's — both run, and Pinpoint's wrapper
    delegates straight through for APIRoute (the fastapi instrumentation
    records the endpoint elsewhere).
    """
    order = []

    async def base_handle(self, scope, receive, send):
        order.append("base")
        return "ok"

    class Route:
        handle = base_handle

    class APIRoute(Route):
        pass

    # Pinpoint wraps Route.handle first.
    def pinpoint_wrap(wrapped, instance, args, kwargs):
        order.append("pinpoint")
        return wrapped(*args, **kwargs)

    wrapt.wrap_function_wrapper(Route, "handle", pinpoint_wrap)

    # A second library wraps the SAME attribute afterwards.
    def other_wrap(wrapped, instance, args, kwargs):
        order.append("other")
        return wrapped(*args, **kwargs)

    wrapt.wrap_function_wrapper(Route, "handle", other_wrap)

    async def receive(): return {}

    async def send(_): pass

    # APIRoute inherits handle via the MRO — invoking it through an APIRoute
    # instance must fire both wrappers plus the base.
    out = asyncio.run(APIRoute().handle({"type": "http"}, receive, send))

    assert out == "ok"
    assert order == ["other", "pinpoint", "base"]


def test_uninstrument_restores_route_handle(monkeypatch):
    """After uninstrument, pinpoint's own wrapped class attributes are restored.

    The wrappers are stamped exactly as ``_util.wrap`` stamps them, so
    uninstrument's ownership check recognises them as pinpoint's and peels them
    off (an unstamped wrapper would be a foreign one and left in place).
    """
    async def original_handle(self, scope, receive, send):
        return "ok"

    def original_build(self):
        return "stack"

    class Route:
        handle = original_handle

    class Starlette:
        build_middleware_stack = original_build

    monkeypatch.setitem(
        sys.modules, "starlette.routing", types.SimpleNamespace(Route=Route),
    )
    monkeypatch.setitem(
        sys.modules, "starlette.applications",
        types.SimpleNamespace(Starlette=Starlette),
    )

    wrapt.wrap_function_wrapper(
        Route, "handle", mark_pinpoint_wrapper(lambda w, i, a, k: w(*a, **k)),
    )
    wrapt.wrap_function_wrapper(
        Starlette, "build_middleware_stack",
        mark_pinpoint_wrapper(lambda w, i, a, k: w(*a, **k)),
    )
    assert hasattr(Route.__dict__["handle"], "__wrapped__")

    # Exactly what BaseInstrumentor's registry restore drives per wrap target.
    restore_pinpoint_target("starlette.routing", "Route.handle")
    restore_pinpoint_target(
        "starlette.applications", "Starlette.build_middleware_stack")

    assert Route.__dict__["handle"] is original_handle
    assert Starlette.__dict__["build_middleware_stack"] is original_build


def test_uninstrument_keeps_foreign_wrapper_layered_on_route_handle(monkeypatch):
    """When another library wraps ``Route.handle`` *after* pinpoint,
    uninstrument must remove only pinpoint's layer and leave the foreign
    wrapper installed and functional.

    Without the ownership check, reassigning ``Route.handle =
    current.__wrapped__`` would strip the *foreign* wrapper and reinstall
    pinpoint's — the opposite of uninstrument.
    """
    calls: list = []

    async def original_handle(self, scope, receive, send):
        calls.append("base")
        return "ok"

    class Route:
        handle = original_handle

    monkeypatch.setitem(
        sys.modules, "starlette.routing", types.SimpleNamespace(Route=Route),
    )
    # No Starlette in this namespace: the build_middleware_stack restore is a
    # no-op, keeping the test focused on Route.handle.
    monkeypatch.setitem(
        sys.modules, "starlette.applications", types.SimpleNamespace(),
    )

    def pinpoint_wrapper(wrapped, instance, args, kwargs):
        calls.append("pinpoint")
        return wrapped(*args, **kwargs)

    # Pinpoint wraps first (stamped, as _util.wrap does) ...
    wrapt.wrap_function_wrapper(
        Route, "handle", mark_pinpoint_wrapper(pinpoint_wrapper),
    )

    # ... then a second library wraps the SAME attribute on top (outermost).
    def foreign_wrapper(wrapped, instance, args, kwargs):
        calls.append("foreign")
        return wrapped(*args, **kwargs)

    wrapt.wrap_function_wrapper(Route, "handle", foreign_wrapper)
    foreign_fw = Route.__dict__["handle"]

    restore_pinpoint_target("starlette.routing", "Route.handle")

    # The foreign wrapper is still the installed outermost object ...
    assert Route.__dict__["handle"] is foreign_fw

    async def receive():
        return {}

    async def send(_):
        pass

    out = asyncio.run(Route().handle({"type": "http"}, receive, send))
    # ... and running it now fires foreign + base but NOT pinpoint (our layer
    # was spliced out of the chain).
    assert out == "ok"
    assert calls == ["foreign", "base"]


def test_uninstrument_leaves_foreign_only_wrapper_untouched(monkeypatch):
    """If pinpoint never wrapped ``Route.handle`` but another library did,
    uninstrument must not strip that foreign wrapper."""
    calls: list = []

    async def original_handle(self, scope, receive, send):
        calls.append("base")
        return "ok"

    class Route:
        handle = original_handle

    monkeypatch.setitem(
        sys.modules, "starlette.routing", types.SimpleNamespace(Route=Route),
    )
    monkeypatch.setitem(
        sys.modules, "starlette.applications", types.SimpleNamespace(),
    )

    def foreign_wrapper(wrapped, instance, args, kwargs):
        calls.append("foreign")
        return wrapped(*args, **kwargs)

    wrapt.wrap_function_wrapper(Route, "handle", foreign_wrapper)
    foreign_fw = Route.__dict__["handle"]

    restore_pinpoint_target("starlette.routing", "Route.handle")

    assert Route.__dict__["handle"] is foreign_fw

    async def receive():
        return {}

    async def send(_):
        pass

    out = asyncio.run(Route().handle({"type": "http"}, receive, send))
    assert out == "ok"
    assert calls == ["foreign", "base"]
