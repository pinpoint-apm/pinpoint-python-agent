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

"""FastAPI instrumentation.

Exercises ``_run_endpoint_wrapper`` against an in-memory span / fake
dependant. The wrapper should open a Python-method span event named after
the endpoint qualname, run the wrapped coroutine, and end the event —
even when the endpoint raises.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading

import pytest

from pinpoint import context as ppctx
from pinpoint.instrumentations import fastapi as fastapi_instr
from pinpoint.propagator import inject_items
from pinpoint.tracer import Span


# The native-span fakes and the `push_span` fixture come from the shared
# _fakes module / tests/conftest.py.


def _make_dependant(call):
    """Stand-in for FastAPI's Dependant model — we only need the .call attr."""
    return type("D", (), {"call": call})()


# ---------------------------------------------------------------------------
# _run_endpoint_wrapper
# ---------------------------------------------------------------------------

def test_run_endpoint_wrapper_emits_event_with_qualname(push_span):
    sp, rec = push_span

    async def get_user_handler():  # endpoint function
        return {"id": 1}

    async def wrapped(*args, **kwargs):
        return await get_user_handler()

    out = asyncio.run(fastapi_instr._run_endpoint_wrapper(
        wrapped, instance=None,
        args=(_make_dependant(get_user_handler),), kwargs={},
    ))
    assert out == {"id": 1}
    # Inner functions get a `<locals>.` prefix in __qualname__, so match on suffix.
    assert any(
        kind == "event_start" and op == "root" and name.endswith(".get_user_handler")
        for kind, op, name in rec.events
    )
    assert any(
        kind == "event_end" and op == "root" and name.endswith(".get_user_handler")
        for kind, op, name in rec.events
    )


def test_run_endpoint_wrapper_records_exception_and_reraises(push_span):
    sp, rec = push_span

    async def boom():
        raise RuntimeError("kaput")

    async def wrapped(*args, **kwargs):
        return await boom()

    with pytest.raises(RuntimeError, match="kaput"):
        asyncio.run(fastapi_instr._run_endpoint_wrapper(
            wrapped, instance=None,
            args=(_make_dependant(boom),), kwargs={},
        ))
    assert any(e[0] == "event_error" for e in rec.events)
    assert any(
        kind == "event_end" and op == "root" and name.endswith(".boom")
        for kind, op, name in rec.events
    )


def test_run_endpoint_wrapper_no_op_without_active_span():
    """No current span (e.g., agent disabled or non-HTTP call) → just delegate."""
    async def handler():
        return "ok"

    async def wrapped(*args, **kwargs):
        return await handler()

    out = asyncio.run(fastapi_instr._run_endpoint_wrapper(
        wrapped, instance=None,
        args=(_make_dependant(handler),), kwargs={},
    ))
    assert out == "ok"


def test_run_endpoint_wrapper_no_op_for_unsampled_span():
    """Unsampled transactions should skip endpoint-name work and event setup."""
    class _UnsampledSpan:
        sampled = False

        def new_span_event(self, *_args, **_kwargs):
            raise AssertionError("unsampled span must not open endpoint events")

    class _Dependant:
        @property
        def call(self):
            raise AssertionError("unsampled span must not resolve operation name")

    async def wrapped(*args, **kwargs):
        return "ok"

    token = ppctx.set_current_span(_UnsampledSpan())  # type: ignore[arg-type]
    try:
        out = asyncio.run(fastapi_instr._run_endpoint_wrapper(
            wrapped, instance=None, args=(_Dependant(),), kwargs={},
        ))
    finally:
        ppctx.reset_current_span(token)

    assert out == "ok"


def test_run_endpoint_wrapper_metadata_failure_falls_back_once(push_span):
    """Broken endpoint metadata must not fail or retry the user request."""
    class _BrokenDependant:
        @property
        def call(self):
            raise RuntimeError("broken endpoint metadata")

    calls = []

    async def wrapped(*args, **kwargs):
        calls.append(1)
        return "ok"

    out = asyncio.run(fastapi_instr._run_endpoint_wrapper(
        wrapped, instance=None, args=(_BrokenDependant(),), kwargs={},
    ))

    assert out == "ok"
    assert calls == [1]


def test_run_endpoint_wrapper_unwraps_decorated_endpoints(push_span):
    """``functools.wraps`` puts the user's qualname under __wrapped__; the
    instrumentation should see through decorators."""
    sp, rec = push_span

    async def real_handler():
        return None

    async def decorator_wrapper():  # what FastAPI sees as `call`
        return await real_handler()

    decorator_wrapper.__wrapped__ = real_handler  # type: ignore[attr-defined]

    async def wrapped(*args, **kwargs):
        return await decorator_wrapper()

    asyncio.run(fastapi_instr._run_endpoint_wrapper(
        wrapped, instance=None,
        args=(_make_dependant(decorator_wrapper),), kwargs={},
    ))
    assert any(
        kind == "event_start" and op == "root" and name.endswith(".real_handler")
        for kind, op, name in rec.events
    )


def test_fastapi_instrumentor_wraps_expected_targets(monkeypatch):
    """FastAPI wraps its own build_middleware_stack and run_endpoint_function.

    APIRoute.handle is left alone — the Starlette wrapper skips APIRoute at
    call time, keeping the Route.handle wrapper chain composable.
    """
    calls = []

    monkeypatch.setattr(
        fastapi_instr, "wrap",
        lambda module, target, wrapper: calls.append((module, target)),
    )

    fastapi_instr.FastAPIInstrumentor()._instrument()

    assert ("fastapi.applications", "FastAPI.build_middleware_stack") in calls
    assert ("fastapi.routing", "run_endpoint_function") in calls
    assert ("fastapi.routing", "run_in_threadpool") in calls
    # Sync ``def`` dependencies and generator dependencies dispatch through
    # module-local aliases bound before the instrumentation hook fires.
    assert ("fastapi.dependencies.utils", "run_in_threadpool") in calls
    assert ("fastapi.concurrency", "run_in_threadpool") in calls
    assert (
        "fastapi.dependencies.utils", "contextmanager_in_threadpool",
    ) in calls
    assert ("fastapi.concurrency", "contextmanager_in_threadpool") in calls


def test_generator_dependency_exit_hands_child_to_worker():
    owner_ident = threading.get_ident()
    seen = []

    class _WorkerSpan:
        sampled = True

        def __init__(self):
            self.enter_ident = None
            self.ended = 0
            self.token = None

        def __enter__(self):
            self.enter_ident = threading.get_ident()
            self.token = ppctx.set_current_span(self)
            return self

        def __exit__(self, *_args):
            ppctx.reset_current_span(self.token)
            self.ended += 1

        def end(self):
            self.ended += 1

    child = _WorkerSpan()

    class _RootSpan:
        sampled = True

        def new_span_event(self, _operation, service_type=None):
            return contextlib.nullcontext()

        def new_async_span(self, _operation, _async_id=0, _async_sequence=0):
            return child

    class _DependencyContextManager:
        def __enter__(self):
            return "dependency"

        def __exit__(self, *_args):
            seen.append((ppctx.current_span(), threading.get_ident()))
            return True

    @contextlib.asynccontextmanager
    async def direct_anyio_style_exit(cm):
        # FastAPI's real helper uses its wrapped run_in_threadpool alias for
        # __enter__, then bypasses that alias with anyio.to_thread.run_sync for
        # __exit__. asyncio.to_thread has the same ContextVar-copy behavior.
        value = await asyncio.to_thread(cm.__enter__)
        try:
            yield value
        except Exception as exc:
            suppressed = await asyncio.to_thread(
                cm.__exit__, type(exc), exc, exc.__traceback__,
            )
            if not suppressed:
                raise
        else:
            await asyncio.to_thread(cm.__exit__, None, None, None)

    async def main():
        token = ppctx.set_current_span(_RootSpan())  # type: ignore[arg-type]
        try:
            managed = fastapi_instr._contextmanager_in_threadpool_wrapper(
                direct_anyio_style_exit,
                instance=None,
                args=(_DependencyContextManager(),),
                kwargs={},
            )
            async with managed as value:
                assert value == "dependency"
                # Preserve FastAPI's exception-suppression contract while the
                # proxy installs the worker child for __exit__.
                raise ValueError("suppressed by dependency context manager")
        finally:
            ppctx.reset_current_span(token)

    asyncio.run(main())

    assert seen == [(child, child.enter_ident)]
    assert child.enter_ident != owner_ident
    assert child.ended == 1


# ---------------------------------------------------------------------------
# End-to-end: a real FastAPI app driven through its ASGI entry point. This
# is the regression bar — when this passes, the integration produces exactly
# one root span (start AND end) and one endpoint span event per request.
#
# Skips if fastapi isn't importable (CI without the framework installed).
# ---------------------------------------------------------------------------


def test_fastapi_real_app_emits_exactly_one_span_per_request(monkeypatch):
    """Drive the actual FastAPI stack and assert the root span lifecycle.

    Why this exists: FastAPI overrides ``build_middleware_stack`` without
    calling ``super``, so wrapping only ``Starlette.build_middleware_stack``
    leaves FastAPI apps un-instrumented. The fastapi instrumentation also
    wraps ``FastAPI.build_middleware_stack``; without that wrap, this test
    sees zero ``new_span`` events.
    """
    pytest.importorskip("fastapi")

    import pinpoint.agent  # local import to keep the optional-fastapi import clean

    events: list[tuple] = []

    class _Native:
        def __init__(self, op, rpc):
            events.append(("new_span", op, rpc))
            self.op = op

        def __getattr__(self, name):
            def _f(*a, **k):
                if name == "end_span":
                    events.append(("end_span", self.op))
                if name == "new_span_event":
                    events.append(("new_span_event", self.op, a[0] if a else ""))

                    class _Ev:
                        def __getattr__(self, n):
                            def _g(*a, **k): return self
                            return _g

                        def get_annotations(self):
                            class _A:
                                def append_int(self, *_): pass
                                def append_string(self, *_): pass
                            return _A()

                    return _Ev()
                return self
            return _f

        def get_annotations(self):
            class _A:
                def append_int(self, *_): pass
                def append_string(self, *_): pass
            return _A()

        def end_span_with_data(self, *_a, **_k):
            # Span.end() flushes through this finalizer (mirroring
            # src/_native.cpp); record it as the span's end.
            events.append(("end_span", self.op))

        def new_async_span(self, operation, _async_id=0, _async_sequence=0):
            events.append(("new_async_span", self.op, operation))
            child = object.__new__(type(self))
            child.op = f"async:{operation}"
            return child

    class _Agent:
        enabled = True
        def new_span(self, op, rpc, headers=None, method=""):
            return Span(_Native(op, rpc), trace_id="trace-id", span_id=7)

    monkeypatch.setattr(pinpoint.agent, "_instance", _Agent())

    # Install just the fastapi wraps directly — calling ``autoload()`` would
    # register process-wide post-import hooks for every integration and
    # immediately instrument any registry module already imported (e.g.
    # ``asgiref.sync``, loaded by a sibling test file). Those hooks cannot be
    # unregistered and nothing uninstruments what they install, so asgiref
    # spans would leak into unrelated Django/ASGI tests later in the run.
    fastapi_instr.FastAPIInstrumentor().instrument()

    from fastapi import FastAPI

    app = FastAPI()
    handler_spans = []
    handler_headers = []

    @app.get("/items/{item_id}")
    def items(item_id: int):
        span = ppctx.current_span()
        assert span is not None
        handler_spans.append(span)
        # Injection rides the innermost open span event, mirroring the HTTP
        # client wrappers (open outbound event -> inject -> send).
        with pinpoint.trace("sync-handler-child"):
            handler_headers.append(dict(inject_items(span)))
        return {"item_id": item_id}

    scope = {
        "type": "http", "method": "GET", "path": "/items/42",
        "raw_path": b"/items/42", "query_string": b"", "headers": [],
        "client": ("127.0.0.1", 1), "server": ("test", 80),
        "scheme": "http", "root_path": "",
    }

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    sent: list[dict] = []

    async def send(msg):
        sent.append(msg)

    asyncio.run(app(scope, receive, send))

    # The handler must have answered (otherwise the assertions below are vacuous).
    assert any(m.get("type") == "http.response.start" and m.get("status") == 200
               for m in sent)
    new_spans = [e for e in events if e[0] == "new_span"]
    root_ends = [e for e in events
                 if e == ("end_span", "FastAPI HTTP Server")]
    worker_ends = [e for e in events if e[0] == "end_span"
                   and str(e[1]).startswith("async:")]
    span_events = [e for e in events if e[0] == "new_span_event"]
    assert len(new_spans) == 1, f"expected one root span, got: {events}"
    assert new_spans[0] == ("new_span", "FastAPI HTTP Server", "/items/42")
    assert len(root_ends) == 1, f"root span must be ended, got: {events}"
    assert len(worker_ends) == 1, f"worker span must be ended, got: {events}"
    assert len(handler_spans) == 1
    assert isinstance(handler_spans[0], Span)
    assert handler_spans[0].sampled is True
    assert handler_headers[0]["Pinpoint-TraceID"] == "trace-id"
    assert handler_headers[0]["Pinpoint-pSpanID"] == "7"
    assert int(handler_headers[0]["Pinpoint-SpanID"]) != 0
    handler_events = [name for _, _, name in span_events
                      if name.endswith(".items")]
    assert len(handler_events) == 1, f"endpoint span event mismatch: {events}"
    assert any(name == "sync-handler-child" for _, _, name in span_events), (
        f"sync handler child event missing: {events}"
    )
