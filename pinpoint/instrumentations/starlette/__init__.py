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

Starlette is the ASGI toolkit underneath FastAPI and many smaller frameworks.
Rather than re-implement the ASGI plumbing, we reuse the generic
``PinpointASGIMiddleware`` and install it once per ``Starlette`` application:

- Wrap ``Starlette.build_middleware_stack`` so the Pinpoint middleware lands
  on the *outside* of every user-registered middleware (closest to the ASGI
  server). That ordering matters — we want to time the entire request,
  including time spent in user middleware.
- Wrap ``Route.handle`` to emit the endpoint span event and, for Starlette's
  own ``Route`` objects, stash ``route.path`` into ``scope['pinpoint.url_pattern']``
  so the outer ASGI middleware can aggregate URL stats per route template.
- Wrap Starlette's ``run_in_threadpool`` entry points so a synchronous endpoint
  receives a linked async child instead of the ASGI root copied by anyio.
"""

from __future__ import annotations

import functools
from ..._log import get_logger
from ...context import current_span
from ...errors import safe_try
from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_PYTHON_METHOD
from .._util import (
    cached_operation_name,
    claim_handoff,
    mint_async_child,
    reclaim_handoff,
    replace_arg,
    span_event_scope,
    wrap,
)
from ..asgi import PinpointASGIMiddleware

_log = get_logger("starlette")
_OPERATION_THREADPOOL = "starlette.threadpool"

# Every module here imports ``run_in_threadpool`` by value before our autoload
# hook fires, so patching only the defining module leaves the aliases raw. The
# exception-handler location moved across Starlette releases, so the set keeps both
# the old middleware modules and the newer private helper; ``wrap`` is best-effort,
# so missing ones are benign.
_THREADPOOL_ALIAS_MODULES = (
    "starlette.concurrency",
    "starlette.routing",
    "starlette.background",
    "starlette.endpoints",
    "starlette._exception_handler",
    "starlette.middleware.errors",
    "starlette.middleware.exceptions",
)


class StarletteInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        # Installs the Pinpoint root middleware exactly once at startup for every
        # Starlette app. FastAPI overrides this method — see that instrumentation.
        wrap(
            "starlette.applications",
            "Starlette.build_middleware_stack",
            _build_middleware_stack_wrapper,
        )
        # ``Route.handle`` is the narrowest async hook after routing and before the
        # endpoint coroutine, so the event here carries the handler qualname.
        # FastAPI's APIRoute inherits it and is skipped *at call time*
        # (``_is_fastapi_api_route``); that instrumentation wraps
        # ``run_endpoint_function`` more precisely.
        #
        # Deliberately not shadowing ``APIRoute.handle`` with a pinned snapshot of
        # the base implementation: that freezes the MRO, bypasses any wrapper another
        # library installs on ``Route.handle`` after us, and leaves a permanent class
        # mutation behind after uninstrument. The live attribute keeps the chain
        # composable for the price of a cached APIRoute check.
        wrap(
            "starlette.routing",
            "Route.handle",
            _route_handle_wrapper,
        )
        # Function routes, HTTPEndpoint methods, background tasks, and sync exception
        # handlers all dispatch through aliases bound before instrumentation, so wrap
        # every version-specific location — the worker needs a linked child, not a
        # detached root view.
        for module in _THREADPOOL_ALIAS_MODULES:
            wrap(module, "run_in_threadpool", _run_in_threadpool_wrapper)


def make_middleware_stack_wrapper(framework_name: str):
    """wrapt-style wrapper for a framework's ``build_middleware_stack``.

    Wraps the assembled stack once with the Pinpoint ASGI middleware, which
    owns the per-request span lifecycle. Starlette caches the result in
    ``self.middleware_stack``, so this runs at app construction time, not
    per-request. The fastapi instrumentation imports this for its own override
    (which never calls super, so Starlette's wrap doesn't fire for it).
    """
    def _wrapper(wrapped, instance, args, kwargs):
        stack = wrapped(*args, **kwargs)
        if isinstance(stack, PinpointASGIMiddleware):
            # Already wrapped (e.g. FastAPI delegating to super in a future
            # version, or a second build after re-instrumentation) — wrapping
            # again would open two root spans per request.
            return stack
        # entry_event=False: the endpoint wrapper below already opens a span
        # event naming the handler that ran.
        return PinpointASGIMiddleware(stack, framework_name=framework_name,
                                      entry_event=False)
    return _wrapper


_build_middleware_stack_wrapper = make_middleware_stack_wrapper("Starlette")


async def _route_handle_wrapper(wrapped, instance, args, kwargs):
    """Open a Python-method span event around the matched endpoint.

    Skipped for FastAPI's ``APIRoute`` because the fastapi instrumentation
    already wraps ``run_endpoint_function`` — wrapping here too would
    double-emit the same logical span.
    """
    scope = args[0] if args else kwargs.get("scope")
    if isinstance(scope, dict) and scope.get("type") == "http":
        _maybe_set_url_pattern(scope, instance)

    span = current_span()
    if span is None or not getattr(span, "sampled", True):
        return await wrapped(*args, **kwargs)

    if _is_fastapi_api_route(instance):
        return await wrapped(*args, **kwargs)

    # Unguarded by safe_wrapper (async body): a native failure — or an
    # ``endpoint`` attribute that raises on access — would escape into routing
    # and 500 the user's request; fall back to the untraced call instead.
    try:
        op_name = _endpoint_qualname(instance)
        if not op_name:
            return await wrapped(*args, **kwargs)
        event = span.new_span_event(op_name, service_type=SERVICE_TYPE_PYTHON_METHOD)
    except Exception:  # noqa: BLE001
        _log.debug("new_span_event failed in Route.handle wrapper", exc_info=True)
        return await wrapped(*args, **kwargs)
    with span_event_scope(event):
        return await wrapped(*args, **kwargs)


async def _run_in_threadpool_wrapper(wrapped, instance, args, kwargs):
    """Give one synchronous Starlette/FastAPI call its own worker span.

    anyio deliberately copies ContextVars into its worker. Copying the ASGI
    root would make the same native Span visible on two OS threads. Replace
    the callable passed to the pool with a recipient that enters a distinct
    async child on the worker instead. The one-element list is an ownership
    latch: if dispatch fails before the worker starts, this coroutine ends the
    orphan; once the worker claims it, only that worker may end it.
    """
    fn = args[0] if args else kwargs.get("func")
    span = current_span()
    if fn is None or span is None or not getattr(span, "sampled", True):
        return await wrapped(*args, **kwargs)

    # The endpoint wrapper already records the user handler name. Keep the
    # hand-off event distinct so one sync handler does not appear twice.
    operation = _OPERATION_THREADPOOL
    worker_span = mint_async_child(span, operation)
    if worker_span is None:
        return await wrapped(*args, **kwargs)

    owner = [worker_span]

    def _worker(*worker_args, **worker_kwargs):
        claimed = claim_handoff(owner)
        if claimed is None:
            # ``run_in_threadpool`` invokes its callable once. Keep a benign
            # fallback for an exotic executor that retries the wrapper.
            return fn(*worker_args, **worker_kwargs)
        with claimed:
            return fn(*worker_args, **worker_kwargs)

    call_args, call_kwargs = replace_arg(args, kwargs, 0, "func", _worker)

    try:
        return await wrapped(*call_args, **call_kwargs)
    except BaseException:
        # Cancellation does not stop a running worker. Only reclaim the child
        # if the worker has not taken ownership yet.
        reclaim_handoff(owner)
        raise


@functools.lru_cache(maxsize=1024)
def _is_fastapi_api_route_class(cls) -> bool:
    return any(klass.__name__ == "APIRoute" and klass.__module__.startswith("fastapi")
               for klass in cls.__mro__)


def _is_fastapi_api_route(instance) -> bool:
    """Detect FastAPI's APIRoute without importing fastapi — works even when
    fastapi isn't installed (matches by class name in the MRO). Route classes
    are fixed at app-definition time, so the walk is memoized per class to keep
    ``_route_handle_wrapper`` (every routed request) off the reflection path."""
    return _is_fastapi_api_route_class(type(instance))


def _endpoint_qualname(route) -> str:
    return cached_operation_name(getattr(route, "endpoint", None))


@safe_try
def _maybe_set_url_pattern(scope: dict, route) -> None:
    if scope.get("pinpoint.url_pattern"):
        return
    # Starlette's BaseRoute exposes `.path` on Route / Mount / WebSocketRoute.
    pattern = getattr(route, "path", None)
    if isinstance(pattern, str) and pattern:
        scope["pinpoint.url_pattern"] = pattern


def instrument() -> None:
    StarletteInstrumentor().instrument()
