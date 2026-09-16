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

The root span lifecycle still belongs to ``PinpointASGIMiddleware``, but
FastAPI overrides ``build_middleware_stack`` *without* calling super, so the
wrap the starlette integration installs on ``Starlette.build_middleware_stack``
never fires here — hence the dedicated hook on ``FastAPI.build_middleware_stack``.

``run_endpoint_function`` is wrapped for the handler-named span event. FastAPI's
``APIRoute`` is deliberately kept off Starlette's generic ``Route.handle``
wrapper (detected at call time, not shadowed by a pinned snapshot) so this
integration records the endpoint more precisely while the wrapper chain on
``Route.handle`` stays composable with other libraries'.

See ``README.md`` for the full list of what gets traced.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from ...context import current_span
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
from ..starlette import (
    _run_in_threadpool_wrapper,
    make_middleware_stack_wrapper,
)

_OPERATION_CONTEXTMANAGER_EXIT = "fastapi.contextmanager.exit"


class FastAPIInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        # FastAPI.build_middleware_stack overrides Starlette's with no super call,
        # so the starlette instrumentation's wrap is bypassed for FastAPI apps.
        # Wrapping the override directly is what gets PinpointASGIMiddleware onto the
        # cached stack, and that middleware owns the per-request span lifecycle.
        wrap(
            "fastapi.applications",
            "FastAPI.build_middleware_stack",
            _build_middleware_stack_wrapper,
        )
        # ``run_endpoint_function`` is the narrowest hook around the endpoint call and
        # runs after dependency resolution, so timing covers user code only — not
        # Pydantic validation or DI graph walks.
        wrap(
            "fastapi.routing",
            "run_endpoint_function",
            _run_endpoint_wrapper,
        )
        # FastAPI binds Starlette's helper into module-local aliases before our hook
        # fires (it triggers on ``fastapi.applications``, last in the cascade), so
        # each alias needs its own wrap for the worker to get a linked child rather
        # than a copied ASGI root: ``routing`` runs sync endpoints,
        # ``dependencies.utils`` sync ``def`` dependencies, and ``concurrency`` backs
        # generator dependencies plus the public re-export.
        wrap(
            "fastapi.routing",
            "run_in_threadpool",
            _run_in_threadpool_wrapper,
        )
        wrap(
            "fastapi.dependencies.utils",
            "run_in_threadpool",
            _run_in_threadpool_wrapper,
        )
        wrap(
            "fastapi.concurrency",
            "run_in_threadpool",
            _run_in_threadpool_wrapper,
        )
        # ``contextmanager_in_threadpool`` uses the alias above for ``cm.__enter__``
        # but dispatches ``cm.__exit__`` straight through anyio.to_thread, so both
        # copies need wrapping for generator teardown to hand off explicitly too.
        wrap(
            "fastapi.dependencies.utils",
            "contextmanager_in_threadpool",
            _contextmanager_in_threadpool_wrapper,
        )
        wrap(
            "fastapi.concurrency",
            "contextmanager_in_threadpool",
            _contextmanager_in_threadpool_wrapper,
        )


# Identical to Starlette's apart from the root span's framework name.
_build_middleware_stack_wrapper = make_middleware_stack_wrapper("FastAPI")


class _ExitHandoffContextManager:
    """Proxy one sync context manager's direct-anyio ``__exit__`` call.

    FastAPI already routes ``__enter__`` through its wrapped
    ``run_in_threadpool`` alias, but calls ``anyio.to_thread.run_sync``
    directly for ``__exit__``. The proxy carries a one-shot child created by
    the event-loop-side async wrapper below; the worker claims and enters that
    child immediately before invoking the real ``__exit__``.
    """

    __slots__ = ("_cm", "_exit_owner")

    def __init__(self, cm) -> None:
        self._cm = cm
        self._exit_owner = None

    def __enter__(self):
        return self._cm.__enter__()

    def __exit__(self, exc_type, exc_val, tb):
        owner = self._exit_owner
        claimed = claim_handoff(owner) if owner else None
        if claimed is None:
            return self._cm.__exit__(exc_type, exc_val, tb)
        with claimed:
            return self._cm.__exit__(exc_type, exc_val, tb)

    def prepare_exit(self) -> None:
        span = current_span()
        if span is None or not getattr(span, "sampled", True):
            return
        child = mint_async_child(span, _OPERATION_CONTEXTMANAGER_EXIT)
        if child is not None:
            self._exit_owner = [child]

    def reclaim_exit(self) -> None:
        owner, self._exit_owner = self._exit_owner, None
        if owner:
            reclaim_handoff(owner)


@asynccontextmanager
async def _contextmanager_exit_scope(delegate, proxy):
    """Prepare the exit hand-off immediately before delegate ``__aexit__``."""
    try:
        async with delegate as value:
            try:
                yield value
            finally:
                proxy.prepare_exit()
    finally:
        # If dispatch failed or cancellation won before the worker claimed the
        # child, this side still owns and ends it. A worker claim empties the
        # shared list first, making this cleanup a no-op.
        proxy.reclaim_exit()


def _contextmanager_in_threadpool_wrapper(wrapped, instance, args, kwargs):
    """Hand a child span to FastAPI generator-dependency teardown."""
    cm = args[0] if args else kwargs.get("cm")
    span = current_span()
    if cm is None or span is None or not getattr(span, "sampled", True):
        return wrapped(*args, **kwargs)

    proxy = _ExitHandoffContextManager(cm)
    call_args, call_kwargs = replace_arg(args, kwargs, 0, "cm", proxy)
    delegate = wrapped(*call_args, **call_kwargs)
    return _contextmanager_exit_scope(delegate, proxy)


async def _run_endpoint_wrapper(wrapped, instance, args, kwargs):
    span = current_span()
    if span is None or not getattr(span, "sampled", True):
        return await wrapped(*args, **kwargs)

    # ``safe_wrapper`` cannot guard failures raised from this async body. Keep all
    # instrumentation setup behind a local boundary so metadata/native failures do
    # not turn an otherwise successful request into a 500 response.
    try:
        dependant = kwargs.get("dependant") or (args[0] if args else None)
        op_name = cached_operation_name(
            getattr(dependant, "call", None), default=None)
        event = (
            span.new_span_event(op_name, service_type=SERVICE_TYPE_PYTHON_METHOD)
            if op_name is not None
            else None
        )
    except Exception:  # noqa: BLE001
        event = None

    if event is None:
        return await wrapped(*args, **kwargs)
    with span_event_scope(event):
        return await wrapped(*args, **kwargs)


def instrument() -> None:
    FastAPIInstrumentor().instrument()
