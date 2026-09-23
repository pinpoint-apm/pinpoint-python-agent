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

"""pinpoint-python-agent: APM tracing for Python.

Public surface:

- :func:`init`              -- initialize the process-wide agent
- :func:`shutdown`          -- shut it down
- :func:`get_agent`         -- current agent (or None)
- :func:`current_span`      -- span attached to the current context
- :func:`trace`             -- decorator / context manager for manual spans
- :func:`async_trace`       -- create an async span for hand-off to another
                               thread/coroutine
- :func:`span`              -- decorator: wrap an entry point so each call gets
                               its own root span (no auto-instrumentation needed)
- :func:`spanevent`         -- decorator: wrap a function so each call records a
                               span event on the current span
- :mod:`pinpoint.propagator` -- distributed tracing helpers
"""

from ._version import __version__
from .agent import Agent, get_agent, init, shutdown
from .context import current_span
from .tracer import Span, SpanEvent

__all__ = [
    "__version__",
    "Agent",
    "Span",
    "SpanEvent",
    "init",
    "shutdown",
    "get_agent",
    "current_span",
    "trace",
    "async_trace",
    "span",
    "spanevent",
]


import contextlib as _contextlib
import functools as _functools
from typing import Any as _Any
from collections.abc import Callable as _Callable

from . import context as _ctx
from .service_type import SERVICE_TYPE_PYTHON_METHOD as _SERVICE_TYPE_PYTHON_METHOD


# Shared no-op context manager returned by trace()/async_trace() when no
# span is current: enters as None (the documented contract), never suppresses.
# A reused instance, not a per-call generator CM — trace() sits on hot paths.
_NOOP_TRACE = _contextlib.nullcontext()


def trace(operation: str, service_type: int = _SERVICE_TYPE_PYTHON_METHOD,
          ) -> "_contextlib.AbstractContextManager[SpanEvent | None]":
    """Create a child span event on the current span, or a no-op if none.

    >>> with pinpoint.trace("compute_invoice"):
    ...     run_business_logic()

    ``Span.new_span_event`` already returns the :class:`SpanEvent` directly
    (its ``__enter__`` yields the event, ``__exit__`` records exceptions and
    ends it), so hand it back as-is instead of re-wrapping it in a per-call
    ``@contextmanager`` generator. When no span is current — or the current
    one is a ``_NullSpan``/``UnSampledSpan``, which return the shared no-op
    event — nothing is allocated at all.
    """
    span = _ctx.current_span()
    if span is None:
        return _NOOP_TRACE
    return span.new_span_event(operation, service_type=service_type)


class _AsyncTraceScope:
    """Context manager returned by :func:`async_trace` for the hand-off span.

    Yields the async span and, if the hand-off block raises before a worker
    took ownership, ends the orphan so it can't leak (``end()`` is idempotent,
    so a race with a worker that already claimed it is a safe no-op).
    """

    __slots__ = ("_async_span",)

    def __init__(self, async_span: "Span") -> None:
        self._async_span = async_span

    def __enter__(self) -> "Span":
        return self._async_span

    def __exit__(self, exc_type, exc_val, tb) -> bool:
        if exc_val is not None:
            self._async_span.end()
        return False


def async_trace(operation: str) -> "_contextlib.AbstractContextManager[Span | None]":
    """Create an async span on the current span and yield it for hand-off.

    Capture once on the originating thread, hand off, finalise from the worker.
    Each call creates a hand-off span event on the parent for the new async
    span to link against; the event ends as the ``with`` exits, and the yielded
    :class:`Span` is owned by the recipient.

    The recipient **must** terminate the async span — typically with
    ``with async_span: ...`` (which calls ``end()`` on exit). If the caller
    raises *before* the hand-off completes, ``async_trace`` ends the orphan
    span itself so it never leaks; once a worker has picked it up, ``end()``
    is the worker's responsibility.

    >>> with pinpoint.async_trace("background_job") as async_span:
    ...     threading.Thread(target=worker, args=(async_span,)).start()
    >>>
    >>> def worker(async_span):
    ...     with async_span:                    # makes it the current span,
    ...         with async_span.new_span_event("step"):  # ends it on exit
    ...             do_work()

    The ``with pinpoint.async_trace(...)`` block itself is always safe to
    enter, but it yields ``None`` when there is no active span (agent disabled
    or outside any traced request). A recipient that uses the ``with
    async_span:`` hand-off pattern must therefore guard against ``None`` (e.g.
    ``if async_span is not None:``) — passing ``None`` to ``with`` raises.
    """
    span = _ctx.current_span()
    if span is None:
        return _NOOP_TRACE
    # Open a hand-off span event for the async span to link against, then
    # close it — the async span itself outlives this scope and is owned by the
    # recipient.
    with span.new_span_event(operation):
        async_span = span.new_async_span(operation)
    return _AsyncTraceScope(async_span)


# Decorators for code auto-instrumentation doesn't cover; see their docstrings.
# Both dispatch on coroutine vs sync at decoration time, keeping the per-call
# path straight.


def _resolve_op(fn: _Callable[..., _Any]) -> str:
    return getattr(fn, "__qualname__", None) or getattr(fn, "__name__", None) or "anonymous"


def spanevent(
    operation: "str | _Callable[..., _Any] | None" = None,
    *,
    service_type: int = _SERVICE_TYPE_PYTHON_METHOD,
) -> _Any:
    """Record a span event on the current span for every call of the wrapped
    function.

    Equivalent to wrapping the function body in ``with pinpoint.trace(op): …``.
    No-op (the function still runs) when no span is current — same as
    :func:`trace`.

    >>> @pinpoint.spanevent
    ... def compute_invoice(order):
    ...     ...
    >>>
    >>> @pinpoint.spanevent("billing.charge")
    ... async def charge(amount):
    ...     ...
    """
    import inspect

    def _wrap(fn: _Callable[..., _Any], op: str | None) -> _Callable[..., _Any]:
        op_name = op or _resolve_op(fn)
        if inspect.iscoroutinefunction(fn):
            @_functools.wraps(fn)
            async def _aw(*a: _Any, **kw: _Any) -> _Any:
                with trace(op_name, service_type=service_type):
                    return await fn(*a, **kw)
            return _aw

        @_functools.wraps(fn)
        def _sw(*a: _Any, **kw: _Any) -> _Any:
            with trace(op_name, service_type=service_type):
                return fn(*a, **kw)
        return _sw

    # `@spanevent` (no parens) — `operation` is the function being decorated.
    if callable(operation):
        return _wrap(operation, None)

    # `@spanevent()` / `@spanevent("name")` / `@spanevent(operation="name", …)`
    op_arg = operation  # already a str or None
    return lambda fn: _wrap(fn, op_arg)


def span(
    operation: "str | _Callable[..., _Any] | None" = None,
    *,
    rpc_point: str | None = None,
) -> _Any:
    """Open a new root span around every call of the wrapped function.

    Use for entry points where auto-instrumentation isn't an option: cron
    jobs, message-queue consumers, CLI commands, scheduled workers. Each
    call becomes its own Pinpoint transaction (a top-level row in the
    application call tree); errors bubbling out of ``fn`` are recorded on
    the span before being re-raised.

    No-op (function runs untraced) when ``init()`` hasn't run or the agent
    is disabled.

    >>> @pinpoint.span("nightly_billing", rpc_point="/cron/billing")
    ... def nightly_billing():
    ...     ...
    >>>
    >>> @pinpoint.span
    ... async def consume_message(msg):
    ...     ...
    """
    import inspect

    def _wrap(
        fn: _Callable[..., _Any],
        op: str | None,
        rpc: str | None,
    ) -> _Callable[..., _Any]:
        op_name = op or _resolve_op(fn)
        rpc_name = rpc or op_name
        if inspect.iscoroutinefunction(fn):
            @_functools.wraps(fn)
            async def _aw(*a: _Any, **kw: _Any) -> _Any:
                agent = get_agent()
                if agent is None or not agent.enabled:
                    return await fn(*a, **kw)
                with agent.new_span(op_name, rpc_name):
                    return await fn(*a, **kw)
            return _aw

        @_functools.wraps(fn)
        def _sw(*a: _Any, **kw: _Any) -> _Any:
            agent = get_agent()
            if agent is None or not agent.enabled:
                return fn(*a, **kw)
            with agent.new_span(op_name, rpc_name):
                return fn(*a, **kw)
        return _sw

    if callable(operation):
        return _wrap(operation, None, rpc_point)

    op_arg = operation
    return lambda fn: _wrap(fn, op_arg, rpc_point)
