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

"""Context-local current span tracking.

``asyncio`` tasks copy ContextVars per PEP 567, but a sampled native span has a
single event stack and cannot safely be shared by concurrently running tasks.
The copied binding therefore remembers its owning task: the first lookup from
a child task lazily switches that task to a linked async span with an
independent native stack. Frameworks also copy ContextVars when an async
request enters a synchronous worker thread. A copied context never exposes the
original live span there: sampled spans become a detached no-op view unless
the hand-off used an explicit async-span helper.
"""

from __future__ import annotations

import _thread
import contextvars
import sys
import weakref
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from .tracer import Span


class _SpanBinding:
    """A span plus the execution context that may use its native stack."""

    __slots__ = ("span", "thread_id", "task_ref", "detached")

    def __init__(self, span: "Span", task: Any) -> None:
        self.span = span
        self.thread_id = _thread.get_ident()
        self.task_ref = weakref.ref(task) if task is not None else None
        # Lazily built no-op view for foreign-thread lookups. Cached on the binding
        # (which copied Contexts share) and NEVER written to the ContextVar:
        # asgiref's ``_restore_context`` copies a worker's contextvar changes back,
        # so storing it there would replace the owner's live binding and untrace the
        # rest of the request after the first ``sync_to_async`` hop.
        self.detached: Any = None


# asyncio function refs, resolved once the module appears in sys.modules and
# cached: this probe runs on every current_span()/set_current_span (once or
# more per trace() call), and re-doing the sys.modules + attribute lookups
# each time is pure overhead — most processes have asyncio loaded transitively.
_asyncio_get_running_loop: Any = None
_asyncio_current_task: Any = None


def _current_asyncio_task() -> Any:
    # Don't import asyncio just because the sync agent was imported; an async app
    # has it loaded already. Guarded with the non-raising C helper because being
    # outside a loop is the hot synchronous path and current_task() raises there.
    global _asyncio_get_running_loop, _asyncio_current_task
    get_running_loop = _asyncio_get_running_loop
    if get_running_loop is None:
        asyncio = sys.modules.get("asyncio")
        if asyncio is None:
            return None
        get_running_loop = asyncio._get_running_loop
        _asyncio_current_task = asyncio.current_task
        _asyncio_get_running_loop = get_running_loop
    # Non-raising C fast path on every supported CPython version.
    if get_running_loop() is None:
        return None
    return _asyncio_current_task()


_current_span: contextvars.ContextVar[Optional[_SpanBinding]] = contextvars.ContextVar(
    "pinpoint_current_span", default=None
)


def current_span() -> Optional["Span"]:
    """The innermost active Span in this execution context, or None."""
    binding = _current_span.get()
    if binding is None:
        return None
    span = binding.span

    # An implicit task span may be force-ended by its lifetime guard while the task
    # keeps running; never hand that stale wrapper back. Every production span type
    # carries _ended, so the except arm (test doubles) costs nothing here.
    try:
        if span._ended:
            return None
    except AttributeError:
        pass

    # Native spans are single-threaded for life, and anyio.to_thread/asgiref may
    # copy this binding into a worker — even a sequential borrow would give one
    # native span two owning OS threads. Foreign threads get a detached no-op view;
    # explicit async_trace hand-offs carry a distinct async child
    # and so arrive same-thread. The view lives on the binding, not the ContextVar
    # (see _SpanBinding.detached), so the copied Context stays unmodified and
    # asgiref's restore has nothing to copy back. Races are benign: equivalent
    # no-op views.
    ident = _thread.get_ident()
    if binding.thread_id != ident:
        detached = binding.detached
        if detached is None:
            factory = getattr(span, "_detached_context_span", None)
            detached = factory() if factory is not None else None
            if detached is not None:
                binding.detached = detached
        return detached

    task = _current_asyncio_task()
    # Event-loop callbacks run outside a Task but are still serialized with the
    # owning task on this thread (as is sync code after asyncio.run()), so the live
    # span can't race its native event stack. Notably, visibility must not depend
    # on whether the weakly referenced owner Task has been collected.
    if task is None:
        return span

    owner_ref = binding.task_ref
    if owner_ref is None:
        # A span installed before the loop starts has no task owner yet. The
        # binding is shared by copied contexts, so the first task to resolve it
        # claims ownership under the GIL and siblings take the fork path below.
        binding.task_ref = weakref.ref(task)
        return span
    else:
        owner = owner_ref()
        if owner is task:
            return span
        if owner is None:
            # The previous loop/task finished, so sequential asyncio.run() calls on
            # this thread can transfer the span: no concurrent owner is left.
            binding.task_ref = weakref.ref(task)
            return span

    # A different task inherited this binding. Sampled Span implements the hook
    # with a linked async span; no-op spans and test doubles keep sharing.
    fork = getattr(span, "_fork_for_async_task", None)
    if fork is None:
        return span
    child = fork(task)
    if child is None:
        return None
    _current_span.set(_SpanBinding(child, task))
    return child


def _adopt_current_span() -> Optional["Span"]:
    """Claim the inherited span for a framework's sequential child task.

    This deliberately bypasses :func:`current_span`'s async-task fork. It is a
    private integration hook for framework-created tasks that continue the
    same request while the original owner is suspended; arbitrary concurrent
    tasks must keep using the normal fork path.
    """
    binding = _current_span.get()
    if binding is None:
        return None
    if binding.thread_id != _thread.get_ident():
        # Only a child task on the same loop thread may adopt; a copied worker
        # context still takes the detached path.
        return current_span()
    _current_span.set(_SpanBinding(binding.span, _current_asyncio_task()))
    return binding.span


class SpanActivation:
    """Reusable activation for repeatedly stepping under the same span
    (streaming response drains).

    Each step still gets its own ContextVar set — the caller resets with the
    returned token, so a drain that migrates threads can never leak a stale
    binding into a pooled worker's context — but the :class:`_SpanBinding` is
    reused while the stepping thread stays the same, skipping the per-step
    binding allocation and asyncio task probe :func:`set_current_span` pays.
    """

    __slots__ = ("_span", "_binding")

    def __init__(self, span: "Span") -> None:
        self._span = span
        self._binding: Optional[_SpanBinding] = None

    def set(self) -> contextvars.Token:
        binding = self._binding
        if binding is None or binding.thread_id != _thread.get_ident():
            binding = _SpanBinding(self._span, _current_asyncio_task())
            self._binding = binding
        return _current_span.set(binding)


def set_current_span(span: Optional["Span"]) -> contextvars.Token:
    binding = None if span is None else _SpanBinding(
        span, _current_asyncio_task())
    return _current_span.set(binding)


def reset_current_span(token: contextvars.Token) -> None:
    _current_span.reset(token)
