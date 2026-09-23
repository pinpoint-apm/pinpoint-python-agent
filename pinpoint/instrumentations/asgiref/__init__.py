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

"""asgiref ``sync_to_async`` instrumentation.

Django's ASGI stack routes every sync view, middleware and ORM call through
``SyncToAsync``. asgiref copies the caller's ContextVars into its worker thread,
but a copied live Span resolves to a detached no-op view there (see
``pinpoint.context``), so the hop needs an explicit hand-off.

``SyncToAsync`` binds the user callable at construction and Django caches the
instances, so it cannot be swapped per call the way the Starlette/FastAPI
``run_in_threadpool`` wrappers are. The hand-off goes through a ContextVar latch
instead: ``__call__`` mints a linked async child and publishes it, asgiref's own
``copy_context()`` carries the latch into the worker, and ``thread_handler``
claims it *inside* the copied context.

The claim is a single GIL-atomic ``list.pop``, so exactly one of
{worker, dispatcher cleanup} wins and the child is ended exactly once, never
driven by two threads. The dispatcher's ``finally`` resets the latch var by
token, which also erases anything asgiref's ``_restore_context`` copied back.
"""

from __future__ import annotations

import contextvars
from typing import Any

from ...context import current_span
from ...instrumentor import BaseInstrumentor
from .._util import claim_handoff, mint_async_child, reclaim_handoff, wrap

_OPERATION_SYNC_TO_ASYNC = "asgiref.sync_to_async"

# One-element hand-off latch, published by the dispatching loop thread and claimed
# by the worker in asgiref's copied context. Claiming mutates the list, never the
# var, so _restore_context has no worker-side change to copy back.
_handoff: contextvars.ContextVar[list[Any] | None] = contextvars.ContextVar(
    "pinpoint_asgiref_handoff", default=None,
)


# Module that defines ``SyncToAsync.__call__``, and so owns the context runner
# it hands ``thread_handler`` (see the shape check in _thread_handler_wrapper).
_ASGIREF_SYNC_MODULE = "asgiref.sync"


class AsgirefInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        wrap(
            "asgiref.sync",
            "SyncToAsync.__call__",
            _sync_to_async_call_wrapper,
        )
        wrap(
            "asgiref.sync",
            "SyncToAsync.thread_handler",
            _thread_handler_wrapper,
        )


async def _sync_to_async_call_wrapper(wrapped, instance, args, kwargs):
    """Mint a worker child on the owning thread and publish the hand-off.

    Mirrors the ownership rules of ``pinpoint.async_trace``: the child is
    created on the event-loop thread that owns the parent span, and exactly
    one consumer ends it — the worker via ``with claimed:`` once it wins the
    latch, or this coroutine's cleanup when dispatch fails or is cancelled
    before the worker ran.
    """
    span = current_span()
    if span is None or not getattr(span, "sampled", True):
        return await wrapped(*args, **kwargs)
    child = mint_async_child(span, _OPERATION_SYNC_TO_ASYNC)
    if child is None:
        return await wrapped(*args, **kwargs)

    latch: list[Any] = [child]
    token = _handoff.set(latch)
    try:
        return await wrapped(*args, **kwargs)
    finally:
        # Token reset restores the pre-call value even if asgiref's
        # _restore_context copied a worker-side state change back in between.
        _handoff.reset(token)
        # asgiref returns only once the future settled, so the worker either
        # finished (and owns the child) or never started (an orphan to end).
        reclaim_handoff(latch)


def _thread_handler_wrapper(wrapped, instance, args, kwargs):
    """Swap the user callable for a recipient that claims the hand-off.

    ``thread_handler(loop, exc_info, task_context, func, child)`` receives the
    callable to run as the final positional argument and invokes ``func(child)``
    — so a recipient substituted for ``child`` runs inside asgiref's copied
    context, where the latch published by ``_sync_to_async_call_wrapper`` is
    visible.

    ``func`` comes in two shapes, and both run ``child`` inside that copy:

    * asgiref ≤ 3.12.0 — ``func`` *is* ``context.run``, a bound method of the
      copied ``Context``.
    * asgiref ≥ 3.12.1 — ``func`` is a closure ``SyncToAsync.__call__`` defines
      which calls ``context.run`` itself (3.12.1 moved ``_restore_context``
      inside the context, next to the call whose storage it re-homes). The
      ``Context`` is captured in that closure rather than bound to the runner,
      so only the defining module still identifies it as asgiref's.

    The shape check pins that layout: the penultimate positional must be one of
    those two runners and the final one the callable it will run. Anything else
    degrades to an untraced passthrough instead of substituting the wrong
    argument — notably asgiref 3.2's dropped no-contextvars branch, where
    ``func`` was the *user's* callable and the trailing positionals their own
    arguments, and any future rearrangement.
    """
    if kwargs or len(args) < 3 or not callable(args[-1]):
        return wrapped(*args, **kwargs)
    runner = args[-2]
    if not (isinstance(getattr(runner, "__self__", None), contextvars.Context)
            or getattr(runner, "__module__", None) == _ASGIREF_SYNC_MODULE):
        return wrapped(*args, **kwargs)
    inner = args[-1]

    def _recipient(*inner_args, **inner_kwargs):
        latch = _handoff.get()
        # No claim when the dispatcher's cleanup won it (cancellation racing a
        # late worker start): run untraced rather than share a span that is
        # already being ended elsewhere.
        claimed = claim_handoff(latch) if latch else None
        if claimed is None:
            return inner(*inner_args, **inner_kwargs)
        with claimed:
            return inner(*inner_args, **inner_kwargs)

    return wrapped(*args[:-1], _recipient, **kwargs)


def instrument() -> None:
    AsgirefInstrumentor().instrument()
