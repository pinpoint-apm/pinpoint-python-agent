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

"""asgiref ``sync_to_async`` hand-off instrumentation.

The end-to-end tests drive the real ``asgiref.sync.SyncToAsync`` (including
its ``_restore_context`` copy-back) so the ContextVar-latch bridge is verified
against the library's actual dispatch, not a simulation.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import threading

import pytest

from pinpoint import context as ppctx
from pinpoint.instrumentations import asgiref as asgiref_instr


class _WorkerSpan:
    """Fake async child recording who entered it and how often it ended."""

    sampled = True

    def __init__(self):
        self.enter_ident = None
        self.ended = 0
        self._token = None

    def __enter__(self):
        self.enter_ident = threading.get_ident()
        self._token = ppctx.set_current_span(self)
        return self

    def __exit__(self, *_args):
        ppctx.reset_current_span(self._token)
        self.ended += 1

    def end(self):
        self.ended += 1


class _RootSpan:
    sampled = True

    def __init__(self):
        self.children = []

    def new_span_event(self, _operation, service_type=None):
        return contextlib.nullcontext()

    def new_async_span(self, _operation, _async_id=0, _async_sequence=0):
        child = _WorkerSpan()
        self.children.append(child)
        return child


def test_asgiref_instrumentor_wraps_expected_targets(monkeypatch):
    calls = []

    monkeypatch.setattr(
        asgiref_instr,
        "wrap",
        lambda module, target, wrapper: calls.append((module, target)),
    )

    asgiref_instr.AsgirefInstrumentor()._instrument()

    assert ("asgiref.sync", "SyncToAsync.__call__") in calls
    assert ("asgiref.sync", "SyncToAsync.thread_handler") in calls


def test_sync_to_async_hands_child_to_worker_and_restores_origin():
    """The worker's sync section sees a linked child it exclusively owns, and
    asgiref's ``_restore_context`` copy-back leaves the caller's live binding
    untouched."""
    asgiref_sync = pytest.importorskip("asgiref.sync")
    instr = asgiref_instr.AsgirefInstrumentor()
    instr.instrument()
    try:
        root = _RootSpan()
        seen = []

        def sync_section(value):
            seen.append((ppctx.current_span(), threading.get_ident()))
            return value + 1

        async def main():
            token = ppctx.set_current_span(root)
            try:
                result = await asgiref_sync.sync_to_async(sync_section)(41)
                return result, ppctx.current_span()
            finally:
                ppctx.reset_current_span(token)

        result, after = asyncio.run(main())
    finally:
        instr.uninstrument()

    assert result == 42
    assert len(root.children) == 1
    child = root.children[0]
    assert seen == [(child, child.enter_ident)]
    assert child.enter_ident != threading.get_ident()
    assert child.ended == 1
    # The request context after the hop still resolves the root span.
    assert after is root


def test_sync_to_async_untraced_without_active_span():
    asgiref_sync = pytest.importorskip("asgiref.sync")
    instr = asgiref_instr.AsgirefInstrumentor()
    instr.instrument()
    try:
        seen = []

        def sync_section():
            seen.append(ppctx.current_span())
            return "ok"

        assert asyncio.run(asgiref_sync.sync_to_async(sync_section)()) == "ok"
    finally:
        instr.uninstrument()

    assert seen == [None]


def test_sync_to_async_ends_orphan_when_dispatch_fails():
    """A child minted for a dispatch that never reaches the worker must not
    leak — the dispatcher reclaims the latch and ends it."""
    asgiref_sync = pytest.importorskip("asgiref.sync")
    from concurrent.futures import ThreadPoolExecutor

    dead_executor = ThreadPoolExecutor(max_workers=1)
    dead_executor.shutdown()

    instr = asgiref_instr.AsgirefInstrumentor()
    instr.instrument()
    try:
        root = _RootSpan()

        def never_runs():  # pragma: no cover - dispatch fails first
            raise AssertionError("executor is shut down")

        async def main():
            token = ppctx.set_current_span(root)
            try:
                await asgiref_sync.sync_to_async(
                    never_runs, thread_sensitive=False, executor=dead_executor,
                )()
            finally:
                ppctx.reset_current_span(token)

        with pytest.raises(RuntimeError):
            asyncio.run(main())
    finally:
        instr.uninstrument()

    assert len(root.children) == 1
    child = root.children[0]
    assert child.enter_ident is None  # never claimed by a worker
    assert child.ended == 1


@pytest.mark.parametrize("layout", ["context_run", "asgiref_closure"])
def test_thread_handler_wrapper_claims_latch_in_both_asgiref_layouts(layout):
    """Both shapes of asgiref's context entry get a latch-claiming recipient.

    ``thread_handler`` always invokes ``func(child)``, but what ``func`` is
    changed in asgiref 3.12.1: up to 3.12.0 it *was* ``context.run``, from
    3.12.1 it is a closure ``SyncToAsync.__call__`` defines which calls
    ``context.run`` itself. Both layouts are driven here so the recognition
    stays pinned whichever asgiref version happens to be installed — the
    end-to-end test above only exercises one of them.
    """
    child = _WorkerSpan()
    # The latch has to be live when the context is copied, as it is in
    # SyncToAsync.__call__: that copy is the only channel into the worker.
    token = asgiref_instr._handoff.set([child])
    try:
        context = contextvars.copy_context()
    finally:
        asgiref_instr._handoff.reset(token)

    if layout == "context_run":
        runner = context.run
    else:
        def runner(inner):
            return context.run(inner)

        runner.__module__ = "asgiref.sync"

    seen = []

    def user_callable():
        seen.append(ppctx.current_span())
        return "ok"

    def thread_handler(_loop, _exc_info, _task_context, func, target):
        return func(target)

    out = asgiref_instr._thread_handler_wrapper(
        thread_handler,
        instance=None,
        args=(None, (None, None, None), [], runner, user_callable),
        kwargs={},
    )

    assert out == "ok"
    assert seen == [child]
    assert child.ended == 1


def test_thread_handler_wrapper_passthrough_on_unexpected_shape():
    """A signature whose penultimate positional is neither of asgiref's
    context runners must pass through unmodified rather than substitute a
    user arg."""
    calls = []

    def wrapped(*args, **kwargs):
        calls.append((args, kwargs))
        return "ran"

    def user_arg():  # pragma: no cover - never invoked
        return None

    out = asgiref_instr._thread_handler_wrapper(
        wrapped,
        instance=None,
        args=("loop", ("exc",), ["task"], "not-a-context-runner", user_arg),
        kwargs={},
    )

    assert out == "ran"
    assert calls[0][0][-1] is user_arg
