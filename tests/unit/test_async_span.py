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

"""Async/threaded span hand-off semantics.

The native ``_native.Span`` is not available in unit-test land, so we use a
``FakeNativeSpan`` recorder to assert the *Python-side* contract of the
``new_async_span`` plumbing: ordering of span-event open/close, parent ↔ child
linkage, and that ``current_span()`` resolves to the async span inside the
worker thread / coroutine.
"""

from __future__ import annotations

import asyncio
import contextvars
import threading

import _fakes

import pinpoint
from pinpoint import context as ppctx
from pinpoint.tracer import Span


class _FakeNativeSpan(_fakes.FakeNativeSpan):
    """Shared fake plus the async-link contract this file asserts on: the
    link record and the id/sequence sanity checks."""

    def new_async_span(self, async_operation, async_id, async_sequence):
        # The wrapper assigns the async link ids from its Python-side event
        # stack and only calls here with an active event, so a real id and a
        # 1-based per-event sequence must always arrive.
        assert async_id != 0
        assert async_sequence >= 1
        self.recorder.events.append(("async_span_create", self.op, async_operation))
        return super().new_async_span(async_operation)


def _make_root_span():
    rec = _fakes.Recorder()
    return Span(_FakeNativeSpan("root", recorder=rec)), rec


# ---------------------------------------------------------------------------
# Span.new_async_span — primary primitive
# ---------------------------------------------------------------------------

def test_new_async_span_returns_span_wrapper():
    root, _ = _make_root_span()
    with root.new_span_event("hand_off"):
        async_span = root.new_async_span("background")
    assert isinstance(async_span, Span)


def test_new_async_span_records_link_and_independent_lifecycle():
    """Async span is created while parent's hand-off event is active, then
    parent and async span end independently."""
    root, rec = _make_root_span()
    with root.new_span_event("hand_off"):
        async_span = root.new_async_span("background")
    # parent's hand-off event is now closed; async span lives on
    with async_span:
        with async_span.new_span_event("step"):
            pass
    root.end()

    kinds = [e[0] for e in rec.events]
    # Events are Python-buffered and reach the fake native when they finish
    # (the conftest bridge replays start+end together at that point), so the
    # async link — recorded at creation — now precedes the hand-off replay.
    assert kinds == [
        "span_start",         # root span created
        "async_span_create",  # link, while the hand-off event is open
        "span_start",         # linked async span created
        "event_start",        # parent: hand_off replayed at its end
        "event_end",
        "event_start",        # async: step
        "event_end",
        "span_end",            # async span ends
        "span_end",            # root span ends
    ]


# ---------------------------------------------------------------------------
# async_trace() helper
# ---------------------------------------------------------------------------

def test_async_trace_yields_none_when_no_current_span():
    with pinpoint.async_trace("orphan") as async_span:
        assert async_span is None


def test_async_trace_yields_async_span_when_current_set():
    root, rec = _make_root_span()
    token = ppctx.set_current_span(root)
    try:
        with pinpoint.async_trace("background") as async_span:
            assert isinstance(async_span, Span)
        # `with` exit must end the parent's hand-off event but *not* the async span
        kinds = [e[0] for e in rec.events]
        assert kinds == ["span_start", "async_span_create", "span_start",
                         "event_start", "event_end"]
        # end the async span explicitly to mirror what the worker would do
        async_span.end()
    finally:
        ppctx.reset_current_span(token)


# ---------------------------------------------------------------------------
# Span context-manager re-entrancy
# ---------------------------------------------------------------------------

def test_reentrant_span_with_restores_current_and_defers_end():
    """Nested ``with span:`` must restore the prior current_span on each inner
    exit and only end the span once the outermost scope unwinds — so a
    re-entered span never strands the contextvar pointing at a dead span."""
    root, rec = _make_root_span()
    assert ppctx.current_span() is None

    with root:
        assert ppctx.current_span() is root
        with root:  # re-enter the same span
            assert ppctx.current_span() is root
        # inner exit: still current, span not yet ended
        assert ppctx.current_span() is root
        assert [e for e in rec.events if e[0] == "span_end"] == []

    # outermost exit: contextvar restored and the span ended exactly once
    assert ppctx.current_span() is None
    assert [e[0] for e in rec.events if e[0] == "span_end"] == ["span_end"]


def test_reentrant_span_restores_prior_current_span_not_none():
    """Re-entering within an existing current-span context restores that prior
    span on exit rather than blanking the contextvar."""
    outer, _ = _make_root_span()
    inner, _ = _make_root_span()
    token = ppctx.set_current_span(outer)
    try:
        with inner:
            with inner:  # re-enter
                assert ppctx.current_span() is inner
            assert ppctx.current_span() is inner
        # both scopes unwound: contextvar restored to the pre-existing span
        assert ppctx.current_span() is outer
    finally:
        ppctx.reset_current_span(token)


# ---------------------------------------------------------------------------
# threading hand-off
# ---------------------------------------------------------------------------


def test_copied_context_uses_detached_noop_span_in_worker_thread():
    """A raw ContextVar copy must not expose the request's native span."""
    root, rec = _make_root_span()
    token = ppctx.set_current_span(root)
    ctx = contextvars.copy_context()
    seen = []

    def worker():
        seen.append(ppctx.current_span())
        with pinpoint.trace("worker-child"):
            pass

    try:
        thread = threading.Thread(target=ctx.run, args=(worker,))
        thread.start()
        thread.join()
    finally:
        ppctx.reset_current_span(token)

    assert len(seen) == 1
    assert seen[0] is not root
    assert seen[0].sampled is False
    # Nothing beyond the root span's own creation record: the worker's no-op
    # span never touched the native layer.
    assert rec.events == [("span_start", "root", "")]


# ---------------------------------------------------------------------------
# asyncio tasks
# ---------------------------------------------------------------------------

def test_plain_asyncio_tasks_get_independent_native_event_stacks():
    """PEP 567 inheritance must not make sibling tasks share root's LIFO.

    ``first`` deliberately closes while ``second`` is still open. On a shared
    native stack that pops second's event and reverses the two event endings.
    Each task should instead lazily receive its own linked async span.
    """
    root, rec = _make_root_span()

    async def main():
        token = ppctx.set_current_span(root)
        first_started = asyncio.Event()
        second_started = asyncio.Event()
        first_done = asyncio.Event()
        seen = {}

        async def first():
            with pinpoint.trace("first"):
                seen["first"] = ppctx.current_span()
                first_started.set()
                await second_started.wait()
            first_done.set()

        async def second():
            await first_started.wait()
            with pinpoint.trace("second"):
                seen["second"] = ppctx.current_span()
                second_started.set()
                await first_done.wait()

        try:
            await asyncio.gather(first(), second())
            # Task done callbacks own async-span finalization.
            await asyncio.sleep(0)
        finally:
            ppctx.reset_current_span(token)
        return seen

    seen = asyncio.run(main())

    assert seen["first"] is not root
    assert seen["second"] is not root
    assert seen["first"] is not seen["second"]
    assert [e[0] for e in rec.events].count("async_span_create") == 2
    assert ("event_start", "root", "first") not in rec.events
    assert ("event_start", "root", "second") not in rec.events

    task_event_ends = [
        event[2]
        for event in rec.events
        if event[0] == "event_end" and event[1] == "async:asyncio.task"
    ]
    assert task_event_ends == ["first", "second"]
    assert [
        event for event in rec.events
        if event == ("span_end", "async:asyncio.task")
    ] == [
        ("span_end", "async:asyncio.task"),
        ("span_end", "async:asyncio.task"),
    ]


def test_implicit_asyncio_task_span_inherits_callstack_gate():
    rec = _fakes.Recorder()
    root = Span(_FakeNativeSpan("root", recorder=rec),
                enable_callstack_trace=True)

    async def main():
        token = ppctx.set_current_span(root)
        try:
            async def worker():
                child = ppctx.current_span()
                assert child is not root
                return child._enable_callstack_trace

            inherited = await asyncio.create_task(worker())
            await asyncio.sleep(0)  # run the task-done span finalizer
            return inherited
        finally:
            ppctx.reset_current_span(token)

    assert asyncio.run(main()) is True


def test_implicit_asyncio_task_span_times_out_while_task_keeps_running():
    """A never-finished inherited Task must not retain a native span forever."""
    root, rec = _make_root_span()
    root._async_task_span_timeout = 0.001

    async def main():
        token = ppctx.set_current_span(root)
        started = asyncio.Event()
        resume = asyncio.Event()
        seen = {}

        async def poll_forever():
            seen["before"] = ppctx.current_span()
            started.set()
            await resume.wait()
            seen["after"] = ppctx.current_span()
            await asyncio.Event().wait()

        task = asyncio.create_task(poll_forever())
        try:
            await started.wait()
            await asyncio.sleep(0.01)
            assert rec.events.count(
                ("span_end", "async:asyncio.task")) == 1

            resume.set()
            await asyncio.sleep(0)
            assert seen["before"] is not root
            assert seen["after"] is None
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            ppctx.reset_current_span(token)

    asyncio.run(main())
    # The eventual done callback is a no-op after timeout finalization.
    assert rec.events.count(("span_end", "async:asyncio.task")) == 1


def test_implicit_task_timeout_waits_for_suspended_span_event():
    """Timeout finalization must not drain a native event still in use."""
    root, rec = _make_root_span()
    root._async_task_span_timeout = 0.001

    async def main():
        token = ppctx.set_current_span(root)
        event_started = asyncio.Event()
        resume = asyncio.Event()

        async def worker():
            assert ppctx.current_span() is not root
            with pinpoint.trace("suspended"):
                event_started.set()
                await resume.wait()
            await asyncio.Event().wait()

        task = asyncio.create_task(worker())
        try:
            await event_started.wait()
            await asyncio.sleep(0.01)
            assert ("span_end", "async:asyncio.task") not in rec.events

            resume.set()
            await asyncio.sleep(0.01)
            assert rec.events.count(
                ("span_end", "async:asyncio.task")) == 1
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            ppctx.reset_current_span(token)

    asyncio.run(main())


def test_implicit_task_timeout_tolerates_a_null_async_span(monkeypatch):
    """``new_async_span`` hands back a ``_NullSpan`` when the parent's innermost
    event overflowed or the parent ended concurrently, and the timer is armed
    for it like any other span. Its callback reads ``_active_events`` off
    whatever it was given, so a null span missing that field turns the reap
    into an ``AttributeError`` logged by the event loop, once per such task."""
    from pinpoint.agent import _NullSpan  # type: ignore[attr-defined]

    root, _rec = _make_root_span()
    root._async_task_span_timeout = 0.001
    monkeypatch.setattr(type(root), "new_async_span",
                        lambda _self, _operation: _NullSpan())

    loop_errors = []

    async def main():
        token = ppctx.set_current_span(root)
        asyncio.get_running_loop().set_exception_handler(
            lambda _loop, context: loop_errors.append(context))

        async def worker():
            # Reading the current span is what forks it and arms the timer; a
            # task that never looks just keeps the parent's binding, so without
            # this the timeout path is never entered at all.
            assert ppctx.current_span() is not root
            await asyncio.Event().wait()      # outlives the timeout

        task = asyncio.create_task(worker())
        try:
            await asyncio.sleep(0.05)         # let the timer fire and reap
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            ppctx.reset_current_span(token)

    asyncio.run(main())

    assert loop_errors == [], loop_errors


def test_implicit_task_timeout_force_ends_leaked_span_event_after_grace(monkeypatch):
    """A SpanEvent that is created but never ended must not defeat the
    lifetime timer: without a bound, the timer re-arms at 1 Hz forever and
    the native span never finalizes — the exact leak the timer exists to
    reap. After the grace period the span ends regardless."""
    import pinpoint.tracer as tracer_mod

    monkeypatch.setattr(tracer_mod, "_TASK_SPAN_GRACE_MIN", 0.0)
    root, rec = _make_root_span()
    root._async_task_span_timeout = 0.001

    async def main():
        token = ppctx.set_current_span(root)
        leaked = asyncio.Event()

        async def worker():
            span = ppctx.current_span()
            assert span is not root
            span.new_span_event("leaked")  # never ended
            leaked.set()
            await asyncio.Event().wait()  # fire-and-forget: runs forever

        task = asyncio.create_task(worker())
        try:
            await leaked.wait()
            # timeout (1ms) + grace (max(1ms, 0)) both elapse well within this.
            await asyncio.sleep(0.05)
            assert rec.events.count(("span_end", "async:asyncio.task")) == 1
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            ppctx.reset_current_span(token)

    asyncio.run(main())


# ---------------------------------------------------------------------------
# Null-agent / disabled path
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# End-of-span guarantees on the failure paths
# ---------------------------------------------------------------------------

def test_async_trace_ends_orphan_span_when_handoff_block_raises():
    """If the caller raises inside `with async_trace(...)` before handing off,
    the async span must still be ended — otherwise it dangles forever."""
    root, rec = _make_root_span()
    token = ppctx.set_current_span(root)
    try:
        try:
            with pinpoint.async_trace("background"):
                # Caller blew up before passing async_span to a worker.
                raise RuntimeError("hand-off failed")
        except RuntimeError:
            pass
    finally:
        ppctx.reset_current_span(token)

    # Last recorded event must be the async span ending.
    assert ("span_end", "async:background") in rec.events


def test_null_span_new_async_span_is_noop():
    from pinpoint.agent import _NullSpan, _NullSpanEvent  # type: ignore[attr-defined]

    null = _NullSpan()
    async_span = null.new_async_span("bg")
    # Returned object behaves like a span with all-noop methods.
    with async_span:
        ev = async_span.new_span_event("inside")
        assert isinstance(ev, _NullSpanEvent)
        ev.end()
    async_span.end()  # idempotent


def test_null_span_fork_shares_self_without_allocating():
    """Unsampled fast path: forking for a child asyncio task returns the same
    stateless null span — no linked async span, no lifetime timer — instead of
    the full sampled-Span machinery, which would run per task under every
    unsampled request and ultimately no-op."""
    from pinpoint.agent import _NullSpan  # type: ignore[attr-defined]

    null = _NullSpan()
    forked = null._fork_for_async_task(task=object())
    assert forked is null
