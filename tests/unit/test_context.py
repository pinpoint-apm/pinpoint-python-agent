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

"""ContextVar-based current span execution-context ownership."""

import asyncio
import contextvars
import threading

from pinpoint import context


class FakeSpan:
    def __init__(self, name: str) -> None:
        self.name = name


class ForkingFakeSpan(FakeSpan):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.forks = []

    def _fork_for_async_task(self, _task):
        child = FakeSpan(f"{self.name}.fork")
        self.forks.append(child)
        return child


def test_current_span_default_is_none():
    assert context.current_span() is None


def test_set_and_reset_round_trip():
    span = FakeSpan("s1")
    token = context.set_current_span(span)
    try:
        assert context.current_span() is span
    finally:
        context.reset_current_span(token)
    assert context.current_span() is None


def test_asyncio_task_inherits_current_span():
    async def _run():
        span = FakeSpan("outer")
        token = context.set_current_span(span)
        try:
            seen = []

            async def child():
                seen.append(context.current_span())

            await asyncio.gather(child(), child())
            assert seen == [span, span]
        finally:
            context.reset_current_span(token)

    asyncio.run(_run())


def test_task_owned_span_remains_visible_outside_task_until_reset():
    """Task ownership must not hide a same-thread span based on Task GC."""
    span = FakeSpan("sync-root")
    token = context.set_current_span(span)
    retained_tasks = []
    seen = []

    async def _run():
        task = asyncio.current_task()
        retained_tasks.append(task)
        assert context.current_span() is span  # task claims ownership

        loop = asyncio.get_running_loop()
        callback_ran = loop.create_future()

        def callback():
            seen.append(("call_soon", context.current_span()))
            callback_ran.set_result(None)

        loop.call_soon(callback)
        await callback_ran

        done = loop.create_future()
        done.add_done_callback(
            lambda _future: seen.append(("done", context.current_span())))
        done.set_result(None)
        await asyncio.sleep(0)

    try:
        asyncio.run(_run())
        # Keep the completed owner alive to prove visibility no longer flips
        # when its weakref happens to clear during GC.
        assert retained_tasks[0] is not None
        assert context.current_span() is span
        assert seen == [("call_soon", span), ("done", span)]
    finally:
        context.reset_current_span(token)


def test_sync_task_probe_does_not_call_current_task(monkeypatch):
    """The normal no-running-loop path must not raise-and-catch."""
    def fail_if_called():
        raise AssertionError("current_task() called outside a running loop")

    monkeypatch.setattr(asyncio, "current_task", fail_if_called)
    assert context._current_asyncio_task() is None


def test_adopt_current_span_skips_only_framework_task_fork():
    async def _run():
        span = ForkingFakeSpan("request")
        token = context.set_current_span(span)
        nested_seen = []

        async def framework_task():
            assert context._adopt_current_span() is span
            assert context.current_span() is span

            async def application_task():
                nested_seen.append(context.current_span())

            await asyncio.create_task(application_task())

        try:
            await asyncio.create_task(framework_task())
        finally:
            context.reset_current_span(token)

        assert len(span.forks) == 1
        assert nested_seen == span.forks

    asyncio.run(_run())


def test_thread_does_not_inherit_without_copy_context():
    span = FakeSpan("outer")
    token = context.set_current_span(span)
    seen_in_thread = []

    def target():
        seen_in_thread.append(context.current_span())

    t = threading.Thread(target=target)
    t.start()
    t.join()
    context.reset_current_span(token)

    # Raw threads do not inherit.
    assert seen_in_thread == [None]


def test_copy_context_hides_live_span_from_worker_thread():
    span = FakeSpan("outer")
    token = context.set_current_span(span)
    ctx = contextvars.copy_context()
    seen = []

    def target():
        seen.append(context.current_span())

    t = threading.Thread(target=ctx.run, args=(target,))
    t.start()
    t.join()
    context.reset_current_span(token)

    # A raw copied Context is not an ownership hand-off. Exposing the same
    # native span here would make it owned by both the origin and worker.
    assert seen == [None]


def test_concurrent_worker_threads_do_not_share_live_span():
    """A sampled native span has a single event stack, so two *concurrently*
    running worker threads (``gather(sync_to_async(f)(), sync_to_async(g)())``)
    must not receive the live span. Both copied contexts run detached instead
    of corrupting the native stack."""
    span = FakeSpan("outer")
    token = context.set_current_span(span)
    first_seen = []
    second_seen = []
    first_claimed = threading.Event()
    second_done = threading.Event()

    def first():
        first_seen.append(context.current_span())
        first_claimed.set()
        second_done.wait(5)  # keep the borrow live while the second looks up

    def second():
        assert first_claimed.wait(5)
        second_seen.append(context.current_span())
        second_done.set()

    t1 = threading.Thread(target=contextvars.copy_context().run, args=(first,))
    t2 = threading.Thread(target=contextvars.copy_context().run, args=(second,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    context.reset_current_span(token)

    assert first_seen == [None]
    assert second_seen == [None]


def test_sequential_worker_threads_never_borrow_live_span():
    """The single-thread contract is lifetime-wide, not merely a ban on
    concurrent calls, so sequential workers also cannot borrow the root."""
    import gc

    span = FakeSpan("outer")
    token = context.set_current_span(span)
    seen = []

    def target():
        seen.append(context.current_span())

    for _ in range(3):
        ctx = contextvars.copy_context()
        t = threading.Thread(target=ctx.run, args=(target,))
        t.start()
        t.join()
        del ctx, t
        gc.collect()
    context.reset_current_span(token)

    assert seen == [None, None, None]


def test_same_worker_thread_gets_no_live_span_from_copied_contexts():
    """Thread-pool reuse does not turn a copied Context into an explicit
    async-span hand-off."""
    span = FakeSpan("outer")
    token = context.set_current_span(span)
    ctx1 = contextvars.copy_context()
    ctx2 = contextvars.copy_context()
    seen = []

    def target():
        seen.append(ctx1.run(context.current_span))
        seen.append(ctx2.run(context.current_span))  # ctx1 still referenced

    t = threading.Thread(target=target)
    t.start()
    t.join()
    context.reset_current_span(token)

    assert seen == [None, None]


class _DetachedView:
    sampled = False


class DetachingFakeSpan(FakeSpan):
    """Fake span exposing tracer.Span's detached-view hook."""

    def _detached_context_span(self):
        return _DetachedView()


def test_worker_lookup_does_not_poison_origin_context():
    """asgiref's ``sync_to_async`` copies contextvar *changes* a worker made
    back into the caller's context (``_restore_context``). A foreign-thread
    lookup must therefore never write its detached view to the ContextVar —
    doing so would replace the owner's live binding after the hop and untrace
    the rest of the request."""
    span = DetachingFakeSpan("outer")
    token = context.set_current_span(span)
    ctx = contextvars.copy_context()
    seen = []

    def worker():
        seen.append(context.current_span())
        seen.append(context.current_span())

    t = threading.Thread(target=lambda: ctx.run(worker))
    t.start()
    t.join()

    # The worker observes a non-None no-op placeholder (nested-server dedup
    # relies on non-None), never the live span — and repeated lookups reuse
    # the one cached on the shared binding.
    assert all(isinstance(v, _DetachedView) for v in seen)
    assert seen[0] is seen[1]

    # Emulate asgiref's restore step, then verify the origin context still
    # resolves the live span.
    for cvar in ctx:
        cvalue = ctx.get(cvar)
        try:
            if cvar.get() != cvalue:
                cvar.set(cvalue)
        except LookupError:
            cvar.set(cvalue)

    assert context.current_span() is span
    context.reset_current_span(token)


def test_span_activation_reuses_binding_on_same_thread():
    span = FakeSpan("stream")
    activation = context.SpanActivation(span)
    token = activation.set()
    first_binding = context._current_span.get()
    assert context.current_span() is span
    context.reset_current_span(token)
    token = activation.set()
    assert context._current_span.get() is first_binding
    assert context.current_span() is span
    context.reset_current_span(token)
    assert context.current_span() is None


def test_span_activation_rebinds_when_thread_changes():
    span = FakeSpan("stream")
    activation = context.SpanActivation(span)
    token = activation.set()
    owner_binding = context._current_span.get()
    context.reset_current_span(token)

    seen = {}

    def _worker():
        t = activation.set()
        seen["binding"] = context._current_span.get()
        context.reset_current_span(t)

    worker = threading.Thread(target=_worker)
    worker.start()
    worker.join()
    assert seen["binding"] is not owner_binding
    assert seen["binding"].thread_id != owner_binding.thread_id
