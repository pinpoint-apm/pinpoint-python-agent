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

"""redis-py instrumentation.

Drives the wrappers against an in-memory fake client/pool — no redis
dependency. Validates that ``execute_command`` and ``Pipeline.execute`` open a
span event, record the command name(s) as the ARG0 annotation, resolve the
pool endpoint (memoized on the pool), and pass through when unsampled / no span.
"""

from __future__ import annotations

import asyncio

import pytest

from pinpoint.annotation import ANNOTATION_ARG0
from pinpoint.instrumentations import redis as redis_instr


# ---------------------------------------------------------------------------
# Fakes (span fakes + ``push_span`` fixture come from _fakes / conftest)
# ---------------------------------------------------------------------------

class _Pool:
    def __init__(self, host="cache.test", port=6379):
        self.connection_kwargs = {"host": host, "port": port}


class _Client:
    def __init__(self, pool=None):
        self.connection_pool = pool if pool is not None else _Pool()


# ---------------------------------------------------------------------------
# execute_command
# ---------------------------------------------------------------------------

def test_execute_command_emits_event_and_records_command(push_span):
    _sp, rec = push_span
    client = _Client()

    def wrapped(*_a, **_kw):
        return b"OK"

    out = redis_instr._execute_command_wrapper(
        wrapped, client, ("SET", "user:42", "v"), {},
    )
    assert out == b"OK"
    op = redis_instr._OPERATION_EXECUTE_COMMAND
    assert ("event_start", "root", op) in rec.events
    assert ("event_end", "root", op) in rec.events

    ev = _sp._native.all_events[-1]
    # Command name recorded as the ARG0 annotation ...
    assert ("str", ANNOTATION_ARG0, "SET") in ev.annotations.entries
    # ... and the redis target resolved from the pool.
    assert ev.endpoint == "cache.test:6379"
    assert ev.destination == "REDIS"


def test_execute_command_no_active_span_passes_through():
    ran = {"n": 0}

    def wrapped(*_a, **_kw):
        ran["n"] += 1
        return b"OK"

    # No current span set — the wrapper must run the command untraced.
    out = redis_instr._execute_command_wrapper(wrapped, _Client(), ("GET", "k"), {})
    assert out == b"OK"
    assert ran["n"] == 1


def test_execute_command_unsampled_passes_through(push_span, monkeypatch):
    """An unsampled span skips event creation entirely (redis annotates only on
    the sampled path)."""
    _sp, rec = push_span
    monkeypatch.setattr(redis_instr, "span_is_sampled", lambda _s: False)

    def wrapped(*_a, **_kw):
        return b"OK"

    out = redis_instr._execute_command_wrapper(wrapped, _Client(), ("GET", "k"), {})
    assert out == b"OK"
    assert not [e for e in rec.events if e[0] == "event_start"]


def test_execute_command_without_args_defaults_command_name(push_span):
    """A call with no positional args must not IndexError — command defaults to
    ``"redis"``."""
    _sp, rec = push_span

    def wrapped(*_a, **_kw):
        return None

    redis_instr._execute_command_wrapper(wrapped, _Client(), (), {})
    ev = _sp._native.all_events[-1]
    assert ("str", ANNOTATION_ARG0, "redis") in ev.annotations.entries


# ---------------------------------------------------------------------------
# asyncio client (redis.asyncio.client) — mirrors the sync wrappers, awaits
# ---------------------------------------------------------------------------

def test_execute_command_decodes_bytes_command_name(push_span):
    """A bytes command name (legal in execute_command) is decoded to str, so
    ARG0 reads ``GET`` rather than ``b'GET'``."""
    _sp, rec = push_span

    def wrapped(*_a, **_kw):
        return b"OK"

    redis_instr._execute_command_wrapper(wrapped, _Client(), (b"GET", "k"), {})
    ev = _sp._native.all_events[-1]
    assert ("str", ANNOTATION_ARG0, "GET") in ev.annotations.entries
    assert ("str", ANNOTATION_ARG0, "b'GET'") not in ev.annotations.entries


def test_async_execute_command_decodes_bytes_command_name(push_span):
    _sp, rec = push_span

    async def wrapped(*_a, **_kw):
        return b"OK"

    asyncio.run(redis_instr._async_execute_command_wrapper(
        wrapped, _Client(), (b"SET", "k", "v"), {},
    ))
    ev = _sp._native.all_events[-1]
    assert ("str", ANNOTATION_ARG0, "SET") in ev.annotations.entries


def test_async_execute_command_emits_event_and_records_command(push_span):
    _sp, rec = push_span
    client = _Client()

    async def wrapped(*_a, **_kw):
        return b"OK"

    out = asyncio.run(redis_instr._async_execute_command_wrapper(
        wrapped, client, ("SET", "user:42", "v"), {},
    ))
    assert out == b"OK"
    op = redis_instr._OPERATION_ASYNC_EXECUTE_COMMAND
    assert ("event_start", "root", op) in rec.events
    assert ("event_end", "root", op) in rec.events
    ev = _sp._native.all_events[-1]
    assert ("str", ANNOTATION_ARG0, "SET") in ev.annotations.entries
    assert ev.endpoint == "cache.test:6379"
    assert ev.destination == "REDIS"


def test_async_execute_command_no_active_span_passes_through():
    ran = {"n": 0}

    async def wrapped(*_a, **_kw):
        ran["n"] += 1
        return b"OK"

    out = asyncio.run(redis_instr._async_execute_command_wrapper(
        wrapped, _Client(), ("GET", "k"), {},
    ))
    assert out == b"OK"
    assert ran["n"] == 1


def test_async_execute_command_unsampled_passes_through(push_span, monkeypatch):
    _sp, rec = push_span
    monkeypatch.setattr(redis_instr, "span_is_sampled", lambda _s: False)

    async def wrapped(*_a, **_kw):
        return b"OK"

    out = asyncio.run(redis_instr._async_execute_command_wrapper(
        wrapped, _Client(), ("GET", "k"), {},
    ))
    assert out == b"OK"
    assert not [e for e in rec.events if e[0] == "event_start"]


def test_async_execute_command_setup_failure_still_runs_untraced(push_span, monkeypatch):
    """An event-creation failure must not break the awaited Redis call — the
    async body has no safe_wrapper guard, so the wrapper handles it itself."""
    _sp, rec = push_span

    def boom(*_a, **_kw):
        raise RuntimeError("native new_span_event failed")
    monkeypatch.setattr(_sp._native, "new_span_event", boom)

    ran = {"n": 0}

    async def wrapped(*_a, **_kw):
        ran["n"] += 1
        return b"OK"

    out = asyncio.run(redis_instr._async_execute_command_wrapper(
        wrapped, _Client(), ("GET", "k"), {},
    ))
    assert out == b"OK"
    assert ran["n"] == 1


def test_async_pipeline_execute_annotates_joined_command_names(push_span):
    _sp, rec = push_span

    class _Pipeline:
        def __init__(self):
            self.connection_pool = _Pool()
            self.command_stack = [
                (("INCR", "counter"), {}),
                (("EXPIRE", "counter", 60), {}),
            ]

    async def wrapped(*_a, **_kw):
        return [1, True]

    out = asyncio.run(redis_instr._async_pipeline_execute_wrapper(
        wrapped, _Pipeline(), (), {},
    ))
    assert out == [1, True]
    op = redis_instr._OPERATION_ASYNC_PIPELINE_EXECUTE
    assert ("event_start", "root", op) in rec.events
    ev = _sp._native.all_events[-1]
    assert ("str", ANNOTATION_ARG0, "INCR,EXPIRE") in ev.annotations.entries


def test_async_instrumentor_wraps_real_redis_asyncio_client():
    """The instrumentor resolves the real async-client module/attr path and
    installs pinpoint's wrapt layer — a typo in the module or method name would
    make ``wrap`` fail silently and leave ``already_wrapped`` False.

    Asserted via ``already_wrapped`` rather than attribute identity so the test
    is robust to process-global wrap state a prior test in the suite may have
    left (``wrap`` is idempotent)."""
    pytest.importorskip("redis.asyncio.client")
    from pinpoint.instrumentations._util import already_wrapped
    from pinpoint.instrumentations.redis import AsyncRedisInstrumentor

    instr = AsyncRedisInstrumentor()
    instr.instrument()
    try:
        assert already_wrapped("redis.asyncio.client", "Redis.execute_command")
        assert already_wrapped("redis.asyncio.client", "Pipeline.execute")
    finally:
        instr.uninstrument()


# ---------------------------------------------------------------------------
# Pipeline.execute
# ---------------------------------------------------------------------------

def test_pipeline_execute_annotates_joined_command_names(push_span):
    _sp, rec = push_span

    class _Pipeline:
        def __init__(self):
            self.connection_pool = _Pool()
            # redis-py stores queued commands as (args_tuple, options_dict).
            self.command_stack = [
                (("INCR", "counter"), {}),
                (("EXPIRE", "counter", 60), {}),
            ]

    def wrapped(*_a, **_kw):
        return [1, True]

    out = redis_instr._pipeline_execute_wrapper(wrapped, _Pipeline(), (), {})
    assert out == [1, True]
    op = redis_instr._OPERATION_PIPELINE_EXECUTE
    assert ("event_start", "root", op) in rec.events
    ev = _sp._native.all_events[-1]
    assert ("str", ANNOTATION_ARG0, "INCR,EXPIRE") in ev.annotations.entries


def test_annotate_pipeline_commands_decodes_bytes_and_skips_malformed():
    """Command names are joined by comma; bytes decode to str, and malformed
    stack entries (empty / non-indexable) are skipped rather than crashing."""
    class _Ev:
        def __init__(self):
            self.recorded = []

        def annotate_string(self, key, value):
            self.recorded.append((key, value))

    ev = _Ev()
    stack = [
        ((b"GET", b"k1"), {}),   # bytes command → decoded
        ((), {}),                # empty args → skipped
        (None, {}),              # non-indexable cmd_args → skipped
        (("SET", "k2", "v"), {}),
    ]
    redis_instr._annotate_pipeline_commands(ev, stack)
    assert ev.recorded == [(ANNOTATION_ARG0, "GET,SET")]


def test_annotate_pipeline_commands_empty_stack_records_nothing():
    class _Ev:
        def __init__(self):
            self.recorded = []

        def annotate_string(self, key, value):
            self.recorded.append((key, value))

    ev = _Ev()
    redis_instr._annotate_pipeline_commands(ev, [])
    assert ev.recorded == []


def test_annotate_pipeline_commands_limits_command_count():
    class _Ev:
        def __init__(self):
            self.recorded = []

        def annotate_string(self, key, value):
            self.recorded.append((key, value))

    limit = redis_instr._MAX_PIPELINE_COMMANDS

    class _LargeStack:
        def __len__(self):
            return 10_000

        def __iter__(self):
            for i in range(limit):
                yield ((f"CMD{i}",), {})
            raise AssertionError("pipeline entries beyond the count cap were inspected")

    ev = _Ev()
    redis_instr._annotate_pipeline_commands(ev, _LargeStack())

    expected = ",".join(
        [*(f"CMD{i}" for i in range(limit)), "...(+9980 commands)"],
    )
    assert ev.recorded == [(ANNOTATION_ARG0, expected)]


def test_annotate_pipeline_commands_limits_annotation_size():
    class _Ev:
        def __init__(self):
            self.recorded = []

        def annotate_string(self, key, value):
            self.recorded.append((key, value))

    class _MustNotStringify:
        def __str__(self):
            raise AssertionError("commands beyond the size cap must not be rendered")

    prefix = "A" * (redis_instr._MAX_PIPELINE_ANNOTATION_CHARS // 2)
    stack = [
        ((prefix,), {}),
        (("B" * redis_instr._MAX_PIPELINE_ANNOTATION_CHARS,), {}),
        ((_MustNotStringify(),), {}),
    ]

    ev = _Ev()
    redis_instr._annotate_pipeline_commands(ev, stack)

    annotation = ev.recorded[0][1]
    assert annotation == f"{prefix},...(+2 commands)"
    assert len(annotation) <= redis_instr._MAX_PIPELINE_ANNOTATION_CHARS


def test_annotate_pipeline_commands_caps_oversized_bytes_before_decode():
    class _Ev:
        def __init__(self):
            self.recorded = []

        def annotate_string(self, key, value):
            self.recorded.append((key, value))

    ev = _Ev()
    stack = [((b"X" * (redis_instr._MAX_PIPELINE_ANNOTATION_CHARS * 10),), {})]
    redis_instr._annotate_pipeline_commands(ev, stack)

    assert ev.recorded == [(ANNOTATION_ARG0, "...(+1 commands)")]
    assert len(ev.recorded[0][1]) <= redis_instr._MAX_PIPELINE_ANNOTATION_CHARS


# ---------------------------------------------------------------------------
# _redis_pool_endpoint
# ---------------------------------------------------------------------------

def test_redis_pool_endpoint_formats_and_memoizes():
    pool = _Pool(host="db.internal", port=6380)
    assert redis_instr._redis_pool_endpoint(pool) == "db.internal:6380"
    # Resolved value is stashed on the pool so every command reuses it ...
    assert pool._pinpoint_endpoint == "db.internal:6380"
    # ... and a later kwargs change does NOT re-resolve (memoized by design).
    pool.connection_kwargs = {"host": "other", "port": 1}
    assert redis_instr._redis_pool_endpoint(pool) == "db.internal:6380"


def test_redis_pool_endpoint_handles_missing_port():
    pool = _Pool(host="just-host", port=None)
    assert redis_instr._redis_pool_endpoint(pool) == "just-host"


def test_redis_pool_endpoint_handles_empty_kwargs():
    class _BarePool:
        connection_kwargs = {}

    assert redis_instr._redis_pool_endpoint(_BarePool()) == ""


# ---------------------------------------------------------------------------
# Setup-failure cleanup
# ---------------------------------------------------------------------------

def _boom(*_a, **_kw):
    raise RuntimeError("annotation blew up")


@pytest.mark.parametrize("wrapper_name, instance", [
    ("_async_execute_command_wrapper", _Client()),
    ("_async_pipeline_execute_wrapper", _Client()),
])
def test_async_setup_failure_ends_the_event_it_opened(
        push_span, monkeypatch, wrapper_name, instance):
    """A setup failure after the event was created must end it before falling
    back to the untraced call.

    A stranded event stays the innermost entry on the span's event stack, so
    later outbound injection rides it and later events nest under it until the
    root span drains the stack at end().
    """
    sp, _rec = push_span
    monkeypatch.setattr(redis_instr, "_annotate_redis_target", _boom)
    ran = {"n": 0}

    async def wrapped(*_a, **_kw):
        ran["n"] += 1
        return b"OK"

    wrapper = getattr(redis_instr, wrapper_name)
    out = asyncio.run(wrapper(wrapped, instance, ("SET", "k", "v"), {}))

    # The user's call still ran exactly once, untraced ...
    assert out == b"OK"
    assert ran["n"] == 1
    # ... and nothing was left open on the span's event stack.
    assert sp._active_events == []
