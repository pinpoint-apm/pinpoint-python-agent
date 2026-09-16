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

"""redis-py instrumentation (sync + asyncio).

Wraps `Redis.execute_command` and pipeline execution. Span event operations
use the wrapped API method, while the Redis command is recorded as metadata.

Both the synchronous client (`redis.client`) and the asyncio client
(`redis.asyncio.client`) are covered — the async client is a distinct class,
so without its own hooks a FastAPI/asyncio service using ``redis.asyncio``
would silently lose every Redis node from its traces while the sync client
was traced. The async wrappers mirror the sync ones but await the wrapped
coroutine inside the span-event scope.
"""

from __future__ import annotations

from itertools import islice
from typing import Any

from ...annotation import ANNOTATION_ARG0
from ...context import current_span
from ...errors import safe_try
from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_REDIS
from .._util import (
    cached_endpoint,
    end_quietly,
    span_event_scope,
    span_is_sampled,
    wrap,
)

_OPERATION_EXECUTE_COMMAND = "redis.client.Redis.execute_command"
_OPERATION_PIPELINE_EXECUTE = "redis.client.Pipeline.execute"
_OPERATION_ASYNC_EXECUTE_COMMAND = "redis.asyncio.client.Redis.execute_command"
_OPERATION_ASYNC_PIPELINE_EXECUTE = "redis.asyncio.client.Pipeline.execute"
_MAX_PIPELINE_COMMANDS = 20
_MAX_PIPELINE_ANNOTATION_CHARS = 1024


class RedisInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        wrap("redis.client", "Redis.execute_command", _execute_command_wrapper)
        wrap("redis.client", "Pipeline.execute", _pipeline_execute_wrapper)


class AsyncRedisInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        wrap(
            "redis.asyncio.client", "Redis.execute_command",
            _async_execute_command_wrapper,
        )
        wrap(
            "redis.asyncio.client", "Pipeline.execute",
            _async_pipeline_execute_wrapper,
        )


def _command_name(args) -> str:
    """Redis command name for the ARG0 annotation. ``execute_command`` accepts
    a bytes command name; decode it so the trace records ``GET`` rather than
    ``b'GET'`` (mirrors the pipeline path's bytes handling)."""
    if not args:
        return "redis"
    cmd = args[0]
    if isinstance(cmd, (bytes, bytearray)):
        return cmd.decode("ascii", "replace")
    return str(cmd)


def _open_event(span, operation, instance):
    event = span.new_span_event(operation, service_type=SERVICE_TYPE_REDIS)
    try:
        _annotate_redis_target(event, instance)
    except Exception:
        # Never hand back a leaked event: the async callers fall back untraced.
        end_quietly(event)
        raise
    return event


def _execute_command_wrapper(wrapped, instance, args, kwargs):
    span = current_span()
    if span is None or not span_is_sampled(span):
        return wrapped(*args, **kwargs)
    event = _open_event(span, _OPERATION_EXECUTE_COMMAND, instance)
    command = _command_name(args)
    if command:
        event.annotate_string(ANNOTATION_ARG0, command)
    with span_event_scope(event):
        return wrapped(*args, **kwargs)


def _pipeline_execute_wrapper(wrapped, instance, args, kwargs):
    span = current_span()
    if span is None or not span_is_sampled(span):
        return wrapped(*args, **kwargs)
    event = _open_event(span, _OPERATION_PIPELINE_EXECUTE, instance)
    _annotate_pipeline_commands(event, getattr(instance, "command_stack", None) or [])
    with span_event_scope(event):
        return wrapped(*args, **kwargs)


async def _async_execute_command_wrapper(wrapped, instance, args, kwargs):
    span = current_span()
    if span is None or not span_is_sampled(span):
        return await wrapped(*args, **kwargs)
    # Unguarded by safe_wrapper (async body): a failure here would escape into the
    # user's awaited Redis call.
    event = None
    try:
        event = _open_event(span, _OPERATION_ASYNC_EXECUTE_COMMAND, instance)
        command = _command_name(args)
        if command:
            event.annotate_string(ANNOTATION_ARG0, command)
    except Exception:  # noqa: BLE001
        end_quietly(event)
        return await wrapped(*args, **kwargs)
    with span_event_scope(event):
        return await wrapped(*args, **kwargs)


async def _async_pipeline_execute_wrapper(wrapped, instance, args, kwargs):
    span = current_span()
    if span is None or not span_is_sampled(span):
        return await wrapped(*args, **kwargs)
    event = None
    try:
        event = _open_event(span, _OPERATION_ASYNC_PIPELINE_EXECUTE, instance)
        # Capture the queued command names before awaiting — redis-py clears
        # ``command_stack`` once execution completes.
        _annotate_pipeline_commands(
            event, getattr(instance, "command_stack", None) or [])
    except Exception:  # noqa: BLE001
        end_quietly(event)
        return await wrapped(*args, **kwargs)
    with span_event_scope(event):
        return await wrapped(*args, **kwargs)


@safe_try
def _annotate_pipeline_commands(event, stack) -> None:
    """Annotate the queued command names, bounded: at most
    ``_MAX_PIPELINE_COMMANDS`` names within ``_MAX_PIPELINE_ANNOTATION_CHARS``,
    the rest summarised as ``...(+N commands)``. Must run before
    ``Pipeline.execute`` since redis-py clears ``command_stack`` once execution
    completes; a large pipeline is never walked or stringified whole.
    """
    names: list = []
    chars = consumed = 0
    for entry in islice(stack, _MAX_PIPELINE_COMMANDS):
        if chars > _MAX_PIPELINE_ANNOTATION_CHARS:
            break  # already over the cap: nothing past here can be shown
        consumed += 1
        try:
            cmd = entry[0][0]
        except (TypeError, IndexError):
            continue
        # Slice before decoding so an oversized command name does not allocate
        # an equally oversized str.
        cap = _MAX_PIPELINE_ANNOTATION_CHARS + 1
        name = (cmd[:cap].decode("ascii", "replace") if isinstance(cmd, bytes)
                else str(cmd)[:cap])
        names.append(name)
        chars += len(name) + 1
    omitted = len(stack) - consumed
    while True:
        text = ",".join(names + ([f"...(+{omitted} commands)"] if omitted else []))
        if len(text) <= _MAX_PIPELINE_ANNOTATION_CHARS or not names:
            break
        names.pop()
        omitted += 1
    if text:
        event.annotate_string(ANNOTATION_ARG0, text)


@safe_try
def _annotate_redis_target(event, client) -> None:
    pool = getattr(client, "connection_pool", None)
    if pool is None:
        return
    event.set_end_point(_redis_pool_endpoint(pool))
    event.set_destination("REDIS")


def _resolve_pool_endpoint(p):
    kwargs = getattr(p, "connection_kwargs", {}) or {}
    return str(kwargs.get("host", "") or ""), kwargs.get("port")


def _redis_pool_endpoint(pool) -> str:
    # A pool's connection_kwargs are fixed, so the endpoint is memoized on the
    # pool via cached_endpoint — every command (including each pipelined one)
    # lands here, so the resolver lives at module scope rather than being a
    # fresh closure per command.
    return cached_endpoint(pool, _resolve_pool_endpoint) or ""


def instrument() -> None:
    RedisInstrumentor().instrument()


def instrument_async(*_args: Any, **_kwargs: Any) -> None:
    AsyncRedisInstrumentor().instrument()
