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

"""aio-pika (async RabbitMQ) instrumentation — producer + consumer.

``Exchange.publish`` is the single publish entry every higher-level helper
(``default_exchange.publish``, ``RobustExchange.publish``) funnels through. The
caller's ``Message`` is user-owned and may be published concurrently — aio-pika
publishes interleave at the channel-lock await — so it is never mutated: each
publish gets a shallow copy carrying this trace's headers in a fresh dict.

Consumers arrive two ways, ``Queue.consume(callback)`` and
``QueueIterator.__anext__``. The iterator registers its own buffering callback
(``QueueIterator.on_message``) through that same ``Queue.consume``, so the
consume wrapper detects and skips it — otherwise every iterator-delivered
message would produce two root spans.

See ``README.md`` for what each path records.
"""

from __future__ import annotations

import copy
import functools
import inspect
from typing import Optional

from ..._log import get_logger
from ...agent import get_agent
from ...context import current_span
from ...instrumentor import BaseInstrumentor
from ...propagator import inject_items
from ...service_type import SERVICE_TYPE_RABBITMQ_CLIENT
from .._amqp import (
    annotate_publish as _amqp_annotate_publish,
    close_consumer_scope,
    open_consumer_scope,
)
from .._util import (
    cached_endpoint,
    end_quietly,
    record_exception_on_span,
    replace_arg,
    span_event_scope,
    span_is_sampled,
    wrap,
)

_log = get_logger("aio_pika")
_OPERATION_EXCHANGE_PUBLISH = "aio_pika.exchange.Exchange.publish"


class AioPikaInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        # ``RobustExchange.publish`` inherits ``Exchange.publish`` unchanged, so the
        # base class covers both. ``RobustQueue.consume`` and
        # ``RobustQueueIterator.__anext__`` are overridden, though, and
        # ``connect_robust`` (the production default) returns the robust variants —
        # so the consumer hooks go on the base class *and* the subclass.
        wrap("aio_pika.exchange", "Exchange.publish", _exchange_publish_wrapper)
        wrap("aio_pika.queue", "Queue.consume", _queue_consume_wrapper)
        wrap(
            "aio_pika.queue",
            "QueueIterator.__anext__",
            _queue_iterator_anext_wrapper,
        )
        wrap(
            "aio_pika.robust_queue",
            "RobustQueue.consume",
            _queue_consume_wrapper,
        )
        wrap(
            "aio_pika.robust_queue",
            "RobustQueueIterator.__anext__",
            _queue_iterator_anext_wrapper,
        )


# ---------------------------------------------------------------- producer

async def _exchange_publish_wrapper(wrapped, instance, args, kwargs):
    """Wrap ``Exchange.publish(message, routing_key=..., **kwargs)``.

    aio-pika passes the body via the ``Message`` object's ``.body`` and headers
    via ``.headers``. The caller's ``Message`` is user-owned and may be
    published concurrently — aio-pika publishes await the channel lock, so two
    ``publish(msg, ...)`` calls on one shared ``msg`` interleave there. So we
    never mutate it in place: we inject this trace's headers into a per-publish
    shallow copy and publish the copy. Each publish then carries its own header
    dict (no cross-publish contamination, no restore, no shared-state race) and
    the user's ``Message`` is left exactly as they created it.
    """
    span = current_span()
    if span is None:
        return await wrapped(*args, **kwargs)

    message = args[0] if args else kwargs.get("message")

    # Unguarded by safe_wrapper (async body): contain the pre-await setup here,
    # ending any event we opened and falling back to an untraced publish of the
    # *original* args. The user ``await`` stays outside the try, so it is never
    # swallowed nor awaited twice.
    event = None
    sampled = False
    publish_args, publish_kwargs = args, kwargs
    try:
        # Open the event before inject: the context written into the message headers
        # must carry this call's own depth/sequence.
        event = span.new_span_event(
            _OPERATION_EXCHANGE_PUBLISH,
            service_type=SERVICE_TYPE_RABBITMQ_CLIENT,
        )
        traced_message = _message_with_trace_headers(message, span)
        if traced_message is not None and traced_message is not message:
            publish_args, publish_kwargs = replace_arg(
                args, kwargs, 0, "message", traced_message
            )
        sampled = span_is_sampled(span)
        if sampled:
            routing_key = (
                args[1] if len(args) > 1 else kwargs.get("routing_key", "")
            ) or ""
            exchange_name = str(getattr(instance, "name", None) or "")
            _amqp_annotate_publish(
                event, exchange_name, routing_key, instance,
                _connection_endpoint,
            )
    except Exception:  # noqa: BLE001
        _log.debug("aio_pika publish instrumentation failed", exc_info=True)
        end_quietly(event)
        return await wrapped(*args, **kwargs)

    # No finally/restore: we published a copy, so the user-owned Message was
    # never touched and there is nothing to undo.
    if not sampled:
        return await wrapped(*publish_args, **publish_kwargs)
    with span_event_scope(event):
        return await wrapped(*publish_args, **publish_kwargs)


def _message_with_trace_headers(message, span):
    """Return a shallow copy of ``message`` carrying this publish's trace
    headers, or ``None`` when there's nothing to inject / no copy is possible.

    The copy gets its own freshly built headers dict (user headers + this
    trace's context), so concurrent publishes of the same user ``Message``
    never race on shared header state and the caller's ``Message`` is left
    untouched. Never raises: a copy/inject failure just means this one publish
    goes out untraced, not a crashed ``publish()``.
    """
    if message is None or span is None:
        return None
    try:
        items = inject_items(span)  # already a tuple; no copy needed
        if not items:
            return None
        original = getattr(message, "headers", None)
        headers = dict(original or {})
        for key, value in items:
            headers[str(key)] = str(value)
        # Shallow copy — aio-pika ``Message`` defines ``__copy__``. The outer
        # except returns None on failure, so an uncopyable message just means
        # this one publish goes out untraced instead of crashing ``publish()``.
        copied = copy.copy(message)
        # aio-pika's ``Message.__copy__`` shares the original headers dict, so
        # assigning a *new* dict (not mutating in place) is what keeps the
        # original untouched. No fallback via ``message.properties`` when the
        # setter rejects it: aio-pika rebuilds that ``Basic.Properties`` on
        # every access, so writing there would report a success that never
        # shipped — the except below reports failure instead.
        copied.headers = headers
        return copied
    except Exception:  # noqa: BLE001
        return None


def _resolve_broker_endpoint(conn):
    url = getattr(conn, "url", None)
    return getattr(url, "host", None), getattr(url, "port", None)


def _connection_endpoint(obj) -> Optional[str]:
    """Best-effort ``host:port`` extraction from ``Exchange.channel.connection``
    (or any object hanging off the same chain)."""
    try:
        channel = getattr(obj, "channel", None)
        if callable(channel):
            channel = channel()
        connection = getattr(channel, "connection", None)
    except Exception:  # noqa: BLE001
        return None

    return cached_endpoint(connection, _resolve_broker_endpoint)


# ---------------------------------------------------------------- consumer

def _queue_consume_wrapper(wrapped, instance, args, kwargs):
    """Wrap the user-provided callback so every delivery opens a span.

    Signature: ``Queue.consume(callback, no_ack=False, ...)``."""
    callback = args[0] if args else kwargs.get("callback")
    if callback is None:
        return wrapped(*args, **kwargs)
    if getattr(callback, "_pinpoint_consumer_wrapped", False):
        # ``RobustQueue.consume`` delegates to ``Queue.consume`` and both are
        # instrumented — the callback arriving here is already our wrapper.
        # Re-wrapping would open two nested root spans per delivery.
        return wrapped(*args, **kwargs)
    if _is_queue_iterator_callback(callback):
        # ``queue.iterator()`` registers its buffering callback through this same
        # ``Queue.consume``, but the iterator path's span comes from the ``__anext__``
        # wrapper — wrapping the buffer too would emit two root spans per message.
        return wrapped(*args, **kwargs)
    new_args, new_kwargs = replace_arg(
        args, kwargs, 0, "callback", _wrap_consumer_callback(callback, instance))
    return wrapped(*new_args, **new_kwargs)


def _is_queue_iterator_callback(callback) -> bool:
    """True when ``callback`` is a ``QueueIterator``'s (or robust subclass's)
    bound ``on_message`` — aio-pika internal plumbing, not a user handler."""
    owner = getattr(callback, "__self__", None)
    if owner is None:
        return False
    try:
        from aio_pika.abc import AbstractQueueIterator
    except Exception:  # noqa: BLE001
        return False
    return isinstance(owner, AbstractQueueIterator)


def _wrap_consumer_callback(callback, queue):
    """Return an async callback that opens a Pinpoint span around the
    user's handler. aio-pika passes a single ``IncomingMessage`` arg."""
    @functools.wraps(callback)
    async def _consumer(message):
        agent = get_agent()
        if agent is None or not agent.enabled:
            return await _invoke_handler(callback, message)

        # This closure replaces the user's callback and is not under safe_wrapper, so
        # a span-setup failure escaping into the dispatch loop would kill the consumer
        # without running the handler. A None scope runs it untraced instead.
        scope = open_consumer_scope(
            agent, message, message, queue, _connection_endpoint,
            "aio_pika.consume", held=True)
        if scope is None:
            return await _invoke_handler(callback, message)
        try:
            return await _invoke_handler(callback, message)
        except BaseException as exc:
            record_exception_on_span(scope[0], exc)
            raise
        finally:
            close_consumer_scope(scope)

    # functools.wraps copied the user callback's attributes; stamp ours after
    # so the RobustQueue→Queue delegation (see _queue_consume_wrapper) can
    # recognize an already-wrapped callback.
    _consumer._pinpoint_consumer_wrapped = True
    return _consumer


async def _invoke_handler(callback, message):
    """Call the user's consume callback, awaiting only when it returns an
    awaitable.

    aio-pika 9.x routes callbacks through ``ensure_awaitable``, which still
    accepts deprecated plain (sync) callbacks. Since our replacement wrapper
    *is* a coroutine function it passes through unchanged — so we must not
    blindly ``await callback(...)``: for a sync callback that would raise
    ``TypeError`` after the handler already ran, failing every delivery.
    """
    result = callback(message)
    if inspect.isawaitable(result):
        return await result
    return result


async def _queue_iterator_anext_wrapper(wrapped, instance, args, kwargs):
    """Each ``async for message in queue_iterator`` step delivers one
    message — open a delivery-only root span for it. The user's handler
    runs *outside* this method, so we close the span as soon as we get the
    message; child events come from downstream instrumentations operating
    inside the user's loop body, not from us."""
    message = await wrapped(*args, **kwargs)

    agent = get_agent()
    if agent is None or not agent.enabled:
        return message

    # Async body: safe_wrapper can't guard it, and an escaping exception here
    # would terminate the user's ``async for`` with the message unacked. The
    # scope helpers never raise — deliver the fetched message either way.
    # No user handler runs here — the span represents just the delivery.
    close_consumer_scope(open_consumer_scope(
        agent, message, message, instance, _connection_endpoint,
        "aio_pika.consume.iterator"))
    return message


def instrument() -> None:
    AioPikaInstrumentor().instrument()
