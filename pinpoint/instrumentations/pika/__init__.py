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

"""pika (RabbitMQ) instrumentation — producer + consumer.

One publish hook covers both client shapes: pika 1.x routes
``BlockingChannel.basic_publish`` through ``pika.channel.Channel.basic_publish``.

Each of the three delivery shapes is wrapped where the *user's* code runs, not
where pika buffers the delivery. The seam that is easy to get wrong is
``BlockingChannel.basic_consume``: down at ``Channel.basic_consume`` pika
registers only its internal ``_on_consumer_message_delivery`` sink, which just
enqueues, so wrapping there would end the span before the handler ever ran —
the ``Channel.basic_consume`` hook detects and skips that sink. Direct
non-blocking ``Channel`` users hand their real callback to the same method, so
that hook still wraps it; ``_pinpoint_consumer_wrapped`` guards double-wrapping.

See ``README.md`` for what each shape records.
"""

from __future__ import annotations

import copy
import functools
import inspect
from typing import Any

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
    agent_disabled,
    cached_endpoint,
    no_current_span,
    record_exception_on_span,
    replace_arg,
    span_event_scope,
    span_is_sampled,
    wrap,
)

_OPERATION_BASIC_PUBLISH = "pika.channel.Channel.basic_publish"


class PikaInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        # Producer: pika.channel.Channel.basic_publish is the canonical
        # publish path; BlockingChannel.basic_publish funnels into it too.
        # Per-message hot path: skip the safe_wrapper machinery when no span
        # is current (the wrapper's own first early-out).
        wrap("pika.channel", "Channel.basic_publish", _basic_publish_wrapper,
             precheck=no_current_span)
        # Non-blocking Channel registration (SelectConnection / adapters) hands the
        # user's real callback straight here, so wrapping it makes the span active
        # during the handler. On the BlockingChannel path this hook receives pika's
        # internal buffering sink instead and skips it.
        wrap("pika.channel", "Channel.basic_consume", _basic_consume_wrapper)
        # BlockingChannel.basic_consume stores the user callback and dispatches it
        # later, so wrap it here to span the actual handler rather than the internal
        # enqueue. One wrapper serves both seams: it resolves the callback from the
        # signature and skips the internal sink.
        wrap(
            "pika.adapters.blocking_connection",
            "BlockingChannel.basic_consume",
            _basic_consume_wrapper,
        )
        # BlockingChannel.consume() is the callback-less generator form; span
        # each yielded delivery with the contextvar set across the yield so the
        # loop body's child calls stitch onto the consumer span.
        wrap(
            "pika.adapters.blocking_connection",
            "BlockingChannel.consume",
            _blocking_consume_wrapper,
        )
        # Sync poll path: BlockingChannel.basic_get.
        wrap(
            "pika.adapters.blocking_connection",
            "BlockingChannel.basic_get",
            _blocking_basic_get_wrapper,
            precheck=agent_disabled,
        )


# ---------------------------------------------------------------- producer

def _basic_publish_wrapper(wrapped, instance, args, kwargs):
    span = current_span()
    if span is None:
        return wrapped(*args, **kwargs)

    # basic_publish(exchange, routing_key, body, properties=None, mandatory=False)
    n = len(args)
    exchange = args[0] if n > 0 else kwargs.get("exchange", "")
    routing_key = args[1] if n > 1 else kwargs.get("routing_key", "")
    properties = args[3] if n > 3 else kwargs.get("properties")

    # Open the event before inject: the context written into the message headers
    # must carry this call's own depth/sequence.
    event = span.new_span_event(
        _OPERATION_BASIC_PUBLISH,
        service_type=SERVICE_TYPE_RABBITMQ_CLIENT,
    )
    properties = _ensure_properties_with_headers(properties, span)
    new_args, new_kwargs = replace_arg(args, kwargs, 3, "properties", properties)

    if not span_is_sampled(span):
        return wrapped(*new_args, **new_kwargs)

    _amqp_annotate_publish(
        event, exchange, routing_key, instance, _connection_endpoint,
    )

    with span_event_scope(event):
        return wrapped(*new_args, **new_kwargs)


@functools.cache
def _basic_properties_cls():
    # Resolved once instead of on every publish that passes ``properties=None``.
    from pika.spec import BasicProperties  # type: ignore[import-not-found]
    return BasicProperties


def _ensure_properties_with_headers(properties, span) -> Any:
    """Return a ``BasicProperties`` carrying our trace headers.

    The caller's object is never modified: pika apps commonly reuse one
    ``BasicProperties`` across publishes, and mutating it in place would
    leak this trace's ``Pinpoint-*`` headers into later publishes made
    outside any span (stitching them into a long-ended trace downstream).
    The caller swaps this shallow copy into the outgoing call, leaving the
    user's object untouched. Creates a fresh instance if the caller passed
    None; on any failure returns the original so the publish itself is never
    at risk.
    """
    try:
        items = inject_items(span)  # already a tuple; no copy needed
        if not items:
            return properties
        if properties is None:
            new_props = _basic_properties_cls()()
        else:
            new_props = copy.copy(properties)
        headers = dict(getattr(new_props, "headers", None) or {})
        for key, value in items:
            headers[str(key)] = str(value)
        new_props.headers = headers
        return new_props
    except Exception:  # noqa: BLE001
        return properties


def _resolve_broker_endpoint(conn):
    params = getattr(conn, "params", None) or getattr(conn, "_params", None)
    if params is None:
        # ``BlockingConnection`` is a facade: its parameters live on the
        # wrapped ``_impl`` (SelectConnection). Without this every sampled
        # delivery resolved to nothing — and a falsy result is never memoized.
        impl = getattr(conn, "_impl", None)
        params = (getattr(impl, "params", None)
                  or getattr(impl, "_params", None))
    return getattr(params, "host", None), getattr(params, "port", None)


def _connection_endpoint(channel) -> str | None:
    """``host:port`` of the broker the channel is attached to, when known."""
    return cached_endpoint(getattr(channel, "connection", None),
                           _resolve_broker_endpoint)


# ---------------------------------------------------------------- consumer

def _basic_consume_wrapper(wrapped, instance, args, kwargs):
    """Wrap the user's consumer callback so each delivery becomes a root
    span seeded from the AMQP message headers, with the span active while the
    handler runs.

    Installed on both ``pika.channel.Channel.basic_consume`` and
    ``BlockingChannel.basic_consume`` — both share the same
    ``(queue, on_message_callback, ...)`` shape, so one wrapper serves both.
    The BlockingChannel seam is where the *user* callback actually arrives;
    the ``Channel.basic_consume`` seam sees the user callback only for direct
    non-blocking (``SelectConnection``) consumers — for BlockingChannel
    consumers it instead receives pika's internal buffering sink, which we
    skip (see below).

    The callback's position differs across pika majors — pika ≥1.0 is
    ``basic_consume(queue, on_message_callback, ...)`` while pika 0.x is
    ``basic_consume(consumer_callback, queue, ...)``. Blindly grabbing
    ``args[1]`` corrupts the call on 0.x (it wraps the queue string and hands
    a function where the queue name is expected), so resolve the callback's
    name/index from the actual signature first."""
    name, index = _callback_param(wrapped)
    callback = _extract_callback(args, kwargs, name, index)
    if not callable(callback):
        # No callback found at the resolved position, or it isn't callable
        # (e.g. a 0.x/≥1.0 mismatch we couldn't resolve) — never wrap a
        # non-callable; pass the call through untouched.
        return wrapped(*args, **kwargs)
    if getattr(callback, "_pinpoint_consumer_wrapped", False):
        # Already our wrapper. The BlockingChannel seam passes the internal sink
        # down here rather than a wrapped callback, but a pika variant that
        # delegated one straight down would otherwise get two nested root spans per
        # delivery. Mirrors the aio_pika RobustQueue→Queue guard.
        return wrapped(*args, **kwargs)
    if _is_blocking_internal_dispatcher(callback):
        # BlockingChannel registers its internal sink down here, and that sink only
        # enqueues the delivery into _pending_events for later dispatch. Wrapping it
        # would span the enqueue and end before the handler runs; the user handler is
        # wrapped at the BlockingChannel seam instead, so pass the sink through.
        return wrapped(*args, **kwargs)
    new_args, new_kwargs = replace_arg(
        args, kwargs, index, name, _wrap_consumer_callback(callback),
    )
    return wrapped(*new_args, **new_kwargs)


def _is_blocking_internal_dispatcher(callback) -> bool:
    """True when ``callback`` is a ``BlockingChannel``'s bound
    ``_on_consumer_message_delivery`` — pika's internal buffering sink that
    enqueues deliveries into ``_pending_events`` for later dispatch to the real
    user callback, not a user handler.

    Cheap ``__name__`` pre-filter, then an ``isinstance`` against the real class
    (mirrors aio_pika's ``_is_queue_iterator_callback``). A failed import means
    pika's blocking adapter isn't loaded, so nothing to skip."""
    owner = getattr(callback, "__self__", None)
    if owner is None:
        return False
    if getattr(callback, "__name__", None) != "_on_consumer_message_delivery":
        return False
    try:
        from pika.adapters.blocking_connection import BlockingChannel
    except Exception:  # noqa: BLE001
        return False
    return isinstance(owner, BlockingChannel)


# pika ≥1.0 names the consumer callback ``on_message_callback`` and puts it
# after ``queue``; pika 0.x names it ``consumer_callback`` and puts it first.
_CALLBACK_PARAM_NAMES = ("on_message_callback", "consumer_callback")


def _callback_param(wrapped) -> tuple[str, int]:
    """Resolve ``(name, positional_index)`` of the consumer-callback
    parameter of ``basic_consume`` from its actual signature.

    ``wrapped`` is safe_wrapper's per-call ``_SafeSentinel``, not the real
    ``basic_consume`` — unwrap it (the sentinel exposes ``__wrapped__``) so
    the true signature is introspected. Falls back to the pika ≥1.0
    convention (``on_message_callback`` at positional index 1) when the
    signature can't be introspected — e.g. a C accelerator or a test double
    declared as ``(*args, **kwargs)`` — since that's the modern, common
    shape. Consumers register rarely, so introspecting per registration is
    cheap enough."""
    try:
        params = list(
            inspect.signature(inspect.unwrap(wrapped)).parameters.values())
    except (TypeError, ValueError):
        return ("on_message_callback", 1)
    positional = [
        p for p in params
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    for idx, p in enumerate(positional):
        if p.name in _CALLBACK_PARAM_NAMES:
            return (p.name, idx)
    # Signature resolved but no known callback parameter — assume ≥1.0.
    return ("on_message_callback", 1)


def _extract_callback(args, kwargs, name, index):
    if len(args) > index:
        return args[index]
    return kwargs.get(name)


def _wrap_consumer_callback(callback):
    """Return a callback that opens a Pinpoint span around the user's
    handler. Signature: ``cb(channel, method, properties, body)``."""
    @functools.wraps(callback)
    def _consumer(channel, method, properties, body):
        agent = get_agent()
        if agent is None or not agent.enabled:
            return callback(channel, method, properties, body)

        # This closure replaces the user's callback (it is not under
        # safe_wrapper), so span-setup failures must not escape into pika's
        # dispatch loop — a None scope runs the handler untraced instead.
        scope = open_consumer_scope(
            agent, method, properties, channel, _connection_endpoint,
            "pika.consume", held=True)
        if scope is None:
            return callback(channel, method, properties, body)
        try:
            return callback(channel, method, properties, body)
        except BaseException as exc:
            record_exception_on_span(scope[0], exc)
            raise
        finally:
            close_consumer_scope(scope)

    # functools.wraps above copied the user callback's attributes onto
    # _consumer; stamp our marker *after* the def so the Channel.basic_consume
    # seam can recognise an already-wrapped callback (see _basic_consume_wrapper).
    _consumer._pinpoint_consumer_wrapped = True
    return _consumer


def _blocking_consume_wrapper(wrapped, instance, args, kwargs):
    """Instrument the callback-less ``BlockingChannel.consume()`` generator.

    ``consume()`` yields ``(method, properties, body)`` per delivery (or
    ``(None, None, None)`` on an inactivity timeout) and the caller processes
    each delivery in a ``for ... in channel.consume(q):`` loop body. Wrap the
    generator so each real delivery opens a root span that stays active across
    the ``yield`` — the loop body then runs with the consumer span current, so
    its DB/HTTP/producer calls stitch onto it. The span closes when the loop
    resumes for the next message (i.e. the body finished) or the generator is
    closed; the blocking wait for the next message is left untraced."""
    gen = wrapped(*args, **kwargs)
    agent = get_agent()
    if agent is None or not agent.enabled:
        return gen
    # instance is the BlockingChannel; used only for best-effort endpoint
    # annotation.
    return _traced_consume_generator(gen, agent, instance)


def _traced_consume_generator(gen, agent, channel):
    """Drive pika's ``consume()`` generator, spanning each yielded delivery.

    Runs outside safe_wrapper (it already returned this generator), so every
    span operation is contained: a tracing failure must never break iteration
    or withhold a delivery from the caller. Only ``next(gen)`` and the ``yield``
    itself are allowed to propagate (pika's own ``ChannelClosed`` /
    ``GeneratorExit``)."""
    scope = None
    try:
        while True:
            # Fetch the next delivery with NO span open, so the blocking wait
            # for a message never inflates a span's duration.
            try:
                item = next(gen)
            except StopIteration:
                # PEP 479: a StopIteration must not escape a generator body.
                return
            scope = _open_consume_scope(agent, item, channel)
            try:
                yield item
            finally:
                # The loop body finished (or the caller broke out / the
                # generator is being closed): end this delivery's span before we
                # loop back and block for the next message.
                scope = _end_consume_scope(scope)
    finally:
        _end_consume_scope(scope)


def _open_consume_scope(agent, item, channel):
    """Open a consumer root span for one yielded delivery and make it current.

    Returns a ``(span, span_event, token)`` tuple, or ``None`` when there's
    nothing to trace (an inactivity ``(None, None, None)`` tick, or a shape we
    don't recognise) or setup failed. Never raises: unlike ``basic_get``'s
    ``method, properties, _ = ...`` unpack — which sees pika's own documented
    return — ``item`` reaches us from a user-driven generator, so an unexpected
    shape must degrade to "untraced", not blow up their ``for`` loop."""
    if not isinstance(item, (tuple, list)) or not item or item[0] is None:
        return None
    properties = item[1] if len(item) > 1 else None
    return open_consumer_scope(
        agent, item[0], properties, channel,
        _connection_endpoint, "pika.consume", held=True)


_end_consume_scope = close_consumer_scope


def _blocking_basic_get_wrapper(wrapped, instance, args, kwargs):
    """``BlockingChannel.basic_get`` returns ``(method, properties, body)``
    or ``(None, None, None)`` if the queue is empty. We open a root span
    only when something was actually fetched."""
    result = wrapped(*args, **kwargs)
    method, properties, _ = (result or (None, None, None))
    if method is None:
        return result

    agent = get_agent()
    if agent is None or not agent.enabled:
        return result

    # The message is already fetched, so a tracing failure must not drop this
    # delivery — the scope helpers never raise; always return ``result``.
    # No user handler runs here — the span represents just the delivery.
    close_consumer_scope(open_consumer_scope(
        agent, method, properties, instance, _connection_endpoint,
        "pika.consume.basic_get"))
    return result


def instrument() -> None:
    PikaInstrumentor().instrument()
