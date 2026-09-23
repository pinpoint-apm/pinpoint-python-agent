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

"""Shared pika / aio-pika (RabbitMQ) helpers, mirroring ``_kafka.py``.

The two clients differ only in how a broker ``host:port`` is resolved from
their objects, so the annotators take the calling module's
``_connection_endpoint`` resolver plus the object to resolve it from.
"""

from __future__ import annotations

from ..annotation import (
    ANNOTATION_RABBITMQ_EXCHANGE,
    ANNOTATION_RABBITMQ_ROUTINGKEY,
)
from ..context import current_span, set_current_span
from ..errors import safe_try
from ..http_helper import has_pinpoint_mapping
from ..service_type import SERVICE_TYPE_RABBITMQ_CLIENT
from ._util import close_span_scope, end_quietly

# The name the AMQP instrumentations (aio_pika, pika) import it under.
close_consumer_scope = close_span_scope

OPERATION_CONSUMER_INVOCATION = "RabbitMQ Consumer Invocation"


def headers_to_str_map(carrier) -> dict[str, str]:
    """``carrier.headers`` as a plain ``str -> str`` dict (bytes decoded,
    ``None`` values dropped); ``{}`` on any surprise."""
    raw = getattr(carrier, "headers", None) or {}
    out: dict[str, str] = {}
    try:
        for k, v in raw.items():
            if v is None:
                continue
            text = (v.decode("utf-8", errors="replace")
                    if isinstance(v, (bytes, bytearray)) else v)
            out[str(k)] = str(text)
    except Exception:  # noqa: BLE001
        return {}
    return out


def routing_target(source) -> str:
    exchange = getattr(source, "exchange", "") or "Unknown"
    return "rabbitmq://exchange=" + exchange


@safe_try
def annotate_publish(event, exchange, routing_key, endpoint_source,
                     connection_endpoint) -> None:
    # `destination` shows up in the Pinpoint UI as the queue/exchange name,
    # whichever side of the AMQP routing graph applies.
    dest = exchange or "Unknown"
    event.set_destination("exchange-" + str(dest))
    event.set_end_point(connection_endpoint(endpoint_source) or "Unknown")
    if exchange:
        event.annotate_string(ANNOTATION_RABBITMQ_EXCHANGE, exchange)
    if routing_key:
        event.annotate_string(ANNOTATION_RABBITMQ_ROUTINGKEY, routing_key)


@safe_try
def annotate_consume(span, source, endpoint_source,
                     connection_endpoint) -> None:
    """Stamp consumer root-span metadata from ``source`` — the delivery's
    method frame (pika) or message (aio-pika), both carrying ``.exchange``
    and ``.routing_key``.

    A no-op on an unsampled span, but the getattr chains and endpoint string
    building are not free, and every delivery path runs this per message —
    so gate on ``span.sampled``."""
    if not span.sampled:
        return
    span.set_service_type(SERVICE_TYPE_RABBITMQ_CLIENT)
    exchange = getattr(source, "exchange", "") or "Unknown"
    routing_key = getattr(source, "routing_key", "") or "Unknown"
    remote = connection_endpoint(endpoint_source) or "Unknown"
    span.set_remote_address(remote)
    span.set_end_point(remote)
    span.set_acceptor_host("exchange-" + exchange)
    if routing_key:
        span.annotate_string(ANNOTATION_RABBITMQ_ROUTINGKEY, routing_key)


def open_consumer_scope(agent, routing_source, headers_source, endpoint_source,
                        connection_endpoint, event_name, *, held=False):
    """Open a consumer root span + one child event and make the span current.

    ``routing_source`` carries ``.exchange``/``.routing_key`` (pika's method
    frame, aio-pika's message); ``headers_source`` carries ``.headers``
    (pika's BasicProperties, aio-pika's same message). Returns a
    ``(span, span_event, token)`` scope for :func:`close_consumer_scope`, or
    ``None`` when setup failed (any partial span is closed first). Never
    raises — the callers replace user callbacks / drive fetched deliveries,
    where an escaping tracing failure would lose a message.

    ``held=True`` marks the callers that keep the span current across the
    caller's *own* code — a replaced consume callback, a delivery yielded from
    ``BlockingChannel.consume`` — and makes them step aside when a span is
    already current: the delivery arrived inside someone else's trace (a
    dispatch loop pumped from a traced request, a ``for`` over ``consume()`` in
    a handler), where making ours current would re-parent the rest of that
    caller's work under this delivery. The delivery-only callers hand the span
    to nobody and end it before returning, so they keep tracing regardless.
    """
    if held and current_span() is not None:
        return None
    span = None
    try:
        # Convert AMQP headers only when an upstream Pinpoint header is
        # present; a brand-new trace's local sampling decision needs no reader.
        headers = None
        if has_pinpoint_mapping(getattr(headers_source, "headers", None) or {}):
            headers = headers_to_str_map(headers_source)
        span = agent.new_span(
            OPERATION_CONSUMER_INVOCATION,
            routing_target(routing_source),
            headers=headers,
        )
        annotate_consume(span, routing_source, endpoint_source,
                         connection_endpoint)
        span_event = span.new_span_event(event_name)
        token = set_current_span(span)
        return (span, span_event, token)
    except Exception:  # noqa: BLE001
        end_quietly(span)
        return None


