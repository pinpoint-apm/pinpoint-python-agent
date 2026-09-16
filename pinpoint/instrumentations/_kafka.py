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

"""Shared Kafka instrumentation helpers."""

from __future__ import annotations

import weakref
from itertools import islice

from ..annotation import (
    ANNOTATION_KAFKA_BATCH,
    ANNOTATION_KAFKA_HEADER,
    ANNOTATION_KAFKA_OFFSET,
    ANNOTATION_KAFKA_PARTITION,
    ANNOTATION_KAFKA_TOPIC,
)
from ..errors import safe_try
from ..propagator import PINPOINT_HEADERS_BY_LOWER, inject_items
from ..context import current_span, set_current_span
from ..service_type import SERVICE_TYPE_KAFKA_CLIENT
from ._util import close_span_scope, end_quietly, span_is_sampled

_OPERATION_CONSUMER_INVOCATION = "Kafka Consumer Invocation"
# The one event a delivery span carries. Named like the AMQP consumer's
# "pika.consume" / "aio_pika.consume" rather than per-client, since the client
# is already identifiable from the application itself.
_OPERATION_CONSUME = "kafka.consume"


# Kafka headers are application-controlled binary data. Keep both propagation
# extraction and sampled annotation work bounded before decoding/copying them.
_KAFKA_HEADER_MAX_SCAN = 128
_KAFKA_HEADER_MAX_ANNOTATIONS = 32
_KAFKA_HEADER_MAX_KEY_BYTES = 256
_KAFKA_HEADER_MAX_VALUE_BYTES = 4096
_KAFKA_HEADER_MAX_TOTAL_CHARS = 16 * 1024
_KAFKA_TRACE_VALUE_MAX_BYTES = 4096
_KAFKA_HEADER_TRUNCATION_SUFFIX = "..."
_KAFKA_HEADER_TRUNCATION_MARKER = "pinpoint: kafka headers truncated"


def kafka_header_context(raw_headers):
    """Return a reusable bounded pair iterable and its Pinpoint context.

    Kafka clients normally expose a list (or occasionally a dict), which is
    returned without copying. A one-shot/custom iterable is snapshotted only up
    to the scan cap plus one sentinel pair, preventing an unbounded allocation
    while still letting annotation report that more headers were omitted.
    """
    pairs = _iter_header_pairs(raw_headers)
    return pairs, _extract_pinpoint_headers(pairs)


@safe_try
def annotate_kafka_headers(target, raw_headers) -> None:
    """Record bounded non-Pinpoint headers as ``kafka.header`` annotations."""
    annotation_count = 0
    total_chars = 0
    truncated = False

    for raw_index, (key, value) in enumerate(_iter_header_pairs(raw_headers)):
        if raw_index >= _KAFKA_HEADER_MAX_SCAN:
            truncated = True
            break

        key_text, key_truncated = _decode_bounded_header_part(
            key, _KAFKA_HEADER_MAX_KEY_BYTES,
        )
        if not key_text or _is_pinpoint_header(key_text):
            continue

        if annotation_count >= _KAFKA_HEADER_MAX_ANNOTATIONS:
            truncated = True
            break

        value_text, value_truncated = _decode_bounded_header_part(
            value, _KAFKA_HEADER_MAX_VALUE_BYTES,
        )
        rendered = f"{key_text}={value_text}"
        if total_chars + len(rendered) > _KAFKA_HEADER_MAX_TOTAL_CHARS:
            truncated = True
            break

        target.annotate_string(
            ANNOTATION_KAFKA_HEADER,
            rendered,
        )
        annotation_count += 1
        total_chars += len(rendered)
        truncated = truncated or key_truncated or value_truncated

    if truncated:
        target.annotate_string(
            ANNOTATION_KAFKA_HEADER,
            _KAFKA_HEADER_TRUNCATION_MARKER,
        )


def first_broker(value) -> str:
    """First broker from a host list/tuple or a comma-separated string."""
    if isinstance(value, (list, tuple)):
        return str(value[0]) if value else ""
    return str(value).split(",", 1)[0].strip()


def client_broker(client, resolver) -> str:
    """Resolved broker string for a producer/consumer, memoized on the client.

    The bootstrap list is fixed for a client's lifetime, while ``resolver``
    walks config dicts / attribute fallbacks plus a split-and-strip — per
    sampled message on both the produce and consume paths without the stash.
    A failed resolve is returned as ``"Unknown"`` but not cached, so a client
    populated late still gets picked up."""
    broker = getattr(client, "_pinpoint_broker", None)
    if broker is not None:
        return broker
    broker = resolver(client)
    if not broker:
        return "Unknown"
    try:
        client._pinpoint_broker = broker
    except Exception:  # noqa: BLE001
        pass
    return broker


def inject_trace_headers(args: tuple, kwargs: dict, span, headers_pos: int) -> tuple:
    """Merge Pinpoint-* trace headers into a send call's ``headers`` slot.

    ``headers`` may arrive positionally (``args[headers_pos]``: index 3 in
    kafka-python's ``KafkaProducer.send(topic, value, key, headers, ...)``,
    5 in aiokafka's ``send``, 7/6 in confluent-kafka's ``produce``) or as a
    keyword. Writing ``kwargs['headers']`` unconditionally would collide with
    a positional ``headers`` and crash the call with ``TypeError: got
    multiple values for 'headers'``.

    The slot holds a list of ``(str, bytes)`` pairs — or, for
    confluent-kafka, possibly a ``dict[str, bytes]``; the caller's format is
    preserved. Header values must be ``bytes``/``str``, so injected trace
    values are UTF-8 encoded.

    Returns ``(new_args, new_kwargs)``; the container that gains the merged
    headers is a copy, so the original call is left untouched — a fallback
    retry in ``safe_wrapper`` must not see the polluted headers. When nothing
    merges (no span, injection failure, nothing to inject), the originals are
    returned as-is: no copy, and nothing was polluted.
    """
    if span is None:
        return args, kwargs

    in_args = len(args) > headers_pos
    existing = args[headers_pos] if in_args else kwargs.get("headers")

    merged = None
    try:
        # A re-published record (retry, dead-letter forward, consumer→producer
        # relay) still carries the *previous* hop's Pinpoint-* headers. Ours
        # must replace them, or the downstream span links to the wrong parent.
        # User headers are left alone.
        items = inject_items(span)
        if isinstance(existing, dict):
            for key, value in items:
                if merged is None:
                    merged = {k: v for k, v in existing.items()
                              if not _is_pinpoint_header(k)}
                merged[str(key)] = str(value).encode("utf-8")
        else:
            for key, value in items:
                if merged is None:
                    merged = [pair for pair in (existing or [])
                              if not _is_pinpoint_header(pair[0])]
                merged.append((str(key), str(value).encode("utf-8")))
    except Exception:  # noqa: BLE001
        return args, kwargs
    if merged is None:
        return args, kwargs

    if in_args:
        new_args = list(args)
        new_args[headers_pos] = merged
        return tuple(new_args), kwargs
    new_kwargs = dict(kwargs)
    new_kwargs["headers"] = merged
    return args, new_kwargs


@safe_try
def annotate_producer(event, topic, producer, bootstrap_server) -> None:
    """Set broker destination/end point and topic on a producer span event.

    ``bootstrap_server`` is the calling module's resolver — each client
    stores its broker list somewhere different."""
    broker = client_broker(producer, bootstrap_server)
    event.set_destination(broker)
    event.set_end_point(broker)
    if topic:
        event.annotate_string(ANNOTATION_KAFKA_TOPIC, str(topic))


def open_producer_event(span, operation, args, kwargs, headers_pos, instance,
                        bootstrap_server, inject=True):
    """Shared producer preamble: event → inject → sample gate → annotate.

    Returns ``(event, new_args, new_kwargs, sampled)``; the caller issues the
    wrapped send itself — an ``await`` stays outside this helper — passing
    ``new_args``/``new_kwargs`` on *every* path, sampled or not, so the
    injected headers still reach the wire when tracing is off.

    ``inject=False`` traces the send but leaves the headers alone, for a
    client that cannot carry them (see the kafka module's message-format
    gate); the message then crosses to the consumer untraced.

    The event is opened before injection so the context written into the
    message headers carries this call's own depth/sequence.

    Failure handling splits by phase, matching what the caller can still do:

    - event/inject/sample failure — the opened event is ended (an unended
      native event leaks) and the exception propagates, so the caller's
      ``safe_wrapper`` (or, for an ``async`` wrapper it cannot guard, the
      caller's own ``except``) falls back to one untraced send.
    - annotation failure — the event is ended and ``sampled=False`` is
      returned, so the caller sends the already-injected headers untraced.
      Both annotate helpers are ``@safe_try``, so this only fires on a
      native-side failure.
    """
    topic = args[0] if args else kwargs.get("topic", "")
    event = span.new_span_event(operation, service_type=SERVICE_TYPE_KAFKA_CLIENT)
    try:
        new_args, new_kwargs = (
            inject_trace_headers(args, kwargs, span, headers_pos) if inject
            else (args, kwargs))
        sampled = span_is_sampled(span)
    except Exception:
        end_quietly(event)
        raise
    if sampled:
        try:
            annotate_producer(event, topic, instance, bootstrap_server)
            # Annotate the PRE-injection headers (positional or keyword):
            # injection only adds Pinpoint-* pairs, which the annotator
            # filters out anyway — reading them back from new_args would
            # decode ~8 just-injected values per send only to discard them.
            annotate_kafka_headers(
                event,
                (args[headers_pos] if len(args) > headers_pos
                 else kwargs.get("headers")) or [],
            )
        except Exception:  # noqa: BLE001
            end_quietly(event)
            sampled = False
    return event, new_args, new_kwargs, sampled


def _new_consume_span(agent, topic, partition, offset, raw_headers, broker,
                      batch_size=None):
    """Open and annotate a consumer root span.

    ``broker`` is either the resolved broker string (batch callers resolve it
    once per batch) or a zero-arg callable, invoked only when the span is
    sampled so single-record callers keep the broker lookup off the unsampled
    hot path.
    """
    # Decode/collect record headers only when an upstream Pinpoint header is
    # present; a brand-new trace's local sampling decision needs no reader.
    header_pairs, headers = kafka_header_context(raw_headers)
    span = agent.new_span(
        _OPERATION_CONSUMER_INVOCATION,
        f"kafka://topic={topic}?partition={partition}&offset={offset}",
        headers=headers,
    )
    # A no-op on an unsampled span, but preparing the arguments is not — the
    # broker config walk, the per-header decode, the topic/partition/offset
    # conversions — and batches multiply it. Gate on span.sampled.
    if span.sampled:
        try:
            span.set_service_type(SERVICE_TYPE_KAFKA_CLIENT)
            if callable(broker):
                broker = broker()
            span.set_remote_address(broker)
            span.set_acceptor_host(broker)
            span.set_end_point(broker)
            if topic:
                span.annotate_string(ANNOTATION_KAFKA_TOPIC, str(topic))
            if partition is not None:
                span.annotate_int(ANNOTATION_KAFKA_PARTITION, int(partition))
            if offset is not None:
                # int64 offset — annotate_int would overflow past 2^31.
                span.annotate_long(ANNOTATION_KAFKA_OFFSET, int(offset))
            if batch_size is not None:
                span.annotate_int(ANNOTATION_KAFKA_BATCH, int(batch_size))
            annotate_kafka_headers(span, header_pairs)
        except Exception:
            # The root span already exists; the @safe_try callers would
            # swallow this with it never ended. Close it, then let them log.
            end_quietly(span)
            raise
    return span


def open_consume_span(agent, topic, partition, offset, raw_headers, broker,
                      batch_size=None) -> None:
    """Open and immediately end a delivery-only consumer root span.

    Used by the *batch* fetch APIs (``KafkaConsumer.poll``, ``getmany``,
    ``Consumer.consume``): they hand the caller a whole collection at once, so
    there is no per-record boundary to close a span on — the user's own loop
    over the batch is invisible from here. The single-record APIs get a span
    that lasts their handling instead; see :func:`advance_consume_scope`.

    Callers are ``@safe_try`` guarded per record, so one bad record cannot
    abort the rest of a batch.
    """
    span = _new_consume_span(agent, topic, partition, offset, raw_headers,
                             broker, batch_size)
    try:
        if span.sampled:
            # Delivery spans carry no child work of their own, so without this
            # the transaction reports an empty call tree. Ended right away —
            # the span covers the delivery, not the user's processing — and
            # before span.end() below, which the event stack requires.
            span.new_span_event(_OPERATION_CONSUME).end()
    finally:
        span.end()


# ---- single-record scopes ---------------------------------------------------
#
# The span lasts the user's handling of the record.
# A pull consumer hands out one record per call and gives us no callback around
# what the caller then does with it — but coming back for the *next* record is
# itself the boundary: whatever they did with the previous one is finished. So
# each fetch closes the scope its predecessor left open and opens a new one,
# and the consumer's close/stop closes the last. The scope rides on the
# consumer instance, so two consumers in one thread don't close each other's.

_SCOPE_ATTR = "_pinpoint_consume_scope"


def open_consume_scope(agent, topic, partition, offset, raw_headers, broker,
                       batch_size=None):
    """Open a consumer root span, make it current, and keep it open.

    Returns a ``(span, span_event, token)`` scope for
    :func:`~._util.close_span_scope`, or ``None`` when a span is already current
    or setup failed (any partial span is closed first). Never raises — the
    callers sit on the delivery path, where an escaping tracing failure would
    lose a message.
    """
    # A dedicated consume loop always reaches here with nothing current: the
    # previous record's scope is closed before every fetch. So a span already
    # current means the fetch came from inside someone else's trace — a poll()
    # in a request handler — where making ours current would re-parent the rest
    # of that request under this record, and the scope would only be closed by
    # this consumer's *next* fetch: possibly never, or from another thread
    # whose context we then cannot reset (leaving a span current that
    # suppresses every later root span there). Leave the record untraced.
    if current_span() is not None:
        return None
    span = None
    try:
        span = _new_consume_span(agent, topic, partition, offset, raw_headers,
                                 broker, batch_size)
        # Held open like the AMQP consumer's event: the user's work nests
        # inside it rather than beside it.
        span_event = span.new_span_event(_OPERATION_CONSUME)
        token = set_current_span(span)
        return (span, span_event, token)
    except Exception:  # noqa: BLE001
        end_quietly(span)
        return None


@safe_try
def close_consumer_scope(consumer) -> None:
    """Close whatever scope ``consumer`` has open, if any.

    Called before every fetch (the previous record is done), before a batch
    fetch (its delivery spans must not nest inside a held record span), and
    from the consumer's close/stop so the last record's span doesn't dangle.
    """
    held = getattr(consumer, _SCOPE_ATTR, None)
    if held is None:
        return
    try:
        setattr(consumer, _SCOPE_ATTR, None)
    except Exception:  # noqa: BLE001
        pass
    scope, finalizer = held
    if finalizer is not None:
        # Closed by the consumer itself: the GC safety net must not keep the
        # scope (and its span) alive until the consumer is collected.
        finalizer.detach()
    close_span_scope(scope)


@safe_try
def advance_consume_scope(consumer, agent, record, broker,
                          extract=None) -> None:
    """Open a held scope for ``record`` on ``consumer``.

    The caller closes the previous scope *before* fetching, so the span never
    covers the fetch's own blocking wait. When the scope cannot be stored on
    the consumer (a C type with no attribute slot) it is closed immediately,
    degrading to the delivery-only span rather than leaking an open one.
    """
    if record is None:
        return
    topic, partition, offset, raw_headers = (
        extract(record) if extract is not None else _record_fields(record))
    scope = open_consume_scope(agent, topic, partition, offset, raw_headers,
                               broker)
    if scope is None:
        return
    # Safety net for a consumer dropped without another fetch or close(): a
    # worker that polls once and moves on would otherwise leave this span
    # current on its thread forever (contextvar binding → strong ref), which
    # suppresses every later root span there. The finalizer ends the span
    # when the consumer is collected; reset_quietly absorbs the cross-context
    # token. Detached on a normal close (see close_consumer_scope).
    try:
        finalizer = weakref.finalize(consumer, close_span_scope, scope)
        finalizer.atexit = False
    except Exception:  # noqa: BLE001
        finalizer = None
    try:
        setattr(consumer, _SCOPE_ATTR, (scope, finalizer))
    except Exception:  # noqa: BLE001
        if finalizer is not None:
            finalizer.detach()
        close_span_scope(scope)


def _record_fields(record):
    """``(topic, partition, offset, headers)`` off a kafka-python / aiokafka
    ``ConsumerRecord``."""
    return (
        getattr(record, "topic", ""),
        getattr(record, "partition", None),
        getattr(record, "offset", None),
        getattr(record, "headers", None) or (),
    )


@safe_try
def trace_consumed_record(agent, record, broker, batch_size=None) -> None:
    """Open a delivery-only span for one consumed record.

    ``@safe_try`` per record, so one bad record cannot abort the rest of a
    batch."""
    open_consume_span(
        agent,
        getattr(record, "topic", ""),
        getattr(record, "partition", None),
        getattr(record, "offset", None),
        getattr(record, "headers", None) or (),
        broker,
        batch_size,
    )


@safe_try
def trace_consumed_batch(agent, fetched, broker,
                         trace_record=trace_consumed_record) -> None:
    """Open one delivery span per record in a fetched batch.

    ``fetched`` is either a ``{TopicPartition: [records]}`` map (kafka-python
    ``poll``, aiokafka ``getmany``) or a flat record list (confluent
    ``consume``). Each record can carry a different producer trace context, so
    a batch cannot be collapsed into one span without breaking trace
    stitching.

    ``broker`` is the resolved broker string or a zero-arg callable, resolved
    once per batch rather than per record. ``trace_record`` is ``@safe_try``
    per record, so one bad record cannot abort the rest of the batch, and this
    walk is ``@safe_try`` so it can never raise into the consumer's fetch.

    Cost: this walk is synchronous and O(records) — two native crossings per
    record (span create + end) — and on asyncio consumers (aiokafka
    ``getmany``) it runs inline in the event loop after the fetch, so a large
    batch stalls other coroutines for roughly records x the unsampled-span
    cost (see benchmark/api_overhead). A native batch API (one crossing
    creating and ending N delivery spans) is the upgrade path if that shows
    up in traces.
    """
    if callable(broker):
        broker = broker()
    record_lists = (
        (fetched,) if isinstance(fetched, (list, tuple)) else fetched.values()
    )
    for record_list in record_lists:
        batch_size = len(record_list or ())
        for record in record_list or ():
            trace_record(
                agent, record, broker,
                batch_size=batch_size if batch_size > 1 else None,
            )


def _iter_header_pairs(raw_headers):
    if raw_headers is None:
        return ()
    if isinstance(raw_headers, dict):
        return raw_headers.items()
    if isinstance(raw_headers, (list, tuple)):
        return raw_headers
    try:
        items = getattr(raw_headers, "items", None)
        if callable(items):
            return tuple(islice(iter(items()), _KAFKA_HEADER_MAX_SCAN + 1))
        return tuple(islice(iter(raw_headers), _KAFKA_HEADER_MAX_SCAN + 1))
    except Exception:  # noqa: BLE001
        return ()


def _extract_pinpoint_headers(header_pairs):
    headers = {}
    try:
        for raw_index, (key, value) in enumerate(header_pairs):
            if raw_index >= _KAFKA_HEADER_MAX_SCAN:
                break
            canonical_name = _canonical_pinpoint_header_name(key)
            if canonical_name is None:
                continue
            value_text = _decode_trace_header_value(value)
            if value_text is not None:
                headers[canonical_name] = value_text
    except Exception:  # noqa: BLE001
        return None
    return headers or None


def _canonical_pinpoint_header_name(key):
    if isinstance(key, (bytes, bytearray)):
        key = bytes(key).decode("ascii", errors="ignore")
    if not isinstance(key, str) or len(key) > _KAFKA_HEADER_MAX_KEY_BYTES:
        return None
    return PINPOINT_HEADERS_BY_LOWER.get(key.lower())


def _decode_trace_header_value(value):
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8", errors="replace")
    if not isinstance(value, str) or len(value) > _KAFKA_TRACE_VALUE_MAX_BYTES:
        return None
    return value


def _decode_bounded_header_part(value, limit: int):
    if value is None:
        return "", False
    if isinstance(value, (bytes, bytearray)):
        # Truncate on byte length *before* decoding, so a multibyte char cut
        # at the boundary yields a replacement char.
        truncated = len(value) > limit
        text = bytes(value[:limit]).decode("utf-8", errors="replace")
    else:
        text = value if isinstance(value, str) else str(value)
        truncated = len(text) > limit
        text = text[:limit]
    if truncated:
        text += _KAFKA_HEADER_TRUNCATION_SUFFIX
    return text, truncated


def _is_pinpoint_header(key) -> bool:
    if isinstance(key, bytes):
        return key.lower().startswith(b"pinpoint-")
    return isinstance(key, str) and key.lower().startswith("pinpoint-")
