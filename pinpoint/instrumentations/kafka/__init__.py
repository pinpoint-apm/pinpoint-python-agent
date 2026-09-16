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

"""kafka-python instrumentation (producer + consumer).

Producer: wraps `KafkaProducer.send` — injects Pinpoint trace headers into
the Kafka record headers (Kafka 0.11+) so downstream consumers can stitch.

Consumer: wraps both delivery surfaces of `KafkaConsumer`, since a record can
reach the caller through either one, with headers extracted from the record:

- `__next__` (iterator interface) — one span per returned message, held open
  and current until the caller comes back for the next record (or closes the
  consumer), so a `for record in consumer:` body is traced inside its span.
- `poll(timeout_ms=...)` (batch interface) — one delivery-only span per record
  in the returned `{TopicPartition: [records]}` map; the common
  `while True: consumer.poll(500)` loop never flows through `__next__`. A batch
  hands over every record at once, so there is no per-record boundary to close
  a held span on — wrap the processing yourself if you want it traced.

`__next__` drives `poll(update_offsets=False)` internally, so the poll wrapper
skips that internal call (`__next__` already covers those records) to avoid
double spans.
"""

from __future__ import annotations

from ...agent import get_agent
from ...context import current_span
from ...instrumentor import BaseInstrumentor
from .._kafka import (
    advance_consume_scope as _advance_consume_scope,
    client_broker,
    close_consumer_scope as _close_consumer_scope,
    first_broker as _first_broker,
    open_producer_event as _open_producer_event,
    trace_consumed_batch as _trace_consumed_batch,
)
from .._util import no_current_span, span_event_scope, wrap

_OPERATION_PRODUCER_SEND = "kafka.producer.kafka.KafkaProducer.send"


class KafkaInstrumentor(BaseInstrumentor):
    def _instrument(self) -> None:
        # Producer calls own no state, so they can bypass the wrapper when no
        # parent span exists. Consumer calls may own a scope from the previous
        # record and must always enter their wrapper to release it, including
        # after the agent is disabled mid-run.
        wrap("kafka.producer.kafka", "KafkaProducer.send",
             _producer_send_wrapper, precheck=no_current_span)
        wrap("kafka.consumer.group", "KafkaConsumer.__next__",
             _consumer_next_wrapper)
        wrap("kafka.consumer.group", "KafkaConsumer.poll",
             _consumer_poll_wrapper)
        # Closes the last held record scope; without it that span would only
        # end when the process does.
        wrap("kafka.consumer.group", "KafkaConsumer.close",
             _consumer_close_wrapper)


# ---------------------------------------------------------------- producer

_SEND_HEADERS_POS = 3  # index of ``headers`` in KafkaProducer.send(
                       # topic, value, key, headers, partition, timestamp_ms).


def _supports_headers(producer) -> bool:
    """Record headers need message format v2, i.e. ``api_version >= (0, 11)``
    (``KafkaProducer.max_usable_produce_magic``). Below that the legacy record
    builder hard-asserts on *any* headers, so injecting would turn every
    traced send into an ``AssertionError`` — an agent-caused failure of a call
    that works untraced. ``api_version`` is resolved eagerly in the producer's
    ``__init__``, so it is a tuple by the time ``send()`` runs; anything we
    cannot read is not the real producer (mock, subclass) and keeps injecting.
    """
    try:
        return producer.config["api_version"] >= (0, 11)
    except Exception:  # noqa: BLE001
        return True


def _producer_send_wrapper(wrapped, instance, args, kwargs):
    span = current_span()
    if span is None:
        return wrapped(*args, **kwargs)

    event, new_args, new_kwargs, sampled = _open_producer_event(
        span, _OPERATION_PRODUCER_SEND, args, kwargs, _SEND_HEADERS_POS,
        instance, _bootstrap_server, inject=_supports_headers(instance),
    )
    if not sampled:
        return wrapped(*new_args, **new_kwargs)

    with span_event_scope(event):
        return wrapped(*new_args, **new_kwargs)


# ---------------------------------------------------------------- consumer
def _consumer_next_wrapper(wrapped, instance, args, kwargs):
    # Asking for the next record means the caller is done with the previous
    # one. Closed before the fetch so the span never covers its blocking wait,
    # and before the agent check — the previous record is done either way, and
    # an agent disabled mid-run would otherwise leave that span open until the
    # consumer is closed, which a loop that simply stops polling never does.
    # (Same order as the confluent_kafka and aiokafka consume wrappers.)
    _close_consumer_scope(instance)
    agent = get_agent()
    if agent is None or not agent.enabled:
        return wrapped(*args, **kwargs)

    record = wrapped(*args, **kwargs)
    _advance_consume_scope(
        instance, agent, record,
        lambda: client_broker(instance, _bootstrap_server),
    )
    return record


def _consumer_close_wrapper(wrapped, instance, args, kwargs):
    _close_consumer_scope(instance)
    return wrapped(*args, **kwargs)


def _consumer_poll_wrapper(wrapped, instance, args, kwargs):
    """``KafkaConsumer.poll(timeout_ms=0, max_records=None, update_offsets=True)``
    returns ``{TopicPartition: [ConsumerRecord, ...]}`` — the batch surface that
    a ``while True: consumer.poll(500)`` loop uses and that never flows through
    ``__next__``. Open one delivery span per returned record.

    Skip the iterator-internal poll: ``__next__`` drives ``poll`` with
    ``update_offsets=False`` and instruments each record it yields itself, so
    tracing here too would double-count (and even span records the iterator has
    not yet handed to the caller). ``update_offsets`` is an undocumented
    internal-only flag that is only ever False on that path."""
    # poll(timeout_ms, max_records, update_offsets) — wrapt strips ``self``.
    if not (args[2] if len(args) > 2 else kwargs.get("update_offsets", True)):
        return wrapped(*args, **kwargs)

    # Otherwise the batch's delivery spans would nest inside whatever record
    # scope the iterator left open. Closed before the agent check, so an agent
    # disabled mid-run releases the held span here too rather than leaving it
    # to the consumer's close.
    _close_consumer_scope(instance)
    agent = get_agent()
    if agent is None or not agent.enabled:
        return wrapped(*args, **kwargs)

    records = wrapped(*args, **kwargs)
    if records:
        _trace_consumed_batch(
            agent, records,
            lambda: client_broker(instance, _bootstrap_server),
        )
    return records


def _bootstrap_server(consumer) -> str:
    """Return the *first* configured broker address.

    ``bootstrap_servers`` accepts either a single ``host[:port]`` string,
    a comma-separated string of hosts, or a list/tuple of hosts. We pick
    the first one for span destination/remote-address since Pinpoint UI
    renders a single endpoint, and the broker pool would otherwise yield
    a long comma-joined label."""
    cfg = getattr(consumer, "config", None)
    if isinstance(cfg, dict):
        servers = cfg.get("bootstrap_servers")
        if servers:
            return _first_broker(servers)
    return ""


def instrument() -> None:
    KafkaInstrumentor().instrument()
