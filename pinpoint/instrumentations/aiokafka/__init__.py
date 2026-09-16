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

"""aiokafka (async Kafka) instrumentation — producer + consumer.

The API mirrors ``kafka-python``, so this mirrors
``pinpoint.instrumentations.kafka``. ``AIOKafkaProducer.send`` is the publish
hook; ``send_and_wait`` is a thin wrapper over it and needs none of its own.

Consumers are hooked at the public ``getone`` / ``getmany`` rather than the
shared ``_fetcher.fetched_records`` underneath them, which gives readable span
names and is what lets ``getone`` — the surface ``async for`` awaits per record
— hold its span open across the loop body while batch ``getmany`` cannot.

See ``README.md`` for what each records.
"""

from __future__ import annotations

from ..._log import get_logger
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
from .._util import span_event_scope, wrap

_log = get_logger("aiokafka")
_OPERATION_PRODUCER_SEND = "aiokafka.producer.producer.AIOKafkaProducer.send"


class AiokafkaInstrumentor(BaseInstrumentor):
    def _instrument(self) -> None:
        wrap(
            "aiokafka.producer.producer",
            "AIOKafkaProducer.send",
            _producer_send_wrapper,
        )
        wrap(
            "aiokafka.consumer.consumer",
            "AIOKafkaConsumer.getone",
            _consumer_getone_wrapper,
        )
        wrap(
            "aiokafka.consumer.consumer",
            "AIOKafkaConsumer.getmany",
            _consumer_getmany_wrapper,
        )
        # Closes the last held record scope; without it that span would only
        # end when the process does.
        wrap(
            "aiokafka.consumer.consumer",
            "AIOKafkaConsumer.stop",
            _consumer_stop_wrapper,
        )


# ---------------------------------------------------------------- producer

_SEND_HEADERS_POS = 5  # index of ``headers`` in AIOKafkaProducer.send(
                       # topic, value, key, partition, timestamp_ms, headers).


async def _producer_send_wrapper(wrapped, instance, args, kwargs):
    """``AIOKafkaProducer.send(topic, value=None, key=None, partition=None,
    timestamp_ms=None, headers=None)``.

    ``send_and_wait`` is a thin wrapper that forwards every argument
    *positionally* to ``send``, so this wrapper must handle ``headers``
    in either ``args`` or ``kwargs`` — otherwise we double-pass the
    keyword and crash with ``TypeError: got multiple values for 'headers'``.
    """
    span = current_span()
    if span is None:
        return await wrapped(*args, **kwargs)

    # Unguarded by safe_wrapper (async body): contain the pre-await setup here and
    # fall back to an untraced send with the original args (``_open_producer_event``
    # has already ended any event it opened). The user ``await`` stays outside the
    # try, so it is never swallowed nor awaited twice.
    try:
        event, new_args, new_kwargs, sampled = _open_producer_event(
            span, _OPERATION_PRODUCER_SEND, args, kwargs, _SEND_HEADERS_POS,
            instance, _bootstrap_server,
        )
    except Exception:  # noqa: BLE001
        _log.debug("aiokafka producer instrumentation failed", exc_info=True)
        return await wrapped(*args, **kwargs)

    if not sampled:
        return await wrapped(*new_args, **new_kwargs)

    with span_event_scope(event):
        return await wrapped(*new_args, **new_kwargs)


# ---------------------------------------------------------------- consumer

async def _consumer_getone_wrapper(wrapped, instance, args, kwargs):
    # ``async for record in consumer`` awaits getone per record, so asking for
    # the next one means the caller is done with the previous. Closed before
    # the await so the span never covers the fetch's own wait.
    _close_consumer_scope(instance)
    record = await wrapped(*args, **kwargs)
    agent = get_agent()
    if agent is None or not agent.enabled or record is None:
        return record
    _advance_consume_scope(
        instance, agent, record,
        lambda: client_broker(instance, _bootstrap_server),
    )
    return record


async def _consumer_stop_wrapper(wrapped, instance, args, kwargs):
    _close_consumer_scope(instance)
    return await wrapped(*args, **kwargs)


async def _consumer_getmany_wrapper(wrapped, instance, args, kwargs):
    # Otherwise the batch's delivery spans would nest inside whatever record
    # scope a previous getone left open.
    _close_consumer_scope(instance)
    fetched = await wrapped(*args, **kwargs)
    agent = get_agent()
    if agent is None or not agent.enabled or not fetched:
        return fetched
    # ``getmany`` returns ``Dict[TopicPartition, List[ConsumerRecord]]``. The
    # shared walk is ``@safe_try``: safe_wrapper cannot guard this async body, so
    # nothing here may raise into the caller's ``await``.
    _trace_consumed_batch(
        agent, fetched, lambda: client_broker(instance, _bootstrap_server),
    )
    return fetched


def _bootstrap_server(kafka_obj) -> str:
    """Return the *first* configured broker address.

    Both ``AIOKafkaConsumer`` (``_client``) and ``AIOKafkaProducer``
    (``client``) keep their bootstrap list on the underlying
    ``AIOKafkaClient._bootstrap_servers``. We pick the first entry only:
    Pinpoint UI shows one endpoint, and a comma-joined list of brokers
    makes for noisy labels."""
    for client_attr in ("_client", "client"):
        client = getattr(kafka_obj, client_attr, None)
        if client is None:
            continue
        for attr in ("_bootstrap_servers", "bootstrap_servers"):
            v = getattr(client, attr, None)
            if v:
                return _first_broker(v)
    return ""


def instrument() -> None:
    AiokafkaInstrumentor().instrument()
