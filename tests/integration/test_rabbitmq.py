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

"""pika / aio_pika against a real RabbitMQ container.

Producer events need only the context-current span (``traced``); the
consumer wrappers open *root* spans via ``get_agent()`` (``consumer_agent``).
Both fixtures share one recorder, so the Pinpoint headers a producer injected
can be asserted against the consumer root span's extracted headers —
propagation end-to-end over a real broker.
"""

from __future__ import annotations

import asyncio

import pytest

pika = pytest.importorskip("pika")

from pinpoint.annotation import ANNOTATION_RABBITMQ_ROUTINGKEY
from pinpoint.service_type import SERVICE_TYPE_RABBITMQ_CLIENT

_OP_PUBLISH = "pika.channel.Channel.basic_publish"


@pytest.fixture(scope="module", autouse=True)
def _instrument():
    from pinpoint.instrumentations import pika as pika_instr

    pika_instr.instrument()


@pytest.fixture
def channel(rabbitmq_container):
    params = pika.ConnectionParameters(
        host=rabbitmq_container["host"],
        port=rabbitmq_container["port"],
        credentials=pika.PlainCredentials(
            rabbitmq_container["user"], rabbitmq_container["password"]),
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    yield ch
    conn.close()


def test_pika_publish_records_event(channel, rabbitmq_container, traced):
    q = channel.queue_declare(queue="", exclusive=True).method.queue
    channel.basic_publish(exchange="", routing_key=q, body=b"ping")

    ev = traced.single(_OP_PUBLISH)
    assert ev.ended
    assert ev.service_type == SERVICE_TYPE_RABBITMQ_CLIENT
    # Default exchange has no name — recorded as "Unknown".
    assert ev.destination == "exchange-Unknown"
    assert ev.endpoint == (
        f"{rabbitmq_container['host']}:{rabbitmq_container['port']}")
    assert ev.ann(ANNOTATION_RABBITMQ_ROUTINGKEY) == [q]


def test_pika_basic_get_opens_linked_root_span(channel, traced, consumer_agent):
    q = channel.queue_declare(queue="", exclusive=True).method.queue
    channel.basic_publish(exchange="", routing_key=q, body=b"ping")

    method, _props, body = channel.basic_get(q, auto_ack=True)
    assert method is not None and body == b"ping"

    producer_span = traced.spans[0]
    consumer_spans = [s for s in traced.spans
                     if s.operation == "RabbitMQ Consumer Invocation"]
    assert len(consumer_spans) == 1
    root = consumer_spans[0]
    assert root.ended
    assert root.rpc == "rabbitmq://exchange=Unknown"
    assert root.service_type == SERVICE_TYPE_RABBITMQ_CLIENT
    assert root.ann(ANNOTATION_RABBITMQ_ROUTINGKEY) == [q]
    # The Pinpoint headers injected at publish came back out of the broker.
    assert root.headers is not None
    assert root.headers.get("Pinpoint-TraceID") == producer_span.trace_id

    consume_events = traced.events_named("pika.consume.basic_get")
    assert len(consume_events) == 1 and consume_events[0].ended


def test_aio_pika_publish_and_consume(rabbitmq_container, traced,
                                      consumer_agent, using_span):
    aio_pika = pytest.importorskip("aio_pika")
    from pinpoint import current_span
    from pinpoint.instrumentations import aio_pika as aio_pika_instr

    aio_pika_instr.instrument()

    url = ("amqp://{user}:{password}@{host}:{port}/"
           .format(**rabbitmq_container))
    seen = {}
    publisher = current_span()  # traced's span, grabbed before we detach

    async def main():
        conn = await aio_pika.connect_robust(url)
        async with conn:
            ch = await conn.channel()
            queue = await ch.declare_queue("", exclusive=True, auto_delete=True)

            done = asyncio.Event()

            async def handler(message):
                # The aio_pika consume wrapper keeps the root span current
                # while the user callback runs.
                seen["span"] = current_span()
                seen["body"] = message.body
                done.set()

            await queue.consume(handler, no_ack=True)
            with using_span(publisher):
                await ch.default_exchange.publish(
                    aio_pika.Message(body=b"hello"), routing_key=queue.name)
            await asyncio.wait_for(done.wait(), timeout=15)

    # aio_pika dispatches the delivery from a task whose context it copied when
    # the connection was built, so detaching around the consume alone would not
    # reach it: the whole coroutine runs with the caller's span dropped and only
    # the publish re-attaches. A delivery arriving inside an active trace is
    # deliberately left to that trace (see the ``using_span`` fixture) — not
    # what this test asserts.
    with using_span():
        asyncio.run(main())

    assert seen["body"] == b"hello"
    assert seen["span"] is not None

    publish_ev = traced.single("aio_pika.exchange.Exchange.publish")
    assert publish_ev.ended
    assert publish_ev.service_type == SERVICE_TYPE_RABBITMQ_CLIENT
    assert publish_ev.destination == "exchange-Unknown"

    producer_span = traced.spans[0]
    consumer_spans = [s for s in traced.spans
                     if s.operation == "RabbitMQ Consumer Invocation"]
    assert len(consumer_spans) == 1
    root = consumer_spans[0]
    assert root.ended
    assert root.headers is not None
    assert root.headers.get("Pinpoint-TraceID") == producer_span.trace_id

    consume_events = traced.events_named("aio_pika.consume")
    assert len(consume_events) == 1 and consume_events[0].ended
