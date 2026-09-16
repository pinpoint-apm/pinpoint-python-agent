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

"""kafka-python / confluent-kafka / aiokafka against one real Kafka broker.

Each client publishes on its own topic inside a traced span, then consumes it
with the recording agent installed — asserting the producer span event, the
consumer root span, and that the Pinpoint headers survived the broker
round-trip (consumer root span links to the producer trace).

confluent-kafka is instrumented at module import time (before any test builds
a Producer/Consumer): its instrumentation rebinds the module-level classes,
so instances constructed earlier would stay untraced.
"""

from __future__ import annotations

import asyncio
import time

import pytest

kafka = pytest.importorskip("kafka")

from pinpoint.annotation import (
    ANNOTATION_KAFKA_OFFSET,
    ANNOTATION_KAFKA_PARTITION,
    ANNOTATION_KAFKA_TOPIC,
)
from pinpoint.service_type import SERVICE_TYPE_KAFKA_CLIENT

_CONSUMER_OP = "Kafka Consumer Invocation"


@pytest.fixture(scope="module", autouse=True)
def _instrument():
    from pinpoint.instrumentations import kafka as kafka_instr

    kafka_instr.instrument()
    try:
        from pinpoint.instrumentations import confluent_kafka as ck_instr

        ck_instr.instrument()
    except ImportError:
        pass
    try:
        from pinpoint.instrumentations import aiokafka as aiokafka_instr

        aiokafka_instr.instrument()
    except ImportError:
        pass


def _assert_consumer_root(recorder, topic, producer_trace_id):
    roots = [s for s in recorder.spans if s.operation == _CONSUMER_OP]
    assert len(roots) == 1, f"expected one consumer root span, got {roots!r}"
    root = roots[0]
    assert root.ended
    assert root.rpc.startswith(f"kafka://topic={topic}?partition=")
    assert root.service_type == SERVICE_TYPE_KAFKA_CLIENT
    assert root.ann(ANNOTATION_KAFKA_TOPIC) == [topic]
    partitions = [a for a in root.annotations
                  if a[1] == ANNOTATION_KAFKA_PARTITION]
    offsets = [a for a in root.annotations if a[1] == ANNOTATION_KAFKA_OFFSET]
    assert partitions and partitions[0][0] == "int"
    # Offsets are 64-bit — recorded through annotate_long.
    assert offsets and offsets[0][0] == "long"
    assert root.headers is not None
    assert root.headers.get("Pinpoint-TraceID") == producer_trace_id
    return root


def test_kafka_python_producer_and_consumer(kafka_container, traced,
                                            consumer_agent, using_span):
    topic = "it-kafka-python"
    bootstrap = kafka_container["bootstrap"]

    producer = kafka.KafkaProducer(bootstrap_servers=bootstrap)
    try:
        producer.send(topic, b"payload", headers=[("h1", b"v1")]).get(timeout=30)
    finally:
        producer.close()

    ev = traced.single("kafka.producer.kafka.KafkaProducer.send")
    assert ev.ended
    assert ev.service_type == SERVICE_TYPE_KAFKA_CLIENT
    assert ev.destination == bootstrap
    assert ev.endpoint == bootstrap
    assert ev.ann(ANNOTATION_KAFKA_TOPIC) == [topic]

    with using_span():
        consumer = kafka.KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            auto_offset_reset="earliest",
            consumer_timeout_ms=30000,
        )
        try:
            record = next(iter(consumer))
        finally:
            consumer.close()
    assert record.value == b"payload"

    _assert_consumer_root(traced, topic, traced.spans[0].trace_id)


def test_confluent_kafka_producer_and_consumer(kafka_container, traced,
                                               consumer_agent, using_span):
    confluent_kafka = pytest.importorskip("confluent_kafka")
    topic = "it-confluent"
    bootstrap = kafka_container["bootstrap"]

    producer = confluent_kafka.Producer({"bootstrap.servers": bootstrap})
    producer.produce(topic, value=b"payload")
    assert producer.flush(30) == 0

    ev = traced.single("confluent_kafka.Producer.produce")
    assert ev.ended
    assert ev.service_type == SERVICE_TYPE_KAFKA_CLIENT
    assert ev.destination == bootstrap
    assert ev.ann(ANNOTATION_KAFKA_TOPIC) == [topic]

    with using_span():
        consumer = confluent_kafka.Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": "it-confluent-group",
            "auto.offset.reset": "earliest",
        })
        consumer.subscribe([topic])
        try:
            deadline = time.monotonic() + 30
            msg = None
            while time.monotonic() < deadline:
                msg = consumer.poll(1.0)
                if msg is not None and msg.error() is None:
                    break
            assert msg is not None and msg.error() is None
            assert msg.value() == b"payload"
        finally:
            consumer.close()

    _assert_consumer_root(traced, topic, traced.spans[0].trace_id)


def test_aiokafka_producer_and_consumer(kafka_container, traced,
                                        consumer_agent, using_span):
    aiokafka = pytest.importorskip("aiokafka")
    topic = "it-aiokafka"
    bootstrap = kafka_container["bootstrap"]

    async def main():
        producer = aiokafka.AIOKafkaProducer(bootstrap_servers=bootstrap)
        await producer.start()
        try:
            await producer.send_and_wait(topic, b"payload")
        finally:
            await producer.stop()

        with using_span():
            consumer = aiokafka.AIOKafkaConsumer(
                topic, bootstrap_servers=bootstrap,
                auto_offset_reset="earliest")
            await consumer.start()
            try:
                return await asyncio.wait_for(consumer.getone(), timeout=30)
            finally:
                await consumer.stop()

    record = asyncio.run(main())
    assert record.value == b"payload"

    ev = traced.single("aiokafka.producer.producer.AIOKafkaProducer.send")
    assert ev.ended
    assert ev.service_type == SERVICE_TYPE_KAFKA_CLIENT
    assert ev.destination == bootstrap
    assert ev.ann(ANNOTATION_KAFKA_TOPIC) == [topic]

    _assert_consumer_root(traced, topic, traced.spans[0].trace_id)
