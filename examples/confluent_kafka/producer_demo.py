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

"""confluent-kafka web producer demo.

Same shape as the kafka-python demo but uses the librdkafka-based
``confluent_kafka.Producer``. ``produce`` is fire-and-forget, so the
demo also calls ``flush(timeout=...)`` inside the handler to surface
delivery results before responding to the HTTP caller.

Run with pinpoint-run:

    pinpoint-run --app-name demo-confluent-kafka-producer \
        --agent-name demo-confluent-kafka-producer --collector localhost -- \
        python examples/confluent_kafka/producer_demo.py

Or directly:

    python examples/confluent_kafka/producer_demo.py

Hit it:

    curl -XPOST 'http://localhost:5007/save?msg=hello'
    curl -XPOST 'http://localhost:5007/save?msg=world&topic=demo.confluent-kafka'

What this shows
---------------
- The Flask root span is opened by ``pinpoint.instrumentations.flask`` on
  every request.
- ``Producer.produce`` is wrapped so a
  ``confluent_kafka.produce <topic>`` span event nests under the Flask
  span. Pinpoint-* trace headers are appended to the record headers
  *before* librdkafka queues the message (headers are immutable after
  ``produce`` returns).
- The span event ends at the *queueing* boundary — actual network IO
  happens later inside the C runtime, so the event measures the
  enqueue step, not the broker-side ack.

Configurable:
    KAFKA_BOOTSTRAP=localhost:9092
    KAFKA_TOPIC=demo.confluent-kafka
    PORT=5007
"""

from __future__ import annotations

import os

import confluent_kafka
from flask import Flask, jsonify, request

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "demo.confluent-kafka")

app = Flask("demo-confluent-kafka-producer")
_producer: confluent_kafka.Producer | None = None
_delivery_results: list = []


def _get_producer() -> confluent_kafka.Producer:
    global _producer
    if _producer is None:
        # Resolve ``Producer`` lazily off the module so we pick up the
        # instrumentation subclass installed by ``pinpoint.autoload`` —
        # the underlying ``cimpl.Producer`` is an immutable C type, so
        # the agent replaces the public ``confluent_kafka.Producer``
        # attribute at autoload time. ``from confluent_kafka import
        # Producer`` at module top would freeze the original class.
        _producer = confluent_kafka.Producer({"bootstrap.servers": BOOTSTRAP})
    return _producer


def _on_delivery(err, msg) -> None:
    if err is not None:
        _delivery_results.append(("error", str(err)))
        return
    _delivery_results.append((
        "ok", msg.topic(), msg.partition(), msg.offset(),
    ))


@app.get("/ping")
def ping():
    return jsonify(service="demo-confluent-kafka-producer", ok=True)


@app.post("/save")
def save():
    topic = request.args.get("topic", DEFAULT_TOPIC)
    msg = request.args.get("msg", "hello")

    producer = _get_producer()
    _delivery_results.clear()
    producer.produce(
        topic,
        value=msg.encode("utf-8"),
        on_delivery=_on_delivery,
    )
    # Drive the delivery callback so we know if the broker accepted the
    # message before returning to the HTTP caller. In production code
    # callers typically poll() / flush() periodically rather than per
    # request — but for a demo this keeps the response meaningful.
    producer.flush(timeout=5)

    if not _delivery_results:
        return jsonify(error="delivery timed out"), 504
    result = _delivery_results[-1]
    if result[0] == "error":
        return jsonify(error=result[1]), 500
    _, t, partition, offset = result
    return jsonify(topic=t, partition=partition, offset=offset, message=msg)


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="demo-cfkafka-producer",
        agent_name="demo-cfkafka-producer-1",
        server_info="Confluent Kafka Producer",
    )
    autoload()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5007")))


if __name__ == "__main__":
    main()
