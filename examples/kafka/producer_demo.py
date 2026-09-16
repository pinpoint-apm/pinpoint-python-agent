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

"""kafka-python web producer demo.

Mirrors the pinpoint-cpp-examples/kafka and pinpoint-go-agent/plugin/sarama
shape: a small HTTP server with a ``/save`` endpoint that produces one
Kafka message per request.

Run with pinpoint-run:

    pinpoint-run --app-name demo-kafka-producer --agent-name demo-kafka-producer \
        --collector localhost -- python examples/kafka/producer_demo.py

Or directly:

    python examples/kafka/producer_demo.py

Hit it:

    curl -XPOST 'http://localhost:5005/save?msg=hello'
    curl -XPOST 'http://localhost:5005/save?msg=world&topic=demo.kafka-python'

What this shows
---------------
- The Flask root span is opened by ``pinpoint.instrumentations.flask`` on
  ``/save`` and ``/ping``.
- ``KafkaProducer.send`` is wrapped so a
  ``kafka.producer.kafka.KafkaProducer.send`` span event nests under the
  Flask span. The wrapper also appends Pinpoint-* trace
  headers to the Kafka record headers (Kafka 0.11+ wire format), so a
  downstream consumer (see consumer_demo.py) can extract them and
  continue the distributed trace.

Configurable:
    KAFKA_BOOTSTRAP=localhost:9092
    KAFKA_TOPIC=demo.kafka-python
    PORT=5005
"""

from __future__ import annotations

import os
from typing import Optional

from flask import Flask, jsonify, request
from kafka import KafkaProducer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "demo.kafka-python")

app = Flask("demo-kafka-producer")
_producer: Optional[KafkaProducer] = None


def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            api_version_auto_timeout_ms=5000,
        )
    return _producer


@app.get("/ping")
def ping():
    return jsonify(service="demo-kafka-producer", ok=True)


@app.post("/save")
def save():
    topic = request.args.get("topic", DEFAULT_TOPIC)
    msg = request.args.get("msg", "hello")
    future = _get_producer().send(topic, value=msg.encode("utf-8"))
    metadata = future.get(timeout=5)
    return jsonify(
        topic=metadata.topic,
        partition=metadata.partition,
        offset=metadata.offset,
        message=msg,
    )


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    # Agent name limit is 24 chars; the demo names hit that ceiling.
    pinpoint.init(
        application_name="demo-kafka-producer",
        agent_name="demo-kafka-producer-1",
        server_info="Kafka Producer",
    )
    autoload()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5005")))


if __name__ == "__main__":
    main()
