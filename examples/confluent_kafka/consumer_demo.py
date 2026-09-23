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

"""confluent-kafka standalone consumer demo.

Subscribes to ``demo.confluent-kafka`` and processes each delivered
message via the canonical ``Consumer.poll`` loop. Pair with
producer_demo.py to see the trace continue across the broker.

Run with pinpoint-run:

    pinpoint-run --app-name demo-confluent-kafka-consumer \
        --agent-name demo-confluent-kafka-consumer --collector localhost -- \
        python examples/confluent_kafka/consumer_demo.py

Or directly:

    python examples/confluent_kafka/consumer_demo.py

What this shows
---------------
- ``Consumer.poll`` is wrapped so every delivered message opens a
  Pinpoint root span. Pinpoint-* headers stamped by the producer are
  extracted from the message headers and seeded into the span, so the
  Pinpoint UI links producer → consumer as one distributed trace.
- The span is held open and current until the next ``poll`` (or
  ``close()``), so everything this loop body does is traced inside that
  message's span. ``consume()`` batches keep the delivery-only span.
- ``_process`` carries ``@pinpoint.spanevent("consumer.process")``, so
  the UI shows the handling as a child event of that message's span.
- Partition EOFs and error messages (``message.error() is not None``)
  are filtered out by the instrumentation so we don't spam empty spans.

Configurable:
    KAFKA_BOOTSTRAP=localhost:9092
    KAFKA_TOPIC=demo.confluent-kafka
    KAFKA_GROUP=demo.confluent-kafka.consumer
"""

from __future__ import annotations

import os
import signal
from typing import Any

import pinpoint
import confluent_kafka
from confluent_kafka import KafkaError

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "demo.confluent-kafka")
GROUP = os.environ.get("KAFKA_GROUP", "demo.confluent-kafka.consumer")


@pinpoint.spanevent("consumer.process")
def _process(message: Any) -> None:
    headers = {}
    for key, raw in (message.headers() or []):
        value = raw
        if isinstance(raw, (bytes, bytearray)):
            value = raw.decode("utf-8", errors="replace")
        headers[key] = value
    payload = message.value()
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", errors="replace")
    print(
        f"[consume] topic={message.topic()} partition={message.partition()} "
        f"offset={message.offset()} value={payload!r} headers={headers}",
        flush=True,
    )


def main() -> None:
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="demo-cfkafka-consumer",
        agent_name="demo-cfkafka-consumer-1",
        server_info="Confluent Kafka Consumer",
    )
    autoload()

    # Resolve ``Consumer`` lazily off the module so we pick up the
    # instrumentation subclass installed by ``pinpoint.autoload``.
    consumer = confluent_kafka.Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    print(
        f"confluent-kafka consumer listening to {TOPIC!r} via {BOOTSTRAP} "
        f"(group={GROUP})",
        flush=True,
    )

    running = True

    def _stop(*_: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    try:
        while running:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            err = message.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[consume] error: {err}", flush=True)
                continue
            _process(message)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
