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

"""kafka-python standalone consumer demo.

Subscribes to ``demo.kafka-python`` and processes each delivered record.
Pair with producer_demo.py to see the distributed trace continue from
the producer's Flask span into a per-message consume span on this side.

Run with pinpoint-run:

    pinpoint-run --app-name demo-kafka-consumer --agent-name demo-kafka-consumer \
        --collector localhost -- python examples/kafka/consumer_demo.py

Or directly:

    python examples/kafka/consumer_demo.py

What this shows
---------------
- ``KafkaConsumer.__next__`` is wrapped so every delivered record opens
  a Pinpoint root span. Pinpoint-* headers stamped by the producer are
  extracted from the record headers and seeded into the span, so the
  Pinpoint UI links producer → consumer as one distributed trace.
- The span is held open and current until the loop asks for the next
  record (or the consumer closes), so everything this body does is
  traced inside that record's span. ``poll()`` batches keep the old
  delivery-only span: a batch arrives all at once, leaving no
  per-record boundary to close one on.
- ``_process`` carries ``@pinpoint.spanevent("consumer.process")``, so
  the UI shows the handling as a child event of that record's span —
  the manual API composing with the auto-instrumented consumer span.

Configurable:
    KAFKA_BOOTSTRAP=localhost:9092
    KAFKA_TOPIC=demo.kafka-python
    KAFKA_GROUP=demo.kafka-python.consumer
"""

from __future__ import annotations

import os
import signal
from typing import Any

import pinpoint
from kafka import KafkaConsumer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "demo.kafka-python")
GROUP = os.environ.get("KAFKA_GROUP", "demo.kafka-python.consumer")


@pinpoint.spanevent("consumer.process")
def _process(record: Any) -> None:
    headers = {
        k: (v.decode("utf-8", errors="replace") if isinstance(v, (bytes, bytearray)) else v)
        for k, v in (record.headers or [])
    }
    payload = record.value
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", errors="replace")
    print(
        f"[consume] topic={record.topic} partition={record.partition} "
        f"offset={record.offset} value={payload!r} headers={headers}",
        flush=True,
    )


def main() -> None:
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="demo-kafka-consumer",
        agent_name="demo-kafka-consumer-1",
        server_info="Kafka Consumer",
    )
    autoload()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP,
        auto_offset_reset="earliest",
        # short poll timeout so signal handlers can break the loop
        consumer_timeout_ms=1000,
    )
    print(
        f"Kafka consumer listening to {TOPIC!r} via {BOOTSTRAP} (group={GROUP})",
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
            # KafkaConsumer iterator raises StopIteration after
            # consumer_timeout_ms with no records; restart so we keep
            # polling while still honoring SIGTERM/SIGINT.
            for record in consumer:
                _process(record)
                if not running:
                    break
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
