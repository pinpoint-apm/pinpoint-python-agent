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

"""aiokafka standalone async consumer demo.

Subscribes to ``demo.aiokafka`` and processes each delivered record
via the asyncio-native ``async for record in consumer:`` loop, which
goes through ``AIOKafkaConsumer.getone`` internally.

Run with pinpoint-run:

    pinpoint-run --app-name demo-aiokafka-consumer \
        --agent-name demo-aiokafka-consumer --collector localhost -- \
        python examples/aiokafka/consumer_demo.py

Or directly:

    python examples/aiokafka/consumer_demo.py

What this shows
---------------
- ``AIOKafkaConsumer.getone`` is wrapped so every delivered record opens
  a Pinpoint root span. Pinpoint-* headers stamped by the producer are
  extracted from the record headers and seeded into the span, so the
  Pinpoint UI links producer → consumer as one distributed trace.
- The ``getone`` span is held open and current until the loop awaits the
  next record (or ``stop()``), so everything this body does is traced
  inside that record's span.
- ``_process`` carries ``@pinpoint.spanevent("consumer.process")``, so
  the UI shows the handling as a child event of that record's span.
- ``getmany`` is wrapped too for callers that batch. It emits one
  ``Kafka Consumer Invocation`` span per returned record so each producer trace
  context remains stitched to its consumer — delivery-only, since a batch
  arrives all at once and leaves no per-record boundary.

Configurable:
    KAFKA_BOOTSTRAP=localhost:9092
    KAFKA_TOPIC=demo.aiokafka
    KAFKA_GROUP=demo.aiokafka.consumer
"""

from __future__ import annotations

import asyncio
import os
import signal
from typing import Any

import pinpoint
from aiokafka import AIOKafkaConsumer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "demo.aiokafka")
GROUP = os.environ.get("KAFKA_GROUP", "demo.aiokafka.consumer")


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


async def _run() -> None:
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    print(
        f"aiokafka consumer listening to {TOPIC!r} via {BOOTSTRAP} (group={GROUP})",
        flush=True,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        while not stop.is_set():
            # Bound getone() with a short timeout so the stop event is
            # responsive to SIGTERM even on an idle topic.
            try:
                record = await asyncio.wait_for(consumer.getone(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            _process(record)
    finally:
        await consumer.stop()


def main() -> None:
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="demo-aiokafka-consumer",
        agent_name="demo-aiokafka-consumer-1",
        server_info="aiokafka Consumer",
    )
    autoload()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
