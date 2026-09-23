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

"""aiokafka FastAPI producer demo.

The async sibling of examples/kafka/producer_demo.py — exposes the
same ``/save`` endpoint, but the broker call is made via
``AIOKafkaProducer`` (asyncio-native).

Run with pinpoint-run:

    pinpoint-run --app-name demo-aiokafka-producer \
        --agent-name demo-aiokafka-producer --collector localhost -- \
        python examples/aiokafka/producer_demo.py

Or directly:

    python examples/aiokafka/producer_demo.py

Hit it:

    curl -XPOST 'http://localhost:5006/save?msg=hello'
    curl -XPOST 'http://localhost:5006/save?msg=world&topic=demo.aiokafka'

What this shows
---------------
- ``pinpoint.instrumentations.fastapi`` opens a Starlette/ASGI root span
  around each request and lifts the route template (``/save``) into the
  url_stat bucket.
- ``AIOKafkaProducer.send`` is wrapped so an
  ``aiokafka.producer.producer.AIOKafkaProducer.send`` span event nests
  under the request span. The wrapper appends
  Pinpoint-* trace headers to the Kafka record headers (Kafka 0.11+
  wire format) so a downstream consumer (consumer_demo.py) can extract
  them and continue the trace.

Configurable:
    KAFKA_BOOTSTRAP=localhost:9092
    KAFKA_TOPIC=demo.aiokafka
    PORT=5006
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import uvicorn
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "demo.aiokafka")

_producer: AIOKafkaProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _producer
    _producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
    await _producer.start()
    try:
        yield
    finally:
        await _producer.stop()
        _producer = None


app = FastAPI(title="demo-aiokafka-producer", lifespan=lifespan)


@app.get("/ping")
async def ping():
    return {"service": "demo-aiokafka-producer", "ok": True}


@app.post("/save")
async def save(msg: str = "hello", topic: str = DEFAULT_TOPIC):
    assert _producer is not None  # set by lifespan
    metadata = await _producer.send_and_wait(topic, value=msg.encode("utf-8"))
    return {
        "topic": metadata.topic,
        "partition": metadata.partition,
        "offset": metadata.offset,
        "message": msg,
    }


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="demo-aiokafka-producer",
        agent_name="demo-aiokafka-producer-1",
        server_info="aiokafka Producer",
    )
    autoload()
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "5006")))


if __name__ == "__main__":
    main()
