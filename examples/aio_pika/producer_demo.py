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

"""aio_pika FastAPI producer demo.

The async sibling of examples/pika/producer_demo.py — exposes the same
``/save`` endpoint, but the broker call is made via aio-pika
(asyncio-native RabbitMQ client). A single robust connection is opened
during lifespan startup and reused across requests, matching the
aiokafka producer's pattern.

Run with pinpoint-run:

    pinpoint-run --app-name demo-aio_pika-producer \
        --agent-name demo-aio_pika-producer --collector localhost -- \
        python examples/aio_pika/producer_demo.py

Or directly:

    python examples/aio_pika/producer_demo.py

Hit it:

    curl -XPOST 'http://localhost:5008/save?msg=hello'
    curl -XPOST 'http://localhost:5008/save?msg=world&queue=demo.aio_pika.alt'

What this shows
---------------
- ``pinpoint.instrumentations.fastapi`` opens a Starlette/ASGI root span
  around each request and lifts the route template (``/save``) into the
  url_stat bucket.
- ``aio_pika.exchange.Exchange.publish`` is wrapped so an
  ``aio_pika.exchange.Exchange.publish`` span event nests under
  the request span. The wrapper merges Pinpoint-* trace headers into
  ``Message.headers`` so a downstream consumer (consumer_demo.py) can
  extract them and continue the trace.
- Publishes go via a *named direct exchange* (``demo.aio_pika.exchange``
  by default) with ``routing_key=<queue>``. We declare and bind the
  queue once at startup; per-request ``?queue=...`` overrides re-declare
  + re-bind on the fly (idempotent broker calls).

Configurable:
    RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/
    RABBITMQ_EXCHANGE=demo.aio_pika.exchange
    RABBITMQ_QUEUE=demo.aio_pika
    PORT=5008
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import aio_pika
import uvicorn
from fastapi import FastAPI

RABBITMQ_URL = os.environ.get(
    "RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/",
)
EXCHANGE = os.environ.get("RABBITMQ_EXCHANGE", "demo.aio_pika.exchange")
DEFAULT_QUEUE = os.environ.get("RABBITMQ_QUEUE", "demo.aio_pika")

_connection: aio_pika.abc.AbstractRobustConnection | None = None
_channel: aio_pika.abc.AbstractRobustChannel | None = None
_exchange: aio_pika.abc.AbstractExchange | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open one robust connection + channel for the lifetime of the
    server. RobustConnection transparently reconnects on broker hiccups,
    so the demo can survive a quick container restart during testing."""
    global _connection, _channel, _exchange
    _connection = await aio_pika.connect_robust(RABBITMQ_URL)
    _channel = await _connection.channel()
    _exchange = await _channel.declare_exchange(
        EXCHANGE, aio_pika.ExchangeType.DIRECT,
        durable=False, auto_delete=False,
    )
    # Declare + bind the default queue up-front so the first ``/save``
    # hit doesn't race with a publish to an unbound queue (direct
    # exchanges silently drop messages with no matching binding).
    queue = await _channel.declare_queue(
        DEFAULT_QUEUE, durable=False, auto_delete=False,
    )
    await queue.bind(_exchange, routing_key=DEFAULT_QUEUE)
    try:
        yield
    finally:
        await _connection.close()
        _connection = None
        _channel = None
        _exchange = None


app = FastAPI(title="demo-aio_pika-producer", lifespan=lifespan)


@app.get("/ping")
async def ping():
    return {"service": "demo-aio_pika-producer", "ok": True}


@app.post("/save")
async def save(msg: str = "hello", queue: str = DEFAULT_QUEUE):
    assert _channel is not None and _exchange is not None  # set by lifespan
    # Declare + bind on every request so callers can pass arbitrary queue
    # names via ?queue=...; both calls are idempotent on the broker side.
    q = await _channel.declare_queue(queue, durable=False, auto_delete=False)
    await q.bind(_exchange, routing_key=queue)
    await _exchange.publish(
        aio_pika.Message(body=msg.encode("utf-8")),
        routing_key=queue,
    )
    return {"exchange": EXCHANGE, "routing_key": queue, "message": msg}


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="demo-aio_pika-producer",
        agent_name="demo-aio_pika-prod-1",
        server_info="aio-pika Producer",
    )
    autoload()
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "5008")))


if __name__ == "__main__":
    main()
