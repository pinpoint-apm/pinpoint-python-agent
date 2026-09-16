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

"""pika standalone consumer demo (sync RabbitMQ client).

Subscribes to ``demo.pika`` and processes each delivery via the
callback-based ``basic_consume`` API. Pair with producer_demo.py to see
the distributed trace continue from the producer's Flask span into a
per-message ``pika.consume`` root span on this side.

Run with pinpoint-run:

    pinpoint-run --app-name demo-pika-consumer --agent-name demo-pika-consumer \
        --collector localhost -- python examples/pika/consumer_demo.py

Or directly:

    python examples/pika/consumer_demo.py

What this shows
---------------
- ``pika.channel.Channel.basic_consume`` is wrapped at *callback
  registration* time. The agent replaces the user's callback with a
  shim that opens a Pinpoint root span around each delivery, extracts
  the Pinpoint-* AMQP headers stamped by the producer, and then runs
  the user's original handler inside the span. The full body of
  ``_handle`` below therefore shows up as work under the consume span.
- The producer publishes to a named direct exchange
  (``demo.pika.exchange``) with ``routing_key=demo.pika``. We declare
  and bind the same exchange/queue here so messages reach this consumer
  regardless of which side comes up first.

Configurable:
    RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/
    RABBITMQ_EXCHANGE=demo.pika.exchange
    RABBITMQ_QUEUE=demo.pika
"""

from __future__ import annotations

import os
import signal
from typing import Any

import pika

RABBITMQ_URL = os.environ.get(
    "RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/",
)
EXCHANGE = os.environ.get("RABBITMQ_EXCHANGE", "demo.pika.exchange")
QUEUE = os.environ.get("RABBITMQ_QUEUE", "demo.pika")


def _handle(channel: Any, method: Any, properties: Any, body: bytes) -> None:
    """User callback — runs *inside* the Pinpoint span the agent opened."""
    headers = {
        str(k): (v.decode("utf-8", errors="replace") if isinstance(v, (bytes, bytearray)) else v)
        for k, v in (getattr(properties, "headers", None) or {}).items()
    }
    payload = body
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", errors="replace")
    print(
        f"[consume] exchange={method.exchange!r} routing_key={method.routing_key!r} "
        f"delivery_tag={method.delivery_tag} value={payload!r} headers={headers}",
        flush=True,
    )
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="demo-pika-consumer",
        agent_name="demo-pika-consumer-1",
        server_info="Pika Consumer",
    )
    autoload()

    params = pika.URLParameters(RABBITMQ_URL)
    conn = pika.BlockingConnection(params)
    channel = conn.channel()
    channel.exchange_declare(
        exchange=EXCHANGE, exchange_type="direct",
        durable=False, auto_delete=False,
    )
    channel.queue_declare(queue=QUEUE, durable=False, auto_delete=False)
    channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=QUEUE)
    channel.basic_qos(prefetch_count=8)
    channel.basic_consume(queue=QUEUE, on_message_callback=_handle, auto_ack=False)
    print(
        f"pika consumer listening to {QUEUE!r} (exchange={EXCHANGE!r}) via {RABBITMQ_URL}",
        flush=True,
    )

    def _stop(*_: Any) -> None:
        # stop_consuming is the canonical way to break out of
        # start_consuming(); it tells the IO loop to return after the
        # current delivery is done.
        try:
            channel.stop_consuming()
        except Exception:  # noqa: BLE001
            pass

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    try:
        channel.start_consuming()
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
