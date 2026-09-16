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

"""pika web producer demo (sync RabbitMQ client).

Mirrors examples/kafka/producer_demo.py but talks to RabbitMQ via the
synchronous reference client (``pika``). Each ``/save`` request opens a
short-lived blocking connection, publishes one message, and replies with
the AMQP routing target so the trace shows both the Flask root span and
the ``pika.channel.Channel.basic_publish`` child event under it.

Run with pinpoint-run:

    pinpoint-run --app-name demo-pika-producer --agent-name demo-pika-producer \
        --collector localhost -- python examples/pika/producer_demo.py

Or directly:

    python examples/pika/producer_demo.py

Hit it:

    curl -XPOST 'http://localhost:5007/save?msg=hello'
    curl -XPOST 'http://localhost:5007/save?msg=world&queue=demo.pika.alt'

What this shows
---------------
- ``pinpoint.instrumentations.flask`` opens the root span on ``/save`` /
  ``/ping``.
- ``pika.channel.Channel.basic_publish`` is wrapped so a
  ``pika.channel.Channel.basic_publish`` span event nests under the
  Flask span. The wrapper merges Pinpoint-* trace headers into the AMQP
  ``BasicProperties.headers`` so a downstream consumer (consumer_demo.py)
  can extract them and continue the trace.
- We publish to a *named direct exchange* (``demo.pika.exchange`` by
  default) and bind the queue to it with ``routing_key=<queue>``. Direct
  exchanges deliver to queues whose binding key exactly matches the
  routing key, so each ``/save`` request lands in exactly one queue.

Configurable:
    RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/
    RABBITMQ_EXCHANGE=demo.pika.exchange
    RABBITMQ_QUEUE=demo.pika
    PORT=5007
"""

from __future__ import annotations

import os

import pika
from flask import Flask, jsonify, request

RABBITMQ_URL = os.environ.get(
    "RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/",
)
EXCHANGE = os.environ.get("RABBITMQ_EXCHANGE", "demo.pika.exchange")
DEFAULT_QUEUE = os.environ.get("RABBITMQ_QUEUE", "demo.pika")

app = Flask("demo-pika-producer")


def _publish(queue: str, body: bytes) -> None:
    """Open → declare → bind → publish → close.

    Re-opening per request keeps the demo simple — pika ``BlockingConnection``
    isn't thread-safe and Flask's default dev server runs requests in
    separate threads, so a per-request connection avoids any sharing
    hazard. Production users would pool connections via a manager
    library; that's orthogonal to the instrumentation, which hooks
    ``Channel.basic_publish`` regardless of how the channel was obtained.

    We publish to a *named direct exchange* bound to the queue with
    ``routing_key=<queue>`` — this is the most common production layout
    (no reliance on the broker's default exchange) and gives the Pinpoint
    UI a real exchange name on both the publish and consume sides.
    """
    params = pika.URLParameters(RABBITMQ_URL)
    conn = pika.BlockingConnection(params)
    try:
        channel = conn.channel()
        channel.exchange_declare(
            exchange=EXCHANGE, exchange_type="direct",
            durable=False, auto_delete=False,
        )
        channel.queue_declare(queue=queue, durable=False, auto_delete=False)
        channel.queue_bind(queue=queue, exchange=EXCHANGE, routing_key=queue)
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=queue,
            body=body,
        )
    finally:
        conn.close()


@app.get("/ping")
def ping():
    return jsonify(service="demo-pika-producer", ok=True)


@app.post("/save")
def save():
    queue = request.args.get("queue", DEFAULT_QUEUE)
    msg = request.args.get("msg", "hello")
    _publish(queue, msg.encode("utf-8"))
    return jsonify(exchange=EXCHANGE, routing_key=queue, message=msg)


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="demo-pika-producer",
        agent_name="demo-pika-producer-1",
        server_info="Pika Producer",
    )
    autoload()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5007")))


if __name__ == "__main__":
    main()
