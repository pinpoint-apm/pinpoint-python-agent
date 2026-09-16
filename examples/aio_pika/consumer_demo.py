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

"""aio_pika standalone async consumer demo.

Subscribes to ``demo.aio_pika`` and processes each delivered message via
the asyncio-native ``async for message in queue:`` iterator pattern.

Run with pinpoint-run:

    pinpoint-run --app-name demo-aio_pika-consumer \
        --agent-name demo-aio_pika-consumer --collector localhost -- \
        python examples/aio_pika/consumer_demo.py

Or directly:

    python examples/aio_pika/consumer_demo.py

What this shows
---------------
- ``aio_pika.queue.QueueIterator.__anext__`` (and its
  ``RobustQueueIterator`` override) is wrapped so every delivered
  message opens a Pinpoint root span seeded from the AMQP
  ``message.headers`` the producer stamped. The span is closed
  immediately on delivery — the user-visible loop body below runs
  *outside* the wrapped method, so any further work in this script
  stands on its own (the iterator-style instrumentation is a delivery
  marker, not a handler wrapper).
- The Pinpoint UI links producer → consumer as one distributed trace via
  the Pinpoint-* AMQP headers extracted from each delivery.
- ``Queue.consume(callback)`` would give full-handler span coverage
  instead; we use ``iterator()`` here because it's the idiomatic
  ``async for`` pattern most aio-pika users write.

Configurable:
    RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/
    RABBITMQ_EXCHANGE=demo.aio_pika.exchange
    RABBITMQ_QUEUE=demo.aio_pika
"""

from __future__ import annotations

import asyncio
import os
import signal

import aio_pika

RABBITMQ_URL = os.environ.get(
    "RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/",
)
EXCHANGE = os.environ.get("RABBITMQ_EXCHANGE", "demo.aio_pika.exchange")
QUEUE = os.environ.get("RABBITMQ_QUEUE", "demo.aio_pika")


def _log_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
    headers = {
        str(k): (v.decode("utf-8", errors="replace") if isinstance(v, (bytes, bytearray)) else v)
        for k, v in (message.headers or {}).items()
    }
    payload = message.body
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", errors="replace")
    print(
        f"[consume] exchange={message.exchange!r} routing_key={message.routing_key!r} "
        f"delivery_tag={message.delivery_tag} value={payload!r} headers={headers}",
        flush=True,
    )


async def _run() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=8)
        exchange = await channel.declare_exchange(
            EXCHANGE, aio_pika.ExchangeType.DIRECT,
            durable=False, auto_delete=False,
        )
        queue = await channel.declare_queue(
            QUEUE, durable=False, auto_delete=False,
        )
        await queue.bind(exchange, routing_key=QUEUE)
        print(
            f"aio_pika consumer listening to {QUEUE!r} "
            f"(exchange={EXCHANGE!r}) via {RABBITMQ_URL}",
            flush=True,
        )

        # The natural ``async for message in q_iter:`` pattern. We make
        # SIGINT/SIGTERM responsive by cancelling the consume task itself
        # — wrapping each ``__anext__`` in ``asyncio.wait_for(..., 1.0)``
        # cancels the underlying aiormq call mid-flight and aio_pika
        # marks the iterator closed (next ``__anext__`` raises
        # ``StopAsyncIteration``), so any subsequent message is lost.
        async def _consume() -> None:
            async with queue.iterator() as q_iter:
                async for message in q_iter:
                    async with message.process():
                        _log_message(message)

        consume_task = asyncio.create_task(_consume())
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, consume_task.cancel)
        try:
            await consume_task
        except asyncio.CancelledError:
            pass


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="demo-aio_pika-consumer",
        agent_name="demo-aio_pika-cons-1",
        server_info="aio-pika Consumer",
    )
    autoload()
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
