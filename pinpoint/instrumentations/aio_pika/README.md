# RabbitMQ (aio-pika)

Instruments [aio-pika](https://aio-pika.readthedocs.io/), the asyncio wrapper
around aiormq with a higher-level `Exchange` / `Queue` API.

| | |
|---|---|
| Target modules | `aio_pika.exchange`, `aio_pika.queue`, `aio_pika.robust_queue` |
| Hook points | `Exchange.publish`, `Queue.consume`, `RobustQueue.consume`, `QueueIterator.__anext__`, `RobustQueueIterator.__anext__` |
| Span role | Span event (publish), **root span** (consume) |
| Service type | `RABBITMQ_CLIENT` |
| Opt-out alias | `aio_pika` (also accepted as `aio-pika`) |

## Producer

`Exchange.publish` is the single async publish entry that every higher-level
helper (`Connection.default_exchange.publish`, `RobustExchange.publish`)
eventually calls, so one hook gives full coverage.

The caller's `Message` is **never mutated**. aio-pika publishes are coroutines
that interleave at the channel-lock await, and the `Message` is user-owned and
may be published concurrently — so the wrapper publishes a per-publish shallow
copy carrying this trace's headers merged into its own fresh dict.

## Consumer

Two delivery shapes:

| API | Span behavior |
|---|---|
| `Queue.consume(callback)` | The user callback is wrapped at registration, so the root span is **active while the handler runs** and child calls attach to it. |
| `Queue.iterator()` (async generator) | A delivery-only root span per message — the `async for message in iterator` loop body runs *outside* the iterator method, so the span cannot cover it. Wrap the body with `pinpoint.trace(...)` if you want the processing traced. |

The iterator internally registers its own buffering callback
(`QueueIterator.on_message`) through the same `Queue.consume`; the consume
wrapper detects and skips it, otherwise every iterator-delivered message would
produce two root spans. `RobustQueue.consume` delegates down to `Queue.consume`,
and a marker guards against double-wrapping through that delegation.

Both paths read `message.headers` for upstream Pinpoint context.

## Usage

```bash
pinpoint-run --app-name my-app --collector localhost -- python consumer.py
```

```python
# publisher — inside a transaction
import aio_pika

conn = await aio_pika.connect_robust("amqp://rabbit.internal/")
channel = await conn.channel()
await channel.default_exchange.publish(
    aio_pika.Message(body=b'{"id": 1}'), routing_key="orders")
# └─ span event: RABBITMQ_CLIENT  publish /orders   (headers injected into a copy)
```

```python
# consumer, callback form — the span is live inside on_message
async def on_message(message: aio_pika.IncomingMessage):
    # Root span: RabbitMQ Consumer Invocation
    async with message.process():
        await handle(message.body)

queue = await channel.declare_queue("orders")
await queue.consume(on_message)
```

```python
# consumer, iterator form — delivery-only span, so trace the body yourself
import pinpoint

async with queue.iterator() as messages:
    async for message in messages:
        with pinpoint.trace("handle_order"):
            await handle(message.body)
```

## See also

- Examples: [`examples/aio_pika/`](../../../examples/aio_pika) —
  `producer_demo.py`, `consumer_demo.py`
- Unit tests: [`test_aio_pika_instrumentation.py`](../../../tests/unit/instrumentations/test_aio_pika_instrumentation.py)
- [pika](../pika/README.md) (sync counterpart) ·
  [Custom Instrumentation Guide §11 — Message queues](../../../docs/custom_instrumentation.md)
