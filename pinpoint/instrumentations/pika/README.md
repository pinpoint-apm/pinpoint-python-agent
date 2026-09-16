# RabbitMQ (pika)

Instruments [pika](https://pika.readthedocs.io/), the synchronous reference
RabbitMQ client, on both sides. Unlike the Kafka pull APIs, pika's dominant
consumer API is callback-based — so here the consumer span is **active while
your handler runs**, and downstream DB/HTTP/producer calls stitch onto it.

| | |
|---|---|
| Target modules | `pika.channel`, `pika.adapters.blocking_connection` |
| Hook points | `Channel.basic_publish`, `Channel.basic_consume`, `BlockingChannel.basic_consume`, `BlockingChannel.consume`, `BlockingChannel.basic_get` |
| Span role | Span event (publish), **root span** (consume) |
| Service type | `RABBITMQ_CLIENT` |
| Opt-out alias | `pika` |

## Producer

`pika.channel.Channel.basic_publish` is the single hook: pika 1.x routes
`BlockingChannel.basic_publish` through this same method, so it covers blocking
and non-blocking publishers alike. Per publish it:

- opens a span event named `pika.publish <exchange>/<routing_key>`;
- injects Pinpoint trace headers into `properties.headers` so an instrumented
  consumer stitches into the trace;
- annotates the AMQP exchange and routing key.

## Consumer

Three delivery shapes, each wrapped where **your** code actually runs — so the
span measures message processing, not an internal buffer append:

| API | Span behavior |
|---|---|
| `BlockingChannel.basic_consume(queue, on_message_callback, ...)` | The user callback is wrapped, so the span is **active while the handler runs**. Child calls (DB, HTTP, further publishes) attach to it. |
| `BlockingChannel.consume()` (generator) | A root span per real delivery, kept active **across the `yield`** so the `for` loop body's child calls attach. It closes when the loop resumes for the next message; the idle wait between messages stays untraced. |
| `BlockingChannel.basic_get()` (sync poll) | Delivery-only root span — there is no user handler to wrap. |

The subtlety worth knowing: pika stores your callback in a `_ConsumerInfo` and
invokes it later from `process_data_events` / `start_consuming`. Down at
`pika.channel.Channel.basic_consume` it registers only its own internal
buffering sink (`BlockingChannel._on_consumer_message_delivery`), which merely
enqueues the delivery. So the wrapper hooks the `BlockingChannel.basic_consume`
seam and the `Channel.basic_consume` hook detects and skips that internal sink —
otherwise the span would wrap the enqueue and end before your handler ever ran.

Direct non-blocking `Channel` users (`SelectConnection`, adapters) hand their
real callback straight to `Channel.basic_consume`, so that hook still wraps it. A
`_pinpoint_consumer_wrapped` marker guards against double-wrapping.

## Usage

```bash
pinpoint-run --app-name my-app --collector localhost -- python consumer.py
```

```python
# publisher — inside a transaction
import pika

conn = pika.BlockingConnection(pika.ConnectionParameters("rabbit.internal"))
channel = conn.channel()
channel.basic_publish(exchange="", routing_key="orders", body=b'{"id": 1}')
# └─ span event: RABBITMQ_CLIENT  pika.publish /orders   (headers injected)
```

```python
# consumer — the span is live inside on_message
import requests

def on_message(ch, method, properties, body):
    # Root span: RabbitMQ Consumer Invocation
    #  └─ span event: GET http://inventory.internal/...  (attaches to it)
    requests.get("http://inventory.internal/reserve")
    ch.basic_ack(method.delivery_tag)

channel.basic_consume(queue="orders", on_message_callback=on_message)
channel.start_consuming()
```

## See also

- Examples: [`examples/pika/`](../../../examples/pika) — `producer_demo.py`,
  `consumer_demo.py`
- Unit tests: [`test_pika_instrumentation.py`](../../../tests/unit/instrumentations/test_pika_instrumentation.py)
- [aio-pika](../aio_pika/README.md) (async counterpart) ·
  [Custom Instrumentation Guide §11 — Message queues](../../../docs/custom_instrumentation.md)
