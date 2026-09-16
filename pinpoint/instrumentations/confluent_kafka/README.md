# confluent-kafka

Instruments
[confluent-kafka-python](https://docs.confluent.io/kafka-clients/python/current/overview.html),
the librdkafka-based client. Both `Producer` and `Consumer` are C extension
types, which changes a few things versus the pure-Python clients.

| | |
|---|---|
| Target module | `confluent_kafka` |
| Hook points | `Producer.produce`, `Consumer.poll`, `Consumer.consume`, plus `SerializingProducer` / `DeserializingConsumer` |
| Span role | Span event (producer), **root span** (consumer) |
| Service type | `KAFKA_CLIENT` |
| Opt-out alias | `confluent_kafka` |

## Producer

`Producer.produce` is fire-and-forget: it queues a delivery callback internally
and the message becomes immutable on return. Pinpoint trace headers therefore
have to be added to the `headers` kwarg **before** `produce` returns, which is
exactly what the wrapper does.

The span event covers the *queueing*, not the network send — librdkafka does the
actual IO later inside its C runtime. The event is opened around the call and
closed before it returns.

## Consumer

`Consumer.poll` and `Consumer.consume` return `Message` objects whose header
bytes are reachable via `message.headers()`. One root span per delivered
message, so every independently propagated producer context is stitched to its
consumer.

`poll` returns a single message, so its span is held open and current until the
next `poll` or `close()` — your handling is traced inside it with no extra code.
`consume` returns a whole batch at once, leaving no per-record boundary, so its
spans stay delivery-only.

## Schema-registry clients

`SerializingProducer` and `DeserializingConsumer` subclass the C
`cimpl.Producer` / `cimpl.Consumer` **directly** — not the
`confluent_kafka.Producer` / `.Consumer` names this integration rebinds — so
they are patched as their own concrete classes. Without that, their
`produce`/`poll` would bypass instrumentation entirely: no producer spans, no
header injection, no downstream stitching.

## Usage

```bash
pinpoint-run --app-name my-app --collector localhost -- python consumer.py
```

```python
# producer — inside a transaction
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "kafka.internal:9092"})
producer.produce("orders", b'{"id": 1}')
# └─ span event: KAFKA_CLIENT  produce   (Pinpoint-* in record headers)
producer.flush()
```

```python
# consumer — each message opens its own transaction
from confluent_kafka import Consumer
import pinpoint

consumer = Consumer({"bootstrap.servers": "kafka.internal:9092",
                     "group.id": "orders-worker"})
consumer.subscribe(["orders"])
while True:
    message = consumer.poll(1.0)
    if message is None or message.error():
        continue
    # Root span: Kafka Consumer Invocation, current until the next poll
    handle(message)
```

## See also

- Examples: [`examples/confluent_kafka/`](../../../examples/confluent_kafka) —
  `producer_demo.py`, `consumer_demo.py`
- Unit tests: [`test_confluent_kafka_instrumentation.py`](../../../tests/unit/instrumentations/test_confluent_kafka_instrumentation.py)
- [kafka-python](../kafka/README.md) · [aiokafka](../aiokafka/README.md)
