# Kafka (kafka-python)

Instruments [kafka-python](https://kafka-python.readthedocs.io/) on both sides:
producers inject trace headers, consumers open a transaction per delivered
message so the two ends stitch into one distributed trace.

| | |
|---|---|
| Target modules | `kafka.producer.kafka`, `kafka.consumer.group` |
| Hook points | `KafkaProducer.send`, `KafkaConsumer.__next__`, `KafkaConsumer.poll`, `KafkaConsumer.close` (ends the last iterator span) |
| Span role | Span event (producer), **root span** (consumer) |
| Service type | `KAFKA_CLIENT` |
| Opt-out alias | `kafka` |

## Producer

`KafkaProducer.send` becomes a span event on the calling transaction, and
Pinpoint trace headers are injected into the Kafka **record headers** (Kafka
0.11+) so a downstream consumer can stitch. Topic, partition, and offset are
annotated.

## Consumer

Both delivery surfaces are wrapped, because a record can reach the caller
through either one:

- **`__next__`** (iterator interface) — one root span per returned message.
- **`poll(timeout_ms=...)`** (batch interface) — one root span per record in the
  returned `{TopicPartition: [records]}` map. The common
  `while True: consumer.poll(500)` loop never flows through `__next__`, which is
  why both are needed.

`__next__` drives `poll(update_offsets=False)` internally, so the poll wrapper
skips that internal call — no double spans.

**The iterator span covers your loop body.** A pull API gives the agent no
callback to wrap, but asking for the *next* record is itself the boundary —
whatever you did with the previous one is finished. So `__next__` closes the
span it held open and starts a new one, leaving each record's span current
while you handle it. Your DB and HTTP calls nest under it with no extra code:

```python
for message in consumer:
    handle(message)          # traced inside this record's consumer span
consumer.close()             # closes the last record's span
```

A consumer abandoned without `close()` leaves its last span open until the
process exits.

**`poll()` batches are delivery-only.** A batch hands over every record at
once, so there is no per-record boundary to close a span on. Open your own
transaction if you want the processing traced:

```python
for tp, records in consumer.poll(500).items():
    for message in records:
        with pinpoint.trace("handle_order"):
            handle(message)
```

## Usage

No code changes for the delivery spans themselves:

```bash
pinpoint-run --app-name my-app --collector localhost -- python consumer.py
```

```python
# producer.py — inside a transaction (an HTTP handler, or @pinpoint.span)
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="kafka.internal:9092")
producer.send("orders", b'{"id": 1}')
# └─ span event: KAFKA_CLIENT  send   (topic "orders", Pinpoint-* in record headers)
```

```python
# consumer.py — each message opens its own transaction
from kafka import KafkaConsumer

consumer = KafkaConsumer("orders", bootstrap_servers="kafka.internal:9092")
for message in consumer:
    # Root span: Kafka Consumer Invocation — linked to the producer's trace
    handle(message)
```

Kafka headers are application-controlled binary data, so header extraction and
annotation are bounded (at most 32 header annotations per record).

## See also

- Examples: [`examples/kafka/`](../../../examples/kafka) — `producer_demo.py`,
  `consumer_demo.py`
- Unit tests: [`test_kafka_instrumentation.py`](../../../tests/unit/instrumentations/test_kafka_instrumentation.py)
- [aiokafka](../aiokafka/README.md) · [confluent-kafka](../confluent_kafka/README.md) ·
  [Custom Instrumentation Guide §11 — Message queues](../../../docs/custom_instrumentation.md)
