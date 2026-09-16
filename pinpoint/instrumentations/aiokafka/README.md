# aiokafka

Instruments [aiokafka](https://aiokafka.readthedocs.io/), the asyncio-native
Kafka client. Its API mirrors kafka-python, so this integration mirrors the
[kafka-python one](../kafka/README.md).

| | |
|---|---|
| Target modules | `aiokafka.producer.producer`, `aiokafka.consumer.consumer` |
| Hook points | `AIOKafkaProducer.send`, `AIOKafkaConsumer.getone`, `AIOKafkaConsumer.getmany` |
| Span role | Span event (producer), **root span** (consumer) |
| Service type | `KAFKA_CLIENT` |
| Opt-out alias | `aiokafka` |

## Producer

`AIOKafkaProducer.send` is a coroutine returning a future of `RecordMetadata`;
it is the publish entry point. `send_and_wait` is a thin wrapper that calls
`send` and awaits the future, so the `send` hook covers it too. Pinpoint trace
headers are injected into the record headers; topic, partition, and offset are
annotated.

## Consumer

Both delivery surfaces are wrapped:

- **`getone()`** — coroutine returning a single `ConsumerRecord`.
- **`getmany()`** — coroutine returning `{TopicPartition: [ConsumerRecord]}`.

Both flow through `AIOKafkaConsumer._fetcher.fetched_records` internally, but
wrapping the public API gives more readable span names.

One root span per record, linked to the producer's trace. `getone()` — which
`async for record in consumer` awaits per record — holds its span open and
current until the next await or `stop()`, so the loop body is traced inside it
with no extra code. `getmany()` hands over a whole batch at once, leaving no
per-record boundary, so its spans stay delivery-only — wrap the processing
yourself there if you want it traced.

**`getmany()` batch cost:** the per-record spans are created synchronously,
inline in the event loop, right after the fetch returns — roughly the
per-span agent overhead (see `benchmark/api_overhead`) times the batch size
before any other coroutine runs. With very large `max_records` batches on a
latency-sensitive loop, budget for that pause or cap the batch size.

## Usage

```bash
pinpoint-run --app-name my-app --collector localhost -- python consumer.py
```

```python
# producer — inside a transaction
from aiokafka import AIOKafkaProducer

producer = AIOKafkaProducer(bootstrap_servers="kafka.internal:9092")
await producer.start()
await producer.send_and_wait("orders", b'{"id": 1}')
# └─ span event: KAFKA_CLIENT  send   (Pinpoint-* in record headers)
```

```python
# consumer — each record opens its own transaction
from aiokafka import AIOKafkaConsumer
import pinpoint

consumer = AIOKafkaConsumer("orders", bootstrap_servers="kafka.internal:9092")
await consumer.start()
async for message in consumer:
    # Root span: Kafka Consumer Invocation
    with pinpoint.trace("handle_order"):     # trace the processing too
        await handle(message)
```

## See also

- Examples: [`examples/aiokafka/`](../../../examples/aiokafka) —
  `producer_demo.py`, `consumer_demo.py`
- Unit tests: [`test_aiokafka_instrumentation.py`](../../../tests/unit/instrumentations/test_aiokafka_instrumentation.py)
- [kafka-python](../kafka/README.md) · [confluent-kafka](../confluent_kafka/README.md)
