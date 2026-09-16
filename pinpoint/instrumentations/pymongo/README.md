# MongoDB (pymongo)

Instruments [pymongo](https://pymongo.readthedocs.io/) through its official
monitoring API rather than by wrapping `Collection` / `Database` methods. One
listener covers every wire-protocol command and stays compatible across pymongo
versions — the same approach as
`opentelemetry-instrumentation-pymongo`.

| | |
|---|---|
| Target module | `pymongo.monitoring` |
| Hook point | a registered `CommandListener` |
| Span role | Span event (child of the caller's transaction) |
| Service type | `MONGO` |
| Opt-out alias | `pymongo` |

## What gets traced

- One span event per command, named `mongo.<command>` — `mongo.find`,
  `mongo.insert`, `mongo.aggregate`, `mongo.update`, and so on, including
  internal commands such as `hello`.
- The collection name and collection options are annotated.
- The **command document** (the query/filter/pipeline itself) is recorded as JSON
  only when `sql_trace_bind_values` is enabled — it carries user data. Off by
  default. When on, the payload is bounded: 64 KB, 32 items, depth 6, 4 KB per
  string, and BSON-only types (`ObjectId`, `datetime`, `Binary`, `Decimal128`, …)
  are stringified.
- Failed commands are recorded as errors on the event.

`CommandStartedEvent` is matched to its `CommandSucceededEvent` /
`CommandFailedEvent` by request id, connection id, and callback-thread id, so
one thread never ends another thread's span event. The in-flight table is capped
at 4096 entries; past that, new commands are traced without an event rather than
growing without bound.

## Usage

No code changes. Clients created before `autoload()` runs also work — the
listener is registered globally with pymongo, not per client.

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
from pymongo import MongoClient

client = MongoClient("mongodb://mongo.internal:27017")
client.shop.users.find_one({"_id": uid})
# └─ span event: MONGO  mongo.find   (collection "users")
```

To see the filter document in the UI, enable bind values — note the privacy
trade-off:

```bash
PINPOINT_PY_SQL_TRACE_BIND_VALUE=true
```

Motor and other async wrappers built on pymongo's monitoring layer are covered
by the same listener.

## See also

- Unit tests: [`test_pymongo_instrumentation.py`](../../../tests/unit/instrumentations/test_pymongo_instrumentation.py)
- [Configuration Guide](../../../docs/config.md) — `sql_trace_bind_values`
