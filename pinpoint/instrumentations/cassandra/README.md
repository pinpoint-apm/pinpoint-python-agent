# Cassandra (cassandra-driver)

Instruments the DataStax
[Python driver for Apache Cassandra](https://docs.datastax.com/en/developer/python-driver/).

| | |
|---|---|
| Target module | `cassandra.cluster` |
| Hook points | `Session.execute`, `Session.execute_async` |
| Span role | Span event (child of the caller's transaction) |
| Service type | `CASSANDRA` (2601 — the cpp agent's `CASSANDRA_EXECUTE_QUERY`) |
| Opt-out alias | `cassandra` |

`execute_concurrent` and `execute_concurrent_with_args` (in
`cassandra.concurrent`) ultimately call `Session.execute_async`, so they are
covered transitively.

## What gets traced

- **`execute(...)`** — synchronous and blocking, so the span event covers the
  full query.
- **`execute_async(...)`** — returns a `ResponseFuture`. The event covers the
  *dispatch* only; result delivery happens later on the driver's IO thread. In
  practice the call just queues, so the event is near-instantaneous — it tells
  you the query was issued, not how long it took.
- Endpoint from the Cluster's `contact_points` and `port`; the Session's
  `keyspace` is recorded as the database name.
- The CQL statement is recorded; bound values only when `sql_trace_bind_values`
  is on (off by default).

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
from cassandra.cluster import Cluster

session = Cluster(["cassandra.internal"]).connect("shop")
session.execute("SELECT name FROM users WHERE id = %s", (uid,))
# └─ span event: CASSANDRA  SELECT name FROM users WHERE id = %s
#    endpoint cassandra.internal:9042, keyspace "shop"

future = session.execute_async("SELECT name FROM users WHERE id = %s", (uid,))
# └─ span event covers the dispatch; future.result() is not traced
future.result()
```

## See also

- Unit tests: [`test_cassandra_instrumentation.py`](../../../tests/unit/instrumentations/test_cassandra_instrumentation.py)
- [Custom Instrumentation Guide §10 — Database instrumentation](../../../docs/custom_instrumentation.md) ·
  [Configuration Guide](../../../docs/config.md) — `sql_trace_bind_values`
