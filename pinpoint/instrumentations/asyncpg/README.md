# asyncpg

Instruments [asyncpg](https://magicstack.github.io/asyncpg/), the native async
PostgreSQL driver. asyncpg implements the PostgreSQL wire protocol directly — no
DB-API, no psycopg underneath — so it does not reuse the
[DB-API core](../dbapi/README.md)'s cursor wrappers.

| | |
|---|---|
| Target module | `asyncpg.connection` |
| Hook points | `Connection.execute`, `executemany`, `fetch`, `fetchrow`, `fetchval`, `cursor` |
| Span role | Span event (child of the caller's transaction) |
| Service type | `POSTGRESQL` |
| Opt-out alias | `asyncpg` |

asyncpg's coroutines do not conform to PEP 249 — different signatures, and
connection metadata is read from `Connection._addr` (host/port) and
`Connection._params` (database). The integration shape mirrors OpenTelemetry's
`opentelemetry-instrumentation-asyncpg`.

## What gets traced

One span event per query method, with the SQL template, endpoint (`host:port`),
and database name:

| Method | Notes |
|---|---|
| `execute(query, *args)` | fire-and-forget statement |
| `executemany(query, args)` | batched — one event for the batch |
| `fetch` / `fetchrow` / `fetchval` | result-returning queries |
| `cursor(query, *args)` | records the SQL when the server-side cursor is created; **iteration itself is not traced** |

Bound values are recorded only when `sql_trace_bind_values` is on (off by
default).

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
import asyncpg

async def user_name(uid):
    conn = await asyncpg.connect("postgresql://app@db.internal/shop")
    row = await conn.fetchrow("SELECT name FROM users WHERE id = $1", uid)
    # └─ span event: POSTGRESQL  SELECT name FROM users WHERE id = $1
    #    endpoint db.internal:5432, database "shop"
    await conn.close()
    return row
```

Pool usage (`asyncpg.create_pool`) is traced too — the pool hands out the same
`Connection` objects.

## See also

- Unit tests: [`test_asyncpg_instrumentation.py`](../../../tests/unit/instrumentations/test_asyncpg_instrumentation.py)
- [psycopg](../psycopg/README.md) · [aiopg](../aiopg/README.md) ·
  [Configuration Guide](../../../docs/config.md) — `sql_trace_bind_values`
