# aiopg

Instruments [aiopg](https://aiopg.readthedocs.io/), the asyncio-friendly facade
over psycopg2, through the shared [DB-API core](../dbapi/README.md)'s async
wrappers.

| | |
|---|---|
| Target module | `aiopg.connection` |
| Hook point | `Cursor.execute` / `executemany` / `callproc` (coroutines) |
| Span role | Span event (child of the caller's transaction) |
| Service type | `POSTGRESQL` |
| Opt-out alias | `aiopg` |

Underneath, aiopg holds a real psycopg2 connection at `cursor._impl` /
`cursor.raw` — that is where host/port/database live. This integration's target
extractor walks through to it and then defers to the DB-API core, which already
knows how to read psycopg2's `get_dsn_parameters()`.

## What gets traced

- One span event per statement, awaited inside the event scope, with the SQL
  template, endpoint (`host:port`), and database name.
- Bound values only when `sql_trace_bind_values` is on (off by default).
- `executemany` produces one event, not one per row.

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
import aiopg

async def user_name(uid):
    async with aiopg.create_pool("host=db.internal dbname=shop user=app") as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT name FROM users WHERE id = %s", (uid,))
                # └─ span event: POSTGRESQL  SELECT name FROM users WHERE id = %s
                return await cur.fetchone()
```

## Disable

```bash
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=aiopg
```

Leave `psycopg2` enabled — aiopg's queries are recorded at the aiopg cursor, so
disabling only `aiopg` is enough to silence them.

## See also

- Unit tests: [`test_aiopg_instrumentation.py`](../../../tests/unit/instrumentations/test_aiopg_instrumentation.py)
- [DB-API core](../dbapi/README.md) · [psycopg](../psycopg/README.md) ·
  [asyncpg](../asyncpg/README.md) (native async alternative)
