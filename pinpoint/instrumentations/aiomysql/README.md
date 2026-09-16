# aiomysql

Instruments [aiomysql](https://aiomysql.readthedocs.io/), the asyncio port of
PyMySQL, through the shared [DB-API core](../dbapi/README.md)'s async wrappers.

| | |
|---|---|
| Target module | `aiomysql.cursors` |
| Hook point | `Cursor.execute` / `executemany` / `callproc` (coroutines) |
| Span role | Span event (child of the caller's transaction) |
| Service type | `MYSQL` |
| Opt-out alias | `aiomysql` |

`DictCursor`, `SSCursor`, and `SSDictCursor` inherit from `Cursor`, so one wrap
covers them. The cursor's `connection` is the aiomysql `Connection`, which stores
host/port/db on plain attributes exactly as pymysql does — so the DB-API core's
default extractor works with no override.

## What gets traced

- One span event per statement, awaited inside the event scope, with the SQL
  template, endpoint (`host:port`), and database name.
- Bound values only when `sql_trace_bind_values` is on (off by default).
- `executemany` produces one event, not one per row. The suppression flag is a
  ContextVar, so it survives the `await` inside the async wrapper.

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
import aiomysql

async def user_name(uid):
    conn = await aiomysql.connect(host="db.internal", user="app", db="shop")
    async with conn.cursor() as cur:
        await cur.execute("SELECT name FROM users WHERE id = %s", (uid,))
        # └─ span event: MYSQL  SELECT name FROM users WHERE id = %s
        return await cur.fetchone()
```

Inside a FastAPI/Starlette/aiohttp handler the event attaches to that request's
transaction automatically. In a bare `asyncio.run` script, open the transaction
yourself with `@pinpoint.span`.

## See also

- Unit tests: [`test_aiomysql_instrumentation.py`](../../../tests/unit/instrumentations/test_aiomysql_instrumentation.py)
- [DB-API core](../dbapi/README.md) · [PyMySQL](../pymysql/README.md) (sync
  counterpart)
