# psycopg (psycopg2 / psycopg3)

Instruments both major versions of the canonical Python PostgreSQL driver. They
share a `Cursor.execute(query, vars=None)` shape but need very different
patching strategies, so they have separate autoload hooks and separate opt-out
aliases.

| | |
|---|---|
| Target modules | `psycopg2` (v2), `psycopg` (v3) |
| Hook points | `psycopg2.connect` + a traced `cursor_factory` (v2); `psycopg.Cursor` / `psycopg.AsyncCursor` (v3) |
| Span role | Span event (child of the caller's transaction) |
| Service type | `POSTGRESQL` |
| Opt-out aliases | `psycopg2`, `psycopg` |

## Why psycopg2 is different

psycopg2's `cursor` and `connection` are immutable C extension types: `setattr`
on them raises `TypeError`, so the wrapt class-patching used for pure-Python
drivers can never attach. Handing back a `wrapt.ObjectProxy` is not an option
either — a proxy satisfies Python-level `isinstance` but fails psycopg2's
C-level `PyObject_TypeCheck`, so every `register_type` / `register_uuid` /
`register_default_jsonb` / `quote_ident` call on the connection would raise
`TypeError: argument 2 must be a connection, cursor or None`, breaking Django and
SQLAlchemy connection setup outright.

So instead: wrap `psycopg2.connect`, install a traced `cursor_factory` (a real
subclass of psycopg2's C cursor) on the **raw** connection, and return that raw
connection unchanged. The caller — and every psycopg2 C API — sees the genuine
object; queries are still traced because `connection.cursor()` now yields the
subclass. Same approach as OpenTelemetry's psycopg2 integration.

Because the traced cursor is a real subclass and not a proxy, only the execute
family is overridden. `fetchone` / `fetchmany` / `fetchall` / `scroll`,
iteration (`for row in cur`), and attribute reads (`rowcount`, `description`)
all run at native C speed with **zero** per-row Python frame. That is the key
performance property: fetch is called once per row and must cost nothing.

psycopg3's `Cursor` and `AsyncCursor` are pure Python and get ordinary
sync/async class wrappers.

## Known limitation (psycopg2 only)

A **per-call** factory override is not traced:

```python
conn.cursor(cursor_factory=DictCursor)     # NOT traced
```

It bypasses `connection.cursor_factory`, and intercepting it would require
monkeypatching the immutable C connection's `cursor()` method, which psycopg2
forbids. The rejected alternative (a connection subclass via
`connection_factory`) would have stopped the returned connection from being the
genuine, unmodified object.

These **are** traced:

```python
psycopg2.connect(..., cursor_factory=DictCursor)   # recorded on the connection
conn.cursor()                                      # connection default
```

## What gets traced

- One span event per statement, with the SQL template, endpoint (`host:port`),
  and database name — psycopg3 exposes them via `conn.info`, psycopg2 via
  `conn.get_dsn_parameters()`, both already understood by the
  [DB-API core](../dbapi/README.md).
- Bound values only when `sql_trace_bind_values` is on (off by default).
- `executemany` produces one event, not one per row.
- Statements driven by SQLAlchemy are left to the
  [SQLAlchemy integration](../sqlalchemy/README.md).

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
# psycopg2
import psycopg2

conn = psycopg2.connect(host="db.internal", dbname="shop", user="app")
with conn.cursor() as cur:
    cur.execute("SELECT name FROM users WHERE id = %s", (uid,))
    # └─ span event: POSTGRESQL  SELECT name FROM users WHERE id = %s
```

```python
# psycopg3 — sync or async
import psycopg

async with await psycopg.AsyncConnection.connect("host=db.internal dbname=shop") as conn:
    async with conn.cursor() as cur:
        await cur.execute("SELECT name FROM users WHERE id = %s", (uid,))
```

## Disable

```bash
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=psycopg2   # v2 only
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=psycopg    # v3 only
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=psycopg,psycopg2
```

## See also

- Unit tests: [`test_psycopg_instrumentation.py`](../../../tests/unit/instrumentations/test_psycopg_instrumentation.py)
- [DB-API core](../dbapi/README.md) · [aiopg](../aiopg/README.md) (async facade
  over psycopg2) · [asyncpg](../asyncpg/README.md) (native async driver)
