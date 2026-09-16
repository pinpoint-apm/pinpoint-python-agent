# PyMySQL

Instruments [PyMySQL](https://pymysql.readthedocs.io/), the pure-Python MySQL
driver, through the shared [DB-API core](../dbapi/README.md).

| | |
|---|---|
| Target module | `pymysql.cursors` |
| Hook point | `Cursor.execute` / `executemany` / `callproc` |
| Span role | Span event (child of the caller's transaction) |
| Service type | `MYSQL` |
| Opt-out alias | `pymysql` |

`DictCursor`, `SSCursor`, and `SSDictCursor` all inherit from `Cursor`, so one
wrap covers every cursor flavor. Connection host/port/database are read from the
plain connection attributes by the DB-API core's default extractor.

## What gets traced

- One span event per statement, with the SQL template, the endpoint
  (`host:port`), and the database name.
- Bound values only when `sql_trace_bind_values` is on (off by default — bind
  values routinely carry PII).
- `executemany` produces **one** event, not one per row.
- Statements driven by SQLAlchemy are left to the
  [SQLAlchemy integration](../sqlalchemy/README.md) so the UI shows one DB node
  per query, not two.

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
import pymysql

conn = pymysql.connect(host="db.internal", user="app", database="shop")
with conn.cursor() as cur:
    cur.execute("SELECT name FROM users WHERE id = %s", (uid,))
    # └─ span event: MYSQL  SELECT name FROM users WHERE id = %s
    #    endpoint db.internal:3306, database "shop"
    cur.fetchone()   # fetch is not traced — one event per statement
```

## See also

- Unit tests: [`test_pymysql_instrumentation.py`](../../../tests/unit/instrumentations/test_pymysql_instrumentation.py)
- [DB-API core](../dbapi/README.md) ·
  [Configuration Guide](../../../docs/config.md) — `sql_trace_bind_values`
