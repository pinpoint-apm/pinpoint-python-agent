# mysql-connector-python

Instruments Oracle's official
[mysql-connector-python](https://dev.mysql.com/doc/connector-python/en/) driver,
both the pure-Python and the C-extension implementation.

| | |
|---|---|
| Target modules | `mysql.connector.cursor`, `mysql.connector.cursor_cext` |
| Hook points | `MySQLCursor` / `CMySQLCursor` execute family, plus `cmd_query` and `cmd_init_db` |
| Span role | Span event (child of the caller's transaction) |
| Service type | `MYSQL` |
| Opt-out alias | `mysql` |

The connector ships several cursor classes — `MySQLCursor`,
`MySQLCursorBuffered`, `MySQLCursorPrepared`, `MySQLCursorDict`, … — all
subclasses of `MySQLCursor`. Instrumenting the base once covers them through
normal method resolution. The C-extension hierarchy (`CMySQLCursor` in
`cursor_cext`) needs its own pass because it overrides `execute` directly.

`cmd_query` and `cmd_init_db` are additionally wrapped so `conn.database = <db>`
— which the pure-Python driver implements as a bare `USE` — is reflected in the
recorded database name.

## What gets traced

- One span event per statement, with the SQL template, endpoint (`host:port`),
  and database name. The target extractor overrides the DB-API default to read
  the connector's underscore-prefixed `_host` / `_port` / `_database`.
- Bound values only when `sql_trace_bind_values` is on (off by default).
- `executemany` produces one event, not one per row.

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
import mysql.connector

conn = mysql.connector.connect(host="db.internal", user="app", database="shop")
cur = conn.cursor()
cur.execute("SELECT name FROM users WHERE id = %s", (uid,))
# └─ span event: MYSQL  SELECT name FROM users WHERE id = %s
#    endpoint db.internal:3306, database "shop"
```

Both `use_pure=True` and the default C extension are traced.

## See also

- Unit tests: [`test_mysql_instrumentation.py`](../../../tests/unit/instrumentations/test_mysql_instrumentation.py)
- [DB-API core](../dbapi/README.md) ·
  [Configuration Guide](../../../docs/config.md) — `sql_trace_bind_values`
