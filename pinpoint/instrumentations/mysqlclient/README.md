# mysqlclient (MySQLdb)

Instruments [mysqlclient](https://github.com/PyMySQL/mysqlclient), the
C-extension fork of MySQL-python that powers Django's default MySQL backend.
Its import name is `MySQLdb` — yes, that capitalization; the package was renamed
but the import name stayed.

| | |
|---|---|
| Target module | `MySQLdb.cursors` |
| Hook point | `BaseCursor.execute` / `executemany` / `callproc` |
| Span role | Span event (child of the caller's transaction) |
| Service type | `MYSQL` |
| Opt-out alias | `mysqldb` (the module name, case-folded — **not** `mysqlclient`) |

`BaseCursor` defines the execute family; the public `Cursor`, `DictCursor`,
`SSCursor` and friends all inherit from it, so one wrap covers every subclass.
Connection host/port/database live on the `Connection` instance under
un-prefixed names, which the [DB-API core](../dbapi/README.md)'s default
extractor already understands.

## What gets traced

- One span event per statement, with the SQL template, endpoint (`host:port`),
  and database name.
- Bound values only when `sql_trace_bind_values` is on (off by default).
- `executemany` produces one event, not one per row.
- Statements driven by SQLAlchemy or the Django ORM's SQLAlchemy layer are left
  to the [SQLAlchemy integration](../sqlalchemy/README.md) when it is in play.

## Usage

No code changes. For Django, this is the integration that traces your ORM
queries:

```bash
pinpoint-run --app-name my-app --collector localhost -- \
    gunicorn -w 4 myproject.wsgi:application
```

```python
# Direct use
import MySQLdb

conn = MySQLdb.connect(host="db.internal", user="app", database="shop")
cur = conn.cursor()
cur.execute("SELECT name FROM users WHERE id = %s", (uid,))
# └─ span event: MYSQL  SELECT name FROM users WHERE id = %s
```

```python
# Through the Django ORM — same span event, no extra setup
User.objects.get(pk=uid)
```

## See also

- Unit tests: [`test_mysqlclient_instrumentation.py`](../../../tests/unit/instrumentations/test_mysqlclient_instrumentation.py)
- [DB-API core](../dbapi/README.md) · [Django integration](../django/README.md)
