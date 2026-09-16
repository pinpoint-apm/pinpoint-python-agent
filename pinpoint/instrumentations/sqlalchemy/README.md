# SQLAlchemy

Instruments [SQLAlchemy](https://www.sqlalchemy.org/) through its own engine
event hooks rather than by patching methods. That plugs in at the right layer —
the DB-API driver — so the recorded SQL is the statement actually sent, with the
real connection endpoint.

| | |
|---|---|
| Target module | `sqlalchemy.engine` |
| Hook points | `Engine` events: `before_cursor_execute`, `after_cursor_execute`, `handle_error` |
| Span role | Span event (child of the caller's transaction) |
| Service type | mapped from the dialect (see below) |
| Opt-out alias | `sqlalchemy` |

Listeners are registered once on the `Engine` class, so every engine — created
before or after — is covered. Registration is guarded by `event.contains`: a
second registration would fire two `before` callbacks per query and leak a
never-ended event per statement.

## Service type by dialect

| Dialect | Service type |
|---|---|
| `mysql`, `mariadb` | `MYSQL` |
| `postgresql`, `postgres` | `POSTGRESQL` |
| `mssql` | `MSSQL` |
| `oracle` | `ORACLE` |
| anything else | `UNKNOWN_DB` |

## What gets traced

- One span event per statement — ORM queries and Core `text()` alike — carrying
  the SQL template, the endpoint, and the database name.
- Bound values only when `sql_trace_bind_values` is on (off by default), capped
  at 1 KB to match the DB-API wrapper.
- Errors are captured from `handle_error`, so a failing statement is recorded
  even when the exception is translated by SQLAlchemy.
- **The driver-level wrapper is suppressed for the duration of the statement.**
  SQLAlchemy owns the trace for cursors it drives, so the
  [pymysql](../pymysql/README.md) / [psycopg](../psycopg/README.md) /
  [mysqlclient](../mysqlclient/README.md) wrapper no-ops instead of producing a
  second, nested event. One ORM query = one DB node in the UI.

## Usage

No code changes and no `event.listen` of your own:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

engine = create_engine("mysql+pymysql://app@db.internal/shop")

with Session(engine) as session:
    session.scalars(select(User).where(User.id == uid)).first()
    # └─ span event: MYSQL  SELECT users.id, users.name FROM users WHERE users.id = %s
    #    (one event — the pymysql wrapper stays quiet)
```

Async engines (`create_async_engine`) are covered too: the suppression flag is a
ContextVar, so it survives `await`.

## Disable

```bash
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=sqlalchemy
```

Queries then fall back to the underlying driver's integration, which records
them one layer down.

## See also

- Unit tests: [`test_sqlalchemy_instrumentation.py`](../../../tests/unit/instrumentations/test_sqlalchemy_instrumentation.py)
- [DB-API core](../dbapi/README.md) ·
  [Configuration Guide](../../../docs/config.md) — `sql_trace_bind_values`
