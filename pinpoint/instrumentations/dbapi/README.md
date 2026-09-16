# DB-API 2.0 (generic)

The shared DB-API ([PEP 249](https://peps.python.org/pep-0249/)) tracing core.
Every relational driver integration in this package delegates here, so SQL
annotations, endpoint extraction, and the de-duplication rules stay identical
across drivers.

| | |
|---|---|
| Autoloaded | **No** — there is no canonical DB-API import. Each driver integration calls into this module for its own cursor class. |
| Public API | `wrap_cursor_class`, `wrap_async_cursor_class`, `TargetInfo`, `connection_target`, `trace_cursor_call` |
| Delegating integrations | [pymysql](../pymysql/README.md), [mysqlclient](../mysqlclient/README.md), [mysql-connector](../mysql/README.md), [psycopg](../psycopg/README.md), [aiomysql](../aiomysql/README.md), [aiopg](../aiopg/README.md) |

The design mirrors OpenTelemetry's `opentelemetry-instrumentation-dbapi`: one
shared core enforcing consistent annotations, rather than the same trace logic
re-implemented per driver.

## What the core records

Per statement, one span event on the current transaction, carrying:

- the **SQL template** — always;
- **bound parameter values** — only when `sql_trace_bind_values` is enabled. Off
  by default: bind values routinely carry PII and secrets. Recorded blobs are
  length-capped either way (1 KB), so a megabyte BLOB never lands in trace data;
- the **endpoint** (`host:port`) and **database name**, read from the cursor's
  connection. Drivers with non-standard connection layouts pass their own
  `extract_target`.

Two de-duplication rules live here, both as ContextVars so they survive `await`:

- **`executemany`** — many drivers implement it as a Python loop calling
  `self.execute(query, row)` per row, and `execute` is wrapped too. A 50k-row
  call would otherwise allocate 50k+1 events on one span. The inner `execute`
  sees the flag and skips its own event: one event per statement.
- **SQLAlchemy** — the [SQLAlchemy integration](../sqlalchemy/README.md) owns
  the trace for cursors it drives (`enter_sqlalchemy_execute` /
  `exit_sqlalchemy_execute`), so the driver-level wrapper no-ops instead of
  producing a duplicated DB node per ORM query.

## Usage

You only touch this module when adding a driver that this package does not cover
yet:

```python
from pinpoint.instrumentations.dbapi import wrap_cursor_class
from pinpoint.service_type import SERVICE_TYPE_MYSQL

wrap_cursor_class(
    "mydriver.cursors", "Cursor",       # module path, class qualname
    service_type=SERVICE_TYPE_MYSQL,
)
```

`execute` / `executemany` / `callproc` are wrapped; subclasses pick the wrapping
up through normal method resolution. Use `wrap_async_cursor_class` for a driver
whose cursor methods are coroutines.

When the connection does not expose host/port/database in a shape the default
extractor understands, supply one:

```python
from pinpoint.instrumentations.dbapi import TargetInfo, wrap_cursor_class

def _extract(cursor):
    conn = cursor.connection
    return TargetInfo(conn.my_host, conn.my_port, conn.my_db)

wrap_cursor_class("mydriver.cursors", "Cursor",
                  service_type=SERVICE_TYPE_MYSQL, extract_target=_extract)
```

Then register the autoload hook — see
[Development Guide § 7](../../../docs/development.md#7-adding-a-bundled-auto-instrumentation).

## Disable

Not autoloaded, so there is no opt-out alias. Disable the driver integration
that calls in (`pymysql`, `psycopg2`, …).

## See also

- Unit tests: [`test_dbapi_instrumentation.py`](../../../tests/unit/instrumentations/test_dbapi_instrumentation.py)
- [Custom Instrumentation Guide §10 — Database instrumentation](../../../docs/custom_instrumentation.md) ·
  [Configuration Guide](../../../docs/config.md) — `sql_trace_bind_values`,
  `sql_max_bind_args_size`
