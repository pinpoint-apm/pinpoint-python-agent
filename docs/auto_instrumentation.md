# Auto-Instrumentation Catalog

The agent ships **35 first-party integrations**. Each one lives in its own
package directory under [`pinpoint/instrumentations/`](../pinpoint/instrumentations)
with a `README.md` next to its source: what it hooks, what ends up in a trace,
a usage snippet, and its opt-out alias (see [Disabling](#disabling)).

This page is the index. For tracing code no integration covers, see the
[Custom Instrumentation Guide](custom_instrumentation.md); to contribute a new
bundled integration, see
[Development Guide § 7](development.md#7-adding-a-bundled-auto-instrumentation).

---

## Enabling them

Nothing to register. Every integration is installed as a `wrapt` post-import
hook, so it fires the moment its target library is imported — and a library your
app never imports costs nothing.

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
# or, in code, before your libraries are used
import pinpoint
import pinpoint.autoload

pinpoint.init(application_name="my-app", collector_host="localhost")
pinpoint.autoload.autoload()
```

See the [Getting Started](getting_started.md) for the full bootstrap, and the
[Pre-fork Guide](prefork.md) for gunicorn/uWSGI.

---

## Reading the tables

| Column | Meaning |
|---|---|
| **Target module** | The import that *triggers* the integration — its key in [`autoload.py`](../pinpoint/autoload.py)'s registry, not the full list of modules it patches once it fires. Only these names are valid opt-out tokens. |
| **Records** | *Root span* = opens a transaction (server, consumer). *Span event* = a child on the transaction already in flight (client, DB, producer). |
| **Opt-out** | Token for `PINPOINT_PY_DISABLED_INSTRUMENTATIONS` — see [Disabling](#disabling) below. It is the **target module's** top-level name, which is not always the package directory name. |

---

## HTTP servers and web frameworks

Each opens a root span per request, honors upstream `Pinpoint-*` headers, and
records HTTP URL / status / endpoint plus URL statistics.

| Integration | Target module | Records | Opt-out |
|---|---|---|---|
| [Flask](../pinpoint/instrumentations/flask/README.md) | `flask.app` | root span + view event | `flask` |
| [Django](../pinpoint/instrumentations/django/README.md) | `django.core.handlers.wsgi` / `.asgi` | root span + view event | `django` |
| [FastAPI](../pinpoint/instrumentations/fastapi/README.md) | `fastapi.applications` | root span + handler event | `fastapi` |
| [Starlette](../pinpoint/instrumentations/starlette/README.md) | `starlette.applications` | root span + endpoint event | `starlette` |
| [Pyramid](../pinpoint/instrumentations/pyramid/README.md) | `pyramid.router` | root span + view event | `pyramid` |
| [Falcon](../pinpoint/instrumentations/falcon/README.md) | `falcon.app` (WSGI + ASGI) | root span + responder event | `falcon` |
| [Tornado](../pinpoint/instrumentations/tornado/README.md) | `tornado.web` | root span + handler event | `tornado` |
| [aiohttp (server)](../pinpoint/instrumentations/aiohttp/README.md) | `aiohttp.web_protocol` | root span + handler event | `aiohttp.web_protocol` |
| [WSGI (generic)](../pinpoint/instrumentations/wsgi/README.md) | — wrap by hand | root span | not autoloaded |
| [ASGI (generic)](../pinpoint/instrumentations/asgi/README.md) | — wrap by hand | root span | not autoloaded |

## HTTP and RPC clients

Each records one span event per outbound call and injects `Pinpoint-*` headers,
so the callee's trace stitches onto yours.

| Integration | Target module | Records | Opt-out |
|---|---|---|---|
| [requests](../pinpoint/instrumentations/requests/README.md) | `requests.sessions` | span event | `requests` |
| [httpx](../pinpoint/instrumentations/httpx/README.md) | `httpx._client` (sync + async) | span event | `httpx` |
| [urllib3](../pinpoint/instrumentations/urllib3/README.md) | `urllib3.connectionpool` | span event | `urllib3` |
| [aiohttp (client)](../pinpoint/instrumentations/aiohttp/README.md) | `aiohttp.client` | span event | `aiohttp.client` |
| [gRPC](../pinpoint/instrumentations/grpc/README.md) | `grpc` | root span (server) + span event (client) | `grpc` |

Nested layers do not double-count: `requests` and `elasticsearch` suppress the
`urllib3` wrapper for the duration of their exchange, so one outbound call
produces exactly one span event whichever combination is installed.

## Databases

Each records one span event per statement with the SQL template, endpoint, and
database name. **Bound parameter values are off by default** (`sql_trace_bind_values`
— they routinely carry PII); `executemany` yields one event, not one per row.

| Integration | Target module | Service type | Opt-out |
|---|---|---|---|
| [SQLAlchemy](../pinpoint/instrumentations/sqlalchemy/README.md) | `sqlalchemy.engine` | per dialect | `sqlalchemy` |
| [PyMySQL](../pinpoint/instrumentations/pymysql/README.md) | `pymysql.cursors` | `MYSQL` | `pymysql` |
| [mysqlclient](../pinpoint/instrumentations/mysqlclient/README.md) | `MySQLdb.cursors` | `MYSQL` | `mysqldb` |
| [mysql-connector](../pinpoint/instrumentations/mysql/README.md) | `mysql.connector` | `MYSQL` | `mysql` |
| [aiomysql](../pinpoint/instrumentations/aiomysql/README.md) | `aiomysql.cursors` | `MYSQL` | `aiomysql` |
| [psycopg2 / psycopg3](../pinpoint/instrumentations/psycopg/README.md) | `psycopg2`, `psycopg` | `POSTGRESQL` | `psycopg2`, `psycopg` |
| [aiopg](../pinpoint/instrumentations/aiopg/README.md) | `aiopg.connection` | `POSTGRESQL` | `aiopg` |
| [asyncpg](../pinpoint/instrumentations/asyncpg/README.md) | `asyncpg.connection` | `POSTGRESQL` | `asyncpg` |
| [Cassandra](../pinpoint/instrumentations/cassandra/README.md) | `cassandra.cluster` | `CASSANDRA` | `cassandra` |
| [DB-API core](../pinpoint/instrumentations/dbapi/README.md) | — shared layer | — | not autoloaded |

SQLAlchemy owns the trace for cursors it drives, so the driver-level wrapper
stays quiet underneath it: one ORM query is one DB node in the UI.

## Cache, document, and search stores

| Integration | Target module | Service type | Opt-out |
|---|---|---|---|
| [Redis](../pinpoint/instrumentations/redis/README.md) | `redis.client`, `redis.asyncio.client` | `REDIS` | `redis` |
| [MongoDB](../pinpoint/instrumentations/pymongo/README.md) | `pymongo.monitoring` | `MONGO` | `pymongo` |
| [Memcached](../pinpoint/instrumentations/pymemcache/README.md) | `pymemcache.client.base` | `MEMCACHED` | `pymemcache` |
| [Elasticsearch](../pinpoint/instrumentations/elasticsearch/README.md) | `elasticsearch` (patches `elastic_transport` too) | `ELASTICSEARCH` | `elasticsearch` |

## Message queues

Producers record a span event and inject trace headers into the message;
consumers open a **root span** per delivered message, so producer and consumer
appear as one distributed trace.

| Integration | Target module | Consumer span covers | Opt-out |
|---|---|---|---|
| [kafka-python](../pinpoint/instrumentations/kafka/README.md) | `kafka` | delivery only (pull API) | `kafka` |
| [aiokafka](../pinpoint/instrumentations/aiokafka/README.md) | `aiokafka.producer.producer` | delivery only (pull API) | `aiokafka` |
| [confluent-kafka](../pinpoint/instrumentations/confluent_kafka/README.md) | `confluent_kafka` | delivery only (pull API) | `confluent_kafka` |
| [pika](../pinpoint/instrumentations/pika/README.md) | `pika.channel` | **the handler** (callback API) | `pika` |
| [aio-pika](../pinpoint/instrumentations/aio_pika/README.md) | `aio_pika.exchange` | the handler (callback) / delivery only (iterator) | `aio_pika` |

Where the span covers delivery only, the client hands you a record and returns —
there is no callback to wrap. Trace the processing yourself:

```python
for message in consumer:
    with pinpoint.trace("handle_order"):
        handle(message)
```

## Support integrations

These record no spans of their own; they keep other traces correct or usable.

| Integration | Target module | What it does | Opt-out |
|---|---|---|---|
| [asgiref](../pinpoint/instrumentations/asgiref/README.md) | `asgiref.sync` | hands the current span off to `SyncToAsync` worker threads, so sync Django views/ORM under ASGI stay traced | `asgiref` |
| [logging](../pinpoint/instrumentations/logging_ext/README.md) | `logging` | stamps `PtxId` / `PspanId` on every log record, flags the span as logged | `logging` |

---

## Disabling

```bash
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=flask,redis
```

Comma-separated, case-insensitive, `-` folds to `_`. Two granularities:

- **An alias** — the target module's top-level package (`django`, `redis`,
  `aiohttp`) — disables *every* hook it owns. `aiohttp` turns off client and
  server together; `django` turns off both transports.
- **A full module name** disables exactly one hook:
  `aiohttp.client`, `redis.asyncio.client`, `django.core.handlers.asgi`.

Mind the aliases that differ from the directory name: `mysqldb` (not
`mysqlclient`), `logging` (not `logging_ext`), and `psycopg2` / `psycopg` as two
separate tokens.

Not-autoloaded packages ([WSGI](../pinpoint/instrumentations/wsgi/README.md),
[ASGI](../pinpoint/instrumentations/asgi/README.md),
[DB-API](../pinpoint/instrumentations/dbapi/README.md)) have no alias — they are
shared layers you invoke yourself, or that other integrations build on.

---

## See also

- [Getting Started](getting_started.md) — install, configure, verify
- [`examples/`](../examples) — a runnable demo per integration
- [Custom Instrumentation Guide](custom_instrumentation.md) — tracing what no
  integration covers
- [Configuration Guide](config.md) — sampling, URL statistics, SQL bind values,
  header recording
- [API Contracts](api_contracts.md) — the rules span objects enforce
- [Troubleshooting](troubleshooting.md)
