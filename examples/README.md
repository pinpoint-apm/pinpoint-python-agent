# Examples

Runnable demo applications for the Pinpoint Python Agent. Each subdirectory is
one integration: the demo source, and a `run.sh` that brings the whole stack up.

```
examples/<name>/
├── <name>_demo.py       the demo application
└── run.sh               starts it (plus any backing container), then smoke-tests it
```

Every demo's module docstring carries the exact `pinpoint-run` command, the
`curl` calls to hit it, and a **What this shows** section naming the wrapped
functions. This page is the map; the docstrings are the detail.

## Running an example

### `run.sh` — the full stack in one command

Every demo has one. It brings up the demo together with its backing container
(if it needs one), waits for readiness, sends a smoke-test round of requests, and
leaves everything running so you can inspect the traces in the Pinpoint UI:

```bash
./examples/flask/run.sh          # up + smoke-test (default)
./examples/flask/run.sh test     # re-send requests against an already-up stack
./examples/flask/run.sh down     # stop the apps and the container
```

These are development drivers: they need the repo's `.venv/`, `docker` for the
demos with a backing service (`tornado`, `wsgi`, `asgi` and `grpc` need none), and the
in-place native build (see the [Development Guide](../docs/development.md)), and
they `pip install` each demo's client libraries on first run. Logs and PID files
land in `/tmp/pinpoint-demo/`. Shared plumbing lives in
[`scripts/_demo-lib.sh`](../scripts/_demo-lib.sh), which `tests/e2e` also uses.

### By hand

Zero-code, via the launcher:

```bash
pinpoint-run --app-name demo-flask --agent-name demo-flask --collector localhost -- python examples/flask/flask_demo.py
```

The `pinpoint-run` console script only exists once the package is
`pip install`-ed. The in-place dev workflow ([Development
Guide](../docs/development.md)) deliberately skips that, so there call the same
entry point as a module:

```bash
python -m pinpoint.bootstrap.cli --app-name demo-flask --agent-name demo-flask --collector localhost -- python examples/flask/flask_demo.py
```

Or run the file directly — each `__main__` block calls `pinpoint.init()` +
`autoload()` itself, so `python examples/flask/flask_demo.py` also traces:

```bash
python examples/flask/flask_demo.py
```

Ports are overridable with `PORT=…` (Django uses `BIND=host:port`). Endpoints
backed by an external service return **503** when the client library isn't
installed or the server is unreachable — the demo still boots, so you can
run any example without standing up its datastore first.

## Web frameworks

| Example | Port | Also demonstrates | Backing service |
|---|---|---|---|
| [flask/](flask) — [demo](flask/flask_demo.py) + [upstream](flask/flask_upstream.py) | 5000 / 5001 | distributed tracing over `requests`; four MySQL clients (`pymysql`, `mysql-connector-python`, `mysqlclient`, `aiomysql`); the manual API — `@pinpoint.span`, `@pinpoint.spanevent`, `pinpoint.async_trace` for threads and asyncio tasks; call-stack capture on `/boom` | MySQL (`/db/*`) |
| [django/](django) | 8000 | single-file Django project; WSGI and ASGI handlers; `pymongo` wire commands as span events | MongoDB (`/db/mongo`, `/mongo/crud`) |
| [fastapi/](fastapi) | 8000 | route templates in url_stat; handler qualname span events; every supported PostgreSQL client (`asyncpg`, `psycopg3`, `psycopg2`, `aiopg`) | PostgreSQL (`/db/*`) |
| [starlette/](starlette) | 8000 | the FastAPI-less path; Pinpoint middleware placed outside user middleware; `cassandra-driver` sync + async queries | Cassandra (`/cassandra`) |
| [pyramid/](pyramid) | 6543 | route pattern lifting; view-callable span events; `pymemcache` commands | Memcached (`/cache`) |
| [falcon/](falcon) | 8000 WSGI, 8001 ASGI | WSGI *and* ASGI (`FALCON_ASGI=1`); URI-template url_stat; responder span events; `redis-py` commands and pipelines | Redis (`/redis`) |
| [tornado/](tornado) | 8888 | `RequestHandler._execute` root span; handler-method span events; `log_exception` → `set_error` | — |
| [aiohttp/](aiohttp) | 8080 | async server instrumentation; canonical resource patterns; `AsyncElasticsearch` index/refresh/search | Elasticsearch (`/elasticsearch`) |

All of them expose the same baseline routes: `/ping`, `/items/{id}`,
`/items/{id}/{action}` (url_stat by template, not by concrete path), and
`/boom` (raises, so the root span is marked failed).

## Framework-less apps

| Example | Port | What it shows |
|---|---|---|
| [wsgi/](wsgi) | 8000 | wrapping a bare WSGI app in `PinpointWSGIMiddleware` by hand — span ends only after the response iterable drains |
| [asgi/](asgi) | 8000 | the same for `PinpointASGIMiddleware` — `http` scopes only, status read from `http.response.start` |

Use these as the template for any framework the agent has no dedicated
integration for.

## Message queues

Each directory holds a producer (an HTTP app with `POST /save`), a standalone
consumer, and a `run.sh` that starts the broker plus both sides. One distributed
trace spans the producer's request span and the consumer's per-message span,
linked by the `Pinpoint-*` headers the producer stamps onto the record.

| Example | Producer port | Topic / queue | Broker |
|---|---|---|---|
| [kafka/](kafka) — `kafka-python`, Flask producer | 5005 | `demo.kafka-python` | Kafka |
| [aiokafka/](aiokafka) — asyncio Kafka, FastAPI producer | 5006 | `demo.aiokafka` | Kafka |
| [confluent_kafka/](confluent_kafka) — librdkafka, Flask producer | 5007 | `demo.confluent-kafka` | Kafka |
| [pika/](pika) — sync RabbitMQ, Flask producer | 5007 | exchange `demo.pika.exchange` → queue `demo.pika` | RabbitMQ |
| [aio_pika/](aio_pika) — asyncio RabbitMQ, FastAPI producer | 5008 | exchange `demo.aio_pika.exchange` → queue `demo.aio_pika` | RabbitMQ |

The pika and confluent_kafka producers share port 5007 — set `PORT=` on one of
them to run both at once. The two RabbitMQ demos also share one broker
container, so bringing up either tears down the other's.

Brokers are configured with `KAFKA_BOOTSTRAP` / `KAFKA_TOPIC` /
`KAFKA_GROUP` and `RABBITMQ_URL` / `RABBITMQ_EXCHANGE` / `RABBITMQ_QUEUE`.

```bash
curl -XPOST 'http://localhost:5005/save?msg=hello'
```

## RPC

[grpc/](grpc) — a `grpcio` server ([grpc_server.py](grpc/grpc_server.py), port
50051) and a Tornado front-end that calls it
([grpc_client.py](grpc/grpc_client.py), port 8888). The `grpcdemo.Hello`
service covers all four RPC patterns, one HTTP route each: `/unary`,
`/server-stream`, `/client-stream`, `/bidi`. `grpc.server(...)` and
`grpc.insecure_channel(...)` are auto-wrapped, so context crosses the call
with no user code; for streaming RPCs the span tracks the full generator
rather than the handler's return.

Regenerate the stubs after editing [testapp.proto](grpc/testapp.proto):

```bash
python -m grpc_tools.protoc -I examples/grpc --python_out=examples/grpc --grpc_python_out=examples/grpc examples/grpc/testapp.proto
```

## Backing services

`run.sh` starts these for you. For the by-hand path, these one-liners match
each demo's default connection settings:

```bash
docker run --rm -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=demo mysql:8
docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=demo postgres:16
docker run --rm -p 27017:27017 mongo:7
docker run --rm -p 6379:6379 redis:7
docker run --rm -p 11211:11211 memcached:1.6
docker run --rm -p 9042:9042 cassandra:5
docker run --rm -p 9200:9200 -e discovery.type=single-node -e xpack.security.enabled=false docker.elastic.co/elasticsearch/elasticsearch:8.15.0
docker run --rm -p 9092:9092 apache/kafka:3.8.0
docker run --rm -p 5672:5672 rabbitmq:3
```

Hosts and credentials are overridable per demo — see the docstring of the
file you're running (`MYSQL_HOST`, `PG_HOST`, `MONGO_HOST`, `REDIS_HOST`,
`MEMCACHED_HOST`, `CASSANDRA_HOST`, `ELASTICSEARCH_URL`, …).

## See also

- [Getting Started](../docs/getting_started.md) — install, launch, first trace
- [Configuration](../docs/config.md) — every option and env var
- [Instrumentations](../docs/auto_instrumentation.md) — the full supported-library list
- [Custom Instrumentation](../docs/custom_instrumentation.md) — the manual API `flask/flask_demo.py` demonstrates
