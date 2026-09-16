# Integration tests — instrumentations × testcontainers, core × mock collector

Every instrumentation module here drives a **real client library** — patched
by the **real instrumentation wrappers** — against a **real backing service**
started with
[testcontainers](https://testcontainers-python.readthedocs.io/). Only the
native span *sink* is swapped: the cpp agent has no in-memory exporter
(finished spans drain straight into the gRPC sender), so
[`_recorder.py`](_recorder.py) implements the native Span/SpanEvent surface
the Python wrappers call and keeps everything inspectable. The full wrapper
pipeline — wrapt patching, annotation buffering, `end_span_*_with_data`
flush, header injection/extraction — runs unmodified. Full-stack soak
testing against a live collector lives in `tests/e2e/` instead.

`test_core.py` inverts the seam to cover the **core modules** (`agent`,
`tracer`, `context`, `config`, `propagator`, `_native`): nothing in the agent
is stubbed — `pinpoint.init()` builds the real C++ agent, which registers,
pings, and drains spans/metadata/stats over the real gRPC wire into
[`_collector.py`](_collector.py), an in-process `grpc.server` implementing
the Pinpoint collector services (stubs generated from the vendored
`pinpoint-grpc-idl` protos at test time). It needs **no Docker daemon** and
asserts on the actual protobufs the collector would receive: agent
registration + handshake, span/span-event/annotation encoding, API / string /
SQL metadata, trace-context injection & continuation, sampling drops, async
span chunks, agent stats, shutdown draining, re-init, `PINPOINT_PY_*` env
overrides, and fork re-init (prefork-server model).

| Module                  | Container(s)              | Coverage                                             |
|-------------------------|---------------------------|-----------------------------------------------------|
| `test_core.py`          | *(none — mock collector)* | core: agent lifecycle, span pipeline, metadata, propagation, sampling, stats, fork |
| `test_web_frameworks.py`| *(none — in-process apps)* | flask, pyramid, starlette, django (WSGI+ASGI), fastapi, tornado, aiohttp server, falcon |
| `test_redis.py`         | redis:7                   | redis sync/async commands, pipelines, errors         |
| `test_mysql.py`         | mysql:8                   | pymysql, mysql-connector (including runtime `USE`), aiomysql, sqlalchemy |
| `test_postgres.py`      | postgres:16               | psycopg (v3 sync/async), asyncpg query/batch/cursor/error paths |
| `test_mongodb.py`       | mongo:6                   | pymongo                                              |
| `test_memcached.py`     | memcached:1.6             | pymemcache                                           |
| `test_rabbitmq.py`      | rabbitmq:3.13             | pika, aio_pika (publish + consume + propagation)     |
| `test_kafka.py`         | cp-kafka:7.5              | kafka-python, confluent-kafka, aiokafka (± propagation) |
| `test_elasticsearch.py` | elasticsearch:8.15        | elasticsearch sync/async (+ urllib3/aiohttp suppression) |
| `test_http_clients.py`  | http-echo                 | requests, urllib3, httpx, aiohttp client (+ dedupe, wire headers) |
| `test_grpc.py`          | *(none — in-process server)* | grpc client + server interceptors (all four RPC shapes, stream errors, abort, propagation) |

## Prerequisites

- A running Docker daemon (no daemon → the container-backed modules skip;
  `test_core.py`, `test_web_frameworks.py` and `test_grpc.py` are marked
  `no_docker` and still run).
- The dev shell so `_native` is importable (drop the preset to keep the wiring
  you already have):
  ```bash
  source scripts/dev.sh debug
  ```
- Client libraries + test tooling (any missing driver skips its module):
  ```bash
  .venv/bin/pip install -r tests/integration/requirements.txt
  ```

## Running

```bash
# serial
.venv/bin/python -m pytest tests/integration -q

# parallel — one xdist worker per test module, so each worker starts only
# the containers its module needs and the services boot concurrently
.venv/bin/python -m pytest tests/integration -q -n auto --dist loadfile
```

or use the wrapper that does both steps:

```bash
tests/integration/run.sh            # parallel by default
tests/integration/run.sh -n 0      # serial
```

`--dist loadfile` matters: container fixtures are **module-scoped**, and one
backing service maps to one test module, so file-granular scheduling gives
each worker exactly one service to boot and never duplicates a container for
tests of the same service.

Container images can be pinned via `PINPOINT_IT_*_IMAGE` env vars (see
`conftest.py`).

## Writing a new module

1. One backing service per file — add a module-scoped container fixture to
   `conftest.py` if the service is new. A module that needs no container
   (like `test_core.py`) marks itself `pytestmark = pytest.mark.no_docker`
   to opt out of the Docker gate.
2. `pytest.importorskip("<client lib>")` at the top; call the
   instrumentation's `instrument()` from a module-scoped autouse fixture
   (idempotent — safe under repeated runs and shared workers).
3. Use `traced` for client-side wrappers (pushes a recording span into the
   context, yields the recorder); add `consumer_agent` for kafka/rabbitmq
   consumer paths (they open root spans via `get_agent()`).
4. Assert on `recorder.events` / `recorder.spans` — operation names,
   service types, endpoint/destination, annotations, SQL, errors.
