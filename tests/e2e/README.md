# Integration test — pinpoint-python-agent

Direct Python port of pinpoint-cpp-agent's `test/it_test`. Two long-running
servers exercise every tracer API the agent exposes, and the Python load
generators hammer them for as long as you care to soak.

This is **not** a pytest suite. There is no in-memory span sink and no
auto-assertions: spans go to a real Pinpoint collector and the verification
loop is "stays up, stays leak-free, spans render correctly in the UI."

## Layout

| File                    | Role                                                          |
|-------------------------|---------------------------------------------------------------|
| `test_server.py`        | FastAPI workhorse on `:8090` — every scenario endpoint        |
| `grpc_server.py`        | grpcio `Hello` service on `:50051`                            |
| `fixed_rps_test.py`     | constant-arrival-rate load test with pass/fail thresholds     |
| `max_throughput_test.py` | unthrottled maximum-throughput test with latency percentiles |

## Prerequisites

1. A running Pinpoint collector reachable at `localhost:9991-9993` (see the
   [official quickstart](https://pinpoint-apm.github.io/pinpoint/quickstart.html)).
2. The dev shell loaded so `_native` is importable (drop the preset to keep
   the wiring you already have):
   ```
   source scripts/dev.sh debug
   ```
3. `.venv` set up per `docs/development.md` with `wrapt fastapi
   uvicorn[standard] grpcio` installed.

## Running

```bash
# terminal 1 — gRPC backend
.venv/bin/python tests/e2e/grpc_server.py

# terminal 2 — HTTP frontend (fans out to gRPC, emits SQL/async/etc.)
.venv/bin/python tests/e2e/test_server.py

# terminal 3 — driver
.venv/bin/python tests/e2e/max_throughput_test.py -m full -d 120 -c 15
```

Both load generators accept the same modes (simple/deep/wide/annotated/mixed/
stress/db-*/grpc-*/full); run either with `--help` for the full list.

### Fixed-RPS load test

Use the dedicated generator when request starts need to stay at a fixed rate
regardless of response latency:

```bash
.venv/bin/python tests/e2e/fixed_rps_test.py \
    --mode mixed --rps 50 --duration 60 --max-in-flight 100
```

Requests rotate deterministically through the selected mode's endpoints. The
intentional HTTP 500 from `/error` counts as success. Arrivals are dropped
instead of queued or emitted as a catch-up burst if the client falls behind or
reaches `--max-in-flight`.

By default the test passes with no unexpected response errors and at most 5%
dropped arrivals. Adjust those gates with `--max-error-rate` and
`--rps-tolerance`. Set `BASE_URL`, or `HOST` and `PORT`, to target a server
other than `http://localhost:8090`; run with `--help` for all options.

### Maximum-throughput load test

To measure throughput without an RPS limit, keep a fixed number of workers
continuously busy:

```bash
.venv/bin/python tests/e2e/max_throughput_test.py \
    --mode mixed --concurrency 100 --warmup 2 --duration 60
```

The warm-up runs at full load but is excluded from throughput and latency
results. The report includes average RPS, error rate, status counts, and
average/p50/p95/p99/max latency using a bounded-memory histogram. Use
`--min-rps` to enforce a performance-regression threshold and
`--max-error-rate` to permit an expected error percentage.

Per-request Uvicorn access logging is disabled by default so logging throughput
does not cap the measurement. Set `PINPOINT_E2E_ACCESS_LOG=true` when request
logs are useful for debugging.

The scenario handlers also pad themselves with a fixed sleep unit, but only
when `PINPOINT_E2E_SYNTHETIC_SLEEP_MS` is set — `=10` restores the 10 ms unit
that used to be unconditional. Leave it unset for any throughput or overhead
measurement: with it on, `time.sleep` sets the ceiling and the agent's cost
disappears into the noise. Turn it on for soak runs, where the padding is what
makes span timelines look realistic in the UI.

### Profiling under load

Attach py-spy to the running HTTP server, then drive a load generator from
another terminal. The load generator is a separate process, so its own CPU
time is not included:

```bash
.venv/bin/pip install py-spy
py-spy record --pid "$(lsof -ti :8090 | head -1)" -d 60 -o build/e2e-profiles/load.svg &
.venv/bin/python tests/e2e/fixed_rps_test.py --mode mixed --rps 50 --duration 60
```

All `py-spy record` flags apply (`--format speedscope`, `--rate N`,
`--native`); attaching may need elevated OS permissions (`sudo -E`). For a
cProfile-based comparison run, see `compare_overhead.py --profile`.

### Real MySQL mode (optional)

By default the `/db-*` endpoints synthesize SQL span events without touching
a database — matching the cpp `it_test`. Pass `--real-db` to
`run-test-server.sh` to route them through `pymysql` against a real MySQL
so the pymysql instrumentation produces the spans end-to-end:

```bash
# terminal 2 — HTTP frontend with a docker mysql:8 spun up for you
./tests/e2e/run-test-server.sh --real-db
```

The script reuses a container named `pinpoint-it-test-mysql` if it's
already running, otherwise launches `mysql:8` on `:3306` and waits for it
to accept connections. Override anything via env vars:

| Env var            | Default                       |
|--------------------|-------------------------------|
| `MYSQL_HOST`       | `127.0.0.1`                   |
| `MYSQL_PORT`       | `3306`                        |
| `MYSQL_USER`       | `root`                        |
| `MYSQL_PASSWORD`   | `root`                        |
| `MYSQL_DATABASE`   | `it_test`                     |
| `MYSQL_IMAGE`      | `mysql:8`                     |
| `MYSQL_CONTAINER`  | `pinpoint-it-test-mysql`      |

Combine `--real-db` with an unset `PINPOINT_E2E_SYNTHETIC_SLEEP_MS` for the
closest thing to a production-shaped load: `full` mode then drives real HTTP,
real gRPC and real MySQL round-trips with no artificial padding.

To bring an external MySQL instead of docker, skip `--real-db` and just
export `PINPOINT_IT_REAL_DB=true` plus the `MYSQL_*` vars before launching
the server directly.

Stop / clean up the container with `docker stop pinpoint-it-test-mysql`.

## Endpoints (mirror the cpp `it_test_server`)

| Endpoint                     | What it exercises                                       |
|------------------------------|---------------------------------------------------------|
| `GET /simple`                | single `trace()` span event                             |
| `GET /deep?depth=N`          | N nested span events, LIFO unwind                       |
| `GET /wide?width=N`          | N sequential span events                                |
| `GET /annotated`             | int / string / string-string annotations + API tag      |
| `GET /mixed`                 | SQL event + HTTP-client event + async span on a thread  |
| `GET /error`                 | event + span both `set_error`, response 500             |
| `GET /db-crud`               | INSERT/SELECT/UPDATE/DELETE span events (no real DB)    |
| `GET /db-batch?size=N`       | N batched INSERTs as span events                        |
| `GET /db-complex`            | JOIN / subquery / aggregation queries                   |
| `GET /grpc-unary`            | gRPC unary-unary call                                   |
| `GET /grpc-stream`           | gRPC unary→stream call                                  |
| `GET /grpc-bidi?count=N`     | gRPC bidirectional N messages                           |
| `GET /grpc-all`              | All four RPC patterns sequentially                      |
| `POST /agent/start`          | Runtime `pinpoint.init()`                               |
| `POST /agent/shutdown`       | Runtime `pinpoint.shutdown()`                           |
| `GET /stats`                 | Untraced JSON: uptime, total/active requests, RPS       |
