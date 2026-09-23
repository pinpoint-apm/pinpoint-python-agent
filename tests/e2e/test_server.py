# pinpoint-python-agent
# Copyright (c) 2026-present NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""FastAPI workhorse for the python integration test.

Direct port of ``test/it_test/it_test_server.cpp`` from pinpoint-cpp-agent.
One endpoint per tracing scenario the agent exposes — drive each with
``fixed_rps_test.py`` / ``max_throughput_test.py`` to soak the full pipeline
against a real Pinpoint collector.

The python equivalents of the cpp tracer APIs:

* ``pinpoint::Agent::NewSpan``         → handled by the FastAPI middleware
                                          (auto-instrumented); the handler
                                          gets at it via ``current_span()``.
* ``Span::NewSpanEvent`` /
  ``ScopedSpanEvent``                  → ``span.new_span_event(op, service_type)``.
* ``Span::NewAsyncSpan``               → ``span.new_async_span(op)`` (must
                                          be created inside an active span
                                          event, exactly like cpp).
* ``Span/SpanEvent::SetAnnotation``    → ``event.annotate_int/string/
                                          string_string(key, val)``.
* ``SetSqlQuery``                      → ``event.set_sql_query(sql, args)``.
* ``RecordHeader`` /
  ``TraceHttpServerRequest`` etc.      → done by the middleware via
                                          ``pinpoint.http_helper``.

By default no MySQL or downstream HTTP server is actually contacted — the
SQL- and HTTP-client-shaped spans just exercise the recorder paths (matches
the cpp it_test, which is also "no real DB / no real HTTP backend").

Set ``PINPOINT_IT_REAL_DB=true`` to route the ``/db-*`` endpoints through
``pymysql`` against a real MySQL instance instead of synthesizing the span
events. The pymysql instrumentation then produces the SQL spans
end-to-end. ``run-test-server.sh --real-db`` boots a throwaway MySQL
container and exports the matching ``MYSQL_*`` env vars.

The scenario handlers pad themselves with a fixed sleep unit only when
``PINPOINT_E2E_SYNTHETIC_SLEEP_MS`` is set (``=10`` gives a 10 ms unit). Leave
it unset for throughput measurement — otherwise the sleeps, not the agent, set
the ceiling.

Run:
    .venv/bin/python tests/e2e/test_server.py
"""

from __future__ import annotations

import contextlib
import os
import random
import sys
import threading
import time
from typing import Any
from collections.abc import Sequence

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

import grpc

# Reuse the demo proto stubs.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(_REPO_ROOT, "examples", "grpc"))

import testapp_pb2  # noqa: E402
import testapp_pb2_grpc  # noqa: E402

import pinpoint  # noqa: E402
from pinpoint.annotation import (  # noqa: E402
    ANNOTATION_API,
    ANNOTATION_HTTP_URL,
)
from pinpoint.autoload import autoload  # noqa: E402
from pinpoint.service_type import (  # noqa: E402
    SERVICE_TYPE_MYSQL,
    SERVICE_TYPE_PYTHON_HTTP_CLIENT,
    SERVICE_TYPE_PYTHON_METHOD,
)


# ---------------------------------------------------------------------------
# Request counters (drives /stats — mirrors cpp ``RequestTracker``).
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_total_requests = 0
_active_requests = 0
_start_time = time.monotonic()

# Opt-in padding latency. 0 (the default) means the handlers do only their real
# work, so a throughput run measures the agent instead of `time.sleep`. Set
# PINPOINT_E2E_SYNTHETIC_SLEEP_MS=10 for a 10 ms unit.
_SYNTHETIC_SLEEP_SECONDS = (
    float(os.environ.get("PINPOINT_E2E_SYNTHETIC_SLEEP_MS", "0") or 0) / 1000.0
)


@contextlib.contextmanager
def _track_request():
    """RAII-style counters mirroring cpp ``RequestTracker``."""
    global _total_requests, _active_requests
    with _lock:
        _total_requests += 1
        _active_requests += 1
    try:
        yield
    finally:
        with _lock:
            _active_requests -= 1


def _synthetic_sleep() -> None:
    """Optional padding latency, applied identically agent-on and agent-off.

    A no-op unless PINPOINT_E2E_SYNTHETIC_SLEEP_MS is set. One guard here
    rather than at the seven call sites, so no caller can miss it.
    """
    if _SYNTHETIC_SLEEP_SECONDS:
        time.sleep(_SYNTHETIC_SLEEP_SECONDS)


# ---------------------------------------------------------------------------
# Shared gRPC stub. The client interceptor is attached when the python agent's
# grpc instrumentation wraps ``grpc.insecure_channel`` — and that wrap is
# installed by ``autoload()``. Module-import-time construction would run
# *before* autoload, producing a vanilla, un-intercepted channel that bypasses
# Pinpoint forever. ``_ensure_grpc_stub`` defers construction until after the
# agent (and therefore the wrap) is in place.
# ---------------------------------------------------------------------------

_GRPC_TARGET = os.environ.get("GRPC_TARGET", "localhost:50051")
_grpc_channel: grpc.Channel | None = None
_grpc_stub: testapp_pb2_grpc.HelloStub | None = None


def _ensure_grpc_stub() -> testapp_pb2_grpc.HelloStub:
    global _grpc_channel, _grpc_stub
    if _grpc_stub is None:
        _grpc_channel = grpc.insecure_channel(_GRPC_TARGET)
        _grpc_stub = testapp_pb2_grpc.HelloStub(_grpc_channel)
    return _grpc_stub


# ---------------------------------------------------------------------------
# Real-DB mode
# ---------------------------------------------------------------------------
#
# When PINPOINT_IT_REAL_DB is truthy, the /db-* endpoints execute their SQL
# against a real MySQL via pymysql so the pymysql instrumentation produces
# the span events. Otherwise the legacy "synthesize a SQL span event without
# touching a DB" path runs — same shape as the cpp it_test.

_REAL_DB = os.environ.get("PINPOINT_IT_REAL_DB", "").strip().lower() in (
    "1", "true", "yes", "on",
)
_ACCESS_LOG = os.environ.get("PINPOINT_E2E_ACCESS_LOG", "").strip().lower() in (
    "1", "true", "yes", "on",
)

_DB_CONFIG: dict[str, Any] = {
    "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("MYSQL_PORT", "3306")),
    "user": os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", "root"),
    "database": os.environ.get("MYSQL_DATABASE", "it_test"),
    "autocommit": True,
}


def _db_connect():
    import pymysql  # type: ignore[import-not-found]  # pip install pymysql
    return pymysql.connect(**_DB_CONFIG)


def _ensure_db_schema() -> None:
    """Create the database + tables the db-* endpoints touch.

    Called once at server startup when PINPOINT_IT_REAL_DB is on. The
    initial connect targets the server without the database so we can
    ``CREATE DATABASE IF NOT EXISTS`` — afterwards we reconnect with the
    database selected and run the table DDL.
    """
    import pymysql  # type: ignore[import-not-found]  # pip install pymysql

    bootstrap_cfg = {k: v for k, v in _DB_CONFIG.items() if k != "database"}
    db_name = _DB_CONFIG["database"]
    conn = pymysql.connect(**bootstrap_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                "DEFAULT CHARACTER SET utf8mb4"
            )
    finally:
        conn.close()

    conn = pymysql.connect(**_DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS it_test_users ("
                "  id INT AUTO_INCREMENT PRIMARY KEY,"
                "  name VARCHAR(64) NOT NULL,"
                "  email VARCHAR(128) NOT NULL,"
                "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                ") ENGINE=InnoDB"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS it_test_batch ("
                "  id INT AUTO_INCREMENT PRIMARY KEY,"
                "  idx INT NOT NULL,"
                "  payload VARCHAR(256) NOT NULL"
                ") ENGINE=InnoDB"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS it_test_orders ("
                "  id INT AUTO_INCREMENT PRIMARY KEY,"
                "  user_id INT NOT NULL,"
                "  total DECIMAL(10,2) NOT NULL,"
                "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                "  INDEX (user_id)"
                ") ENGINE=InnoDB"
            )
            _seed_db_fixture(cur)
    finally:
        conn.close()


# /db-complex joins users against orders and aggregates them. Against the empty
# tables the DDL above leaves behind, all three of its queries match zero rows
# and the endpoint measures query planning, not query execution — so give it a
# small fixed corpus to actually work on. Sized to stay in the buffer pool: the
# point is a real result set, not a large one.
_SEED_USERS = 200
_SEED_ORDERS_PER_USER = 5


def _seed_db_fixture(cur) -> None:
    """Populate users/orders once. No-op when the corpus is already there."""
    cur.execute("SELECT COUNT(*) FROM it_test_orders")
    if (cur.fetchone() or (0,))[0]:
        return

    # IGNORE, because this insert names its ids: a run that seeded users and
    # then failed before the orders insert leaves the orders guard above open
    # while ids 1.._SEED_USERS already exist, and the retry would die on
    # duplicate-key instead of finishing the corpus. The orders insert needs no
    # such tolerance — its ids are generated, and the guard is what stops it
    # from doubling the corpus.
    cur.executemany(
        "INSERT IGNORE INTO it_test_users (id, name, email) VALUES (%s, %s, %s)",
        [(i, f"seed-user-{i}", f"seed{i}@example.com")
         for i in range(1, _SEED_USERS + 1)],
    )
    # Spread created_at over 30 days so the GROUP BY DATE(...) aggregation
    # returns a real multi-row result instead of one bucket.
    cur.executemany(
        "INSERT INTO it_test_orders (user_id, total, created_at) "
        "VALUES (%s, %s, DATE_SUB(NOW(), INTERVAL %s DAY))",
        [(u, 50.0 + (u * 7 + k * 13) % 400, (u + k) % 30)
         for u in range(1, _SEED_USERS + 1)
         for k in range(_SEED_ORDERS_PER_USER)],
    )


def _real_db_crud() -> None:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO it_test_users (name, email) VALUES (%s, %s)",
                ("Alice", "alice@example.com"),
            )
            user_id = cur.lastrowid
            cur.execute(
                "SELECT id, name, email FROM it_test_users WHERE id = %s",
                (user_id,),
            )
            cur.fetchall()
            cur.execute(
                "UPDATE it_test_users SET email = %s WHERE id = %s",
                ("alice@new.com", user_id),
            )
            cur.execute("DELETE FROM it_test_users WHERE id = %s", (user_id,))
    finally:
        conn.close()


def _real_db_batch(size: int) -> None:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO it_test_batch (idx, payload) VALUES (%s, %s)",
                [(i, f"payload-{i}") for i in range(size)],
            )
            cur.execute("SELECT COUNT(*) FROM it_test_batch")
            cur.fetchone()
    finally:
        conn.close()


def _real_db_complex() -> None:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT u.id, u.name, COUNT(o.id) AS order_count "
                "FROM it_test_users u "
                "LEFT JOIN it_test_orders o ON o.user_id = u.id "
                "GROUP BY u.id, u.name "
                "HAVING COUNT(o.id) > %s "
                "ORDER BY order_count DESC",
                (0,),
            )
            cur.fetchall()
            cur.execute(
                "SELECT * FROM it_test_users WHERE id IN ("
                "SELECT DISTINCT user_id FROM it_test_orders WHERE total > %s"
                ")",
                (100,),
            )
            cur.fetchall()
            cur.execute(
                "SELECT DATE(created_at) AS day, SUM(total) AS revenue "
                "FROM it_test_orders WHERE created_at >= %s "
                "GROUP BY DATE(created_at) ORDER BY day DESC",
                ("2024-01-01",),
            )
            cur.fetchall()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Scenario helpers
# ---------------------------------------------------------------------------


def _emit_sql_event(query: str, params: Sequence[Any] = ()) -> None:
    """Emit a SERVICE_TYPE_MYSQL span event with set_sql_query.

    Mirrors cpp ``trace_sql`` — no actual DB connection, ~30% error injection
    so the soak driver produces realistic error/timing distributions.
    """
    span = pinpoint.current_span()
    if span is None:
        _synthetic_sleep()
        return
    with span.new_span_event("mysql.execute", service_type=SERVICE_TYPE_MYSQL) as ev:
        if ev is None:
            _synthetic_sleep()
            return
        ev.set_destination("it-test-mysql")
        ev.set_end_point("localhost:3306")
        ev.set_sql_query(query, params)
        _synthetic_sleep()
        if random.random() < 0.3:
            ev.set_error("MysqlError", "simulated random failure")


def _emit_http_client_event(host: str, url: str) -> None:
    """Synthesize an outbound HTTP-client event without making the real call.

    Matches the cpp ``/mixed`` handler, which records an HTTP-client-shaped
    event for tracer-API coverage; no downstream HTTP backend is contacted.
    """
    span = pinpoint.current_span()
    if span is None:
        return
    with span.new_span_event("http.client", service_type=SERVICE_TYPE_PYTHON_HTTP_CLIENT) as ev:
        if ev is None:
            return
        ev.set_destination(host)
        ev.set_end_point(host)
        ev.annotate_string(ANNOTATION_HTTP_URL, url)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="pinpoint-python it_test_server")


# /stats is excluded from tracing via PINPOINT_PY_HTTP_SERVER_EXCLUDE_URL so the
# counters report cleanly — see __main__ below.


@app.middleware("http")
async def _counter_middleware(request: Request, call_next):
    """Bump request counters around the FastAPI request lifecycle."""
    if request.url.path == "/stats":
        return await call_next(request)
    with _track_request():
        return await call_next(request)


# --- /simple ---------------------------------------------------------------
@app.get("/simple")
def simple() -> dict[str, Any]:
    span = pinpoint.current_span()
    if span is not None:
        with span.new_span_event("simple.work"):
            pass
    return {"endpoint": "simple"}


# --- /deep -----------------------------------------------------------------
@app.get("/deep")
def deep(depth: int = Query(10, ge=1, le=200)) -> dict[str, Any]:
    """N nested span events; unwind LIFO via ExitStack."""
    span = pinpoint.current_span()
    if span is None:
        return {"endpoint": "deep", "depth": depth}
    with contextlib.ExitStack() as stack:
        for i in range(depth):
            stack.enter_context(span.new_span_event(f"deep.level_{i}"))
    return {"endpoint": "deep", "depth": depth}


# --- /wide -----------------------------------------------------------------
@app.get("/wide")
def wide(width: int = Query(20, ge=1, le=2000)) -> dict[str, Any]:
    """N sequential start/end pairs."""
    span = pinpoint.current_span()
    if span is not None:
        for i in range(width):
            with span.new_span_event(f"wide.step_{i}"):
                pass
    return {"endpoint": "wide", "width": width}


# --- /annotated ------------------------------------------------------------
@app.get("/annotated")
def annotated() -> dict[str, Any]:
    """Exercise int / string / string-string annotations + ANNOTATION_API."""
    span = pinpoint.current_span()
    if span is None:
        return {"endpoint": "annotated"}

    with span.new_span_event("annotated.with_service_type",
                    service_type=SERVICE_TYPE_PYTHON_METHOD) as ev:
        if ev is not None:
            ev.set_destination("annotation-target")
            ev.set_end_point("localhost:0")
            ev.annotate_string(ANNOTATION_API, "annotated.with_service_type")

    with span.new_span_event("annotated.with_ints") as ev:
        if ev is not None:
            ev.annotate_int(100, 42)
            ev.annotate_int(101, 12345)

    with span.new_span_event("annotated.with_strings") as ev:
        if ev is not None:
            ev.annotate_string(102, "hello-world")
            ev.annotate_string_string(103, "key", "value")

    return {"endpoint": "annotated"}


# --- /mixed ----------------------------------------------------------------
@app.get("/mixed")
def mixed() -> dict[str, Any]:
    """SQL + HTTP-client + async span hand-off — combo workout for tracer."""
    if _REAL_DB:
        conn = _db_connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM it_test_users WHERE id = %s", (42,))
                cur.fetchall()
        finally:
            conn.close()
    else:
        _emit_sql_event("SELECT * FROM it_test_users WHERE id = ?", [42])
    _emit_http_client_event("downstream.local", "http://downstream.local/items/42")

    # Hand off an async span to a detached thread. ``pinpoint.async_trace``
    # wraps ``span.new_async_span(...)`` so the link to the parent event is
    # set up correctly; the recipient thread enters ``with async_span:`` so
    # the cleanup runs in the worker's context.
    done = threading.Event()

    with pinpoint.async_trace("mixed.background") as async_span:
        def _worker(handoff: pinpoint.Span | None) -> None:
            try:
                if handoff is not None:
                    with handoff:
                        with handoff.new_span_event("mixed.background.step"):
                            _synthetic_sleep()
                else:
                    _synthetic_sleep()
            finally:
                done.set()

        threading.Thread(target=_worker, args=(async_span,), daemon=True).start()

    # Wait briefly so the soak driver gets a chance to see the async span
    # finish in the same request — mirrors cpp's "detach + small join".
    done.wait(timeout=0.5)

    # A few more sequential fixed-latency units after the hand-off.
    span = pinpoint.current_span()
    for i in range(3):
        if span is not None:
            with span.new_span_event(f"mixed.tail_{i}"):
                _synthetic_sleep()
        else:
            _synthetic_sleep()

    return {"endpoint": "mixed"}


# --- /error ----------------------------------------------------------------
class _SyntheticBoom(Exception):
    """Marker exception so the agent records a proper Python exception name."""


@app.get("/error")
def error() -> JSONResponse:
    """Mark both the event and the root span as errored, then 500."""
    span = pinpoint.current_span()
    if span is not None:
        with contextlib.suppress(_SyntheticBoom):
            with span.new_span_event("error.work") as ev:
                exc = _SyntheticBoom("synthetic failure")
                if ev is not None:
                    ev.set_error(exc)
                raise exc
        span.set_error("HTTPInternalError", "synthetic failure")

    # 5xx → status code falls into Http.Server.StatusCodeErrors → root span
    # is also marked errored on the cpp side via the response middleware.
    return JSONResponse({"endpoint": "error"}, status_code=500)


# --- SQL endpoints ---------------------------------------------------------
@app.get("/db-crud")
def db_crud() -> dict[str, Any]:
    if _REAL_DB:
        _real_db_crud()
        return {"endpoint": "db-crud", "backend": "real"}
    _emit_sql_event(
        "INSERT INTO it_test_users (name, email) VALUES (?, ?)",
        ["Alice", "alice@example.com"],
    )
    _emit_sql_event("SELECT id, name, email FROM it_test_users WHERE id = ?", [1])
    _emit_sql_event("UPDATE it_test_users SET email = ? WHERE id = ?",
                    ["alice@new.com", 1])
    _emit_sql_event("DELETE FROM it_test_users WHERE id = ?", [1])
    return {"endpoint": "db-crud"}


@app.get("/db-batch")
def db_batch(size: int = Query(10, ge=1, le=1000)) -> dict[str, Any]:
    if _REAL_DB:
        _real_db_batch(size)
        return {"endpoint": "db-batch", "size": size, "backend": "real"}
    for i in range(size):
        _emit_sql_event(
            "INSERT INTO it_test_batch (idx, payload) VALUES (?, ?)",
            [i, f"payload-{i}"],
        )
    _emit_sql_event("SELECT COUNT(*) FROM it_test_batch")
    return {"endpoint": "db-batch", "size": size}


@app.get("/db-complex")
def db_complex() -> dict[str, Any]:
    if _REAL_DB:
        _real_db_complex()
        return {"endpoint": "db-complex", "backend": "real"}
    _emit_sql_event(
        "SELECT u.id, u.name, COUNT(o.id) AS order_count "
        "FROM it_test_users u "
        "LEFT JOIN it_test_orders o ON o.user_id = u.id "
        "GROUP BY u.id, u.name "
        "HAVING COUNT(o.id) > ? "
        "ORDER BY order_count DESC",
        [0],
    )
    _emit_sql_event(
        "SELECT * FROM it_test_users WHERE id IN ("
        "SELECT DISTINCT user_id FROM it_test_orders WHERE total > ?"
        ")",
        [100],
    )
    _emit_sql_event(
        "SELECT DATE(created_at) AS day, SUM(total) AS revenue "
        "FROM it_test_orders WHERE created_at >= ? "
        "GROUP BY DATE(created_at) ORDER BY day DESC",
        ["2024-01-01"],
    )
    return {"endpoint": "db-complex"}


# --- gRPC endpoints --------------------------------------------------------
def _grpc_unary() -> str:
    return _ensure_grpc_stub().UnaryCallUnaryReturn(
        testapp_pb2.Greeting(msg="hello from it_test_server")
    ).msg


def _grpc_stream() -> list[str]:
    return [
        resp.msg
        for resp in _ensure_grpc_stub().UnaryCallStreamReturn(
            testapp_pb2.Greeting(msg="stream from it_test_server")
        )
    ]


def _grpc_bidi(count: int) -> list[str]:
    def _requests():
        for i in range(count):
            yield testapp_pb2.Greeting(msg=f"bidi-{i}")

    return [
        resp.msg for resp in _ensure_grpc_stub().StreamCallStreamReturn(_requests())
    ]


def _grpc_client_stream(count: int) -> str:
    def _requests():
        for i in range(count):
            yield testapp_pb2.Greeting(msg=f"client-stream-{i}")

    return _ensure_grpc_stub().StreamCallUnaryReturn(_requests()).msg


@app.get("/grpc-unary")
def grpc_unary() -> dict[str, Any]:
    return {"endpoint": "grpc-unary", "response": _grpc_unary()}


@app.get("/grpc-stream")
def grpc_stream() -> dict[str, Any]:
    return {"endpoint": "grpc-stream", "responses": _grpc_stream()}


@app.get("/grpc-bidi")
def grpc_bidi(count: int = Query(3, ge=1, le=200)) -> dict[str, Any]:
    return {"endpoint": "grpc-bidi", "responses": _grpc_bidi(count)}


@app.get("/grpc-all")
def grpc_all() -> dict[str, Any]:
    """Hit all four RPC patterns from a single request."""
    return {
        "endpoint": "grpc-all",
        "unary": _grpc_unary(),
        "server_stream": _grpc_stream(),
        "client_stream": _grpc_client_stream(3),
        "bidi": _grpc_bidi(3),
    }


# --- Agent lifecycle -------------------------------------------------------
@app.post("/agent/start")
def agent_start() -> dict[str, Any]:
    """Re-initialize the agent if it was shut down. ``init()`` is idempotent
    when an agent already exists, matching C++ StartAgent semantics."""
    pinpoint.init(
        application_name=os.environ.get("PINPOINT_PY_APPLICATION_NAME", "py-e2e-test-server"),
        agent_name=os.environ.get("PINPOINT_PY_AGENT_NAME", "py-e2e-test-agent"),
        server_info=os.environ.get("PINPOINT_PY_SERVER_INFO", "FastAPI IT Test Server"),
    )
    return {"endpoint": "agent/start", "enabled": bool(
        pinpoint.get_agent() and pinpoint.get_agent().enabled
    )}


@app.post("/agent/shutdown")
def agent_shutdown() -> dict[str, Any]:
    pinpoint.shutdown()
    return {"endpoint": "agent/shutdown"}


# --- /stats (untraced) -----------------------------------------------------
@app.get("/stats")
def stats() -> dict[str, Any]:
    with _lock:
        total = _total_requests
        active = _active_requests
    uptime = time.monotonic() - _start_time
    rps = total / uptime if uptime > 0 else 0.0
    agent = pinpoint.get_agent()
    return {
        "uptime_seconds": round(uptime, 3),
        "total_requests": total,
        "active_requests": active,
        "rps": round(rps, 3),
        # Registration finishes on a background thread and the
        # instrumentations gate span creation on it, so a measurement started
        # before this flips would sample the untraced path and understate the
        # agent's cost (compare_overhead.py polls it).
        "agent_enabled": bool(agent and agent.enabled),
    }


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


def _init_agent_from_env() -> None:
    """Initialize the agent. PINPOINT_PY_* env vars steer config; the defaults
    below give a developer who just runs ``python test_server.py`` the
    full-feature setup (mirroring pinpoint-cpp-agent's it_test config) without
    exporting a dozen env vars.

    Set ``PINPOINT_DISABLE=true`` to skip init and autoload entirely — used by
    ``compare_overhead.py`` to measure the same server with the agent off
    vs. on. Note: this is stronger than ``PINPOINT_PY_ENABLE=false`` because the
    latter still runs init() (and the instrumentation wraps), it just makes
    ``agent.enabled`` return False. ``PINPOINT_DISABLE`` bypasses both.
    """

    if os.environ.get("PINPOINT_DISABLE", "").strip().lower() in (
        "1", "true", "yes", "on",
    ):
        print("pinpoint instrumentation disabled (PINPOINT_DISABLE set)", flush=True)
        return

    def _default(name: str, value: str) -> None:
        os.environ.setdefault(name, value)

    _default("PINPOINT_PY_APPLICATION_NAME", "py-e2e-test-server")
    _default("PINPOINT_PY_AGENT_NAME", "py-e2e-test-agent")
    _default("PINPOINT_PY_LOG_LEVEL", "info")
    _default("PINPOINT_PY_SAMPLING_TYPE", "COUNTER")
    _default("PINPOINT_PY_SAMPLING_COUNTER_RATE", "100")
    # 0 == unlimited in the python/cpp agent. The cpp it_test config uses
    # -1 for the same meaning, but the runtime rejects -1 with a warning.
    _default("PINPOINT_PY_SAMPLING_NEW_THROUGHPUT", "0")
    _default("PINPOINT_PY_SAMPLING_CONTINUE_THROUGHPUT", "0")
    _default("PINPOINT_PY_SPAN_QUEUE_SIZE", "4096")
    _default("PINPOINT_PY_SPAN_EVENT_CHUNK_SIZE", "128")
    _default("PINPOINT_PY_ENABLE_CALLSTACK_TRACE", "true")
    _default("PINPOINT_PY_HTTP_COLLECT_URL_STAT", "true")
    _default("PINPOINT_PY_HTTP_URL_STAT_ENABLE_TRIM_PATH", "true")
    _default("PINPOINT_PY_HTTP_URL_STAT_TRIM_PATH_DEPTH", "4")
    _default("PINPOINT_PY_HTTP_URL_STAT_METHOD_PREFIX", "true")
    _default("PINPOINT_PY_HTTP_SERVER_EXCLUDE_URL", "/stats")
    _default("PINPOINT_PY_HTTP_SERVER_RECORD_REQUEST_HEADER",
             "User-Agent,Content-Type,Accept,Host,X-Request-ID,X-Forwarded-For")
    _default("PINPOINT_PY_HTTP_SERVER_RECORD_REQUEST_COOKIE", "session_id,token")
    _default("PINPOINT_PY_HTTP_SERVER_RECORD_RESPONSE_HEADER",
             "Content-Type,X-Response-Time,X-Request-ID")
    _default("PINPOINT_PY_HTTP_CLIENT_RECORD_REQUEST_HEADER",
             "User-Agent,Content-Type,Host")
    _default("PINPOINT_PY_HTTP_CLIENT_RECORD_RESPONSE_HEADER", "Content-Type")
    _default("PINPOINT_PY_SQL_ENABLE_SQL_STATS", "true")
    _default("PINPOINT_PY_SQL_MAX_BIND_ARGS_SIZE", "2048")
    _default("PINPOINT_PY_STAT_BATCH_COUNT", "6")
    _default("PINPOINT_PY_STAT_BATCH_INTERVAL", "5000")
    _default("PINPOINT_PY_IS_CONTAINER", "true")

    pinpoint.init(
        server_info=os.environ.get("PINPOINT_PY_SERVER_INFO", "FastAPI IT Test Server"),
    )
    autoload()


def main() -> None:
    _init_agent_from_env()

    if _REAL_DB:
        print(
            f"real-db mode: using mysql at "
            f"{_DB_CONFIG['host']}:{_DB_CONFIG['port']}/{_DB_CONFIG['database']}",
            flush=True,
        )
        _ensure_db_schema()

    import uvicorn

    port = int(os.environ.get("PORT", "8090"))
    host = os.environ.get("HOST", "0.0.0.0")
    print(f"it_test HTTP server listening on http://{host}:{port}", flush=True)
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
        access_log=_ACCESS_LOG,
    )


if __name__ == "__main__":
    main()
