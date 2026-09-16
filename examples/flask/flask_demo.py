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

"""Minimal Flask + requests + multi-MySQL-client demo (frontend half).

Pair with flask_upstream.py to see distributed tracing across two
services. Start the upstream first so /ping has somewhere to call:

    # terminal 1
    pinpoint-run --app-name demo-upstream --agent-name demo-upstream \
        --collector localhost -- python examples/flask/flask_upstream.py
    # terminal 2
    pinpoint-run --app-name demo --agent-name demo-frontend \
        --collector localhost -- python examples/flask/flask_demo.py
    # terminal 3
    curl http://localhost:5000/ping
    curl http://localhost:5000/db                  # pymysql
    curl http://localhost:5000/db/connector        # mysql-connector-python
    curl http://localhost:5000/db/mysqlclient      # MySQLdb (mysqlclient)
    curl http://localhost:5000/db/aiomysql         # aiomysql (async)

Or manually:
    import pinpoint
    pinpoint.init(application_name="demo", agent_name="demo-frontend", server_info="Flask")
    pinpoint.autoload.autoload()

The upstream URL is overridable for split-host deployments:
    UPSTREAM_URL=http://other-host:5001/echo python examples/flask/flask_demo.py

The /db endpoints need a MySQL the agent can reach. Defaults match
`docker run --rm -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=demo
 -p 3306:3306 mysql:8`. Override via:
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

Each /db/* endpoint exercises a different MySQL client library so the
Pinpoint UI shows one trace per instrumentation backend (pymysql,
mysql-connector-python, mysqlclient, aiomysql). A library that isn't
installed in the venv simply returns 503 — the demo still boots.
"""

import asyncio
import os
import threading
import time

import flask
import requests

import pinpoint
from pinpoint.context import current_span
from pinpoint.service_type import SERVICE_TYPE_PYTHON_METHOD

try:
    import pymysql  # type: ignore[import-not-found]
except ImportError:
    pymysql = None  # type: ignore[assignment]

try:
    import mysql.connector as mysql_connector  # type: ignore[import-not-found]
except ImportError:
    mysql_connector = None  # type: ignore[assignment]

try:
    import MySQLdb  # type: ignore[import-not-found]
except ImportError:
    MySQLdb = None  # type: ignore[assignment]

try:
    import aiomysql  # type: ignore[import-not-found]
except ImportError:
    aiomysql = None  # type: ignore[assignment]

app = flask.Flask(__name__)

UPSTREAM_URL = os.environ.get("UPSTREAM_URL", "http://localhost:5001/echo")

DB_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("MYSQL_PORT", "3306")),
    "user": os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", "root"),
    "database": os.environ.get("MYSQL_DATABASE", "demo"),
}


@app.route("/ping")
def ping():
    # Set a session cookie on the outbound call so the upstream request
    # carries a ``Cookie: session_id=…`` header. Pair with the upstream's
    # ``http_server_record_request_cookie=["session_id", ...]`` config to see
    # the cookie value land on the upstream root span in the Pinpoint UI.
    cookies = {"session_id": "ping-session-42"}
    r = requests.get(UPSTREAM_URL, cookies=cookies, timeout=5)
    return {"upstream_status": r.status_code, "upstream_body": r.json()}, 200


@app.route("/db")
def db():
    if pymysql is None:
        return {"error": "pymysql not installed (pip install pymysql)"}, 503
    try:
        conn = pymysql.connect(**DB_CONFIG)
    except Exception as e:  # noqa: BLE001
        return {"error": f"connect failed: {e}"}, 503
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION(), %s AS hello", ("world",))
            row = cur.fetchone()
        return {"mysql_version": row[0], "hello": row[1]}, 200
    finally:
        conn.close()


@app.route("/crud")
def crud():
    """Exercise INSERT/SELECT/UPDATE/DELETE so each pymysql codepath emits a span.

    Pair with PINPOINT_PY_SQL_ENABLE_SQL_STATS=true to verify the URL-stat / SQL-stat
    aggregation in the C++ agent.
    """
    if pymysql is None:
        return {"error": "pymysql not installed (pip install pymysql)"}, 503
    try:
        conn = pymysql.connect(**DB_CONFIG)
    except Exception as e:  # noqa: BLE001
        return {"error": f"connect failed: {e}"}, 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS demo_items ("
                "  id INT AUTO_INCREMENT PRIMARY KEY,"
                "  name VARCHAR(64) NOT NULL,"
                "  value INT NOT NULL"
                ")"
            )
            cur.execute("DELETE FROM demo_items")
            cur.executemany(
                "INSERT INTO demo_items (name, value) VALUES (%s, %s)",
                [("alpha", 1), ("beta", 2), ("gamma", 3)],
            )
            inserted = cur.rowcount

            cur.execute("SELECT id, name, value FROM demo_items ORDER BY id")
            rows = [list(r) for r in cur.fetchall()]

            cur.execute(
                "UPDATE demo_items SET value = value * %s WHERE name = %s",
                (10, "beta"),
            )
            updated = cur.rowcount

            cur.execute("DELETE FROM demo_items WHERE value < %s", (15,))
            deleted = cur.rowcount

            cur.execute("SELECT COUNT(*) FROM demo_items")
            remaining = cur.fetchone()[0]
        conn.commit()
        return {
            "inserted": inserted,
            "selected": rows,
            "updated": updated,
            "deleted": deleted,
            "remaining": remaining,
        }, 200
    finally:
        conn.close()


@app.route("/db/connector")
def db_connector():
    """SELECT VERSION() via mysql-connector-python.

    The `mysql` instrumentation hooks ``MySQLCursor.execute`` so the
    SELECT below opens a SQL span under the Flask request span. The
    C-extension cursor (``CMySQLCursor``) is wrapped separately by the
    same module.
    """
    if mysql_connector is None:
        return {"error": "mysql-connector-python not installed"}, 503
    try:
        conn = mysql_connector.connect(**DB_CONFIG)
    except Exception as e:  # noqa: BLE001
        return {"error": f"connect failed: {e}"}, 503
    try:
        cur = conn.cursor()
        cur.execute("SELECT VERSION(), %s AS hello", ("world",))
        row = cur.fetchone()
        cur.close()
        return {"mysql_version": row[0], "hello": row[1]}, 200
    finally:
        conn.close()


@app.route("/db/mysqlclient")
def db_mysqlclient():
    """SELECT VERSION() via mysqlclient (the C-extension MySQLdb driver).

    The `mysqlclient` instrumentation wraps ``MySQLdb.cursors.BaseCursor``
    so every subclass (``Cursor``, ``DictCursor``, ``SSCursor``, …) is
    instrumented from one hook. ``mysqlclient`` is the default driver for
    Django's MySQL backend.
    """
    if MySQLdb is None:
        return {"error": "mysqlclient not installed"}, 503
    cfg = dict(DB_CONFIG)
    # MySQLdb uses 'db' rather than 'database'.
    cfg["db"] = cfg.pop("database")
    try:
        conn = MySQLdb.connect(**cfg)
    except Exception as e:  # noqa: BLE001
        return {"error": f"connect failed: {e}"}, 503
    try:
        cur = conn.cursor()
        cur.execute("SELECT VERSION(), %s AS hello", ("world",))
        row = cur.fetchone()
        cur.close()
        return {"mysql_version": row[0], "hello": row[1]}, 200
    finally:
        conn.close()


@app.route("/db/aiomysql")
def db_aiomysql():
    """SELECT VERSION() via aiomysql, bridged from the sync Flask handler.

    aiomysql is an asyncio driver; the Flask worker is sync so we drive
    one short-lived event loop via ``asyncio.run``. The `aiomysql`
    instrumentation wraps ``aiomysql.cursors.Cursor.execute`` so the
    query span opens under whatever Pinpoint span is current — here that
    is the Flask request span.
    """
    if aiomysql is None:
        return {"error": "aiomysql not installed"}, 503

    async def _run():
        conn = await aiomysql.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            db=DB_CONFIG["database"],
        )
        try:
            cur = await conn.cursor()
            await cur.execute("SELECT VERSION(), %s AS hello", ("world",))
            row = await cur.fetchone()
            await cur.close()
            return row
        finally:
            conn.close()

    try:
        row = asyncio.run(_run())
    except Exception as e:  # noqa: BLE001
        return {"error": f"aiomysql failed: {e}"}, 503
    return {"mysql_version": row[0], "hello": row[1]}, 200


@app.route("/items/<int:item_id>")
@app.route("/items/<int:item_id>/<string:action>")
def items(item_id: int, action: str = "view"):
    """Exercise Flask's URL pattern matching so url_stat buckets by route template.

    Pair with PINPOINT_PY_HTTP_COLLECT_URL_STAT=true: regardless of the concrete
    path (`/items/1`, `/items/42/edit`, …), the agent reports the matched rule
    (`/items/<int:item_id>` or `/items/<int:item_id>/<string:action>`) as the
    url_stat key, so the Pinpoint UI groups requests per endpoint.
    """
    return {"item_id": item_id, "action": action}, 200


@app.route("/decorated")
def decorated():
    """Manual tracing via @spanevent under the auto-instrumented Flask span.

    The view-function span itself is added by the Flask instrumentation;
    each @spanevent call below records a child span event so the Pinpoint
    UI shows nested method calls without any per-call boilerplate.
    """
    return {"result": _decorated_compute(7) + _decorated_lookup("alpha")}, 200


@pinpoint.spanevent("compute")
def _decorated_compute(x: int) -> int:
    return x * x


@pinpoint.spanevent  # name defaults to qualname
def _decorated_lookup(key: str) -> int:
    return len(key)


@app.route("/decorated/job")
def decorated_job():
    """Trigger a @span-decorated entry point as if it were a cron job.

    The job runs inside an HTTP request here for ease of triggering, but
    `@span` opens its own root transaction independent of the Flask span
    — a real cron scheduler would call `_nightly_billing()` directly with
    no surrounding span, and the trace would still appear in Pinpoint as
    a top-level transaction.
    """
    _nightly_billing()
    return {"ok": True}, 200


@pinpoint.span("nightly_billing", rpc_point="/cron/billing")
def _nightly_billing() -> None:
    _decorated_compute(11)
    _decorated_lookup("beta")


@app.route("/async/thread")
def async_thread():
    """Hand work off to a background thread under a Pinpoint async span.

    The Pinpoint UI renders the thread's work as a separate sub-trace linked
    to this request, with the async service type marking the hop. Any
    instrumented call inside the worker (requests, pymysql, redis, …)
    attaches under the async span automatically because it is the current
    span inside the thread.
    """
    threads = []
    for tag in ("alpha", "beta"):
        with pinpoint.async_trace(f"thread:{tag}") as async_span:
            t = threading.Thread(
                target=_background_work, args=(async_span, tag),
            )
        t.start()
        threads.append(t)
    for t in threads:
        t.join(timeout=5)
    return {"ran": [t.name for t in threads]}, 200


@app.route("/async/task")
def async_task():
    """Same hand-off pattern, but to asyncio Tasks instead of threads.

    Run on a sync Flask worker by entering an event loop briefly. Each
    coroutine receives its own async span via `create_task`; the gather
    awaits both before the response is sent.
    """
    async def driver():
        tasks = []
        for tag in ("one", "two"):
            with pinpoint.async_trace(f"task:{tag}") as async_span:
                tasks.append(asyncio.create_task(_async_work(async_span, tag)))
        return await asyncio.gather(*tasks)

    results = asyncio.run(driver())
    return {"results": results}, 200


def _background_work(async_span, tag: str) -> None:
    # ``with async_span:`` makes it the current span inside the thread and
    # ends it on exit — child spans below nest under it, not under the
    # original Flask request span. async_span is None when the agent is off.
    if async_span is None:
        return
    with async_span:
        with pinpoint.trace(f"compute:{tag}", service_type=SERVICE_TYPE_PYTHON_METHOD):
            time.sleep(0.05)
            try:
                requests.get(UPSTREAM_URL, timeout=5)  # outbound call appears as a child
            except Exception:  # noqa: BLE001
                pass


async def _async_work(async_span, tag: str) -> str:
    if async_span is None:
        return tag
    with async_span:
        with pinpoint.trace(f"await:{tag}"):
            await asyncio.sleep(0.05)
    return tag


@app.route("/boom")
def boom():
    """Trigger a nested traceback so the call-stack panel has something to show.

    Run with PINPOINT_PY_ENABLE_CALLSTACK_TRACE=true and the SpanEvent's
    __exit__ will hand the captured frames to the C++ agent; the Pinpoint
    UI then renders them under the failed event's call-stack tab.
    """
    span = current_span()
    if span is None:
        return {"error": "no active span — flask instrumentation off?"}, 500
    try:
        with span.new_span_event("compute", service_type=SERVICE_TYPE_PYTHON_METHOD):
            _layer_one()
    except Exception as exc:  # noqa: BLE001
        span.set_error(exc)  # also mark the root transaction as failed
        return {"error": type(exc).__name__, "message": str(exc)}, 500
    return {"ok": True}, 200


def _layer_one():
    _layer_two(seed=7)


def _layer_two(seed):
    _layer_three(seed * 6)


def _layer_three(value):
    raise RuntimeError(f"simulated downstream failure (value={value})")


if __name__ == "__main__":
    # Manual init path (remove this block if launching via pinpoint-run).
    import pinpoint
    from pinpoint.autoload import autoload

    # HTTP header tracing — both inbound (server) and outbound (requests) headers
    # are recorded only when explicitly allow-listed. Equivalent env vars
    # (comma-separated): PINPOINT_PY_HTTP_SERVER_RECORD_REQUEST_HEADER, …
    #   _RECORD_REQUEST_COOKIE, _RECORD_RESPONSE_HEADER,
    # PINPOINT_PY_HTTP_CLIENT_RECORD_REQUEST_HEADER, _RECORD_REQUEST_COOKIE,
    # _RECORD_RESPONSE_HEADER. Use a single ``HEADERS-ALL`` entry to dump every
    # header (debug only). Keys mirror pinpoint-cpp-agent's pinpoint-config.yaml
    # ``Http.Server.RecordRequestHeader`` etc.
    pinpoint.init(
        application_name="python-demo",
        agent_name="python-flask-demo-1",
        server_info="Flask",
        http_server_record_request_header=[
            "User-Agent", "Content-Type", "Accept", "Host",
            "X-Request-ID", "X-Forwarded-For",
        ],
        http_server_record_response_header=[
            "Content-Type", "X-Response-Time", "X-Request-ID",
        ],
        http_client_record_request_header=[
            "User-Agent", "Content-Type", "Host",
        ],
        http_client_record_response_header=["Content-Type"],
        http_client_record_request_cookie=["HEADERS-ALL"],
    )
    autoload()

    app.run(host="0.0.0.0", port=5000)
