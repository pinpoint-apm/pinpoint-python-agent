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

"""Minimal FastAPI demo — exercises the Pinpoint ``fastapi`` +
``starlette`` instrumentations plus every PostgreSQL client library
covered by the agent (asyncpg, psycopg3, psycopg2, aiopg).

Run with pinpoint-run:

    pinpoint-run --app-name demo-fastapi --agent-name demo-fastapi \
        --collector localhost -- python examples/fastapi/fastapi_demo.py

Or directly:

    python examples/fastapi/fastapi_demo.py

Hit it:

    curl http://localhost:8000/ping
    curl http://localhost:8000/items/42
    curl http://localhost:8000/items/42/edit
    curl http://localhost:8000/users/alice
    curl http://localhost:8000/db/asyncpg
    curl http://localhost:8000/db/psycopg
    curl http://localhost:8000/db/psycopg2
    curl http://localhost:8000/db/aiopg
    curl http://localhost:8000/db/crud
    curl http://localhost:8000/boom

What this shows
---------------
- ``/items/{item_id}`` exercises Starlette route templates — the agent
  lifts ``scope['route'].path`` into ``scope['pinpoint.url_pattern']``
  so url_stat aggregates by template, not by concrete id.
- The FastAPI instrumentation additionally emits a Python-method span
  event named after the endpoint qualname (e.g. ``items``, ``UserAPI.get``),
  so the Pinpoint UI shows the user's handler name.
- ``/db/*`` endpoints each exercise a different PostgreSQL driver so the
  Pinpoint UI shows one SQL span per instrumentation backend. A library
  that isn't installed simply returns 503 — the demo still boots.
- ``/db/crud`` runs INSERT/SELECT/UPDATE/DELETE via psycopg3's async
  cursor so each codepath emits a span.
- ``/boom`` raises — the root span is marked failed via Starlette's
  ASGI middleware re-raising path.

The /db/* endpoints need a Postgres the agent can reach. Defaults match
``docker run --rm -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=demo
 -p 5432:5432 postgres:16``. Override via:
    PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE
"""

from __future__ import annotations

import asyncio
import os

from fastapi import APIRouter, FastAPI

try:
    import asyncpg  # type: ignore[import-not-found]
except ImportError:
    asyncpg = None  # type: ignore[assignment]

try:
    import psycopg  # type: ignore[import-not-found]
except ImportError:
    psycopg = None  # type: ignore[assignment]

try:
    import psycopg2  # type: ignore[import-not-found]
except ImportError:
    psycopg2 = None  # type: ignore[assignment]

try:
    import aiopg  # type: ignore[import-not-found]
except ImportError:
    aiopg = None  # type: ignore[assignment]


PG_CONFIG = {
    "host": os.environ.get("PG_HOST", "127.0.0.1"),
    "port": int(os.environ.get("PG_PORT", "5432")),
    "user": os.environ.get("PG_USER", "postgres"),
    "password": os.environ.get("PG_PASSWORD", "postgres"),
    "database": os.environ.get("PG_DATABASE", "demo"),
}


def _dsn() -> str:
    return (
        f"postgresql://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
        f"@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"
    )


app = FastAPI(title="pinpoint-fastapi-demo")


@app.get("/ping")
def ping():
    return {"service": "demo-fastapi", "ok": True}


@app.get("/items/{item_id}")
def items(item_id: int, action: str = "view"):
    return {"item_id": item_id, "action": action}


@app.get("/items/{item_id}/{action}")
def items_with_action(item_id: int, action: str):
    return {"item_id": item_id, "action": action}


class UserAPI:
    """A trivial class-based handler so the FastAPI instrumentation's
    span-event name shows ``UserAPI.get`` rather than just ``get``."""

    router = APIRouter(prefix="/users", tags=["users"])

    @staticmethod
    @router.get("/{name}")
    def get(name: str) -> dict:
        return {"user": name}


app.include_router(UserAPI.router)


@app.get("/db/asyncpg")
async def db_asyncpg():
    """SELECT version() via asyncpg — the native async PostgreSQL driver.

    asyncpg doesn't use DB-API; the `asyncpg` instrumentation hooks
    ``Connection.execute / fetch / fetchrow / fetchval / cursor`` directly
    so each coroutine call emits its own SQL span.
    """
    if asyncpg is None:
        return _err(503, "asyncpg not installed")
    try:
        conn = await asyncpg.connect(
            host=PG_CONFIG["host"], port=PG_CONFIG["port"],
            user=PG_CONFIG["user"], password=PG_CONFIG["password"],
            database=PG_CONFIG["database"],
        )
    except Exception as e:  # noqa: BLE001
        return _err(503, f"connect failed: {e}")
    try:
        row = await conn.fetchrow("SELECT version() AS v, $1::text AS hello",
                                  "world")
        return {"pg_version": row["v"], "hello": row["hello"]}
    finally:
        await conn.close()


@app.get("/db/psycopg")
async def db_psycopg():
    """SELECT version() via psycopg3 in async mode.

    The `psycopg` instrumentation wraps both ``Cursor.execute`` (sync) and
    ``AsyncCursor.execute`` (async) so this codepath produces a SQL span
    without bridging through a thread pool.
    """
    if psycopg is None:
        return _err(503, "psycopg (v3) not installed")
    try:
        aconn = await psycopg.AsyncConnection.connect(_dsn())
    except Exception as e:  # noqa: BLE001
        return _err(503, f"connect failed: {e}")
    try:
        async with aconn.cursor() as cur:
            await cur.execute("SELECT version(), %s AS hello", ("world",))
            row = await cur.fetchone()
        return {"pg_version": row[0], "hello": row[1]}
    finally:
        await aconn.close()


@app.get("/db/psycopg2")
async def db_psycopg2():
    """SELECT version() via psycopg2 — the legacy sync driver, bridged
    through ``asyncio.to_thread`` so it works under FastAPI's event loop.

    The `psycopg` instrumentation wraps ``psycopg2.extensions.cursor.execute``;
    the span still attaches to the FastAPI request span because the
    Pinpoint context is preserved across ``to_thread`` (it uses
    ``contextvars`` under the hood).
    """
    if psycopg2 is None:
        return _err(503, "psycopg2 not installed")

    def _run():
        conn = psycopg2.connect(
            host=PG_CONFIG["host"], port=PG_CONFIG["port"],
            user=PG_CONFIG["user"], password=PG_CONFIG["password"],
            dbname=PG_CONFIG["database"],
        )
        try:
            cur = conn.cursor()
            cur.execute("SELECT version(), %s AS hello", ("world",))
            row = cur.fetchone()
            cur.close()
            return row
        finally:
            conn.close()

    try:
        row = await asyncio.to_thread(_run)
    except Exception as e:  # noqa: BLE001
        return _err(503, f"psycopg2 failed: {e}")
    return {"pg_version": row[0], "hello": row[1]}


@app.get("/db/aiopg")
async def db_aiopg():
    """SELECT version() via aiopg — psycopg2 wrapped in an asyncio facade.

    The `aiopg` instrumentation wraps ``aiopg.connection.Cursor.execute``
    (a coroutine) so the SQL span is emitted on the async path. The
    underlying psycopg2 wire calls are not re-instrumented to avoid
    double-counting.
    """
    if aiopg is None:
        return _err(503, "aiopg not installed")
    try:
        conn = await aiopg.connect(dsn=_dsn())
    except Exception as e:  # noqa: BLE001
        return _err(503, f"connect failed: {e}")
    try:
        cur = await conn.cursor()
        await cur.execute("SELECT version(), %s AS hello", ("world",))
        row = await cur.fetchone()
        cur.close()
        return {"pg_version": row[0], "hello": row[1]}
    finally:
        conn.close()


@app.get("/db/crud")
async def db_crud():
    """Exercise INSERT/SELECT/UPDATE/DELETE via psycopg3 async so every
    SQL codepath emits a span.

    Pair with ``PINPOINT_PY_SQL_ENABLE_SQL_STATS=true`` to verify URL-stat /
    SQL-stat aggregation in the C++ agent.
    """
    if psycopg is None:
        return _err(503, "psycopg (v3) not installed")
    try:
        aconn = await psycopg.AsyncConnection.connect(_dsn(), autocommit=True)
    except Exception as e:  # noqa: BLE001
        return _err(503, f"connect failed: {e}")
    try:
        async with aconn.cursor() as cur:
            await cur.execute(
                "CREATE TABLE IF NOT EXISTS demo_items ("
                "  id SERIAL PRIMARY KEY,"
                "  name VARCHAR(64) NOT NULL,"
                "  value INT NOT NULL"
                ")"
            )
            await cur.execute("DELETE FROM demo_items")
            await cur.executemany(
                "INSERT INTO demo_items (name, value) VALUES (%s, %s)",
                [("alpha", 1), ("beta", 2), ("gamma", 3)],
            )

            await cur.execute(
                "SELECT id, name, value FROM demo_items ORDER BY id"
            )
            rows = [list(r) for r in await cur.fetchall()]

            await cur.execute(
                "UPDATE demo_items SET value = value * %s WHERE name = %s",
                (10, "beta"),
            )
            updated = cur.rowcount

            await cur.execute(
                "DELETE FROM demo_items WHERE value < %s", (15,)
            )
            deleted = cur.rowcount

            await cur.execute("SELECT COUNT(*) FROM demo_items")
            remaining_row = await cur.fetchone()
            remaining = remaining_row[0] if remaining_row else 0
        return {
            "inserted": 3,
            "selected": rows,
            "updated": updated,
            "deleted": deleted,
            "remaining": remaining,
        }
    finally:
        await aconn.close()


@app.get("/boom")
def boom():
    raise RuntimeError("simulated fastapi failure")


def _err(code: int, msg: str):
    """JSONResponse-shaped helper so callers can ``return _err(...)``.

    FastAPI will serialize the dict; the explicit status code is conveyed
    via raising or via a Response. Keep it simple with a tuple-less form
    by using JSONResponse directly.
    """
    from fastapi.responses import JSONResponse

    return JSONResponse({"error": msg}, status_code=code)


def main() -> None:
    import uvicorn

    import pinpoint
    from pinpoint.autoload import autoload

    # Allow-list HTTP headers to capture under the request/response span.
    # See pinpoint-cpp-agent/test/it_test/pinpoint-config.yaml for the
    # canonical YAML equivalents under Http.{Server,Client}.Record*Header.
    # Equivalent env vars (comma-separated): PINPOINT_PY_HTTP_SERVER_RECORD_REQUEST_HEADER, …
    # _RECORD_REQUEST_COOKIE, _RECORD_RESPONSE_HEADER,
    # PINPOINT_PY_HTTP_CLIENT_RECORD_REQUEST_HEADER, _RECORD_RESPONSE_HEADER.
    pinpoint.init(
        application_name="python-demo-fastapi",
        agent_name="python-demo-fastapi-1",
        server_info="FastAPI",
        http_server_record_request_header=[
            "User-Agent", "Content-Type", "Accept", "Host",
            "X-Request-ID", "X-Forwarded-For",
        ],
        http_server_record_request_cookie=["session_id", "token"],
        http_server_record_response_header=[
            "Content-Type", "X-Response-Time", "X-Request-ID",
        ],
        http_client_record_request_header=[
            "User-Agent", "Content-Type", "Host",
        ],
        http_client_record_response_header=["Content-Type"],
    )
    autoload()

    uvicorn.run(app, host="0.0.0.0",
                port=int(os.environ.get("PORT", "8000")), log_level="info")


if __name__ == "__main__":
    main()
