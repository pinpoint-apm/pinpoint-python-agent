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

"""Minimal Starlette demo — exercises the Pinpoint ``starlette``
instrumentation (the FastAPI-less path).

Run with pinpoint-run:

    pinpoint-run --app-name demo-starlette --agent-name demo-starlette \
        --collector localhost -- python examples/starlette/starlette_demo.py

Or directly:

    python examples/starlette/starlette_demo.py

Hit it:

    curl http://localhost:8000/ping
    curl http://localhost:8000/items/42
    curl http://localhost:8000/items/42/edit
    curl http://localhost:8000/cassandra
    curl http://localhost:8000/boom

What this shows
---------------
- ``Starlette.build_middleware_stack`` is wrapped so the Pinpoint ASGI
  middleware lands on the *outside* of the user middleware stack — the
  root span covers the entire request including custom middleware time.
- ``Starlette.__call__`` is wrapped to lift the matched ``Route.path``
  template into ``scope['pinpoint.url_pattern']`` so url_stat aggregates
  per route, not per concrete URL.
- The custom ``X-Demo`` header middleware sits *inside* the Pinpoint
  middleware, so its execution time is included in the Pinpoint span.
- ``/cassandra`` issues INSERT + SELECT (sync ``execute``) and a
  SELECT via ``execute_async`` against a local Cassandra — each call
  becomes its own span event (``cassandra.cluster.Session.execute`` /
  ``cassandra.cluster.Session.execute_async``) annotated with the CQL and keyspace.
  Override target via ``CASSANDRA_HOST`` / ``CASSANDRA_PORT`` /
  ``CASSANDRA_KEYSPACE``; the endpoint returns 503 if the driver isn't
  installed or the cluster is unreachable.
"""

from __future__ import annotations

import os
import random

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.routing import Route

try:
    from cassandra.cluster import Cluster, NoHostAvailable  # type: ignore[import-not-found]
    from cassandra import DriverException  # type: ignore[import-not-found]
except ImportError:
    Cluster = None  # type: ignore[assignment,misc]
    NoHostAvailable = Exception  # type: ignore[assignment,misc]
    DriverException = Exception  # type: ignore[assignment,misc]


CASSANDRA_CONFIG = {
    "contact_points": [os.environ.get("CASSANDRA_HOST", "127.0.0.1")],
    "port": int(os.environ.get("CASSANDRA_PORT", "9042")),
    "keyspace": os.environ.get("CASSANDRA_KEYSPACE", "demo"),
}

# Lazily created on first /cassandra hit so the demo still boots when the
# cluster is down. uvicorn defaults to one worker / one event loop, so a
# simple module global is race-free without a lock.
_cassandra_session = None


def _get_cassandra_session():
    global _cassandra_session
    if _cassandra_session is not None:
        return _cassandra_session
    cluster = Cluster(
        contact_points=CASSANDRA_CONFIG["contact_points"],
        port=CASSANDRA_CONFIG["port"],
    )
    session = cluster.connect()
    keyspace = CASSANDRA_CONFIG["keyspace"]
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace(keyspace)
    session.execute(
        "CREATE TABLE IF NOT EXISTS items (id int PRIMARY KEY, name text)"
    )
    _cassandra_session = session
    return session


async def ping(_request):
    return JSONResponse({"service": "demo-starlette", "ok": True})


async def items(request):
    item_id = int(request.path_params["item_id"])
    action = request.path_params.get("action", "view")
    return JSONResponse({"item_id": item_id, "action": action})


async def cassandra_view(_request):
    """Exercise the cassandra-driver instrumentation. The synchronous
    ``Session.execute`` blocks the event loop briefly — fine for a demo,
    and lets us emit ``cassandra.cluster.Session.execute`` +
    ``cassandra.cluster.Session.execute_async``
    span events under the request span."""
    if Cluster is None:
        return JSONResponse(
            {"error": "cassandra-driver not installed"}, status_code=503,
        )
    try:
        session = _get_cassandra_session()
        item_id = random.randrange(1, 1_000_000)
        session.execute(
            "INSERT INTO items (id, name) VALUES (%s, %s)",
            (item_id, "demo"),
        )
        row = session.execute(
            "SELECT id, name FROM items WHERE id = %s", (item_id,),
        ).one()
        # execute_async returns a ResponseFuture; .result() blocks until
        # the driver IO thread delivers — both the call and the wait are
        # covered by the cassandra.cluster.Session.execute_async span event.
        future = session.execute_async(
            "SELECT id FROM items WHERE id = %s", (item_id,),
        )
        async_row = future.result().one()
    except (NoHostAvailable, DriverException, OSError) as exc:
        return JSONResponse(
            {"error": f"{type(exc).__name__}: {exc}"}, status_code=503,
        )
    return JSONResponse({
        "id": row.id,
        "name": row.name,
        "async_id": async_row.id,
    })


async def boom(_request):
    raise RuntimeError("simulated starlette failure")


class DemoHeaderMiddleware(BaseHTTPMiddleware):
    """User middleware — the Pinpoint span timing covers this layer too."""

    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["X-Demo"] = "pinpoint-starlette"
        return response


routes = [
    Route("/ping", ping),
    Route("/items/{item_id:int}", items),
    Route("/items/{item_id:int}/{action}", items),
    Route("/cassandra", cassandra_view),
    Route("/boom", boom),
]

app = Starlette(
    debug=True,
    routes=routes,
    middleware=[Middleware(DemoHeaderMiddleware)],
)


def main() -> None:
    import uvicorn

    import pinpoint
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="python-demo-starlette",
        agent_name="python-demo-starlette-1",
        server_info="Starlette",
        # HTTP header tracing — same shape as Http.{Server,Client}.Record*Header
        # in pinpoint-cpp-agent's pinpoint-config.yaml. Equivalent env vars:
        # PINPOINT_PY_HTTP_{SERVER,CLIENT}_RECORD_{REQUEST,RESPONSE}_{HEADER,COOKIE}.
        http_server_record_request_header=[
            "User-Agent", "Content-Type", "Accept", "Host",
            "X-Request-ID", "X-Forwarded-For",
        ],
        http_server_record_request_cookie=["session_id", "token"],
        http_server_record_response_header=[
            "Content-Type", "X-Response-Time", "X-Request-ID",
        ],
    )
    autoload()

    uvicorn.run(app, host="0.0.0.0",
                port=int(os.environ.get("PORT", "8000")), log_level="info")


if __name__ == "__main__":
    main()
