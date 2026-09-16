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

"""Minimal aiohttp demo — exercises the Pinpoint ``aiohttp`` server
instrumentation and the ``elasticsearch`` async-client instrumentation.

Run with pinpoint-run:

    pinpoint-run --app-name demo-aiohttp --agent-name demo-aiohttp \
        --collector localhost -- python examples/aiohttp/aiohttp_demo.py

Or directly:

    python examples/aiohttp/aiohttp_demo.py

Hit it:

    curl http://localhost:8080/ping
    curl http://localhost:8080/items/42
    curl http://localhost:8080/items/42/edit
    curl http://localhost:8080/elasticsearch
    curl http://localhost:8080/boom

What this shows
---------------
- ``aiohttp.web_protocol.RequestHandler._handle_request`` is wrapped so
  every inbound request opens a Pinpoint root span before any
  application middleware runs.
- ``aiohttp.web_urldispatcher.UrlDispatcher.resolve`` is wrapped so the
  matched ``Resource.canonical`` pattern (e.g. ``/items/{id}``) ends up
  on ``request['pinpoint.url_pattern']`` for url_stat aggregation.
- The user middleware below runs *inside* the Pinpoint span, so its
  time is counted in the request's total.
- ``/elasticsearch`` drives ``AsyncElasticsearch`` (v8+) against a local
  cluster — index + refresh + search each become their own
  ``elasticsearch.<METHOD> <url>`` span event under the aiohttp request
  span, annotated with the rendered DSL (256-char cap) and ``ElasticSearch``
  destination. The async wrapper covers ``elastic_transport.AsyncTransport``,
  so each call's timing includes the awaited network round-trip.

The /elasticsearch endpoint needs an Elasticsearch the agent can reach.
Defaults match::

    docker run --rm -e discovery.type=single-node \
        -e xpack.security.enabled=false \
        -p 9200:9200 docker.elastic.co/elasticsearch/elasticsearch:8.15.0

Override via: ELASTICSEARCH_URL (default ``http://127.0.0.1:9200``) and
ELASTICSEARCH_INDEX (default ``demo-aiohttp``). A library that isn't
installed simply returns 503 — the demo still boots.
"""

from __future__ import annotations

import os
import random

from aiohttp import web

try:
    from elasticsearch import AsyncElasticsearch  # type: ignore[import-not-found]
    from elasticsearch import ApiError, TransportError  # type: ignore[import-not-found]
except ImportError:
    AsyncElasticsearch = None  # type: ignore[assignment,misc]
    ApiError = Exception  # type: ignore[assignment,misc]
    TransportError = Exception  # type: ignore[assignment,misc]


ELASTICSEARCH_URL = os.environ.get("ELASTICSEARCH_URL", "http://127.0.0.1:9200")
ELASTICSEARCH_INDEX = os.environ.get("ELASTICSEARCH_INDEX", "demo-aiohttp")

# Stored on the aiohttp Application as app["es_client"]; opened on
# startup and closed on cleanup so the connection pool's lifetime is
# bound to the server, not to each request.
_ES_APP_KEY = "es_client"


@web.middleware
async def demo_header_middleware(request, handler):
    """User middleware — adds a response header. The Pinpoint span covers
    the entire request lifecycle including this layer."""
    response = await handler(request)
    response.headers["X-Demo"] = "pinpoint-aiohttp"
    return response


async def ping(_request):
    return web.json_response({"service": "demo-aiohttp", "ok": True})


async def items(request):
    item_id = int(request.match_info["item_id"])
    action = request.match_info.get("action", "view")
    return web.json_response({"item_id": item_id, "action": action})


async def elasticsearch_view(request):
    """Exercise the elasticsearch async-client instrumentation.

    Issues ``index`` + ``indices.refresh`` + ``search`` against a local
    cluster — each ``perform_request`` becomes a single span event under
    the aiohttp request span. The rendered DSL (request body) ends up on
    the event as an annotation, capped at 256 chars to match
    pinpoint-go-agent's ``plugin/goelastic``.
    """
    if AsyncElasticsearch is None:
        return web.json_response(
            {"error": "elasticsearch not installed (pip install elasticsearch)"},
            status=503,
        )
    client = request.app.get(_ES_APP_KEY)
    if client is None:
        return web.json_response(
            {"error": "elasticsearch client not initialized"}, status=503,
        )
    try:
        doc_id = str(random.randrange(1, 1_000_000))
        await client.index(
            index=ELASTICSEARCH_INDEX,
            id=doc_id,
            document={"name": "demo", "tag": "aiohttp"},
        )
        # Force the indexed doc to be searchable immediately — also emits
        # its own elasticsearch.POST /<index>/_refresh span event.
        await client.indices.refresh(index=ELASTICSEARCH_INDEX)
        result = await client.search(
            index=ELASTICSEARCH_INDEX,
            query={"term": {"_id": doc_id}},
            size=1,
        )
    except (ApiError, TransportError, OSError) as exc:
        return web.json_response(
            {"error": f"{type(exc).__name__}: {exc}"}, status=503,
        )
    hits = result.get("hits", {}).get("hits", [])
    return web.json_response({
        "indexed_id": doc_id,
        "hit_count": len(hits),
        "first_hit": hits[0]["_source"] if hits else None,
    })


async def boom(_request):
    raise RuntimeError("simulated aiohttp failure")


async def _es_startup(app: web.Application) -> None:
    """Open the AsyncElasticsearch client when the server starts.

    The client owns a connection pool and event loop bindings — keeping
    one per-app instance (rather than per-request) means the pool, and
    every ``perform_request`` span event below it, stays warm across
    requests."""
    if AsyncElasticsearch is None:
        return
    app[_ES_APP_KEY] = AsyncElasticsearch(ELASTICSEARCH_URL)


async def _es_cleanup(app: web.Application) -> None:
    client = app.get(_ES_APP_KEY)
    if client is not None:
        await client.close()


def build_app() -> web.Application:
    app = web.Application(middlewares=[demo_header_middleware])
    app.router.add_get("/ping", ping)
    app.router.add_get("/items/{item_id}", items)
    app.router.add_get("/items/{item_id}/{action}", items)
    app.router.add_get("/elasticsearch", elasticsearch_view)
    app.router.add_get("/boom", boom)
    app.on_startup.append(_es_startup)
    app.on_cleanup.append(_es_cleanup)
    return app


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    # Header tracing config — same shape as Http.{Server,Client}.Record*Header
    # in pinpoint-cpp-agent/test/it_test/pinpoint-config.yaml.
    pinpoint.init(
        application_name="python-demo-aiohttp",
        agent_name="python-demo-aiohttp-1",
        server_info="aiohttp",
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

    web.run_app(build_app(), host="0.0.0.0",
                port=int(os.environ.get("PORT", "8080")))


if __name__ == "__main__":
    main()
