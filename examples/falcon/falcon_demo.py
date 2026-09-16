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

"""Minimal Falcon demo — exercises the Pinpoint ``falcon`` instrumentation.

Run with pinpoint-run:

    pinpoint-run --app-name demo-falcon --agent-name demo-falcon \
        --collector localhost -- python examples/falcon/falcon_demo.py

Or directly:

    python examples/falcon/falcon_demo.py              # WSGI (default, served by wsgiref)
    FALCON_ASGI=1 python examples/falcon/falcon_demo.py # ASGI variant, served by uvicorn

Hit it:

    curl http://localhost:8000/ping
    curl http://localhost:8000/items/42
    curl http://localhost:8000/items/42/edit
    curl http://localhost:8000/redis
    curl http://localhost:8000/boom

What this shows
---------------
- ``falcon.App.__call__`` (WSGI) and ``falcon.asgi.App.__call__`` (ASGI)
  are wrapped to open a Pinpoint root span per request — the latter
  hooks lazily via a post-import hook so importing ``falcon`` alone is
  enough.
- ``App._get_responder`` is wrapped to lift the matched URI template
  (e.g. ``/items/{item_id:int}``) into ``req.env`` / ``req.scope`` under
  ``pinpoint.url_pattern`` so url_stat aggregates per route, not per
  concrete path.
- The same wrap also swaps the matched responder with a tracing wrapper
  so the responder's method qualname (``ItemsResource.on_get``, …) shows
  up as its own span event under the root span.
- ``/redis`` issues SET / GET / HSET / HGETALL and a small pipeline
  through redis-py — each command shows up as its own span event under
  the responder span (service type REDIS). Override target via
  ``REDIS_HOST`` / ``REDIS_PORT`` / ``REDIS_DB``; the endpoint returns
  503 if ``redis`` isn't installed or the server isn't reachable.
- ``/boom`` raises — the root span is marked failed by ``set_error``.
"""

from __future__ import annotations

import os

try:
    import redis  # type: ignore[import-not-found]
except ImportError:
    redis = None  # type: ignore[assignment]


REDIS_CONFIG = {
    "host": os.environ.get("REDIS_HOST", "127.0.0.1"),
    "port": int(os.environ.get("REDIS_PORT", "6379")),
    "db": int(os.environ.get("REDIS_DB", "0")),
}


def _redis_roundtrip() -> dict:
    """SET / GET / HSET / HGETALL plus a small pipeline. Shared by the
    WSGI and ASGI resources — the sync redis-py client works in both
    contexts (it just blocks the event loop on the ASGI path, which is
    fine for a demo). The Pinpoint redis instrumentation wraps
    ``Redis.execute_command`` and ``Pipeline.execute`` so each command
    emits its own span event."""

    client = redis.Redis(decode_responses=True, **REDIS_CONFIG)
    client.set("pinpoint:falcon:greet", "hello")
    value = client.get("pinpoint:falcon:greet")
    client.hset("pinpoint:falcon:item", mapping={"id": "42", "name": "demo"})
    item = client.hgetall("pinpoint:falcon:item")

    pipe = client.pipeline()
    pipe.incr("pinpoint:falcon:counter")
    pipe.expire("pinpoint:falcon:counter", 60)
    counter, _expired = pipe.execute()

    return {"value": value, "item": item, "counter": counter}


class PingResource:
    def on_get(self, _req, resp):
        resp.media = {"service": "demo-falcon", "ok": True}


class ItemsResource:
    """Single resource handling both ``/items/{item_id}`` and
    ``/items/{item_id}/{action}`` — falcon dispatches by the matched
    template, and the Pinpoint url_stat key reflects the template, not
    the concrete path."""

    def on_get(self, _req, resp, item_id, action="view"):
        resp.media = {"item_id": int(item_id), "action": action}


class RedisResource:
    def on_get(self, _req, resp):
        import falcon

        if redis is None:
            resp.status = falcon.HTTP_503
            resp.media = {"error": "redis package not installed"}
            return
        try:
            resp.media = {"backend": "redis-py", **_redis_roundtrip()}
        except redis.exceptions.RedisError as exc:
            resp.status = falcon.HTTP_503
            resp.media = {"error": f"{type(exc).__name__}: {exc}"}


class BoomResource:
    def on_get(self, _req, _resp):
        raise RuntimeError("simulated falcon failure")


# ---- async variants (used only when FALCON_ASGI=1) ------------------------


class AsyncPingResource:
    async def on_get(self, _req, resp):
        resp.media = {"service": "demo-falcon-asgi", "ok": True}


class AsyncItemsResource:
    async def on_get(self, _req, resp, item_id, action="view"):
        resp.media = {"item_id": int(item_id), "action": action}


class AsyncRedisResource:
    async def on_get(self, _req, resp):
        import falcon

        if redis is None:
            resp.status = falcon.HTTP_503
            resp.media = {"error": "redis package not installed"}
            return
        try:
            resp.media = {"backend": "redis-py", **_redis_roundtrip()}
        except redis.exceptions.RedisError as exc:
            resp.status = falcon.HTTP_503
            resp.media = {"error": f"{type(exc).__name__}: {exc}"}


class AsyncBoomResource:
    async def on_get(self, _req, _resp):
        raise RuntimeError("simulated falcon failure")


# ---- factories -------------------------------------------------------------


def build_wsgi_app():
    import falcon

    app = falcon.App()
    app.add_route("/ping", PingResource())
    app.add_route("/items/{item_id:int}", ItemsResource())
    app.add_route("/items/{item_id:int}/{action}", ItemsResource())
    app.add_route("/redis", RedisResource())
    app.add_route("/boom", BoomResource())
    return app


def build_asgi_app():
    import falcon.asgi

    app = falcon.asgi.App()
    app.add_route("/ping", AsyncPingResource())
    app.add_route("/items/{item_id:int}", AsyncItemsResource())
    app.add_route("/items/{item_id:int}/{action}", AsyncItemsResource())
    app.add_route("/redis", AsyncRedisResource())
    app.add_route("/boom", AsyncBoomResource())
    return app


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    use_asgi = os.environ.get("FALCON_ASGI") == "1"
    name_suffix = "asgi" if use_asgi else "wsgi"

    pinpoint.init(
        application_name=f"python-demo-falcon-{name_suffix}",
        agent_name=f"python-demo-falcon-{name_suffix}-1",
        server_info=f"Falcon {name_suffix.upper()}",
        # HTTP header tracing — see pinpoint-cpp-agent's pinpoint-config.yaml.
        # Equivalent env vars: PINPOINT_PY_HTTP_{SERVER,CLIENT}_RECORD_*.
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

    port = int(os.environ.get("PORT", "8000"))

    if use_asgi:
        import uvicorn

        uvicorn.run(build_asgi_app(), host="0.0.0.0", port=port,
                    log_level="info")
    else:
        from wsgiref.simple_server import make_server

        server = make_server("0.0.0.0", port, build_wsgi_app())
        print(f"falcon demo (WSGI) listening on http://0.0.0.0:{port}")
        server.serve_forever()


if __name__ == "__main__":
    main()
