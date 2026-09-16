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

"""Minimal Pyramid demo — exercises the Pinpoint ``pyramid``
instrumentation.

Run with pinpoint-run:

    pinpoint-run --app-name demo-pyramid --agent-name demo-pyramid \
        --collector localhost -- python examples/pyramid/pyramid_demo.py

Or directly:

    python examples/pyramid/pyramid_demo.py

Hit it:

    curl http://localhost:6543/ping
    curl http://localhost:6543/items/42
    curl http://localhost:6543/items/42/edit
    curl http://localhost:6543/cache
    curl http://localhost:6543/boom

What this shows
---------------
- ``pyramid.router.Router.__call__`` is wrapped to open a Pinpoint root
  span around the full WSGI request lifecycle.
- ``pyramid.view._call_view`` is wrapped to (a) lift the matched
  ``Route.pattern`` into ``request.environ['pinpoint.url_pattern']``
  so url_stat aggregates per route, and (b) emit a Python-method span
  event named after the view callable's qualname.
- ``/cache`` issues set / get / add / incr / delete + set_many / get_many
  through pymemcache — each command shows up as its own span event under
  the view span (service type MEMCACHED). Override target via
  ``MEMCACHED_HOST`` / ``MEMCACHED_PORT``; the endpoint returns 503 if
  ``pymemcache`` isn't installed or the server isn't reachable.
- ``/boom`` raises — the root span is marked failed.
"""

from __future__ import annotations

import os
from wsgiref.simple_server import make_server

from pyramid.config import Configurator
from pyramid.httpexceptions import HTTPServiceUnavailable
from pyramid.response import Response

try:
    from pymemcache.client.base import Client as MemcachedClient  # type: ignore[import-not-found]
    from pymemcache.exceptions import MemcacheError  # type: ignore[import-not-found]
except ImportError:
    MemcachedClient = None  # type: ignore[assignment,misc]
    MemcacheError = Exception  # type: ignore[assignment,misc]


MEMCACHED_CONFIG = {
    "host": os.environ.get("MEMCACHED_HOST", "127.0.0.1"),
    "port": int(os.environ.get("MEMCACHED_PORT", "11211")),
}


def ping(_request):
    return {"service": "demo-pyramid", "ok": True}


def items(request):
    item_id = int(request.matchdict["item_id"])
    action = request.matchdict.get("action", "view")
    return {"item_id": item_id, "action": action}


def cache(_request):
    """Exercise the pymemcache instrumentation. Each method call below
    becomes its own span event (``pymemcache.client.base.Client.set``,
    ``pymemcache.client.base.Client.get``,
    …) annotated with the cache key (or ``xN`` for multi-key ops)."""
    if MemcachedClient is None:
        raise HTTPServiceUnavailable(json_body={"error": "pymemcache not installed"})

    client = MemcachedClient(
        (MEMCACHED_CONFIG["host"], MEMCACHED_CONFIG["port"]),
        connect_timeout=2,
        timeout=2,
    )
    try:
        client.set(b"pinpoint:pyramid:greet", b"hello", expire=60)
        value = client.get(b"pinpoint:pyramid:greet")
        client.add(b"pinpoint:pyramid:once", b"1", expire=60)
        client.set(b"pinpoint:pyramid:counter", b"0", expire=60)
        counter = client.incr(b"pinpoint:pyramid:counter", 1)
        client.set_many({
            b"pinpoint:pyramid:a": b"1",
            b"pinpoint:pyramid:b": b"2",
        }, expire=60)
        many = client.get_many([b"pinpoint:pyramid:a", b"pinpoint:pyramid:b"])
        client.delete(b"pinpoint:pyramid:greet")
    except (OSError, MemcacheError) as exc:
        raise HTTPServiceUnavailable(
            json_body={"error": f"{type(exc).__name__}: {exc}"},
        )
    finally:
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass

    return {
        "value": value.decode() if isinstance(value, (bytes, bytearray)) else value,
        "counter": int(counter) if counter is not None else None,
        "many_count": len(many),
    }


def boom(_request) -> Response:
    raise RuntimeError("simulated pyramid failure")


def build_app():
    config = Configurator()
    config.add_route("ping", "/ping")
    config.add_route("items", "/items/{item_id}")
    config.add_route("items_action", "/items/{item_id}/{action}")
    config.add_route("cache", "/cache")
    config.add_route("boom", "/boom")
    config.add_view(ping, route_name="ping", renderer="json")
    config.add_view(items, route_name="items", renderer="json")
    config.add_view(items, route_name="items_action", renderer="json")
    config.add_view(cache, route_name="cache", renderer="json")
    config.add_view(boom, route_name="boom")
    return config.make_wsgi_app()


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="python-demo-pyramid",
        agent_name="python-demo-pyramid-1",
        server_info="Pyramid",
        # HTTP header tracing — same shape as Http.{Server,Client}.Record*Header
        # in pinpoint-cpp-agent's pinpoint-config.yaml.
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

    port = int(os.environ.get("PORT", "6543"))
    server = make_server("0.0.0.0", port, build_app())
    print(f"pyramid demo listening on http://0.0.0.0:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
