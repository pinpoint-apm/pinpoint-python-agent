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

"""Generic WSGI demo — exercises ``PinpointWSGIMiddleware``.

Unlike Flask/Django/Pyramid, plain WSGI has no canonical hook the
agent can register at import time, so users opt in by wrapping their
app with the middleware. This demo shows the wrap and exercises the
features it provides.

Run with pinpoint-run:

    pinpoint-run --app-name demo-wsgi --agent-name demo-wsgi \
        --collector localhost -- python examples/wsgi/wsgi_demo.py

Or directly:

    python examples/wsgi/wsgi_demo.py

Hit it:

    curl http://localhost:8000/ping
    curl http://localhost:8000/items/42
    curl http://localhost:8000/boom

What this shows
---------------
- ``PinpointWSGIMiddleware`` opens a root span on entry, wraps
  ``start_response`` to capture status, and ends the span only after
  the response body iterable is fully drained (correct for streaming).
- A bare WSGI app has no router, so the middleware stashes the
  literal path into url_stat. If a router downstream knows a template
  it can set ``environ['pinpoint.url_pattern']`` before
  ``start_response`` is called.
- ``/boom`` raises — the middleware records ``set_error`` and re-raises
  so the outer WSGI server can produce a 500.
"""

from __future__ import annotations

import json
import os
import re
from wsgiref.simple_server import make_server

from pinpoint.instrumentations.wsgi import PinpointWSGIMiddleware

# Tiny hand-rolled router so we can demonstrate url_pattern stashing.
_ITEMS_RE = re.compile(r"^/items/(?P<item_id>\d+)(?:/(?P<action>[a-zA-Z]+))?$")


def _json(start_response, status: str, body: dict):
    payload = json.dumps(body).encode("utf-8")
    start_response(
        status,
        [
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(payload))),
        ],
    )
    return [payload]


def application(environ, start_response):
    path = environ.get("PATH_INFO", "/")

    if path == "/ping":
        return _json(start_response, "200 OK",
                     {"service": "demo-wsgi", "ok": True})

    m = _ITEMS_RE.match(path)
    if m is not None:
        # Tell the Pinpoint middleware which template matched so url_stat
        # aggregates per route, not per concrete id.
        environ["pinpoint.url_pattern"] = (
            "/items/{item_id}/{action}" if m.group("action")
            else "/items/{item_id}"
        )
        return _json(
            start_response,
            "200 OK",
            {
                "item_id": int(m.group("item_id")),
                "action": m.group("action") or "view",
            },
        )

    if path == "/boom":
        raise RuntimeError("simulated wsgi failure")

    return _json(start_response, "404 Not Found", {"error": "not found"})


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    # HTTP header tracing — allow-list inbound and outbound headers.
    # The names mirror pinpoint-cpp-agent's Http.{Server,Client}.Record*Header
    # YAML keys; equivalent env vars are PINPOINT_PY_HTTP_SERVER_RECORD_REQUEST_HEADER,
    # _RECORD_REQUEST_COOKIE, _RECORD_RESPONSE_HEADER, PINPOINT_PY_HTTP_CLIENT_RECORD_*.
    pinpoint.init(
        application_name="python-demo-wsgi",
        agent_name="python-demo-wsgi-1",
        server_info="WSGI",
        http_server_record_request_header=[
            "User-Agent", "Content-Type", "Accept", "Host",
            "X-Request-ID", "X-Forwarded-For",
        ],
        http_server_record_request_cookie=["session_id", "token"],
        http_server_record_response_header=[
            "Content-Type", "Content-Length", "X-Request-ID",
        ],
    )
    autoload()

    wrapped = PinpointWSGIMiddleware(application)
    port = int(os.environ.get("PORT", "8000"))
    server = make_server("0.0.0.0", port, wrapped)
    print(f"wsgi demo listening on http://0.0.0.0:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
