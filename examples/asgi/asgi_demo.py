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

"""Generic ASGI demo — exercises ``PinpointASGIMiddleware``.

Use this as a template when running a framework-less ASGI app, or any
app whose framework isn't covered by a dedicated Pinpoint integration.
The framework integrations (FastAPI, Starlette, aiohttp) install the
ASGI middleware automatically — here we do it by hand.

Run with pinpoint-run:

    pinpoint-run --app-name demo-asgi --agent-name demo-asgi \
        --collector localhost -- python examples/asgi/asgi_demo.py

Or directly:

    python examples/asgi/asgi_demo.py

Hit it:

    curl http://localhost:8000/ping
    curl http://localhost:8000/items/42
    curl http://localhost:8000/items/42/edit
    curl http://localhost:8000/boom

What this shows
---------------
- ``PinpointASGIMiddleware`` only opens a span for ``scope['type'] ==
  'http'``. ``lifespan`` and ``websocket`` scopes are passed through.
- Status is captured from the outbound ``http.response.start`` event —
  the only place ASGI exposes it to middleware.
- Setting ``scope['pinpoint.url_pattern']`` before sending the response
  routes url_stat to the template instead of the concrete path.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

from pinpoint.instrumentations.asgi import PinpointASGIMiddleware


_ITEMS_RE = re.compile(r"^/items/(?P<item_id>\d+)(?:/(?P<action>[a-zA-Z]+))?$")


async def _send_json(send, status: int, body: dict) -> None:
    payload = json.dumps(body).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(payload)).encode("ascii")),
            ],
        }
    )
    await send({"type": "http.response.body", "body": payload})


async def app(scope: dict[str, Any], receive, send) -> None:
    if scope["type"] != "http":
        return  # lifespan / websocket — Pinpoint middleware also passes through

    path = scope.get("path", "/")

    if path == "/ping":
        await _send_json(send, 200, {"service": "demo-asgi", "ok": True})
        return

    m = _ITEMS_RE.match(path)
    if m is not None:
        scope["pinpoint.url_pattern"] = (
            "/items/{item_id}/{action}" if m.group("action")
            else "/items/{item_id}"
        )
        await _send_json(
            send,
            200,
            {
                "item_id": int(m.group("item_id")),
                "action": m.group("action") or "view",
            },
        )
        return

    if path == "/boom":
        # The middleware catches this, calls set_error on the span, then
        # re-raises so the ASGI server returns its own 500.
        raise RuntimeError("simulated asgi failure")

    await _send_json(send, 404, {"error": "not found"})


wrapped = PinpointASGIMiddleware(app)


def main() -> None:
    import uvicorn

    import pinpoint
    from pinpoint.autoload import autoload

    # HTTP header tracing — see the comments in flask_demo.py for the
    # equivalent ``PINPOINT_PY_HTTP_{SERVER,CLIENT}_RECORD_*`` env vars and
    # the YAML keys mirrored from pinpoint-cpp-agent's pinpoint-config.yaml.
    pinpoint.init(
        application_name="python-demo-asgi",
        agent_name="python-demo-asgi-1",
        server_info="ASGI",
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

    uvicorn.run(wrapped, host="0.0.0.0",
                port=int(os.environ.get("PORT", "8000")), log_level="info")


if __name__ == "__main__":
    main()
