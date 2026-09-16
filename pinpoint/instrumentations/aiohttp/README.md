# aiohttp

Instruments both halves of [aiohttp](https://docs.aiohttp.org/): the server
(inbound requests become transactions) and the `ClientSession` (outbound
requests become span events with trace headers injected). aiohttp ships its own
asyncio HTTP stack — no ASGI — so both sides are hooked directly.

| | |
|---|---|
| Target modules | `aiohttp.web_protocol` + `aiohttp.web_urldispatcher` (server), `aiohttp.client` (client) |
| Hook points | `RequestHandler._handle_request` (root span), `UrlDispatcher.resolve` (route template), `ClientSession._request` (span event) |
| Service type | `PYTHON_HTTP_SERVER` / `PYTHON_METHOD` (server), `PYTHON_HTTP_CLIENT` (client) |
| Opt-out aliases | `aiohttp` (both), or `aiohttp.client` / `aiohttp.web_protocol` for one side |

The two sides are **separate instrumentors with separate autoload hooks**, on
purpose: `aiohttp.client` is imported before the server modules, so one shared
install guard would let the client hook consume the server installation and
break transport-specific opt-out.

## What gets traced

**Server**
- Root span per request, opened at `RequestHandler._handle_request` — the
  per-request entry called once per parsed HTTP message. It therefore opens
  before any application middleware runs and closes once the response is fully
  assembled, regardless of how `Application(middlewares=[...])` is configured.
- The matched `Resource` template (`/items/{id}`) is lifted off
  `UrlDispatcher.resolve` onto the request for URL-statistics aggregation.
- Upstream `Pinpoint-*` headers stitch the request into the caller's trace.

**Client**
- One span event per outbound request, from `ClientSession._request` — the
  single coroutine every `session.get/post/...` funnels through.
- Pinpoint headers are injected into a per-request **copy** of the caller's
  headers, never into the caller's own mapping, so cross-service traces stitch
  through every aiohttp hop exactly as they do for httpx/requests/urllib3.

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
import aiohttp
from aiohttp import web

async def user_detail(request):
    # Root span   : aiohttp HTTP Server  (GET /users/{uid})
    #  └─ event   : user_detail
    #      └─ event: GET http://profile.internal/...   <- client side, headers injected
    async with aiohttp.ClientSession() as session:
        async with session.get("http://profile.internal/profiles/1") as resp:
            await resp.text()
    return web.json_response({"id": request.match_info["uid"]})

app = web.Application()
app.add_routes([web.get("/users/{uid}", user_detail)])
web.run_app(app, port=8080)
```

## Disable

```bash
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=aiohttp              # both sides
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=aiohttp.client       # client only
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=aiohttp.web_protocol # server only
```

## See also

- Example: [`aiohttp_demo.py`](../../../examples/aiohttp/aiohttp_demo.py)
- Unit tests: [`test_aiohttp_instrumentation.py`](../../../tests/unit/instrumentations/test_aiohttp_instrumentation.py)
- [Custom Instrumentation Guide §9 — HTTP server and client tracing](../../../docs/custom_instrumentation.md)
