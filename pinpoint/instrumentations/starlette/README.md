# Starlette

Instruments [Starlette](https://www.starlette.io/) by installing the generic
[Pinpoint ASGI middleware](../asgi/README.md) into every application, plus a
per-endpoint span event.

| | |
|---|---|
| Target modules | `starlette.applications`, `starlette.routing`, `starlette.concurrency` |
| Hook points | `Starlette.build_middleware_stack` (root span), `Route.handle` (span event), `run_in_threadpool` (async hand-off) |
| Service type | `PYTHON_HTTP_SERVER` (span), `PYTHON_METHOD` (endpoint event) |
| Opt-out alias | `starlette` |

## What gets traced

- **Root span per request** — `build_middleware_stack` is wrapped so the
  Pinpoint middleware lands on the *outside* of every user-registered
  middleware, closest to the ASGI server. That ordering is deliberate: the span
  then times the whole request, user middleware included.
- **Endpoint span event** — from `Route.handle`, named after the endpoint
  callable, with `route.path` stashed into `scope['pinpoint.url_pattern']` so
  URL statistics aggregate per route template.
- **Sync endpoints** — Starlette runs `def` endpoints on a thread through
  `run_in_threadpool`; those entry points are wrapped so the endpoint receives a
  linked async child rather than the ASGI root span copied by anyio (a copied
  live span is a detached no-op on another thread).
- Upstream `Pinpoint-*` headers stitch the request into the caller's trace.

## Usage

No code changes — no `add_middleware` call needed:

```bash
pinpoint-run --app-name my-app --collector localhost -- \
    uvicorn app:app --host 0.0.0.0 --port 8000
```

```python
import httpx
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

async def user_detail(request):
    # Root span   : Starlette HTTP Server  (GET /users/{uid})
    #  └─ event   : user_detail
    #      └─ event: GET http://profile.internal/...   <- httpx, headers injected
    async with httpx.AsyncClient() as client:
        await client.get("http://profile.internal/profiles/1")
    return JSONResponse({"id": 1})

app = Starlette(routes=[Route("/users/{uid}", user_detail)])
```

## Disable

```bash
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=starlette
```

A FastAPI app also needs `fastapi` disabled — see below.

## See also

- Example: [`starlette_demo.py`](../../../examples/starlette/starlette_demo.py)
- Unit tests: [`test_starlette_instrumentation.py`](../../../tests/unit/instrumentations/test_starlette_instrumentation.py)
- [FastAPI integration](../fastapi/README.md) — builds on this one ·
  [generic ASGI layer](../asgi/README.md) ·
  [Custom Instrumentation Guide](../../../docs/custom_instrumentation.md)
