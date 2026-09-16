# FastAPI

Instruments [FastAPI](https://fastapi.tiangolo.com/). FastAPI builds on
Starlette, and the root span lifecycle still belongs to the generic
[Pinpoint ASGI middleware](../asgi/README.md) — but FastAPI overrides
`build_middleware_stack` *without* calling `super()`, so the wrap installed by
the [Starlette integration](../starlette/README.md) never fires for a FastAPI
app. Hence a dedicated hook.

| | |
|---|---|
| Target modules | `fastapi.applications`, `fastapi.routing`, `fastapi.concurrency`, `fastapi.dependencies.utils` |
| Hook points | `FastAPI.build_middleware_stack` (root span), `run_endpoint_function` (span event), `run_in_threadpool` / `contextmanager_in_threadpool` (async hand-off) |
| Service type | `PYTHON_HTTP_SERVER` (span), `PYTHON_METHOD` (handler event) |
| Opt-out alias | `fastapi` |

## What gets traced

- **Root span per request**, opened outside every user middleware.
- **Handler span event** — from `run_endpoint_function`, named after the user's
  handler qualname (`UserService.create_user`), so the UI shows the actual
  handler and not just the route.
- **URL statistics per route template** — FastAPI's `APIRoute.matches` exposes
  the matched route under `scope['route']`, which the outer ASGI middleware
  reads at span end. No extra send wrapper needed.
- **Sync endpoints and sync dependencies** — `run_in_threadpool` and
  `contextmanager_in_threadpool` hand off a linked async child, so `def`
  endpoints and `yield` dependencies running on a worker thread stay traced.
- FastAPI's `APIRoute` is deliberately kept off Starlette's generic
  `Route.handle` endpoint wrapper, because this integration records the endpoint
  more precisely. The Starlette wrapper detects `APIRoute` at call time and
  returns without emitting an event, so a FastAPI request only pays a cached
  check — and the wrapper chain on `Route.handle` stays composable with other
  libraries' wrappers.

## Usage

No code changes and no middleware registration:

```bash
pinpoint-run --app-name my-app --collector localhost -- \
    uvicorn app:app --host 0.0.0.0 --port 8000
```

```python
import httpx
from fastapi import FastAPI

app = FastAPI()

@app.get("/users/{uid}")
async def user_detail(uid: int):
    # Root span   : FastAPI HTTP Server  (GET /users/{uid})
    #  └─ event   : user_detail
    #      └─ event: GET http://profile.internal/...   <- httpx, headers injected
    async with httpx.AsyncClient() as client:
        await client.get(f"http://profile.internal/profiles/{uid}")
    return {"id": uid}
```

Multi-worker uvicorn/gunicorn deployments: see the
[Pre-fork Guide](../../../docs/prefork.md).

## Disable

```bash
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=fastapi,starlette
```

Disabling `fastapi` alone leaves the Starlette hooks active (harmless, but they
add nothing for a FastAPI app since its middleware stack is built elsewhere).

## See also

- Example: [`fastapi_demo.py`](../../../examples/fastapi/fastapi_demo.py)
- Unit tests: [`test_fastapi_instrumentation.py`](../../../tests/unit/instrumentations/test_fastapi_instrumentation.py)
- [Starlette integration](../starlette/README.md) ·
  [generic ASGI layer](../asgi/README.md) ·
  [Custom Instrumentation Guide](../../../docs/custom_instrumentation.md)
