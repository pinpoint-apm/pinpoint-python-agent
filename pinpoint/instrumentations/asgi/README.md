# ASGI (generic)

A framework-less ASGI 3.0 middleware that turns every inbound HTTP request into
a Pinpoint root span. Use it for a hand-rolled ASGI app or a framework without a
dedicated integration.

| | |
|---|---|
| Autoloaded | **No** — there is no canonical ASGI module to hook. Wrap your app by hand. |
| Public API | `PinpointASGIMiddleware`, `asgi_entry_wrapper(operation)` |
| Service type | `PYTHON_HTTP_SERVER` |

It is also the foundation that [Starlette](../starlette/README.md),
[FastAPI](../fastapi/README.md), and [Falcon](../falcon/README.md) (ASGI mode)
delegate their root span to.

## What the middleware does

- Opens a root span per `http` scope. **Lifespan and websocket scopes pass
  through untouched.**
- Honors upstream `Pinpoint-*` headers so the request stitches into the caller's
  trace.
- Captures the status from the outbound `http.response.start` event — the only
  place ASGI reliably exposes it to a middleware.
- Records URL statistics against the matched route template when a downstream
  layer stashed one in `scope['pinpoint.url_pattern']` or exposes a matched route
  object under `scope['route']`; otherwise the raw path.
- Routes uncaught exceptions into the span via `set_error` and re-raises.
- Sets `scope['pinpoint.root_span_active']`, so a nested Pinpoint layer (a
  Falcon app mounted inside FastAPI, an autoloaded app also wrapped by hand)
  cannot open a second root span for the same request.

Compatible with both ASGI 2 (instance call) and ASGI 3 (single async callable) —
it always exposes the ASGI 3 shape.

## Usage

```python
import pinpoint
import pinpoint.autoload
from pinpoint.instrumentations.asgi import PinpointASGIMiddleware

pinpoint.init(application_name="my-app", collector_host="localhost")
pinpoint.autoload.autoload()

async def app(scope, receive, send):
    await send({"type": "http.response.start", "status": 200,
                "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": b"hello"})

# Wrap outermost — closest to the ASGI server (uvicorn, hypercorn, …).
app = PinpointASGIMiddleware(app)
```

To label the span operation: `PinpointASGIMiddleware(app, "MyFramework HTTP Server")`.

## Disable

Not autoloaded, so there is no opt-out alias — remove the wrapper to disable it.

## See also

- Example: [`asgi_demo.py`](../../../examples/asgi/asgi_demo.py)
- Unit tests: [`test_asgi_instrumentation.py`](../../../tests/unit/instrumentations/test_asgi_instrumentation.py)
- [Custom Instrumentation Guide §9 — HTTP server tracing](../../../docs/custom_instrumentation.md) ·
  [WSGI counterpart](../wsgi/README.md)
