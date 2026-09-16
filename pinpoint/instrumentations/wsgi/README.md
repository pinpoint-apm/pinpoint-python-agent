# WSGI (generic)

A framework-less WSGI middleware that turns every inbound request into a
Pinpoint root span. Use it when you embed a hand-rolled WSGI app, or as a
fallback for a framework that has no dedicated integration yet.

| | |
|---|---|
| Autoloaded | **No** — there is no canonical WSGI module to hook. Wrap your app by hand. |
| Public API | `PinpointWSGIMiddleware`, `wsgi_entry_wrapper(operation)` |
| Service type | `PYTHON_HTTP_SERVER` |

This is also the shared foundation the [Flask](../flask/README.md),
[Django](../django/README.md), [Pyramid](../pyramid/README.md), and
[Falcon](../falcon/README.md) integrations build their root span on, so a fix
here reaches all of them.

## What the middleware does

- Opens a root span per request and ends it once the response body has been
  fully iterated (streaming responses included).
- Honors upstream `Pinpoint-*` headers (case-insensitive) so the request links
  into the caller's trace.
- Annotates HTTP URL, status, client address, and the configured request/
  response header allow-list; records URL statistics against
  `environ['pinpoint.url_pattern']` when a framework layer stashed a route
  template there, else the raw path.
- Routes uncaught exceptions into the span via `set_error` while still re-raising
  them for the outer server.
- Sets `environ['pinpoint.root_span_active']` for the request's lifetime, so a
  nested Pinpoint layer (a hand-wrapped app whose framework is also autoloaded)
  cannot open a second root span for the same request.

## Usage

```python
import pinpoint
import pinpoint.autoload
from pinpoint.instrumentations.wsgi import PinpointWSGIMiddleware

pinpoint.init(application_name="my-app", collector_host="localhost")
pinpoint.autoload.autoload()

def application(environ, start_response):
    start_response("200 OK", [("Content-Type", "text/plain")])
    return [b"hello"]

# Wrap outermost — closest to the WSGI server.
application = PinpointWSGIMiddleware(application)
```

The second argument labels the span operation in the UI:

```python
application = PinpointWSGIMiddleware(application, "MyFramework")  # -> "MyFramework HTTP Server"
```

## Disable

Not autoloaded, so there is no opt-out alias — remove the wrapper to disable it.

## See also

- Example: [`wsgi_demo.py`](../../../examples/wsgi/wsgi_demo.py)
- Unit tests: [`test_wsgi_instrumentation.py`](../../../tests/unit/instrumentations/test_wsgi_instrumentation.py)
- [Custom Instrumentation Guide §9 — HTTP server tracing](../../../docs/custom_instrumentation.md) ·
  [ASGI counterpart](../asgi/README.md)
