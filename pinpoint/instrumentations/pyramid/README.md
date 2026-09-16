# Pyramid

Instruments [Pyramid](https://trypyramid.com/) at its WSGI router entry point.

| | |
|---|---|
| Target modules | `pyramid.router`, `pyramid.view` |
| Hook points | `Router.__call__` (root span), `_call_view` (span event) |
| Service type | `PYTHON_HTTP_SERVER` (span), `PYTHON_METHOD` (view event) |
| Opt-out alias | `pyramid` |

## What gets traced

- **Root span per request** — `Router.__call__` is the top-level WSGI entry, so
  the span covers the full request lifecycle including tweens.
- **View span event** — `_call_view` is Pyramid's internal helper that resolves
  and invokes the matched view callable. The wrapper names the event after the
  view qualname and lifts the matched route pattern onto
  `request.environ['pinpoint.url_pattern']` for URL-statistics aggregation.
- Upstream `Pinpoint-*` headers stitch the request into the caller's trace.
- Uncaught exceptions are recorded on the span and re-raised.

`_call_view` is wrapped on **both** the `pyramid.router` and `pyramid.view`
module bindings: `pyramid.router` does `from pyramid.view import _call_view` at
import time and `Router.handle_request` resolves the name through its own module
globals, so wrapping only `pyramid.view` (the historical hook) would miss every
routed request.

## Usage

No code changes — launch under `pinpoint-run`:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
from pyramid.config import Configurator
from pyramid.response import Response

def user_detail(request):
    # Root span   : Pyramid HTTP Server  (GET /users/{uid})
    #  └─ event   : user_detail
    return Response(f"user {request.matchdict['uid']}")

with Configurator() as config:
    config.add_route("user_detail", "/users/{uid}")
    config.add_view(user_detail, route_name="user_detail")
    app = config.make_wsgi_app()
```

## See also

- Example: [`pyramid_demo.py`](../../../examples/pyramid/pyramid_demo.py)
- Unit tests: [`test_pyramid_instrumentation.py`](../../../tests/unit/instrumentations/test_pyramid_instrumentation.py)
- [generic WSGI layer](../wsgi/README.md) ·
  [Custom Instrumentation Guide](../../../docs/custom_instrumentation.md)
