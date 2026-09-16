# Falcon

Instruments [Falcon](https://falcon.readthedocs.io/) on both transports. Falcon
ships two `App` classes — `falcon.app.App` (WSGI) and `falcon.asgi.app.App`
(an ASGI subclass of it) — and both are covered.

| | |
|---|---|
| Target module | `falcon.app` (the ASGI app module is hooked separately, since `import falcon` does not pull it in) |
| Hook points | `App.__call__` on both apps (root span), `App._get_responder` (span event + route template) |
| Service type | `PYTHON_HTTP_SERVER` (span), `PYTHON_METHOD` (responder event) |
| Opt-out alias | `falcon` |

## What gets traced

- **Root span per request** — separate wrappers for the WSGI and ASGI
  `__call__`, since they are distinct methods. The ASGI root-span lifecycle
  reuses the framework-agnostic [ASGI runner](../asgi/README.md) rather than
  duplicating the scope/header machinery; Falcon adds nothing transport-specific
  on top.
- **Responder span event** — `_get_responder` is the shared resolver (the ASGI
  App inherits it, so one wrap covers both transports). It returns
  `(responder, params, resource, uri_template)`; the wrapper stashes the matched
  URI template on `req.env` / `req.scope` for URL statistics and swaps the
  responder for a span-event-emitting wrapper named after the user's resource
  method (`UserResource.on_get`).
- Upstream `Pinpoint-*` headers stitch the request into the caller's trace.

A Falcon app mounted inside another ASGI app will not open a second root span —
the ASGI middleware's `scope['pinpoint.root_span_active']` guard covers that.

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
import falcon

class UserResource:
    def on_get(self, req, resp, uid):
        # Root span   : Falcon HTTP Server  (GET /users/{uid})
        #  └─ event   : UserResource.on_get
        resp.media = {"id": uid}

app = falcon.App()
app.add_route("/users/{uid}", UserResource())
```

The ASGI flavor (`falcon.asgi.App`) is traced the same way.

## See also

- Example: [`falcon_demo.py`](../../../examples/falcon/falcon_demo.py)
- Unit tests: [`test_falcon_instrumentation.py`](../../../tests/unit/instrumentations/test_falcon_instrumentation.py)
- [generic WSGI](../wsgi/README.md) / [ASGI](../asgi/README.md) layers ·
  [Custom Instrumentation Guide](../../../docs/custom_instrumentation.md)
