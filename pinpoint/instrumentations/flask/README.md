# Flask

Instruments [Flask](https://flask.palletsprojects.com/) at its WSGI entry point,
so every inbound request becomes a Pinpoint transaction.

| | |
|---|---|
| Target module | `flask.app` |
| Hook points | `Flask.wsgi_app` (root span), `Flask.dispatch_request` (span event), `Flask.finalize_request` (marks a buffered response body) |
| Service type | `PYTHON_HTTP_SERVER` (span), `PYTHON_METHOD` (view event) |
| Opt-out alias | `flask` |

## What gets traced

- **Root span per request** — opened on `Flask.wsgi_app`, the outermost Flask
  layer, so the span covers user middleware and response streaming. Stamped with
  the HTTP URL, status code, client address, and the configured request/response
  header allow-list.
- **View span event** — a child event named after the resolved view function
  (`views.user_detail`), with the matched route template (`/users/<int:uid>`)
  handed to URL statistics so buckets are per route, not per concrete path.
- Upstream `Pinpoint-*` headers are honored, so the request stitches into the
  caller's trace. Responses do not write them back — propagation is outbound only.
- Uncaught exceptions are recorded on the span and re-raised untouched.
  `flask.abort(404)` and other sub-500 `HTTPException`s count as control flow,
  not errors.

## Usage

No code changes — launch the app under `pinpoint-run`:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
import flask
import requests

app = flask.Flask(__name__)

@app.route("/users/<int:uid>")
def user_detail(uid):
    # Root span   : Flask HTTP Server  (GET /users/<int:uid>)
    #  └─ event   : views.user_detail
    #      └─ event: GET http://profile.internal/...   <- requests, headers injected
    requests.get(f"http://profile.internal/profiles/{uid}")
    return {"id": uid}
```

For an in-code bootstrap (`pinpoint.init()` + `autoload()`) and for prefork
servers (gunicorn, uWSGI), see the [Getting Started](../../../docs/getting_started.md)
and the [Pre-fork Guide](../../../docs/prefork.md).

## See also

- Examples: [`flask_demo.py`](../../../examples/flask/flask_demo.py) with
  [`flask_upstream.py`](../../../examples/flask/flask_upstream.py) — a two-service
  distributed trace
- Unit tests: [`test_flask_instrumentation.py`](../../../tests/unit/instrumentations/test_flask_instrumentation.py)
- [Custom Instrumentation Guide](../../../docs/custom_instrumentation.md) ·
  [Configuration Guide](../../../docs/config.md) ·
  [generic WSGI layer](../wsgi/README.md)
