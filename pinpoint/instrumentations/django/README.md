# Django

Instruments [Django](https://www.djangoproject.com/) at its WSGI and ASGI
handler boundaries. Both transports are covered by separate autoload hooks, so
an app served either way — or both — is traced.

| | |
|---|---|
| Target modules | `django.core.handlers.wsgi`, `django.core.handlers.asgi` (both also patch `django.core.handlers.base`) |
| Hook points | `WSGIHandler.__call__` / `ASGIHandler.__call__` (root span), `BaseHandler._get_response[_async]` (span event), `ASGIHandler.run_get_response` |
| Service type | `PYTHON_HTTP_SERVER` (span), `PYTHON_METHOD` (view event) |
| Opt-out alias | `django` |

## What gets traced

- **Root span per request** — the handler `__call__` is the outermost layer, so
  the span stays alive until the response body has been fully sent. Stamped with
  HTTP URL, status, client address, and the configured header allow-list.
- **View span event** — the resolved view is recorded from
  `BaseHandler._get_response` (sync) or `_get_response_async` (async), named
  after the view callable, with the matched URL pattern used for URL statistics.
- Upstream `Pinpoint-*` headers stitch the request into the caller's trace.
- Uncaught exceptions are recorded on the span and re-raised.

Under ASGI, Django routes every synchronous view, middleware, and ORM call
through asgiref's `SyncToAsync`. Keeping those sync sections traced on their
worker thread is the [asgiref instrumentation](../asgiref/README.md)'s job — it
loads automatically alongside this one.

## Usage

No code changes and no `MIDDLEWARE` entry. Launch under `pinpoint-run`:

```bash
pinpoint-run --app-name my-app --collector localhost -- \
    gunicorn -w 4 myproject.wsgi:application
```

```python
# views.py
import requests
from django.http import JsonResponse

def user_detail(request, uid):
    # Root span   : Django HTTP Server  (GET /users/<int:uid>/)
    #  └─ event   : views.user_detail
    #      └─ event: SELECT ... (whichever DB driver is installed)
    #      └─ event: GET http://profile.internal/...
    requests.get(f"http://profile.internal/profiles/{uid}")
    return JsonResponse({"id": uid})
```

Prefork servers (gunicorn, uWSGI) need the note in the
[Pre-fork Guide](../../../docs/prefork.md); a master that initializes the agent
before forking must pass `prefork=True`.

## Disable

```bash
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=django
```

Disables both transports. Django's DB queries come from the underlying driver
(`mysqlclient`, `psycopg`, …) and are disabled by their own aliases.

## See also

- Example: [`django_demo.py`](../../../examples/django/django_demo.py)
- Unit tests: [`test_django_instrumentation.py`](../../../tests/unit/instrumentations/test_django_instrumentation.py)
- [Custom Instrumentation Guide](../../../docs/custom_instrumentation.md) ·
  [Configuration Guide](../../../docs/config.md) ·
  [generic WSGI](../wsgi/README.md) / [ASGI](../asgi/README.md) layers
