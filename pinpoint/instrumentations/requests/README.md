# requests

Instruments the [requests](https://requests.readthedocs.io/) HTTP client. Every
outbound request becomes a span event on the current transaction, with Pinpoint
trace headers injected so the callee stitches into the same trace.

| | |
|---|---|
| Target module | `requests.sessions` |
| Hook point | `Session.send` — every `requests.get/post/...` and `Session` method funnels through it |
| Span role | Span event (child of the caller's transaction) |
| Service type | `PYTHON_HTTP_CLIENT` |
| Opt-out alias | `requests` |

## What gets traced

- One span event per outbound request, annotated with the method, URL,
  destination host, response status, and the configured
  `http_client_record_request_header` / `..._response_header` allow-list.
- `Pinpoint-*` headers are injected so the receiving service's server-side
  instrumentation continues this trace.
- **No duplicate event for the transport.** requests uses urllib3 underneath;
  the wrapper opens a suppression scope for the exchange, so the
  [urllib3 integration](../urllib3/README.md) skips its own event. One outbound
  call produces exactly one span event, whichever combination is installed.
- With no current span (a background thread outside any transaction), the call
  passes through untouched — no span event, no headers.

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
import requests

# Inside a transaction (an HTTP handler, or @pinpoint.span for a script):
requests.get("http://profile.internal/profiles/1")
# └─ span event: GET http://profile.internal/profiles/1   (Pinpoint-* injected)
```

For a script or cron job with no HTTP entry point, open the transaction
yourself so the client call has a parent:

```python
import pinpoint

@pinpoint.span("sync_profiles", rpc_point="/cron/sync-profiles")
def sync_profiles():
    requests.get("http://profile.internal/profiles")
```

## Disable

```bash
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=requests
```

Outbound calls then fall through to the [urllib3](../urllib3/README.md) layer,
which records them one level lower if it is still enabled.

## See also

- Example: [`flask_demo.py`](../../../examples/flask/flask_demo.py) +
  [`flask_upstream.py`](../../../examples/flask/flask_upstream.py) — a two-service
  distributed trace over requests
- Unit tests: [`test_http_client_dedupe.py`](../../../tests/unit/instrumentations/test_http_client_dedupe.py)
- [Custom Instrumentation Guide §9 — HTTP client tracing](../../../docs/custom_instrumentation.md) ·
  [Configuration Guide](../../../docs/config.md)
