# httpx

Instruments the [httpx](https://www.python-httpx.org/) HTTP client, sync and
async. Every outbound request becomes a span event on the current transaction
with Pinpoint trace headers injected.

| | |
|---|---|
| Target module | `httpx._client` |
| Hook points | `Client.send`, `AsyncClient.send` |
| Span role | Span event (child of the caller's transaction) |
| Service type | `PYTHON_HTTP_CLIENT` |
| Opt-out alias | `httpx` |

Both `send` methods live in `httpx._client` and both receive a fully-built
`Request` — the same hook shape the [requests integration](../requests/README.md)
uses. Every `httpx.get(...)`, `client.post(...)`, and streaming call funnels
through them.

## What gets traced

- One span event per outbound request, annotated with method, URL, destination
  host, response status, and the configured client header allow-list.
- `Pinpoint-*` headers are injected so the callee continues this trace.
- The wrapper honors the HTTP-client suppression scope, so a higher-level client
  that already recorded the exchange does not get a duplicate event.
- With no current span, the call passes through untouched.

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
import httpx

# Sync
httpx.get("http://profile.internal/profiles/1")

# Async — same span event, awaited inside the event scope
async def fetch():
    async with httpx.AsyncClient() as client:
        await client.get("http://profile.internal/profiles/1")
# └─ span event: GET http://profile.internal/profiles/1   (Pinpoint-* injected)
```

## See also

- Unit tests: [`test_httpx_instrumentation.py`](../../../tests/unit/instrumentations/test_httpx_instrumentation.py)
- [Custom Instrumentation Guide §9 — HTTP client tracing](../../../docs/custom_instrumentation.md) ·
  [Configuration Guide](../../../docs/config.md)
