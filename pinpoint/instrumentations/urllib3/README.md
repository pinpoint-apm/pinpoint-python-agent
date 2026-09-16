# urllib3

Instruments [urllib3](https://urllib3.readthedocs.io/) at the connection pool.
This is the transport under `requests`, so it doubles as a safety net: direct
urllib3 users get traced, and so does any library that reaches the network
through it.

| | |
|---|---|
| Target module | `urllib3.connectionpool` |
| Hook point | `HTTPConnectionPool.urlopen` |
| Span role | Span event (child of the caller's transaction) |
| Service type | `PYTHON_HTTP_CLIENT` |
| Opt-out alias | `urllib3` |

## What gets traced

- One span event per outbound request, annotated with method, URL, destination
  host, and response status.
- `Pinpoint-*` headers are injected into the outgoing request.
- **Deferred to the higher-level client when there is one.** The
  [requests](../requests/README.md) and
  [elasticsearch](../elasticsearch/README.md) integrations open a suppression
  scope around their exchange, and this wrapper checks it — so a
  `requests.get()` produces one span event at the requests layer, not two.
  Only calls that reach urllib3 without a recognized higher-level client are
  recorded here.
- With no current span, the call passes through untouched.

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
import urllib3

http = urllib3.PoolManager()
http.request("GET", "http://profile.internal/profiles/1")
# └─ span event: GET http://profile.internal/profiles/1   (Pinpoint-* injected)
```

## Disable

```bash
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=urllib3
```

`requests` traffic is unaffected — it is recorded one layer up.

## See also

- Unit tests: [`test_http_client_dedupe.py`](../../../tests/unit/instrumentations/test_http_client_dedupe.py)
- [requests integration](../requests/README.md) ·
  [Custom Instrumentation Guide §9](../../../docs/custom_instrumentation.md)
