# Elasticsearch

Instruments the official
[Elasticsearch Python client](https://elasticsearch-py.readthedocs.io/) at the
transport layer, so every call the client issues — `search`, `index`, `get`,
`bulk`, … — becomes a single span event. Modeled on pinpoint-go-agent's
`plugin/goelastic` and OpenTelemetry's
`opentelemetry-instrumentation-elasticsearch`.

| | |
|---|---|
| Target modules | `elastic_transport` (8.x), `elasticsearch` (7.x) |
| Hook point | `Transport.perform_request` / `AsyncTransport.perform_request` |
| Span role | Span event (child of the caller's transaction) |
| Service type | `ELASTICSEARCH` |
| Opt-out alias | `elasticsearch` |

Two client lineages are supported and whichever is installed gets wrapped:

- **7.x** ships its own transport inside the package as `elasticsearch.Transport`;
- **8.x** split it into the separate `elastic_transport` package, exposing
  `Transport` (sync) and `AsyncTransport` (async), both with the same
  `perform_request` entry point.

Wrapping the transport rather than every client method (`Elasticsearch.search`,
`.index`, …) gives full coverage with one hook and stays stable across versions.

## What gets traced

- One span event per request, with the HTTP method, path, destination endpoint,
  and response status.
- The **rendered DSL** — the request body, or the `q` query parameter —
  truncated to 256 characters, matching the Go agent exactly so the Pinpoint UI
  renders DSL the same way for Python and Go services.
- **No duplicate event for the HTTP transport.** The wrapper opens a suppression
  scope, so the [urllib3](../urllib3/README.md) /
  [requests](../requests/README.md) layer underneath stays quiet. One search =
  one span event.

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
from elasticsearch import Elasticsearch

es = Elasticsearch("http://es.internal:9200")
es.search(index="products", query={"match": {"name": "keyboard"}})
# └─ span event: ELASTICSEARCH  POST /products/_search
#    DSL: {"query":{"match":{"name":"keyboard"}}}   (first 256 chars)
```

```python
# 8.x async client — same event
from elasticsearch import AsyncElasticsearch

async def search(term):
    es = AsyncElasticsearch("http://es.internal:9200")
    return await es.search(index="products", query={"match": {"name": term}})
```

## Disable

```bash
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=elasticsearch
```

Requests then surface one layer down as plain HTTP-client events, if
`urllib3`/`requests` are still enabled.

## See also

- Unit tests: [`test_elasticsearch_instrumentation.py`](../../../tests/unit/instrumentations/test_elasticsearch_instrumentation.py)
- [Custom Instrumentation Guide](../../../docs/custom_instrumentation.md)
