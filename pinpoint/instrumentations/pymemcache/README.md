# Memcached (pymemcache)

Instruments [pymemcache](https://pymemcache.readthedocs.io/). Every cache
command becomes a span event on the current transaction.

| | |
|---|---|
| Target module | `pymemcache.client.base` |
| Hook point | the command methods on `Client` |
| Span role | Span event (child of the caller's transaction) |
| Service type | `MEMCACHED` |
| Opt-out alias | `pymemcache` |

pymemcache exposes its commands on three client classes, and wrapping the base
`Client` covers all of them:

- `Client` — single server, text protocol (wrapped directly);
- `PooledClient` — delegates to `Client`, so it inherits the wrappers;
- `HashClient` (in `pymemcache.client.hash`) — constructs `Client` instances on
  demand, which also inherit.

## What gets traced

One span event per command — `get`, `get_many`, `gets`, `gets_many`, `set`,
`set_many`, `set_multi`, `add`, `replace`, `append`, `prepend`, `cas`, `delete`,
`delete_many`, and the rest of the canonical surface — named after the wrapped
API method and annotated with:

- the destination server (`host:port`, or the unix socket path);
- the cache key, where the command has one.

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
from pymemcache.client.base import Client

cache = Client("cache.internal:11211")
cache.set("user:1", b"alice")
# └─ span event: MEMCACHED  set   (key "user:1", cache.internal:11211)
cache.get("user:1")
# └─ span event: MEMCACHED  get
```

```python
# Pooled and sharded clients need nothing extra
from pymemcache.client.hash import HashClient

cache = HashClient(["cache-a.internal:11211", "cache-b.internal:11211"])
cache.get("user:1")   # traced, annotated with the shard actually used
```

## See also

- Unit tests: [`test_pymemcache_instrumentation.py`](../../../tests/unit/instrumentations/test_pymemcache_instrumentation.py)
- [Custom Instrumentation Guide](../../../docs/custom_instrumentation.md)
