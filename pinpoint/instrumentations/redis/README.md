# Redis (redis-py)

Instruments [redis-py](https://redis.readthedocs.io/), both the synchronous
client and the asyncio one.

| | |
|---|---|
| Target modules | `redis.client`, `redis.asyncio.client` |
| Hook points | `Redis.execute_command`, `Pipeline.execute` (both sync and async) |
| Span role | Span event (child of the caller's transaction) |
| Service type | `REDIS` |
| Opt-out aliases | `redis` (both), or `redis.client` / `redis.asyncio.client` for one |

The async client is a **distinct class**, so it gets its own hooks. Without them
a FastAPI/asyncio service using `redis.asyncio` would silently lose every Redis
node from its traces while the sync client stayed traced. The async wrappers
mirror the sync ones and await the wrapped coroutine inside the span-event scope.

## What gets traced

- One span event per command. The event operation is the wrapped API method, and
  the Redis command itself is recorded as metadata (so `GET`, `SETEX`, `HGETALL`
  are all visible without exploding operation-name cardinality).
- The destination endpoint (`host:port`), resolved once per connection and
  cached.
- The first argument (typically the key) is annotated, so you can see *which*
  key a slow command touched.
- **Pipelines** produce one event for the `execute()` round-trip, not one per
  queued command — that is the actual network operation.

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
import redis

r = redis.Redis(host="cache.internal")
r.get("user:1")
# └─ span event: REDIS  Redis.execute_command   (GET user:1, cache.internal:6379)

with r.pipeline() as pipe:
    pipe.get("user:1").get("user:2")
    pipe.execute()
    # └─ one span event for the pipeline round-trip
```

```python
# asyncio client — same events
import redis.asyncio as aioredis

async def get_user(uid):
    r = aioredis.Redis(host="cache.internal")
    return await r.get(f"user:{uid}")
```

## Disable

```bash
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=redis                 # both clients
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=redis.asyncio.client  # async client only
```

## See also

- Unit tests: [`test_redis_instrumentation.py`](../../../tests/unit/instrumentations/test_redis_instrumentation.py)
- [Custom Instrumentation Guide](../../../docs/custom_instrumentation.md)
