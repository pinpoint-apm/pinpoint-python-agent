# Tornado

Instruments [Tornado](https://www.tornadoweb.org/)'s HTTP server. Tornado has
its own asyncio stack — no WSGI, no ASGI — so this integration hooks the
framework directly.

| | |
|---|---|
| Target module | `tornado.web` |
| Hook points | `RequestHandler._execute` (root span), `RequestHandler.log_exception` (error capture) |
| Service type | `PYTHON_HTTP_SERVER` (span), `PYTHON_METHOD` (handler event) |
| Opt-out alias | `tornado` |

## What gets traced

- **Root span per request** — `RequestHandler._execute` is the framework
  coroutine that resolves arguments, runs `prepare()`, dispatches to
  `get`/`post`/…, and calls `finish()`. Wrapping it covers the entire request
  lifecycle from a single point.
- **Handler span event** — named after the resolved handler method
  (`UserHandler.get`), which is the most user-meaningful identifier in Tornado.
- **URL statistics** are recorded against the request **path**, not a route
  template. Set `http_url_stat_enable_trim_path` (on by default) to keep
  high-cardinality paths from flooding the URL-stat table — see the
  [Configuration Guide](../../../docs/config.md).
- **Errors** — `log_exception` is the canonical sink Tornado routes every
  request exception through, including the ones it converts to an HTTP error
  response, so the span gets the error even when the handler itself never raises
  to the caller.
- Upstream `Pinpoint-*` headers stitch the request into the caller's trace.

## Usage

No code changes:

```bash
pinpoint-run --app-name my-app --collector localhost -- python app.py
```

```python
import asyncio
import tornado.web

class UserHandler(tornado.web.RequestHandler):
    async def get(self, uid):
        # Root span   : Tornado HTTP Server  (GET /users/1)
        #  └─ event   : UserHandler.get
        self.write({"id": uid})

async def main():
    app = tornado.web.Application([(r"/users/(\d+)", UserHandler)])
    app.listen(8888)
    await asyncio.Event().wait()

asyncio.run(main())
```

## See also

- Example: [`tornado_demo.py`](../../../examples/tornado/tornado_demo.py)
- Unit tests: [`test_tornado_instrumentation.py`](../../../tests/unit/instrumentations/test_tornado_instrumentation.py)
- [Custom Instrumentation Guide](../../../docs/custom_instrumentation.md) ·
  [Configuration Guide](../../../docs/config.md)
