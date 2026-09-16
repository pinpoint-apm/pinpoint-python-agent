# Pinpoint Python Agent — Custom Instrumentation Guide

Reference for adding Pinpoint tracing to a library, framework, or in-house
application the agent does not already cover. It maps the public API surface of
`pinpoint-python-agent` to the situations you would reach for it, and shows the
patterns the first-party instrumentations rely on.

The vocabulary — Span, SpanEvent, Annotation, propagation headers, service
types — is Pinpoint's own, so anything you record through this API renders in
the Pinpoint UI like the spans the bundled integrations produce.

---

## 1. Core Concepts

A **transaction** is a tree of spans rooted at a single entry point — an HTTP
request, an RPC method, a queue message, a scheduled job.

| Type | Where it comes from | What it represents |
|---|---|---|
| `Agent` | [`pinpoint.init()`](../pinpoint/agent.py) returns the process-wide singleton; reachable via [`pinpoint.get_agent()`](../pinpoint/agent.py). | Connection to the collector, span factory, lifecycle owner. |
| `Span` | [`Agent.new_span(operation, rpc_point, headers=…)`](../pinpoint/agent.py) or any auto-instrumentation. | One transaction. Top-level row in the Pinpoint call tree. |
| `SpanEvent` | [`Span.new_span_event(operation, service_type=…)`](../pinpoint/tracer.py). | An operation *inside* a span — a DB query, HTTP client call, function block. Multiple events form the call-stack view. |
| Annotation | `Span.annotate_*` / `SpanEvent.annotate_*`. | Structured key/value metadata attached to a span or event. |
| Propagator | [`pinpoint.propagator`](../pinpoint/propagator.py). | `extract_pinpoint_headers` and `inject_items` for distributed-tracing headers (`Pinpoint-TraceID`, …). |
| Service type | Integer codes in [`pinpoint.service_type`](../pinpoint/service_type.py). | Tells the Pinpoint UI what icon / color to render for each span and event. |
| Annotation key | Integer codes in [`pinpoint.annotation`](../pinpoint/annotation.py). | Pre-assigned keys for common fields (`HTTP_URL`, `HTTP_STATUS_CODE`, `KAFKA_TOPIC`, …). |

### Current-span context

The active span lives in a
[`contextvars.ContextVar`](https://docs.python.org/3/library/contextvars.html),
so it follows coroutines, asyncio Tasks, and any caller that copies a context.
Read it with [`pinpoint.current_span()`](../pinpoint/context.py); set it with
`pinpoint.context.set_current_span(span)` / `reset_current_span(token)` when an
instrumentation has to install a span manually (HTTP middleware, signal
handlers).

asyncio Tasks inherit `contextvars` per [PEP 567](https://peps.python.org/pep-0567/);
threading does not — see [§13](#13-asynchronous-and-background-work) for the
hand-off helpers.

---

## 2. Two Ways to Instrument

Both use the same primitives; only the caller differs.

- **Manual tracing** — decorators or `with` blocks in code you own, for anything
  the auto-instrumentor cannot reach
  ([§4](#4-manual-tracing--decorators-and-context-managers)).
- **Auto-instrumentation** — a `wrapt` monkey-patch installed at import time, as
  [`pinpoint/instrumentations/`](../pinpoint/instrumentations/) does for Flask,
  Django, requests, SQLAlchemy, Kafka and the rest
  ([§14](#14-building-an-auto-instrumentation)).

---

## 3. Bootstrapping the Agent

The user (or our `pinpoint-run` launcher) initializes the agent. Instrumentation
code never bootstraps it — it reaches for `pinpoint.get_agent()` and quietly does
nothing when the agent is missing or disabled.

```python
import pinpoint

pinpoint.init(
    application_name="my-service",
    agent_name="my-service-web",
    server_info="Python Application",
    collector_host="collector.internal",
)
```

Or via the launcher, which prepends a `sitecustomize.py` to `PYTHONPATH` so the
agent is initialized before any user code runs:

```bash
pinpoint-run --app-name my-service --collector collector.internal -- python app.py
```

Every configuration option is reachable in three ways: kwargs to
[`pinpoint.init()`](../pinpoint/agent.py), `PINPOINT_PY_*` environment variables,
or an `extra_yaml=` escape hatch. See [`pinpoint/config.py`](../pinpoint/config.py)
for the full list.

### <a id="guard"></a>Defensive lookup pattern

An instrumentation runs inside the user's process and cannot assume `init()` was
called. Every entry point starts with this guard — later examples in this
document elide it:

```python
from pinpoint.agent import get_agent

def my_wrapper(wrapped, instance, args, kwargs):
    agent = get_agent()
    if agent is None or not agent.enabled:
        return wrapped(*args, **kwargs)
    # ... open span, do work ...
```

Query `enabled` on every call, never cache it: the gRPC handshake
runs in a background thread and flips it from `False` to `True` seconds after
`init()` returns.

---

## 4. Manual Tracing — Decorators and Context Managers

For code paths auto-instrumentation cannot reach — cron jobs, queue consumers,
CLI commands, library helpers.

### `@pinpoint.span` — open a root transaction

Use at an entry point. Each call becomes its own top-level row in the call tree.

```python
import pinpoint

@pinpoint.span("nightly_billing", rpc_point="/cron/billing")
def nightly_billing():
    ...

@pinpoint.span                                  # bare form
async def consume_message(msg):
    ...
```

Without `init()`, or with the agent disabled, the wrapped function runs untraced.

### `@pinpoint.spanevent` — record a child event

Adds a span event under whichever span is currently active. No-op (function still
runs) when no span is current.

```python
@pinpoint.spanevent("billing.charge")
def charge(amount):
    ...

@pinpoint.spanevent
def helper():
    ...
```

### `pinpoint.trace(...)` — context-manager equivalent

```python
with pinpoint.trace("compute_invoice"):
    run_business_logic()
```

Errors raised inside the block are recorded on the event before being re-raised.

These decorators are aimed at *users*. An instrumentation author works one level
lower — `Span` / `SpanEvent` directly — to attach implementation-specific
metadata (SQL text, HTTP URL, topic) the generic decorators know nothing about.

---

## 5. Creating Spans for Incoming Work

A **root span** marks the entry of a transaction: HTTP request handler, RPC
server method, message consumer callback, scheduled job. Create one at the
outermost point you can intercept.

### `Agent.new_span(operation, rpc_point, headers=…)`

```python
span = agent.new_span(
    operation="Custom HTTP Server",  # human-readable label shown in the UI
    rpc_point="/users/<id>",         # the routed path / RPC method
    headers=upstream_headers,        # case-insensitive Mapping[str, str]
)
```

With no `Pinpoint-*` entries in `headers`, the span starts a **new** transaction;
with them (`Pinpoint-TraceID`, `Pinpoint-SpanID`, …) it **continues** the parent
transaction and is linked into its trace. Header lookup is case-insensitive.

### Make it the current span

Child code and other auto-instrumentations find the root span through the
contextvar, so it has to be installed there:

```python
token = set_current_span(span)      # from pinpoint.context
try:
    return inner(...)
finally:
    reset_current_span(token)
    span.end()
```

Or use the span as a context manager, which does both:

```python
with span:                       # set_current_span on enter, end+reset on exit
    return inner(...)
```

### Record request properties before ending

```python
from pinpoint.annotation import ANNOTATION_HTTP_URL

span.set_remote_address(client_ip)      # who called us
span.set_end_point(host_header)         # our logical endpoint (Host: header)
span.set_acceptor_host(host_header)     # mirror of end_point for the UI
span.set_status_code(response.status)
span.annotate_string(ANNOTATION_HTTP_URL, full_url)
span.set_url_stat(url_pattern, method, status)   # for the URL-stat aggregator
span.set_logging()                      # ids were written to an app log line
```

`set_logging()` is what the stdlib `logging` integration calls when it stamps
the trace/span ids on a record; call it yourself only when another logger
writes `span.trace_id` / `span.span_id_str` (see the
[logging integration](../pinpoint/instrumentations/logging_ext/README.md)).

### One-shot HTTP server / client helpers

`pinpoint.http_helper` is a Python port of
`pinpoint::helper::TraceHttp{Server,Client}{Request,Response}` from
`include/pinpoint/tracer.h`. It packages the call sequences above into four
functions that buffer metadata on the Python wrappers (flushed in the one
native finalize call at `end()`), and records request/response headers only
when the allow-list configured under `Http.{Server,Client}.Record*Header`
(or `PINPOINT_PY_HTTP_{SERVER,CLIENT}_RECORD_REQUEST_HEADER` etc.) is set —
so the common request pays no extra native calls. A
`Pinpoint-Proxy{Apache,Nginx,App}` monitoring header is likewise parsed
interpreter-side and buffered as one composite proxy annotation.

```python
from pinpoint.http_helper import (
    trace_http_server_request,
    trace_http_server_response,
    trace_http_client_request,
    trace_http_client_response,
)
from pinpoint.service_type import SERVICE_TYPE_PYTHON_HTTP_CLIENT

# inbound (root span):
trace_http_server_request(
    span,
    remote_addr=environ["REMOTE_ADDR"],   # also honours X-Forwarded-For / X-Real-Ip
    endpoint=host_header,
    request_headers=request.headers,      # any Mapping or list[(name, value)]
    cookie_reader=parsed_cookies,         # optional; matches RecordRequestCookie
)
# … later, after status is known …
trace_http_server_response(
    span, url_pattern, method, status_code, response_headers,
)

# outbound (span event). The helper stamps SERVICE_TYPE_PYTHON_HTTP_CLIENT
# (9900) on the event itself; pass it to new_span_event too, so an unsampled
# transaction — which skips the helper — still names the event correctly.
event = span.new_span_event("requests.sessions.Session.send",
                            service_type=SERVICE_TYPE_PYTHON_HTTP_CLIENT)
trace_http_client_request(
    event, host, url, request.headers,
    cookie_reader=parsed_cookies,         # optional; matches RecordRequestCookie
)
trace_http_client_response(event, response.status_code, response.headers)
```

The bundled WSGI/ASGI/Flask/Django/Pyramid/Falcon/Tornado/aiohttp integrations
already call these — you need them only when writing a new HTTP framework
integration by hand.

### Inspecting span state

```python
span.trace_id        # str — distributed-trace ID
span.span_id         # int — local span ID
span.sampled         # bool — False means data won't be shipped; skip expensive
                     #         annotation collection in that branch
```

### Rules of the road

- Every successful `new_span()` **must** be paired with `span.end()`. Prefer the
  context-manager form so exits and exceptions both close the span.
- Always install the span on the contextvar — without it, child instrumentations
  (DB drivers, HTTP clients) find no parent and silently drop their events.
- Pick a descriptive, stable server `operation` (`Flask HTTP Server`) and keep the
  routed path in `rpc_point` (`/users/<id>`) — the latter feeds the URL-stat
  aggregator.

---

## 6. Recording Span Events

Span events form the call stack inside a span: an outbound HTTP call, a SQL
query, a cache lookup, a business-logic block.

### Basic usage

`Span.new_span_event(...)` opens the event, records any raised exception on it,
and ends it on exit:

```python
with span.new_span_event("redis.client.Redis.execute_command", service_type=SERVICE_TYPE_REDIS) as event:
    event.set_end_point("redis.internal:6379")
    event.set_destination("session-cache")
    return client.get(key)
```

Where an event's scope does not match a block, pair `new_span_event()` with
`event.end()` in a `finally` by hand, under the pairing rules below.

### Nesting

Each `new_span_event` opens one level deeper in the call-stack view:

```python
with span.new_span_event("processRequest"):
    with span.new_span_event("validateInput"):
        validate(payload)
    with span.new_span_event("businessLogic"):
        with span.new_span_event("queryDatabase", service_type=SERVICE_TYPE_MYSQL):
            run_query()
```

### Per-event properties

```python
event.set_service_type(int_code)        # one of SERVICE_TYPE_*
event.set_operation_name("SELECT users")
event.set_destination("user_database")  # logical name of the downstream
event.set_end_point("mysql:3306")       # physical endpoint
event.set_sql_query(sql, args=[])       # typed bind values — see §10
event.set_error(exc_or_name, message=None)
event.annotate_string(key, value)
event.annotate_int(key, value)
event.annotate_string_string(key, s1, s2)
```

All setters return `self`, so the fluent style works:

```python
event.set_service_type(SERVICE_TYPE_REDIS).set_destination("session-cache").set_end_point(...)
```

### Pairing rules

- One `new_span_event()`, exactly one `event.end()`. The context-manager form and
  the `@spanevent` decorator handle this for you.
- Do not keep an event open across an `await` if the same span will be ended by
  another coroutine in the meantime. Events are serialized per span; an
  over-long-lived event pins the span until you end it.

---

## 7. Annotations

Annotations enrich spans and events with structured metadata. The Pinpoint UI
knows how to render the predefined keys — status codes in the response panel, SQL
in the database panel, Kafka topic in the messaging panel.

### Predefined keys

Defined in [`pinpoint/annotation.py`](../pinpoint/annotation.py):

```python
from pinpoint.annotation import (
    ANNOTATION_API,
    ANNOTATION_HTTP_URL,
    ANNOTATION_HTTP_COOKIE,
    ANNOTATION_HTTP_STATUS_CODE,
    ANNOTATION_HTTP_REQUEST_HEADER,
    ANNOTATION_HTTP_RESPONSE_HEADER,
    ANNOTATION_KAFKA_TOPIC,
    ANNOTATION_KAFKA_PARTITION,
    ANNOTATION_KAFKA_OFFSET,
    ANNOTATION_RABBITMQ_EXCHANGE,
    ANNOTATION_RABBITMQ_ROUTINGKEY,
    ANNOTATION_MONGO_JSON_DATA,
    ANNOTATION_MONGO_COLLECTION_INFO,
    ANNOTATION_ELASTICSEARCH_DSL,
)
```

### Usage

```python
span.annotate_string(ANNOTATION_HTTP_URL, "https://api.example.com/users/123")
event.annotate_int(ANNOTATION_HTTP_STATUS_CODE, 200)
event.annotate_string_string(ANNOTATION_MONGO_JSON_DATA, query_json, bindings_json)
```

### Custom keys

For a key Pinpoint does not predefine, pick a value high enough to avoid
collisions (the predefined range stays under ~1000) and keep it local to one
integration — two callers sharing a custom key in the same trace must agree on
its meaning.

### What not to record

Annotations land in the Pinpoint datastore and are visible to anyone with UI
access. No passwords, secrets, full card numbers, or free-text PII. Sanitize
bound SQL parameters and request payloads before annotating.

---

## 8. Distributed Tracing — Inject and Extract

Pinpoint propagates a transaction across services using the `Pinpoint-*` string
headers defined as `HEADER_*` constants in
[`pinpoint/propagator.py`](../pinpoint/propagator.py). You almost never write or
parse them directly — the agent does it for you.

### Receiving — extract

Pass the inbound headers (any case-insensitive `Mapping[str, str]`) to
`Agent.new_span(..., headers=...)`; the agent extracts the propagation headers
and seeds the trace context from them:

```python
span = agent.new_span("Custom HTTP Server", path, headers=incoming_headers)
```

For non-HTTP transports (queue messages, gRPC metadata, custom protocols),
flatten the carrier into a `Mapping[str, str]` and pass it the same way. See the
[Kafka](../pinpoint/instrumentations/kafka/__init__.py) and
[aio-pika](../pinpoint/instrumentations/aio_pika/__init__.py) integrations for
carriers where the headers live on a message object.

### Sending — inject

```python
from pinpoint.propagator import inject_items

for k, v in inject_items(span):   # yields the Pinpoint-* pairs
    request.headers[k] = v        # apply to whatever carrier you have
```

`inject_items` yields nothing when the span is None, the agent is disabled, or
the native module is missing — safe to call defensively.

### Implementing a custom carrier adapter

Most inbound carriers fit `Mapping[str, str]`. For an exotic inbound shape,
pass any object with a case-insensitive `get(key: str) -> Optional[str]`
(the contract of [`pinpoint.http_helper.HeaderReader`](../pinpoint/http_helper.py));
`new_span` probes it for the ~10 `Pinpoint-*` names only.

Outbound carriers need no adapter at all: open the outbound span event first
(injection rides the innermost open event, and the generated child span id is
recorded on it), then apply the `inject_items` pairs shown above through the
carrier's own mutation API.

---

## 9. HTTP Server and HTTP Client Tracing

### Server side — open a root span per request

The agent ships two framework-agnostic middlewares you can drop in directly, or
use as a model when wrapping a new server framework:

- [`pinpoint.instrumentations.wsgi.PinpointWSGIMiddleware`](../pinpoint/instrumentations/wsgi/__init__.py)
- [`pinpoint.instrumentations.asgi.PinpointASGIMiddleware`](../pinpoint/instrumentations/asgi/__init__.py)

Both follow this skeleton (after the [§3 guard](#guard)):

```python
def __call__(self, environ, start_response):
    # 1. Wrap the inbound headers in a lazy reader — no per-request dict build;
    #    new_span() also accepts any Mapping[str, str].
    headers = EnvironHeaderReader(environ)  # from pinpoint.http_helper

    # 2. Open the root span (continues the trace if upstream headers present).
    span = agent.new_span("WSGI HTTP Server", path, headers=headers)
    span.set_remote_address(client_ip)
    span.set_end_point(host)
    span.annotate_string(ANNOTATION_HTTP_URL, url)

    # 3. Install on the contextvar so child code sees it as the current span.
    token = set_current_span(span)

    try:
        return self._app(environ, wrapped_start_response)
    except Exception as exc:
        span.set_error(exc)
        raise
    finally:
        reset_current_span(token)
        span.set_status_code(status_holder["code"])
        span.set_url_stat(url_pattern, method, status_holder["code"])
        span.end()
```

Details worth copying:

- Wrap the framework's `start_response` (WSGI) or `send` (ASGI) callable to read
  the status code. Frameworks rarely hand it back any other way.
- Stash the matched route template — Flask's `request.url_rule.rule`, Starlette's
  `scope["route"]`, others vary — and pass it as `url_pattern` to `set_url_stat`.
  The URL-stat aggregator needs bucket-by-route, not bucket-by-concrete-URL.
- A WSGI response body may be drained by a different server thread. Create a
  linked async child on the request thread, end the request root there, and let
  the drain thread exclusively own and end the child.

### Client side — record outbound calls as span events

```python
from pinpoint.context import current_span
from pinpoint.propagator import inject_items
from pinpoint.service_type import SERVICE_TYPE_PYTHON_HTTP_CLIENT
from pinpoint.annotation import ANNOTATION_HTTP_URL, ANNOTATION_HTTP_STATUS_CODE

def _send_wrapper(wrapped, instance, args, kwargs):
    span = current_span()
    if span is None:
        return wrapped(*args, **kwargs)

    request = args[0]
    event = span.new_span_event(
        "requests.sessions.Session.send",
        service_type=SERVICE_TYPE_PYTHON_HTTP_CLIENT,
    )
    event.set_end_point(host).set_destination(host)
    event.annotate_string(ANNOTATION_HTTP_URL, request.url)

    for k, v in inject_items(span):   # trace headers onto the outgoing request
        request.headers[k] = v

    try:
        response = wrapped(*args, **kwargs)
    except Exception as exc:
        event.set_error(exc)
        event.end()
        raise

    event.annotate_int(ANNOTATION_HTTP_STATUS_CODE, response.status_code)
    event.end()
    return response
```

See [`instrumentations/requests/`](../pinpoint/instrumentations/requests/__init__.py)
for the production version. The shape is identical for `httpx`, `urllib3`,
`aiohttp` client, and any library exposing a single
"actually-send-the-request" method to hook.

---

## 10. Database and Backend Instrumentation

PEP 249 (DB-API 2.0) gives every Python SQL driver — pymysql, mysql-connector,
psycopg, MySQLdb, sqlite3 — the same `connect()` /
`Cursor.execute(sql[, params])` shape. The shared base in
[`pinpoint.instrumentations.dbapi`](../pinpoint/instrumentations/dbapi/__init__.py)
turns that shape into a span event with consistent annotations.

### Synchronous drivers — `wrap_cursor_class`

```python
from pinpoint.instrumentations.dbapi import wrap_cursor_class
from pinpoint.service_type import SERVICE_TYPE_MYSQL

def _instrument():
    wrap_cursor_class(
        module="pymysql.cursors",
        cursor_qualname="Cursor",
        service_type=SERVICE_TYPE_MYSQL,
    )
```

That one call wraps `execute`, `executemany`, and `callproc`, opens a span event
named after the wrapped cursor API (`pymysql.cursors.Cursor.execute`), extracts
the connection's `host`/`port`/`database` for `end_point` and `destination`, and
records the SQL via `event.set_sql_query(sql, params)`. Bound values are omitted
by default because they can contain credentials or PII. With
`sql_trace_bind_values=True` (or `PINPOINT_PY_SQL_TRACE_BIND_VALUE=true`), a
cursor's `mogrify` output is preferred when available and the native agent caps
the recorded value at `sql_max_bind_args_size` bytes. A config file works too,
but its default differs — see
[Configuration Guide § SQL](config.md#sql-configuration).

### Async drivers — `wrap_async_cursor_class`

Same signature, but produces `async def` wrappers for drivers whose `execute` is
a coroutine (aiomysql, aiopg).

### Non-DB-API drivers

`asyncpg` speaks PostgreSQL natively and does not implement DB-API. The
[`asyncpg` integration](../pinpoint/instrumentations/asyncpg/__init__.py) wraps
each coroutine query method individually but follows the same annotation contract
— endpoint, destination, `set_sql_query`. Use it as a template for any client
with its own wire protocol.

### Service types

Take the constant for your backend from
[`pinpoint/service_type.py`](../pinpoint/service_type.py) — `SERVICE_TYPE_MYSQL`,
`SERVICE_TYPE_POSTGRESQL`, `SERVICE_TYPE_REDIS`, and the rest. The value each
bundled integration uses is listed in the
[Auto-Instrumentation Catalog](auto_instrumentation.md#databases).

### Recording rules

- One span event per query — not per cursor, not per transaction.
- `end_point` is the physical address (`mysql.internal:3306`); `destination` is
  the logical name (the schema/database, the cache namespace).
- `set_sql_query` accepts `None`, strings, booleans, integers, and floats in a
  list or tuple, formatted as comma-separated bind values. Any other element type
  (`datetime`, `Decimal`, `UUID`, …) is recorded as `str(value)`, and any other
  `args` object (a dict of named params, bytes, …) is recorded whole as a single
  `str(value)` — passing driver params through never raises into the traced
  query. A single string is still accepted for compatibility.
- Keep `sql_trace_bind_values=False` unless literal values are required.
  `sql_enable_raw_sql_cache=True` avoids repeating normalization for identical
  raw SQL; `sql_max_bind_args_size` bounds the recorded bind-value annotation.

---

## 11. Message Queues — Producer and Consumer

Brokers are the canonical distributed-tracing case across an async hop. The
pattern is symmetric: producers **inject** headers into the outgoing message,
consumers **extract** them when opening the root span for each delivery.

### Producer — record a span event on the calling transaction

Same shape as the HTTP client wrapper in [§9](#9-http-server-and-http-client-tracing);
only the carrier differs:

```python
event = span.new_span_event(
    "kafka.producer.kafka.KafkaProducer.send",
    service_type=SERVICE_TYPE_KAFKA_CLIENT,
)
event.set_destination(brokers).set_end_point(brokers)
event.annotate_string(ANNOTATION_KAFKA_TOPIC, topic)

headers_list = list(kwargs.get("headers") or [])
for k, v in inject_items(span):
    headers_list.append((k, v.encode("utf-8") if isinstance(v, str) else v))
kwargs["headers"] = headers_list
```

### Consumer — open a root span per delivered message

A consumer is a fresh entry point with no calling Python frame to inherit context
from. Open a **root span** seeded with the headers extracted from the delivery:

```python
def _consume(record, consumer):
    headers = decode_message_headers(record)
    rpc = f"kafka://topic={record.topic}?partition={record.partition}&offset={record.offset}"
    span = agent.new_span("Kafka Consumer Invocation", rpc, headers=headers)
    span.set_service_type(SERVICE_TYPE_KAFKA_CLIENT)
    span.set_end_point(broker)
    span.annotate_int(ANNOTATION_KAFKA_OFFSET, record.offset)
    span.end()
```

If the framework dispatches to a user handler, install the span on the contextvar
around that call (`set_current_span` / `reset_current_span`, or `with span:`) so
child instrumentations can find it. See
[`kafka/`](../pinpoint/instrumentations/kafka/__init__.py) and
[`aio_pika/`](../pinpoint/instrumentations/aio_pika/__init__.py) for full
implementations.

---

## 12. Error Reporting and Call Stacks

### Recording errors

Both `Span` and `SpanEvent` expose `set_error`:

```python
span.set_error("DatabaseError", "Connection timeout after 30s")
event.set_error(exception_instance)   # accepts BaseException; extracts type + str
event.set_error("MyError")            # name only
```

Given an `Exception`, the agent reads `type(exc).__name__` and `str(exc)` — no
need to format them yourself.

### Call-stack capture

With `Config.enable_callstack_trace` on (`PINPOINT_PY_ENABLE_CALLSTACK_TRACE=true`
or `init(enable_callstack_trace=True)`), `set_error` attaches a Python frame trace
to the span event automatically: the [`callstack.frames_for`](../pinpoint/callstack.py)
hook fires inside `set_error` and ships up to 64 frames.

### Exception-handling pattern in a wrapper

```python
try:
    result = wrapped(*args, **kwargs)
except BaseException as exc:
    if isinstance(exc, Exception):
        try:
            event.set_error(exc)
        except Exception:
            pass
    event.end()
    raise
event.end()
return result
```

Catch, record and re-raise follow
[API Contracts §9](api_contracts.md#9-error-recording-and-call-stacks). One rule
is specific to wrappers: guard the `set_error` call itself — the agent must never
send the user's code down a different error path.

---

## 13. Asynchronous and Background Work

A Span is single-threaded ([API Contracts §1](api_contracts.md#1-a-span-is-single-threaded)):
a framework moving a synchronous handler to a worker thread, or request
processing into an internal Task, is a sequential continuation and keeps the same
current span. For independently scheduled work, the originator creates an
**async span**, hands it off, and the worker enters it.

Plain `asyncio.create_task()` inherits the current ContextVar and is forked
lazily on its first `current_span()` lookup; those *implicit* task spans carry a
lifetime cap so a fire-and-forget task cannot pin a native span for the process
lifetime ([API Contracts §7](api_contracts.md#7-async-spans)).

### The hand-off — `async_trace` / `new_async_span`

The originator creates the linked child; the worker enters and ends it:

```python
import pinpoint, threading

with pinpoint.async_trace("background") as async_span:
    if async_span is not None:
        threading.Thread(target=worker, args=(async_span,)).start()
    else:
        threading.Thread(target=worker_untraced).start()

def worker(async_span):
    with async_span:                      # set as current span; end() on exit
        with async_span.new_span_event("step"):    # child events attach normally
            do_work()
```

Or `Span.new_async_span` directly, for full control:

```python
with span.new_span_event("schedule_work"):     # async link is recorded against this event
    async_span = span.new_async_span("background")
# hand `async_span` to the worker; it calls `with async_span:` to enter and end.
```

The async link is recorded against the parent's **currently active span
event**, so `new_async_span` must be called inside an enclosing `span.new_span_event(...)`
block. Outside one it returns a no-op span and the async work is silently
untracked.

### Rules

- Per PEP 567, asyncio Tasks inherit `contextvars` automatically. You still want
  an async span when the work runs concurrently with the awaiter — the UI then
  renders it as a separate sub-trace with its own timing, instead of folding it
  into the calling task.
- Threads do **not** inherit contextvars; the worker's `with async_span:`
  installs the async span on the worker thread's context.
- A contextvars copy into a framework threadpool resolves to a detached no-op
  view, so instrumentation must hand the worker a linked async child explicitly.
  The bundled Starlette/FastAPI `run_in_threadpool` and asgiref `sync_to_async`
  integrations install exactly that hand-off, keeping sync endpoints, sync
  dependencies and Django-ASGI sync sections traced.

---

## 14. Building an Auto-Instrumentation

Everything above applies to instrumenting code you own. Adding an integration to
**the agent itself** — an entry in its autoload registry, a package under
`pinpoint/instrumentations/`, its tests and demo — is contributor work and lives
in the [Development Guide § 7](development.md#7-adding-a-bundled-auto-instrumentation).

To wrap a third-party library from your own codebase without modifying the
agent, patch it yourself (`wrapt.wrap_function_wrapper`, a decorator, a
subclass) and open spans from the wrapper with the API in §5–§13. The
[bundled integrations](../pinpoint/instrumentations/) are worked examples of
exactly that shape.

---

## See Also

- [Auto-Instrumentation Catalog](auto_instrumentation.md) — per-library READMEs
- [API Contracts](api_contracts.md) — the rules span objects enforce
- [Configuration Guide](config.md) — every option and environment variable
- [Pinpoint concepts overview](https://pinpoint-apm.gitbook.io/pinpoint/want-a-quick-tour/techdetail)
- [Pinpoint plugin development guide](https://pinpoint-apm.gitbook.io/pinpoint/documents/plugin-dev-guide)
