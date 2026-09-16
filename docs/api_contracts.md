# Pinpoint Python Agent — Span, SpanEvent, and Annotation Contracts

The API contracts enforced by [`Span`](../pinpoint/tracer.py) and
[`SpanEvent`](../pinpoint/tracer.py), complementing the usage patterns in the
[Custom Instrumentation Guide](custom_instrumentation.md).

Breaking one of these contracts is a **silent no-op that never raises into your
code** — the agent will not fail a request to complain about instrumentation.
The flip side is that a violation rarely announces itself: it shows up as a
distorted or missing trace. These are the rules that keep traces correct.

---

## 1. A Span Is Single-Threaded

A `Span` — including every `SpanEvent` it hands out — is meant to be driven by
**one thread (or one logical coroutine chain) for its entire lifetime**.

Sharing a span across threads cannot crash the process: every recording method
is serialized through one per-span lock, `end()` is a one-shot latch, and late
calls degrade to no-ops. What the lock does **not** buy is a correct trace —
concurrent use can drop annotations, mis-nest events, or truncate the trace.

To trace work on another thread or task, do not share the span:

- Use `pinpoint.async_trace(...)` — it creates a linked async child on the
  owning side for you to hand over.
- Or call `span.new_async_span(...)` on the owning side and hand the child
  over ([§7](#7-async-spans)).
- A `contextvars` copy to a worker thread (framework threadpools) resolves to
  a **detached no-op view** — recording there is silently dropped. This is
  deliberate; hand over an async child instead.

## 2. End Exactly Once, and Record Before Ending

`span.end()` and `event.end()` are terminal and **idempotent**: the first call
finalizes and ships the data; every later call — a manual `end()` followed by
a `with`-exit, a done-callback race — is a safe no-op.

- After `end()`, every recording method (`set_*`, `annotate_*`,
  `set_sql_query`, `set_error`) silently does nothing. Record status codes,
  errors, and annotations **before** ending.
- A span that is never ended is **never sent** — its data is lost, and it
  keeps its native buffers alive until garbage collection. Prefer the
  context-manager forms, which end on every path including exceptions:

```python
with agent.new_span("MyService", "/endpoint", headers=hdrs) as span:
    with span.new_span_event("queryDatabase", service_type=SERVICE_TYPE_MYSQL) as event:
        event.set_sql_query(sql, params)
        run_query()
# exceptions are recorded via set_error and both are ended, on every path
```

- `with span:` is re-entrant: only the **outermost** exit calls `end()`, so
  nested `with` blocks on the same span are safe and an inner one cannot leave
  later work on a dead span.

## 3. End Span Events in Nesting (LIFO) Order

Span events form a stack on the Python `Span` wrapper (which also assigns
each event's sequence and depth). Ending an outer event while an inner one is
still open implicitly finishes everything nested above it, and `span.end()`
force-finishes all still-open events — the trace survives, but the implicitly
finished events get the wrong end time. The context-manager forms
(`with span.new_span_event(...)`, `@pinpoint.spanevent`) make LIFO ordering automatic;
keep manual `new_span_event()` / `event.end()` pairs strictly nested.

Do not keep an event open across an `await` if another coroutine may end the
same span in the meantime — events are serialized per span, and an
over-long-lived event pins the span until it ends.

## 4. Annotations and Properties Are Buffered Until `end()`

`annotate_*` and most `set_*` calls are buffered rather than recorded as you
make them; a span event's `end()` only stamps its end time and hands the
completed record to its span. Finished events ship in batches of
`span_event_chunk_size` (default 20) as they complete, so a long transaction
streams instead of holding everything; the rest flushes **when `span.end()`
runs**. Buffering does not distort the trace — every event keeps the
timestamps, sequence and depth it was recorded with. Consequences:

- At most **5000 annotations** are buffered per span/event; beyond the cap
  they are dropped and a single marker annotation
  (`pinpoint: annotations truncated at 5000`) records the truncation.
- `annotate_int` travels as **int32**. Use `annotate_long` for values that
  can exceed it — Kafka offsets, byte counts, timestamps — an overflowing
  `annotate_int` is dropped at finalize.
- There is no key de-duplication: the same key twice records two annotations.
- Every `annotate_string` / `annotate_string_string` value — HTTP header
  recording included, since that is what it is built on — is cut at **64 KiB**,
  silently. The bound sits far above any real URL or header, so it fires only
  on a pathological input such as a batch-API URL built out of a long id list.
  Raw SQL is the exception: `set_sql_query` never cuts, it **drops** a
  statement over 1 MiB whole, because a truncated statement normalizes to a
  different SQL id (see [Configuration Guide § SQL](config.md#sql-configuration)).
- Every annotation byte is shipped to the collector — keep them small and
  sanitized (no secrets, no PII; see [Custom Instrumentation Guide §7](custom_instrumentation.md#7-annotations)).
- On unsampled and no-op spans, annotation calls return immediately and
  nothing is materialized — annotating defensively is free.

`set_sql_query`, recording-event `set_error`, and HTTP header recording are
buffered the same way and flushed with the annotations at `end()` — the
5000-entry cap above includes them. A recording event's `set_error` still
captures the Python call stack **where you called it**, not where the buffer is
flushed, so the recorded frames point at the failure. Overflow and unsampled
errors retain only the error name and message; they never collect call stacks
or count against the annotation cap.

HTTP recording lists, including user proxy header names, use the configuration
resolved when the span starts. Reloads affect new spans; existing spans and
async descendants retain their original lists.

A transaction has exactly one endpoint, so `Span.set_end_point()` is
**first-writer-wins**: once a non-empty endpoint is recorded, later calls are
ignored. The outermost
instrumentation is the one that knows the endpoint, and a nested hook — a
framework integration running inside a transport integration — must not
relabel the transaction. `SpanEvent.set_end_point()` stays last-wins: it
describes one outbound step. `set_status_code()` and the `annotate_*` family
are last-wins on both.

## 5. Event Depth and Count Limits

Per span, `span_max_event_depth` (default 64) allows `max + 1` nesting levels
(depths 1 through 65 by default). Total
event count is exactly `span_max_event_sequence` (default 5000). Beyond a limit,
the wrapper hands out a disabled placeholder event. Operation/timing data,
annotations, SQL, error detail, exception metadata and call stacks are all
discarded, but `set_error()` still applies the native error policy and marks
the sampled trace root with exception bit `2` when allowed. Distributed-context
injection still works, so downstream traces stay intact. Overflow limits
profiling detail; it is not a sampling or transaction-success decision. If it
happens regularly, create fewer, coarser events or raise the limits (`-1` =
unlimited).

## 6. Unsampled and No-op Spans Are Deliberately Silent

`Agent.new_span()` never returns `None`. Depending on the agent's decision you
receive one of three shapes, all with the same surface:

| | `Span` (sampled) | `UnSampledSpan` | `_NullSpan` (no-op) |
|---|---|---|---|
| When | sampling accepted | sampling rejected the transaction | request filtered by `http_server_exclude_url` / `exclude_method`, agent missing/disabled/shut down, detached views |
| `sampled` | `True` | `False` | `False` |
| `trace_id` | real | `""` | `""` |
| `span_id` | real | real | `0` |
| Recording | shipped | profiling detail dropped; error verdicts batch to native | dropped |
| `end()` | ships the span | **still reaches native** — releases lifecycle state and flushes URL stats | no-op |

Rules that follow:

- Use `span.sampled` to skip *expensive data collection only*. Do **not**
  skip creating events or injecting outbound headers:
  `propagator.inject_items` on an unsampled span still yields
  `Pinpoint-Sampled: s0`, which tells downstream agents not to trace the
  request. Skipping injection makes downstream treat the call as a brand-new
  transaction and produces broken partial traces.
- Still call `end()` on unsampled spans (the context-manager forms do) — it
  releases the native lifecycle registration and records response-time and
  URL statistics, which unsampled requests dominate when sampling is on.
- An unsampled span/event `set_error()` keeps only the error name and message
  until that same `end()` crossing. Native `Span.IgnoreErrors` and error-category
  masks decide whether its URL-stat entry is failed, even for HTTP 200. No span,
  event, `exceptionInfo`, exception metadata, or call stack is emitted. Pure
  no-op spans remain allocation-free shared no-ops and do not retain a verdict.

## 7. Async Spans

`span.new_async_span(operation)` creates a child span for hand-off to a
thread, executor, or task ([Custom Instrumentation Guide §13](custom_instrumentation.md#13-asynchronous-and-background-work)).
Its contracts:

- **Call it inside an active span event.** The native agent records the async
  link against the parent's *currently active* event, so `new_async_span`
  must run within a `with span.new_span_event(...)` block (the
  `pinpoint.async_trace(...)` helper does this for you). Outside one it
  returns a no-op span and the async work is silently untracked.
- **The recipient ends it exactly once** — typically `with async_span:` in
  the worker. `end()` is idempotent, so belt-and-suspenders cleanup
  (done-callbacks, finalizers) is safe; the high-level helpers install these
  for you, covering cancellation and never-started threads.
- The child follows the same single-thread rule on its own thread.
- The child inherits the parent's resolved config generation, including its
  callstack-capture flag. A reload between child creation and execution cannot
  change that flag; implicit asyncio-task forks follow the same rule.
- **Implicit asyncio-task spans have a lifetime cap.** A task created with
  plain `asyncio.create_task()` inherits the current span via `contextvars`
  and is forked into a task-local async span on first use. So that
  fire-and-forget tasks cannot pin a native span forever, these implicit
  spans end after `asyncio_task_span_timeout_ms` (default 5 minutes; `0`
  disables), waiting for any open event scope to close first. Explicit spans
  from `pinpoint.async_trace()` / `new_async_span()` keep their
  caller-managed lifetime.

## 8. Keep Operation and Error Names Low-Cardinality

The `operation` passed to `new_span()` / `new_span_event()` /
`new_async_span()` and the error *name* passed to `set_error()` are interned
in the native agent's bounded caches, and **every new unique string enqueues a
metadata message to the collector**. Per-request unique names churn the cache
and flood the collector:

```python
# DON'T: unique operation name per request
span.new_span_event(f"getUser-{user_id}")

# DO: fixed operation name, variable data as an annotation
event = span.new_span_event("getUser")
event.annotate_string(CUSTOM_USER_ID_KEY, user_id)
```

The `rpc_point` argument of `new_span()` is not interned — it may safely carry
the actual request path.

## 9. Error Recording and Call Stacks

- `set_error()` on the **span** records that error there; `set_error()` on a
  recording **event** records it on that step. Either one also marks the trace
  root failed. An overflow event records no step but still marks the root; an
  unsampled span/event records no profiling data but can fail its URI stat.
- Both accept an exception instance (name and message are extracted) or a
  name plus optional message. The context-manager and decorator forms record
  exceptions automatically before re-raising.
- `set_error(..., mark_error=False)` records the error but leaves the
  transaction successful.
  Use it for a failure the application handles as a normal outcome (a retried
  call, an expected lookup miss) that is still worth seeing in the trace. On
  an unsampled span or an overflow event the verdict is the only thing
  `set_error()` had to record, so with `mark_error=False` the call is a no-op.
- Call-stack capture (up to 64 Python frames, attached on
  a recording `SpanEvent.set_error`) is gated by `enable_callstack_trace`
  (default off) from the resolved native config generation fixed when the span
  starts. Reloads affect only new root spans; async children inherit the parent.
  Unsampled, no-op, and overflow paths never collect frames.
- With call-stack capture on, a recording `SpanEvent.set_error(exc)` also
  records the exception chain: `exc.__cause__` (`raise X from Y`), else
  `exc.__context__` unless `__suppress_context__` (`raise X from None`),
  repeated until the chain ends, revisits an exception, or reaches
  `exception_chain_max_depth` entries in total (default 5, `0` = unlimited).
  Every entry is one `PException` with its own frames; all entries of one
  `set_error` share `exceptionId` with `exceptionDepth` 0, 1, 2, ... and the
  event's `exceptionInfo` and `-52` chain-id annotation carry the raised
  (outermost) exception. A chain consumes one unit of
  `callstack_trace_new_throughput`; an exception without a cause costs nothing
  extra. Root `Span.set_error` never records chains.
- `callstack_trace_new_throughput` caps how much new exception-chain metadata
  is admitted per second, but **not** the cost of collecting it: the frames are
  walked before that limit applies. Turn `enable_callstack_trace` off to remove
  the capture cost itself.
- `span_ignore_errors` rules flagged `match_subclasses` / `match_cause` are
  resolved in Python on every `set_error` path (recording, overflow, unsampled);
  a match records the concrete class as usual but hands native a
  never-marking `SetIgnoredError`. Exact-name rules stay native.
- Native resolved `Span.IgnoreErrors`, `Span.ErrorMark`, and
  `Span.ErrorMarkExclude` apply to recording, overflow, and unsampled paths.
  A repeated error only ORs the same category bit, but each distinct name is
  evaluated so a later non-ignored error is not hidden by an ignored one.
- An error first reported **after** the root span has already been sent cannot
  retroactively fail it. Record errors while the transaction is still open —
  the context-manager forms do.
- In wrappers, catch `BaseException` for cleanup but record only `Exception`
  — `KeyboardInterrupt` / `SystemExit` are not errors in the Pinpoint sense —
  and always re-raise (see [Custom Instrumentation Guide §12](custom_instrumentation.md#12-error-reporting-and-call-stacks)).

## 10. Agent Lifecycle

- `pinpoint.init()` is idempotent — the second call returns the existing
  instance and ignores its kwargs. Unknown kwargs are ignored with a
  `WARNING` naming the closest real option.
- **One transaction per thread at a time.** Opening a root span while another
  is active on the same thread (two integrations tracing one entry point, or
  `@pinpoint.span` on a function called inside a traced request) splits the
  work into two unrelated transactions. The second span is created anyway — the
  trace is split, not lost — and one `WARNING` per process names the problem,
  because failing a user request over an instrumentation mistake would be the
  worse trade. Use `pinpoint.trace()` / `@pinpoint.spanevent` for work
  inside an existing transaction. A worker thread that inherited a copied
  context, and a span left bound after `end()`, are not nesting and stay
  silent.
- `agent.enabled` flips `True` only after the background gRPC registration
  succeeds, seconds after `init()` returns. Query it per call as a fast-fail
  guard; never treat it as a startup success check and never cache `False`.
  See [Verifying Agent Startup](troubleshooting.md#verifying-agent-startup).
- `pinpoint.shutdown()` is terminal for that agent instance and idempotent.
  Spans created before the shutdown may still be ended safely afterwards;
  their data is dropped. A later `init()` builds a fresh agent — see
  [Stopping and Resuming](troubleshooting.md#stopping-and-resuming-the-agent).

## 11. Native Log Bridge

`native_log_to_python=True` replaces the native agent's built-in stdout/file
sink with an asynchronous bridge to the `pinpoint.native` Python logger. It is
opt-in; the disabled path allocates no queue, callback or consumer thread.

**Your handlers never run on a native thread.** Records are copied into a
bounded queue by the native side and delivered by one daemon Python thread through
ordinary `logging.Logger.log` calls, so handler and filter behavior is standard
and a slow or throwing handler can delay or drop Python delivery without ever
stalling the agent.

- The queue holds 2–4096 records (1024 by default) and 4 MiB of payload,
  whichever comes first; a single message is capped at 4 KiB, truncated at a
  valid UTF-8 boundary.
- When it is full the **new** record is dropped — the agent never blocks to
  make room. `agent.native_log_dropped` reports the cumulative count, and the
  consumer logs at most one drop warning per bridge, so a flood costs one line.
- Native `debug`, `info`, `warning`, and `error` map to their Python
  equivalents; an unrecognized level becomes `WARNING`.
- Ordering is preserved per producer; concurrent native threads may interleave.

Lifecycle:

- Startup errors from the native agent are delivered — the bridge is running
  before its configuration is parsed. If startup fails, queued records are
  drained before the original exception is re-raised.
- `shutdown()` drains what is queued and waits **at most one second** for the
  consumer, so a handler that never returns cannot hold up teardown. Repeated
  shutdown is a no-op; a later `init()` builds a fresh bridge.
- At interpreter exit the daemon stops without assuming handlers are still
  usable.
- With `prefork=True` the master creates no bridge or thread; each worker owns
  its own, so records cannot mix across siblings and one worker's shutdown does
  not affect another. A child forked from an already-started agent gets no
  bridge — it is not traced at all ([Pre-fork Guide](prefork.md)).
