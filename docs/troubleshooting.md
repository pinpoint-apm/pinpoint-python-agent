# Pinpoint Python Agent - Troubleshooting Guide

This guide helps you diagnose and resolve common issues with the Pinpoint
Python Agent. For build/import problems in a source checkout (dylib paths, ABI
tags, dev.sh), see the [Development Guide](development.md#troubleshooting).

---

## Verifying Agent Startup

This section is the canonical description of the startup contract; the other
guides link here rather than repeating it.

**`init()` returns before the agent is registered.** It starts the native
agent, whose background thread opens the gRPC channels and registers with the
collector, retrying indefinitely until the collector accepts it.
`get_agent().enabled` flips `True` only after that registration succeeds, so
it is normally still `False` right after `init()` returns — that is not an
error, and checking it there proves nothing. Use `enabled` only as a fast-fail
guard before creating a span, never as a startup success check.

**`init()` never raises for a bad configuration.** Failures degrade to a
disabled no-op agent, logged at `WARNING`/`ERROR` on the Python side:

| Python-side log | Meaning |
|---|---|
| `pinpoint init without application_name — agent disabled` | Neither `application_name=` nor a config file was provided. |
| `pinpoint native agent startup failed — disabling` | The native `StartAgent()` raised synchronously; the traceback names the cause. |
| `pinpoint agent initialized (app=… collector=…)` | Python-side init completed; registration continues in the background. |

**The native agent log is the authoritative startup signal.** The default log
level is `warn`, so a healthy startup is silent — set
`PINPOINT_PY_LOG_LEVEL=INFO` (or `init(log_level="info")`) to see it:

| In the native log (stdout) | Meaning |
|---|---|
| `success to register the agent` / `AgentInfo sent` | The agent registered with the collector. Startup succeeded. |
| `agent start failed: ...` | Configuration or setup error; the line names the cause. |
| `failed to send AgentInfo` | The collector is not reachable yet. Retried indefinitely. |

At `INFO`, the native agent also dumps the configuration it **resolved**
(the `config:` line) — after the kwargs-YAML, any config file, environment
variables and clamping. This is the fastest way to catch a setting that never
took effect.

Whatever the log says, **a failed agent start never affects the application**:
every tracing call degrades to a safe no-op; only the traces are lost.

### Routing the agent's own logs

The Python-side logs above go to the `pinpoint` logger, which `init()` gives its
own `stderr` handler with `propagate = False`. That is deliberate — the agent has
to be diagnosable regardless of how (or whether) the application configures
logging, and propagating as well would double every line for an app that
configures the root logger. The consequence is that the agent's logs do not
reach your handlers by default.

To collect them yourself, add a handler to that logger after `init()`:

```python
import logging
logging.getLogger("pinpoint").addHandler(my_json_handler)
```

Or hand them to the root logger instead, accepting the duplicate-line
trade-off:

```python
pinpoint_log = logging.getLogger("pinpoint")
pinpoint_log.handlers.clear()
pinpoint_log.propagate = True
```

The **native** agent log is separate and not a Python logger at all: it writes
to stdout, or to a file when `log_output=` is set. See
[Configuration Guide § Logging](config.md#logging-configuration).

---

## Disabling the Agent

To disable tracing without removing the agent from your code:

```bash
export PINPOINT_PY_ENABLE=false
```

or `init(enabled=False)`. The native side receives `Enable: false` and the
Python instrumentations short-circuit; `enabled` stays `False` and every span
is a no-op. Individual integrations can be disabled instead with
`PINPOINT_PY_DISABLED_INSTRUMENTATIONS=flask,redis` — see the
[Configuration Guide](config.md#python-only-options).

---

## Stopping and Resuming the Agent

`pinpoint.shutdown()` stops the agent at runtime with no application restart:
it flushes pending data, stops the native workers, and clears the singleton.
It is idempotent, and terminal for that agent instance. The application keeps
running normally; it just stops being traced.

To resume tracing later, call `pinpoint.init(...)` again — because the
singleton was cleared, it builds a **fresh** agent (this is also the reset
between tests):

```python
pinpoint.shutdown()          # stop tracing; app keeps working
# ... later ...
pinpoint.init(application_name="my-service", collector_host="collector-host")
```

Points to keep in mind:

- Spans created before the shutdown are safe to end afterwards; their data is
  simply dropped ([API Contracts §10](api_contracts.md#10-agent-lifecycle)).
- Each cycle registers as a new agent instance with a freshly generated agent
  id. Set `agent_name` for a stable label in the UI across cycles.
- You rarely need to call `shutdown()` yourself: `init()` installs an atexit
  hook for normal interpreter exit and a SIGTERM handler that drains the
  agent before the process dies (chaining to any handler you installed
  earlier; a SIG_IGN or C-installed disposition is left untouched).

---

## Common Issues

### Agent Not Starting

**Symptoms:** the application runs but no data appears in the Pinpoint UI.

1. **Check `application_name`** — without it (or a config file) the agent is
   disabled at init, with a Python-side warning.
2. **Read the logs** — see [Verifying Agent Startup](#verifying-agent-startup);
   remember the default level hides everything below `warn`.
3. **Verify collector connectivity** — see
   [Cannot Connect to Collector](#cannot-connect-to-collector).
4. **Check the resolved configuration** with `PINPOINT_PY_LOG_LEVEL=INFO`
   (the `config:` dump) — catches a typo'd kwarg (silently ignored), a stale
   `PINPOINT_PY_*` variable overriding the code, or a config file replacing
   your kwargs wholesale.

### No Data in the Pinpoint UI

The agent registers successfully but no traces appear.

1. **Check sampling** — set `sampling_type="PERCENT"` and
   `sampling_percent_rate=100.0` together to sample every transaction.
2. **Verify spans are ended** — a span that never reaches `end()` is never
   sent. Prefer the context-manager forms
   ([API Contracts §2](api_contracts.md#2-end-exactly-once-and-record-before-ending)).
3. **Check excluded URLs and methods** — a filter match produces a no-op
   span, so the transaction disappears entirely. Temporarily clear
   `http_server_exclude_url` / `http_server_exclude_method`.
4. **Wait for collection** — data may take 5–10 seconds to appear.
5. **Check the collector side** — the agent can register cleanly and spans
   still be rejected downstream. Review the Pinpoint collector logs before
   assuming the agent is at fault.

### An Auto-Instrumentation Isn't Firing

1. **Autoload must be activated.** `pinpoint-run` does it automatically;
   in-code setups must call `pinpoint.autoload.autoload()` after `init()`.
   Modules already imported at that point are instrumented immediately, so
   ordering relative to your imports is not the issue.
2. **Check `PINPOINT_PY_DISABLED_INSTRUMENTATIONS`** — a top-level alias
   (`django`) disables every hook of that package.
3. **Check the target is actually the instrumented module** — the registry in
   [`autoload.py`](../pinpoint/autoload.py) lists exactly which import
   triggers each integration. WSGI/ASGI are protocols, not modules: their
   middlewares are opt-in
   ([Custom Instrumentation Guide §9](custom_instrumentation.md#9-http-server-and-http-client-tracing)).
4. **Child events need a current span** — HTTP-client and DB instrumentations
   attach to `current_span()` and do nothing outside a traced request. Entry
   points without HTTP need `@pinpoint.span` or a server middleware.

### Incomplete Traces

1. **Raise the limits** — `span_max_event_depth` / `span_max_event_sequence`
   (`-1` = unlimited); at the cap, extra events record nothing
   ([API Contracts §5](api_contracts.md#5-event-depth-and-count-limits)).
2. **Check event pairing** — every `new_span_event()` needs an `end()` on
   every path; the `with span.new_span_event(...)` form guarantees it.
3. **Background work needs a hand-off** — a span shared with another thread
   records nothing there. Use `async_trace` or `new_async_span`
   ([API Contracts §7](api_contracts.md#7-async-spans)).

### Missing Distributed Traces

1. **Server side**: pass the inbound headers to
   `new_span(..., headers=...)` — with no `Pinpoint-*` entries the span
   starts a brand-new transaction.
2. **Client side**: outbound calls must inject —
   `propagator.inject_items(span)`. The bundled HTTP-client
   instrumentations do this; custom carriers are on you
   ([Custom Instrumentation Guide §8](custom_instrumentation.md#8-distributed-tracing--inject-and-extract)).
3. **Do not skip injection on unsampled spans** — `Pinpoint-Sampled: s0` is
   what keeps downstream from starting broken partial traces
   ([API Contracts §6](api_contracts.md#6-unsampled-and-no-op-spans-are-deliberately-silent)).
4. **Check no proxy strips or rewrites** the `Pinpoint-*` headers between
   services.

### Fork Warning / Workers Not Traced

A log line like `process N inherited an already-started agent; tracing is
disabled because gRPC cannot survive fork()` means a warm agent forked.
Initialize in each worker, or use `prefork=True` in the master — the
[Pre-fork Integration Guide](prefork.md) covers gunicorn, uWSGI, and
`multiprocessing`.

### High Memory Usage

1. **Always end spans** — an unended span holds its buffers for as long as it
   lives.
2. **Reduce buffer sizes**: `span_queue_size` (default 1024),
   `span_max_event_sequence`, `http_url_stat_limit`.
3. **Long-lived fire-and-forget asyncio tasks** are already bounded by
   `asyncio_task_span_timeout_ms` (default 5 min) — don't disable it (set to
   `0`) unless you end those spans yourself.

### High CPU Usage or Slow Responses

1. **Reduce sampling, or cap it by throughput**:

   ```python
   pinpoint.init(..., sampling_type="PERCENT", sampling_percent_rate=1.0,
                 sampling_new_throughput=100, sampling_continue_throughput=200)
   ```

2. **Disable what you do not read** — `http_collect_url_stat`,
   `sql_enable_sql_stats`, `sql_trace_bind_values`, `stat_enabled`.
3. **Reduce work per transaction** — fewer, coarser span events; smaller
   annotations; `HEADERS-ALL` is a debugging setting, not a production one.
4. **Know the baseline** — measured on an M4 Pro (2026-08-26), tracing costs
   **≈1.15 µs per span event on a ≈3.0 µs per-span floor**; per public-API
   call the median is 0.86 µs, but the 0.58–1.49 µs range makes the
   span/event split the better predictor. With the agent disabled the same
   calls cost 18–46 ns each. An unsampled request adds 4.8 µs against a
   sampled one's 11.2 µs, so sampling remains the cheapest lever. Numbers are
   only comparable within one measurement record — re-measure rather than
   diffing against these:
   [benchmark/api_overhead/RESULTS.md](../benchmark/api_overhead/RESULTS.md).
5. **Heavily threaded services** pay GIL-handoff convoying on the traced
   path, but it does not compound: traced aggregate throughput falls only
   1.20× from 1 to 8 threads (83.5 K → 69.3 K ops/s) and per-op p50 stays
   flat at 11.6–13.6 µs. What moves is the tail — p99 climbs to ~1.3 ms at 8
   threads, because threads run batches of operations between GIL handoffs
   and then pay one long wait. Read `s6_threads_N` as a throughput cost, not
   a latency curve, and measure with your own workload.

### Cannot Connect to Collector

Logs show connection or gRPC errors.

1. Verify `collector_host` and the three ports (`agent` 9991, `stat` 9992,
   `span` 9993) point at a running, healthy collector.
2. Test connectivity to each port, e.g. `nc -vz <host> 9991`.
3. Allow gRPC ports 9991–9993 through firewalls and network policies
   (e.g. Kubernetes).

---

## Tracing Instrumentation Callbacks

At `DEBUG` the agent logs the entry and exit of every instrumentation
callback:

```
[pinpoint.instrument] DEBUG: interceptor before _wsgi_app_wrapper
[pinpoint.instrument] DEBUG: interceptor after _wsgi_app_wrapper
```

Use it when a library looks untraced: an entry line with no matching exit
means the hook ran and failed (the failure is on the next `DEBUG` line), while
no lines at all mean the hook was never installed — check
`PINPOINT_PY_DISABLED_INSTRUMENTATIONS` and that `autoload()` ran before the
library was used. Only the callback name is logged, never the traced call's
arguments. Async callbacks log an entry line only: that call merely builds the
coroutine, so its return says nothing about the hook finishing.

The switch is the `pinpoint.*` log level resolved at `init()`
(`log_level="debug"` or `PINPOINT_PY_LOG_LEVEL=debug`); raising the level by
hand afterwards takes effect on the next `init()`.

---

## Getting Help

Enable `PINPOINT_PY_LOG_LEVEL=DEBUG`, then open an issue at
[pinpoint-apm/pinpoint-python-agent](https://github.com/pinpoint-apm/pinpoint-python-agent/issues)
with the agent version, your sanitized configuration, the relevant log lines,
and minimal reproduction steps.
