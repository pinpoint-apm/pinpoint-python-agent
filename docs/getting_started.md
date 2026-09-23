# Pinpoint Python Agent - Getting Started Guide

This guide helps you get started with the Pinpoint Python Agent
(`pinpoint-python-agent`) for monitoring your Python applications.

---

## Prerequisites

- **Pinpoint Collector**: Version 3.1.0 or higher
- **Python**: 3.11 or higher (CPython)
- **Operating System**: Linux or macOS (Windows is not supported)

Wheels bundle the native extension. An sdist install additionally needs
CMake ≥ 3.21 and a C++17 compiler.

---

## Installation

```bash
pip install pinpoint-python-agent
```

To build from a source checkout instead (git clone), see the
[Development Guide](development.md) — the native extension is compiled from the
bundled native core.

---

## Configuration

Configure the agent with `init()` kwargs, environment variables, or a YAML
config file. Environment variables take the highest priority; a config file
replaces the kwargs; built-in defaults fill the rest.

### Option 1: `init()` kwargs

```python
import pinpoint

pinpoint.init(
    application_name="MyApplication",   # required
    agent_name="my-agent-name",         # optional label; agent id is auto-generated
    collector_host="localhost",         # your Pinpoint collector host
    sampling_type="PERCENT",
    sampling_percent_rate=100.0,        # sample all requests
    log_level="info",                   # debug, info, warn, error
)
```

### Option 2: Environment variables

The native agent reads every option as `PINPOINT_PY_<SUFFIX>`:

```bash
export PINPOINT_PY_APPLICATION_NAME="MyApplication"
export PINPOINT_PY_AGENT_NAME="my-agent-name"
export PINPOINT_PY_COLLECTOR_HOST="localhost"
export PINPOINT_PY_COLLECTOR_AGENT_PORT="9991"
export PINPOINT_PY_COLLECTOR_SPAN_PORT="9993"
export PINPOINT_PY_COLLECTOR_STAT_PORT="9992"
```

### Option 3: Configuration file

Pass `config_file_path=` to `init()`, set `PINPOINT_PY_CONFIG_FILE`, or
launch with `pinpoint-run --config-file <path>` (add `--active-profile <name>`
to select a `Profile.<name>` section). The file uses the native YAML keys and
**replaces** the inline configuration rendered from kwargs — see the
[Configuration Guide](config.md) for the full key list:

```yaml
ApplicationName: "MyApplication"
Collector:
  Host: "localhost"
Sampling:
  Type: "PERCENT"
  PercentRate: 100.0
```

For the complete option list and the exact precedence rules, see the
[Configuration Guide](config.md).

---

## Basic Usage

### Zero-code: `pinpoint-run`

The launcher initializes the agent before any of your code runs and activates
auto-instrumentation for every supported library your app imports:

```bash
pinpoint-run --app-name my-first-app --collector localhost -- python my_app.py
```

```bash
pinpoint-run --app-name my-first-app --collector localhost -- \
    gunicorn -w 4 myapp:app          # prefork servers: see the note below
```

### In-code: `init()` + `autoload()`

Call `init()` early in startup, then activate auto-instrumentation. Libraries
already imported at that point are instrumented immediately; everything else is
instrumented the moment it is imported.

```python
import pinpoint
import pinpoint.autoload

pinpoint.init(
    application_name="my-first-app",
    server_info="Flask",             # server metadata label shown in the UI
    collector_host="localhost",
)
pinpoint.autoload.autoload()

import flask, requests

app = flask.Flask(__name__)

@app.route("/users")
def users():
    # Traced automatically: the Flask instrumentation opens the root span,
    # and the requests instrumentation records this call as a child event
    # with distributed-tracing headers injected.
    requests.get("http://user-service.internal/users")
    return {"users": []}

app.run(host="0.0.0.0", port=8090)
```

```bash
curl http://localhost:8090/users
```

The request appears in the Pinpoint Web UI as a transaction for
`my-first-app`. No `end()` calls, no shutdown hooks: the middleware ends each
span, and the agent drains itself at interpreter exit (atexit) and on SIGTERM.

> **Prefork servers (gunicorn, uWSGI, `multiprocessing`):** a started agent
> cannot survive `fork()`. Workers that import the app per process need
> nothing special; masters that initialize before forking need
> `prefork=True`. See the [Pre-fork Integration Guide](prefork.md).

### Manual tracing — entry points without HTTP

For code the auto-instrumentor has no entry point for (cron jobs, queue
consumers, CLI commands), the decorators open the transaction:

```python
@pinpoint.span("nightly_billing", rpc_point="/cron/billing")
def nightly_billing():
    with pinpoint.trace("compute_invoices"):
        ...
```

`@pinpoint.span` opens a root transaction per call; `@pinpoint.spanevent` and
`pinpoint.trace(...)` record child events under the current span. All of them
degrade to plain function calls when the agent is not initialized.

### Verify

The default log level is `warn`, so a healthy startup is silent. Set
`log_level="info"` (or `PINPOINT_PY_LOG_LEVEL=INFO`) and look for the native
agent's registration lines on stdout:

```
[info][pinpoint][grpc.cpp:...] success to register the agent
[info][pinpoint][grpc.cpp:...] AgentInfo sent
```

`init()` returns before registration completes — `get_agent().enabled` flips
`True` only after the collector accepts the agent, seconds later. The log is
the authoritative startup signal; see
[Verifying Agent Startup](troubleshooting.md#verifying-agent-startup).

---

## Next Steps

1. **Learn the API**: the [Custom Instrumentation Guide](custom_instrumentation.md) covers manual
   tracing, HTTP/DB/message-queue patterns, distributed tracing, and async
   work. The rules the span objects enforce are in the
   [API Contracts](api_contracts.md).
2. **Explore Examples**: `examples/` has complete working programs — Flask,
   Django, FastAPI, Kafka, gRPC, and a two-service distributed-tracing pair
   (`flask_demo.py` + `flask_upstream.py`).
3. **Configure Advanced Options**: see the [Configuration Guide](config.md)
   for sampling strategies, URL statistics, SQL bind values, header recording,
   and logging.

---

## Troubleshooting

**Start with the agent log** — set `PINPOINT_PY_LOG_LEVEL=DEBUG` and the
native agent also dumps the configuration it actually resolved.

Nothing in the UI despite a successful registration? The three usual causes
are sampling (`sampling_type="PERCENT"` with `sampling_percent_rate=100.0`
samples everything — use both while testing), a span that is never ended
(use the context-manager forms), and the collection interval (wait a few
seconds).

For everything else — connection failures, silent instrumentations, fork
warnings — see the [Troubleshooting Guide](troubleshooting.md).
