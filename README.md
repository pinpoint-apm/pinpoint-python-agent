# Pinpoint Python Agent

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/python-3.11%20|%203.12%20|%203.13%20|%203.14-blue.svg)](https://www.python.org/)

The Python Agent for [Pinpoint APM](https://github.com/pinpoint-apm/pinpoint), an open-source Application Performance Management tool for large-scale distributed systems.

Pinpoint Python Agent enables you to monitor Python applications using Pinpoint. Name your application, start it through `pinpoint-run`, and 35 first-party integrations — web frameworks, HTTP clients, databases, caches, message queues — collect traces, analyze distributed call chains, and visualize service maps in the Pinpoint Web UI, with no changes to your code. The tracing hot path runs in the embedded [pinpoint-cpp-agent](https://github.com/pinpoint-apm/pinpoint-cpp-agent).

## Requirements

| Requirement | Version |
|---|---|
| Pinpoint Collector | 3.1.0+ |
| Python | 3.11+ (CPython) |
| OS | Linux, macOS (Windows is not supported) |

Wheels bundle the native extension, so nothing is compiled at install time. An sdist install builds it and additionally needs CMake ≥ 3.21 and a C++17 compiler.

## Installation

```bash
pip install pinpoint-python-agent
```

## Building from Source

Clone the repository with its submodules (the C++ agent lives in a git submodule):

```bash
git clone --recurse-submodules https://github.com/pinpoint-apm/pinpoint-python-agent.git
```

Or, if you already cloned without the flag:

```bash
git submodule update --init --recursive third_party/pinpoint-cpp-agent
```

Then install from the checkout:

```bash
pip install .
```

See the [Development Guide](docs/development.md) for building the extension in place, running the test suite, and the dev shell that resolves the native libraries.

## Getting Started

### Zero-code: `pinpoint-run`

The launcher starts the agent before any of your code runs and activates auto-instrumentation for every supported library your application imports:

```bash
pinpoint-run --app-name my-service --agent-name my-service-web \
    --collector localhost -- python my_app.py
```

### In-code: `init()` + `autoload()`

Call `init()` early in startup, then activate auto-instrumentation. Libraries already imported are instrumented immediately; everything else the moment it is imported.

```python
import pinpoint
import pinpoint.autoload

pinpoint.init(
    application_name="my-service",   # required
    agent_name="my-service-web",     # optional display label
    server_info="Flask",             # server metadata shown in the UI
    collector_host="localhost",
)
pinpoint.autoload.autoload()

import flask
app = flask.Flask(__name__)
```

Configuration comes from `init()` kwargs, `PINPOINT_PY_*` environment variables, or a YAML config file — see the [Configuration Guide](docs/config.md) for every option and the precedence rules.

### Manual tracing

For code paths no integration covers — cron jobs, queue consumers, in-house workers — open a span or a span event with the decorators:

```python
import pinpoint

@pinpoint.span("nightly_billing", rpc_point="/cron/billing")   # a new transaction
def nightly_billing():
    charge(100)

@pinpoint.spanevent("billing.charge")                          # a child of the current span
def charge(amount):
    ...
```

Both work on `async def` too. Background hand-offs, context managers, annotations and header propagation are covered in the [Custom Instrumentation Guide](docs/custom_instrumentation.md).

## Examples

The [`examples/`](examples) directory holds one subdirectory per integration — the demo application plus a `run.sh` that starts its backing container and smoke-tests the stack. Each demo's module docstring carries its exact `pinpoint-run` command and a `curl` to hit it.

- **Web frameworks** — [flask_demo.py](examples/flask/flask_demo.py) + [flask_upstream.py](examples/flask/flask_upstream.py) (two services: distributed tracing over `requests`, plus four MySQL clients), [django_demo.py](examples/django/django_demo.py), [fastapi_demo.py](examples/fastapi/fastapi_demo.py) (every supported PostgreSQL client), [starlette_demo.py](examples/starlette/starlette_demo.py), [pyramid_demo.py](examples/pyramid/pyramid_demo.py), [falcon_demo.py](examples/falcon/falcon_demo.py) (WSGI and ASGI), [tornado_demo.py](examples/tornado/tornado_demo.py), [aiohttp_demo.py](examples/aiohttp/aiohttp_demo.py) (with `elasticsearch`)
- **Framework-less apps** — [wsgi_demo.py](examples/wsgi/wsgi_demo.py), [asgi_demo.py](examples/asgi/asgi_demo.py): wrapping an app in the Pinpoint middleware by hand
- **Message queues** — producer/consumer pairs for [kafka](examples/kafka), [aiokafka](examples/aiokafka), [confluent_kafka](examples/confluent_kafka), [pika](examples/pika) and [aio_pika](examples/aio_pika)
- **RPC** — [grpc](examples/grpc): client and server, with context propagated across the call

## Documentation

| Document | Description |
|---|---|
| [Getting Started Guide](docs/getting_started.md) | Step-by-step setup: install, configure, first traced request |
| [Configuration Guide](docs/config.md) | Every option, as an `init()` kwarg, environment variable and config-file key, with precedence rules |
| [Auto-Instrumentation Catalog](docs/auto_instrumentation.md) | The 35 supported libraries, what each records, and how to turn one off |
| [Custom Instrumentation Guide](docs/custom_instrumentation.md) | Python API reference: spans, span events, annotations, async hand-offs, distributed tracing |
| [API Contracts](docs/api_contracts.md) | Threading, end-exactly-once and overflow rules the agent enforces on spans, events and annotations |
| [Pre-fork Integration Guide](docs/prefork.md) | Running the agent under gunicorn, uWSGI and `multiprocessing` |
| [Troubleshooting](docs/troubleshooting.md) | Startup verification, agent logs, common issues and solutions |
| [Development Guide](docs/development.md) | **Contributors:** building the native extension from source, running the tests, and adding a bundled integration |

## Contributing

We are looking forward to your contributions via pull requests.

To report bugs or request features, please create an [Issue](https://github.com/pinpoint-apm/pinpoint-python-agent/issues).

## Community

- [Pinpoint APM](https://github.com/pinpoint-apm/pinpoint) - Main Pinpoint project
- [Pinpoint Documentation](https://pinpoint-apm.github.io/pinpoint/) - Official documentation
- [Pinpoint C++ Agent](https://github.com/pinpoint-apm/pinpoint-cpp-agent) - The native core this agent embeds

## License

Pinpoint Python Agent is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for full license text.
