# Pre-fork Integration Guide

How to run the Pinpoint Python agent inside pre-fork servers — the most common
Python deployment shape: gunicorn's worker model, uWSGI's default mode,
mod_wsgi, and `multiprocessing` with the `fork` start method.

```
master process                — makes NO native agent calls
 ├─ fork → worker 1           — StartAgent() → traces requests
 ├─ fork → worker 2           — StartAgent() → traces requests
 └─ fork → worker N           — StartAgent() → traces requests
```

---

## The contract

**Once an agent has started in a process, that process can never hand a
working tracing runtime to a forked child.** Neither the agent's background
threads nor the gRPC runtime they run on survive `fork()` — a gRPC limitation,
not a Pinpoint one: gRPC objects must be created *after* the last fork.

So the native agent must start **in the worker**, never in the master. Each
worker then owns its own agent — its own registration, sender threads, queues,
and collector connections — fully independent of its siblings.

## The two supported patterns

### 1. Initialize in each worker, after the fork (preferred)

This happens naturally when `pinpoint.init()` runs at app-import time and the
server imports the app **per worker**:

- gunicorn *without* `--preload` (the default)
- uWSGI with `lazy-apps = true`

Nothing special to configure — `init()` simply runs N times, once per worker.

### 2. Initializing in the master? Use prefork mode

Whenever `init()` runs in the master before workers fork — gunicorn
`--preload`, an init inside a gunicorn server hook, or `pinpoint-run` wrapping
any prefork server (its bootstrap initializes at interpreter start, i.e. in
the master, whether or not `--preload` is used) — pass `prefork=True` or set
`PINPOINT_PY_PREFORK=1`.

In this mode the master stores only Python configuration and **makes no native
agent API calls**. An `os.register_at_fork` after-in-child hook performs each
worker's first native `StartAgent()` call, so no thread or gRPC state ever
crosses the fork boundary. The master itself records no spans (`enabled` stays
`False` there — it serves no traffic anyway).

```python
# gunicorn.conf.py, used with --preload
import pinpoint

def on_starting(server):
    pinpoint.init(
        application_name="my-app",
        service_name="my-service",
        collector_host="collector-host",
        prefork=True,
    )
```

```bash
# Or with pinpoint-run (which always initializes in the master):
PINPOINT_PY_PREFORK=1 pinpoint-run --app-name my-app --service-name my-service \
    --collector collector-host -- gunicorn -w 4 myapp:app
```

## What happens on misuse

If a **started** (warm) agent does fork — a default `init()` followed by
`os.fork()` or a `multiprocessing` fork — the agent detects it and degrades
safely instead of crashing:

| Situation in the child | Behavior |
|---|---|
| Warm agent inherited by the child | The child logs a warning naming the two patterns above and switches to a no-op agent. It runs normally — it just isn't traced, and it never touches the inherited agent again. |
| The parent | Unaffected; its tracing continues. |
| Grandchild of a traced worker (fork without exec) | Cannot trace — gRPC cannot be re-initialized there. Trace in the process generation that called `StartAgent()`. |
| `fork()+exec()` (spawning tools/subprocesses) | Unaffected — the misuse case is only fork *without* exec followed by tracing in the child. |

## Host-specific notes

### gunicorn

- **Without `--preload`** (default): pattern 1 — `init()` at app-import time,
  nothing else needed.
- **With `--preload`**: pattern 2 — `prefork=True` in the master (see the
  `gunicorn.conf.py` recipe above; calling `init(prefork=True)` at app-import
  time works identically, since the preloaded app is imported in the master).
- Worker lifecycle is automatic: each worker's agent installs its own SIGTERM
  drain hook and atexit shutdown, so gunicorn's graceful worker stop flushes
  queued spans.

#### Worker classes: sync, gthread, gevent, eventlet

All four are supported, with the same setup: `init()` at app-import time
without `--preload`, or `prefork=True` with `--preload`. There is no
worker-specific option. `tests/integration/test_gunicorn_workers.py` runs each
class against a mock collector, with concurrent requests that call a slow
downstream through `requests`, and checks that every request is answered,
every outbound call carries its own transaction's headers, and every span
reaches the collector. It was last run with gunicorn 22.0.0, gevent 24.2.1
(greenlet 3.0.3) and eventlet 0.41.3.

Why the cooperative workers (gevent, eventlet) need nothing special:

- **The native agent's threads never run Python.** Span sending, stats, ping
  and command handling run on C++ threads that never take the GIL and never
  call back into the interpreter, so monkey-patched `threading`, `socket` and
  `time` do not reach them. They are real OS threads beside the event loop and
  keep sending while greenlets wait on I/O.
- **The calls into native return without waiting.** A span's native calls
  queue work for those threads and return. None of them blocks on the network
  or on the collector, so a greenlet never stalls the hub inside the agent.
  An unreachable collector only means the queued spans are dropped.
- **The active span is per greenlet.** The agent keeps the current span in a
  `contextvars.ContextVar`, and greenlet (1.0 and later, which both gevent and
  eventlet use) gives each greenlet its own context. Concurrent requests that
  interleave on one OS thread therefore keep separate transactions.
- **Patch before the agent is imported.** gunicorn's gevent and eventlet
  workers patch the worker before they import the app, so `init()` at
  app-import time already runs patched. If you run a gevent or eventlet
  server yourself, call `gevent.monkey.patch_all()` (or
  `eventlet.monkey_patch()`) first, as both libraries require, then import
  `pinpoint`. With `--preload` the master imports the app before the worker
  patches it. That is why `prefork=True` makes the master's `init()` store the
  configuration only: each worker then starts its agent after the patching.
- **`native_log_to_python=True` works there too.** Its consumer is a
  `threading.Thread`, which becomes a greenlet once patched. It polls a queue
  that native fills without taking the GIL.

A request that hangs only with the agent enabled is a bug. Report it with the
worker's Python stack (`py-spy dump --pid <worker pid>`) and native stack
(`gdb -p <worker pid> -batch -ex "thread apply all bt"`).

### uWSGI

uWSGI forks workers from C code that, by default, does not run Python's fork
hooks in the children:

- Pattern 1 requires `lazy-apps = true` (the app — and `init()` — run per
  worker).
- Pattern 2 (prefork mode) additionally needs uWSGI's
  `py-call-osafterfork = true` option so `os.register_at_fork` hooks fire in
  the workers.

### `multiprocessing`

- **`fork` start method** from an already-traced process: the child is
  disabled with a warning (see the misuse table). If the child needs tracing,
  prefer the **`spawn`** start method — a fresh interpreter re-runs your
  startup code (and `pinpoint.init()`) on its own, so no hook is involved and
  none is needed. `forkserver` behaves like `spawn` as long as the fork
  server itself never started an agent.
- To trace work on other processes as part of the same transaction tree,
  remember spans do not cross process boundaries — each process is its own
  agent instance; propagate context explicitly if the work flows through a
  carrier (queue, HTTP), as in [Custom Instrumentation Guide §11](custom_instrumentation.md#11-message-queues--producer-and-consumer).

## Worker identity

Each worker appears as one agent instance in the Pinpoint UI. The agent id is
always auto-generated (a fresh UUIDv7 per worker process), so sibling workers
can never collide — but the id changes on worker restart. `agent_name` is the
human-readable display label and need not be unique; all workers can share
one. Left unset, it defaults to `<hostname>-<pid>`, so each worker gets its
own label.

## Logging

Multiple workers must not share one rotated log file — the native size
rotation is not multi-process safe. Give each worker its own file with the
`%pid%` placeholder:

```python
pinpoint.init(..., log_output="/var/log/pinpoint/agent-%pid%.log",
              log_max_file_size=50)
```

With `native_log_to_python=True`, the pending prefork master creates neither a
native agent nor a logging queue/consumer thread. Each worker's first
`StartAgent()` creates that worker's independent bridge, drop counter and daemon
consumer, so records cannot mix across siblings and one worker's shutdown does
not affect another. The Python sink replaces native file/stdout output; the
`%pid%` advice above applies only while the sink is disabled.

A child forked from an already-started warm agent gets no bridge: the parent's
consumer thread does not survive `fork()`, and that child is not traced at all
(see the misuse table above), so nothing is started in its place.

## Operational notes

- **Instance count**: N workers = N agent instances on the collector and in
  the UI. Size the worker count (and the collector) with that in mind.
- **Sampling is per worker**: `sampling_percent_rate=1.0` samples 1% *in
  each worker*, which also gives ~1% of the fleet's traffic. A rate holds
  globally like that; `sampling_counter_rate`'s exact 1-in-N does not — it is
  1-in-N per worker, not across workers.
- **Collector connections**: each worker maintains its own gRPC channels;
  registration retries are independent per worker.
- **Shutdown**: a worker's agent drains on SIGTERM and at interpreter exit
  automatically. A worker killed with SIGKILL simply loses whatever was still
  queued in that process.
- **Single-process applications** need none of this: call `init()` at startup
  as shown in the [Getting Started Guide](getting_started.md).
