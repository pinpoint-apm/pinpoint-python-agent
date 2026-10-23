# Celery

Instruments [Celery](https://docs.celeryq.dev/) on both sides: a producer's
`apply_async` / `delay` becomes a span event that stamps the trace context onto
the task message, and the worker opens a **root span** per task execution that
continues that trace. The task body runs **inside** the worker span, so its DB,
HTTP and nested `apply_async` calls stitch onto it.

| | |
|---|---|
| Target module | `celery.app.task` |
| Hook points | `Task.apply_async` (wrapt), signals `before_task_publish`, `task_prerun`, `task_postrun` |
| Span role | Span event (producer), **root span** (worker) |
| Service type | `CELERY` (1702, producer event) · `CELERY_WORKER` (1703, worker span) |
| Opt-out alias | `celery` |

## Producer

`celery.app.task.Task.apply_async` is the one wrap — `delay()`, signatures,
chains and `task.retry()` all funnel into it. Per call it:

- opens a span event named `celery.app.task.Task.apply_async`, service type
  `CELERY`, with the broker `host:port` as end point;
- from the `before_task_publish` signal, sets the **resolved** routing key
  (what `task_routes` / `queue=` actually chose) as the event's destination and
  writes the `Pinpoint-*` headers into the message headers. Before the signal
  fires the destination is the task's own `queue` or `task_default_queue`,
  which is what remains under `task_always_eager`, where nothing is published;
- records a publish failure (broker unreachable) as an event error.

`app.send_task(name, ...)` bypasses `Task.apply_async`, so it records no event
of its own, but the signal still injects the current span's context, and the
worker-side span continues the trace.

The headers travel as ordinary Celery message headers, so a worker running this
agent continues the trace whatever the broker (RabbitMQ, Redis, SQS, ...). The
legacy pinpoint-c-agent carried its context in `_pp_*` task **kwargs**; those are
neither written nor read here.

## Worker

`task_prerun` opens the root span, `task_postrun` ends it. Celery sends postrun
from the `finally` of its task tracer, so the span ends exactly once on success,
failure, `Retry`, `Ignore` and `Reject` alike. The span records:

| | |
|---|---|
| Operation | `Celery Worker Invocation` |
| RPC point | the task name (`orders.charge`) |
| Service type | `CELERY_WORKER` |
| Remote address / end point | broker `host:port` from `broker_url` (credentials dropped) |
| Acceptor host | the delivery's routing key (= the producer event's destination, so the UI draws one queue node between the two applications) |
| Error | the exception, when postrun reports `FAILURE`. A `Retry` is not an error: the retried delivery gets its own span, parented on this one by the headers `task.retry()` re-publishes. |

Every span carries one child event, `celery.run`, so the transaction is never
empty in the call tree. An upstream `Pinpoint-Sampled: s0` yields an unsampled
worker span, which records nothing and keeps propagating `s0` to anything the
task enqueues.

A task executed inline from a traced request — `task.apply()` or
`task_always_eager=True` — opens no root span: the work already belongs to the
caller's transaction, and a nested root span would re-parent it.

## Worker pools and `fork()`

The default `prefork` pool forks its child processes from the worker master. An
agent started in the master does not survive the fork, so run the worker as the
[Pre-fork Guide](../../../docs/prefork.md) describes: `prefork=True` /
`PINPOINT_PY_PREFORK=1`, and each child starts its own agent from the after-fork
hook. The `solo`, `threads`, `gevent` and `eventlet` pools fork nothing and need
no special handling. (On macOS billiard *spawns* its children instead of
forking; the after-fork hook never runs there, so a spawned child must reach
`pinpoint.init()` on its own, at app-import time.)

## Usage

```bash
# worker — prefork pool
PINPOINT_PY_PREFORK=1 pinpoint-run --app-name my-worker --service-name my-service \
    --collector localhost -- celery -A tasks worker

# worker — no fork
pinpoint-run --app-name my-worker --service-name my-service \
    --collector localhost -- celery -A tasks worker --pool=solo
```

```python
# producer — inside a transaction
from tasks import charge

charge.delay(order_id=1)
# └─ span event: CELERY  celery.app.task.Task.apply_async   destination: orders
#    (Pinpoint-* headers written into the task message)
```

```python
# worker — the span is live inside the task
import requests
from celery import Celery

app = Celery("tasks", broker="amqp://rabbit.internal//")

@app.task(name="orders.charge", queue="orders")
def charge(order_id):
    # Root span: Celery Worker Invocation  rpc=orders.charge
    #  └─ span event: GET http://billing.internal/...  (attaches to it)
    requests.post("http://billing.internal/charge", json={"id": order_id})
```

## See also

- Examples: [`examples/celery/`](../../../examples/celery) — `producer_demo.py`,
  `worker_demo.py`
- Unit tests: [`test_celery_instrumentation.py`](../../../tests/unit/instrumentations/test_celery_instrumentation.py)
- [pika](../pika/README.md) (RabbitMQ without Celery) ·
  [Custom Instrumentation Guide §11 — Message queues](../../../docs/custom_instrumentation.md)
