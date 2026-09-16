# asgiref

Keeps synchronous code traced when it runs inside an async stack. This is a
support integration — there is no "asgiref transaction"; it exists so spans
survive the hop onto asgiref's worker thread.

| | |
|---|---|
| Target module | `asgiref.sync` |
| Hook points | `SyncToAsync.__call__`, `SyncToAsync.thread_handler` |
| Span role | Linked async child span per hand-off |
| Opt-out alias | `asgiref` |

## Why it exists

Django's ASGI stack (and Channels) routes every synchronous view, middleware,
and ORM operation through `asgiref.sync.SyncToAsync`. asgiref copies the
caller's ContextVars into its worker thread, but a copied *live* span resolves
to a detached no-op view there (see [`pinpoint/context.py`](../../context.py)),
so without an explicit hand-off every sync hop would run untraced — the DB
queries and outbound HTTP calls of a sync Django view under ASGI would simply
not appear.

`SyncToAsync` binds the user callable at construction time and Django caches the
instances, so the callable cannot be swapped per call the way the
Starlette/FastAPI `run_in_threadpool` wrappers do. The hand-off is bridged
through a ContextVar latch instead:

1. `__call__` (event-loop thread) mints a linked async child on the current span
   and publishes it in a one-element latch; asgiref's own `copy_context()`
   carries the latch into the worker.
2. `thread_handler` (worker thread) claims the latch *inside* the copied context
   and runs the original callable under `with claimed:`, so `current_span()` in
   the sync code resolves to a child that the worker exclusively owns and ends.

The latch is claimed with a single GIL-atomic `list.pop`, so exactly one of
{worker, dispatcher cleanup} wins: the child ends exactly once and is never
driven by two threads.

## Usage

Nothing to call. It activates whenever `asgiref.sync` is imported — which
Django does for you.

```python
# A sync view served over ASGI: the ORM call lands on a linked child span
# instead of vanishing.
def user_detail(request, uid):
    return JsonResponse({"name": User.objects.get(pk=uid).name})
```

The same applies to hand-written `sync_to_async(...)` calls.

## Disable

```bash
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=asgiref
```

Expect sync views/ORM under Django ASGI to lose their child spans if you do.

## See also

- Unit tests: [`test_asgiref_instrumentation.py`](../../../tests/unit/instrumentations/test_asgiref_instrumentation.py)
- [Custom Instrumentation Guide §13 — Asynchronous and background work](../../../docs/custom_instrumentation.md) ·
  [Django integration](../django/README.md)
