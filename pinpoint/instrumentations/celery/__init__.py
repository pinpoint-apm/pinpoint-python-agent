# pinpoint-python-agent
# Copyright (c) 2026-present NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Celery instrumentation — producer (``apply_async``) + worker (task run).

Producer: ``celery.app.task.Task.apply_async`` (``delay`` funnels into it)
becomes a span event on the calling transaction. The trace context is written
into the *message headers* from the ``before_task_publish`` signal, which is
the one place Celery hands out the final, mutable header dict together with
the resolved exchange / routing key — the wrapper alone sees only the options
the caller passed, not what ``task_routes`` made of them. The event is opened
in the wrapper rather than in the signal because no signal fires when a
publish fails, and an event opened in ``before_task_publish`` would then leak.

Worker: ``task_prerun`` opens a root span seeded from the message headers
(they arrive as attributes of ``task.request``), ``task_postrun`` ends it —
Celery sends postrun from the ``finally`` of ``trace_task``, so the span ends
exactly once on success, failure, retry, ``Ignore`` and ``Reject`` alike.

The legacy pinpoint-c-agent carried its context in ``_pp_*`` task kwargs; those
are not read here. Honouring them would mean stripping kwargs before the task
body runs, which no signal can do.

See ``README.md`` for what each side records.
"""

from __future__ import annotations

import contextvars
import functools
from urllib.parse import urlsplit

from ...agent import get_agent
from ...context import current_span, set_current_span
from ...errors import safe_try
from ...instrumentor import BaseInstrumentor
from ...propagator import inject_items
from ...service_type import SERVICE_TYPE_CELERY, SERVICE_TYPE_CELERY_WORKER
from .._util import (
    close_span_scope,
    end_quietly,
    no_current_span,
    record_exception_on_span,
    span_event_scope,
    span_is_sampled,
    wrap,
)

_OPERATION_APPLY_ASYNC = "celery.app.task.Task.apply_async"
_OPERATION_WORKER_INVOCATION = "Celery Worker Invocation"
# The one event a task-run span carries, named like "kafka.consume".
_OPERATION_RUN = "celery.run"
# Celery's own task state constant, inlined so the module imports without it.
_STATE_FAILURE = "FAILURE"
# Where the worker side parks its open scope between prerun and postrun: the
# per-execution ``task.request`` Context, which both signals see and Celery
# pops only after postrun.
_SCOPE_ATTR = "_pinpoint_scope"

# The apply_async event, visible to the before_task_publish receiver that runs
# inside the same call so it can stamp the resolved routing key on it.
_publish_event: contextvars.ContextVar = contextvars.ContextVar(
    "pinpoint_celery_publish_event", default=None)


class CeleryInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        from celery import signals  # type: ignore[import-not-found]

        # Per-call hot path: skip the safe_wrapper machinery when no span is
        # current (the wrapper's own first early-out).
        wrap("celery.app.task", "Task.apply_async", _apply_async_wrapper,
             precheck=no_current_span)
        signals.before_task_publish.connect(_on_before_publish, weak=False)
        signals.task_prerun.connect(_on_prerun, weak=False)
        signals.task_postrun.connect(_on_postrun, weak=False)

    def _uninstrument(self) -> None:
        from celery import signals  # type: ignore[import-not-found]

        signals.before_task_publish.disconnect(_on_before_publish)
        signals.task_prerun.disconnect(_on_prerun)
        signals.task_postrun.disconnect(_on_postrun)


# ---------------------------------------------------------------- producer

def _apply_async_wrapper(wrapped, instance, args, kwargs):
    span = current_span()
    if span is None:
        return wrapped(*args, **kwargs)

    # Open the event before the publish: the context the signal receiver
    # writes into the message headers must carry this call's own sequence.
    event = span.new_span_event(
        _OPERATION_APPLY_ASYNC, service_type=SERVICE_TYPE_CELERY)
    if not span_is_sampled(span):
        return wrapped(*args, **kwargs)

    _annotate_publish(event, instance)
    token = _publish_event.set(event)
    try:
        with span_event_scope(event):
            return wrapped(*args, **kwargs)
    finally:
        _publish_event.reset(token)


@safe_try
def _annotate_publish(event, task) -> None:
    # The routing key is stamped later, by the publish receiver; until then the
    # task's own queue (or the default) is the best destination we have — and
    # the one that stays when ``task_always_eager`` publishes nothing.
    event.set_destination(_static_queue(task) or "Unknown")
    event.set_end_point(_broker_endpoint(task.app) or "Unknown")


def _static_queue(task) -> str:
    queue = getattr(task, "queue", None)
    if not queue:
        try:
            queue = task.app.conf.task_default_queue
        except Exception:  # noqa: BLE001
            queue = None
    return str(queue) if queue else ""


@safe_try
def _on_before_publish(headers=None, routing_key=None, exchange=None, **_):
    """Stamp the resolved destination on the open event, then inject.

    ``headers`` is the dict Celery publishes as the message headers; on the
    worker they come back as attributes of ``task.request``. Destination
    before inject: the context writer derives ``Pinpoint-Host`` from it.
    """
    span = current_span()
    if span is None or headers is None:
        return
    event = _publish_event.get()
    if event is not None:
        event.set_destination(routing_key or exchange or "Unknown")
    for key, value in inject_items(span):
        headers[str(key)] = str(value)


def _broker_endpoint(app) -> str:
    """``host:port`` of the app's broker, parsed once per distinct URL.

    Read off ``conf.broker_url`` — the configuration string, not a connection —
    so nothing here opens or touches a transport.
    """
    try:
        url = app.conf.broker_url
    except Exception:  # noqa: BLE001
        return ""
    if isinstance(url, (list, tuple)):
        url = url[0] if url else ""
    return _parse_broker_endpoint(str(url or ""))


@functools.lru_cache(maxsize=16)
def _parse_broker_endpoint(url: str) -> str:
    # ``amqp://`` with no host means localhost to kombu; mirror that rather
    # than report nothing.
    try:
        parts = urlsplit(url.split(";", 1)[0])
    except Exception:  # noqa: BLE001
        return ""
    host = parts.hostname or ("localhost" if parts.scheme else "")
    if not host:
        return ""
    try:
        port = parts.port
    except ValueError:
        port = None
    return f"{host}:{port}" if port else host


# ---------------------------------------------------------------- worker

class _RequestHeaders:
    """Lazy ``Pinpoint-*`` reader over ``task.request`` for ``Agent.new_span``.

    Protocol 2 spreads the message headers over the request Context as
    attributes, so ``request.get(name)`` finds them; the nested ``headers``
    mapping covers a producer that put them there instead.
    """

    __slots__ = ("_request", "_nested")

    def __init__(self, request) -> None:
        self._request = request
        nested = getattr(request, "headers", None)
        self._nested = nested if isinstance(nested, dict) else None

    def get(self, name, default=None):
        value = self._request.get(name)
        if value is None and self._nested is not None:
            value = self._nested.get(name)
        return default if value is None else value


@safe_try
def _on_prerun(task=None, sender=None, **_):
    agent = get_agent()
    if agent is None or not agent.enabled:
        return
    # A span is already current when the task runs inline — ``apply()`` or
    # ``task_always_eager`` inside a traced request — and that caller's trace
    # is where the work belongs; a root span here would re-parent it.
    if current_span() is not None:
        return
    task = task if task is not None else sender
    request = task.request
    span = None
    try:
        span = agent.new_span(
            _OPERATION_WORKER_INVOCATION, task.name,
            headers=_RequestHeaders(request),
        )
        _annotate_run(span, task, request)
        event = span.new_span_event(_OPERATION_RUN)
        token = set_current_span(span)
        setattr(request, _SCOPE_ATTR, (span, event, token))
    except Exception:  # noqa: BLE001
        end_quietly(span)
        raise


@safe_try
def _annotate_run(span, task, request) -> None:
    # A no-op on an unsampled span, but the lookups are not free and this
    # runs per task — so gate on ``span.sampled``.
    if not span.sampled:
        return
    span.set_service_type(SERVICE_TYPE_CELERY_WORKER)
    delivery = getattr(request, "delivery_info", None) or {}
    queue = delivery.get("routing_key") or delivery.get("exchange")
    # Matches the producer event's destination, so the UI draws one queue node
    # between the two applications.
    span.set_acceptor_host(str(queue) if queue else "Unknown")
    broker = _broker_endpoint(task.app) or "Unknown"
    span.set_remote_address(broker)
    span.set_end_point(broker)


@safe_try
def _on_postrun(task=None, sender=None, retval=None, state=None, **_):
    task = task if task is not None else sender
    request = task.request
    scope = getattr(request, _SCOPE_ATTR, None)
    if scope is None:
        return
    try:
        delattr(request, _SCOPE_ATTR)
    except Exception:  # noqa: BLE001
        pass
    # ``retval`` is the exception on FAILURE. RETRY / IGNORED / REJECTED carry
    # Celery control-flow exceptions, not failures of the task.
    if state == _STATE_FAILURE and isinstance(retval, BaseException):
        record_exception_on_span(scope[0], retval)
    close_span_scope(scope)


def instrument() -> None:
    CeleryInstrumentor().instrument()
