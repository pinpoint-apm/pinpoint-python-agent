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

"""Django instrumentation at the framework's WSGI/ASGI boundaries.

The root span wraps ``WSGIHandler.__call__`` or ``ASGIHandler.__call__`` so it
stays alive until the response body has been fully sent.  The synchronous and
asynchronous ``BaseHandler._get_response*`` methods are wrapped separately to
record the resolved view as a child span event.
"""

from __future__ import annotations

from typing import Any

import wrapt  # type: ignore[import-not-found]

from ..._log import get_logger
from ...context import _adopt_current_span, current_span
from ...errors import safe_try
from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_PYTHON_METHOD
from .._util import (
    cached_operation_name,
    mro_has,
    span_event_scope,
    span_is_sampled,
    wrap,
)
from ..asgi import asgi_entry_wrapper
from ..wsgi import wsgi_entry_wrapper

_log = get_logger("django")
_OPERATION_SERVER = "Django HTTP Server"
# Placeholder name for the view span event until resolver_match is known;
# survives only when Django never resolved the URL (e.g. 404 during routing).
_OPERATION_VIEW_FALLBACK = "django.request"


class DjangoInstrumentor(BaseInstrumentor):
    """Explicit ``instrument()`` entry point: both transports, each installed
    by its transport-scoped instrumentor once that handler module has finished
    importing (autoload registers those two directly)."""

    def _instrument(self) -> None:
        # Explicit instrumentation can run before either transport is imported, so
        # register lazily and let WSGI-only / ASGI-only apps skip the unused sibling.
        wrapt.register_post_import_hook(
            self._transport_hook(DjangoWSGIInstrumentor), "django.core.handlers.wsgi",
        )
        wrapt.register_post_import_hook(
            self._transport_hook(DjangoASGIInstrumentor), "django.core.handlers.asgi",
        )

    def _transport_hook(self, transport):
        # wrapt import hooks cannot be unregistered, so a hook that fires after
        # uninstrument must be a no-op (the same rule _wrap_target follows).
        @safe_try
        def _hook(_module) -> None:
            if self._installing or self._installed:
                transport().instrument()
        return _hook

    def uninstrument(self) -> None:
        # Autoload installs transport-scoped instrumentors so one disabled
        # transport cannot be revived by the other. Keep the public all-Django
        # instrumentor's teardown semantics by removing those installations too.
        super().uninstrument()
        DjangoWSGIInstrumentor().uninstrument()
        DjangoASGIInstrumentor().uninstrument()


class DjangoWSGIInstrumentor(BaseInstrumentor):
    """Transport-scoped instrumentor used by the WSGI autoload hook."""


    def _instrument(self) -> None:
        wrap(
            "django.core.handlers.base",
            "BaseHandler._get_response",
            _handler_wrapper,
        )
        wrap(
            "django.core.handlers.wsgi",
            "WSGIHandler.__call__",
            _wsgi_handler_wrapper,
        )


class DjangoASGIInstrumentor(BaseInstrumentor):
    """Transport-scoped instrumentor used by the ASGI autoload hook."""


    def _instrument(self) -> None:
        wrap(
            "django.core.handlers.base",
            "BaseHandler._get_response_async",
            _async_handler_wrapper,
        )
        wrap(
            "django.core.handlers.asgi",
            "ASGIHandler.__call__",
            _asgi_handler_wrapper,
        )
        # Django 5+ processes requests in a framework-owned child Task, so adopt the
        # inherited span at the first child-task entry point — before middleware or
        # _get_response_async resolves it and triggers the generic task fork.
        wrap(
            "django.core.handlers.asgi",
            "ASGIHandler.run_get_response",
            _asgi_run_get_response_wrapper,
        )


# ---- WSGI root span --------------------------------------------------------


_wsgi_handler_wrapper = wsgi_entry_wrapper(_OPERATION_SERVER)


# ---- ASGI root span --------------------------------------------------------


async def _asgi_run_get_response_wrapper(wrapped, instance, args, kwargs):
    """Make Django's internal request Task the owner of the request span."""
    try:
        _adopt_current_span()
    except Exception:  # noqa: BLE001
        # Context adoption is tracing-only. If it ever fails, preserve the
        # request and let the ordinary current_span() fork behavior take over.
        _log.debug("django ASGI span adoption failed", exc_info=True)
    return await wrapped(*args, **kwargs)


_asgi_handler_wrapper = asgi_entry_wrapper(_OPERATION_SERVER)


def _handler_wrapper(wrapped, instance, args, kwargs):
    """Wrap ``BaseHandler._get_response`` so the resolved view shows up as
    its own span event named after the Python callable that actually runs,
    mirroring flask's ``dispatch_request`` hook.

    URL resolution is *not* re-run here: Django resolves the path exactly
    once inside ``_get_response`` and stores the result on
    ``request.resolver_match`` — an O(#routes) regex walk we'd otherwise
    duplicate on every request. The span event opens under a placeholder
    name and is renamed from ``resolver_match`` just before it ends
    (buffered metadata flushes at ``end()``, so the late rename is the
    name the collector sees). The matched route template is stashed on the
    request's transport mapping for root-span URL-stat finalization.
    """
    span = current_span()
    if span is None:
        return wrapped(*args, **kwargs)

    request = args[0] if args else kwargs.get("request")
    if request is None:
        return wrapped(*args, **kwargs)

    if not span_is_sampled(span):
        # Unsampled: no span event; only recover the route template for
        # URL-stat bucketing after Django's own resolution ran — and only
        # when URL stats are collected (the flask/pyramid hooks gate the
        # same way).
        if not span._collect_url_stat:
            return wrapped(*args, **kwargs)
        try:
            return wrapped(*args, **kwargs)
        finally:
            _capture_resolver_match(request)

    event = span.new_span_event(
        _OPERATION_VIEW_FALLBACK, service_type=SERVICE_TYPE_PYTHON_METHOD,
    )
    # ``raise Http404`` is Django's idiomatic "return a 404" — control flow, not a
    # view error, so it must not flag the event. Any other exception does; the root
    # span captures the response status either way.
    with span_event_scope(event, _is_control_flow_exception):
        try:
            response = wrapped(*args, **kwargs)
            _stash_buffered_hint(request, response)
            return response
        finally:
            _capture_resolver_match(request, event)


async def _async_handler_wrapper(wrapped, instance, args, kwargs):
    """Async counterpart of :func:`_handler_wrapper` for Django ASGI.

    ``BaseHandler._get_response_async`` performs route resolution and awaits
    the selected async/sync-adapted view. Keeping the event open across that
    await captures the actual view duration and errors.
    """
    span = current_span()
    if span is None:
        return await wrapped(*args, **kwargs)

    request = args[0] if args else kwargs.get("request")
    if request is None:
        return await wrapped(*args, **kwargs)

    if not span_is_sampled(span):
        # See the sync hook: stash the route template only when URL stats
        # are collected.
        if not span._collect_url_stat:
            return await wrapped(*args, **kwargs)
        try:
            return await wrapped(*args, **kwargs)
        finally:
            _capture_resolver_match(request)

    # safe_wrapper can't guard an async body (only coroutine creation), so a
    # failure here would escape into the handler and 500 a request the view
    # would have served; fall back to the untraced call instead. Same guard as
    # the asgi/starlette/fastapi/falcon async wrappers.
    try:
        event = span.new_span_event(
            _OPERATION_VIEW_FALLBACK, service_type=SERVICE_TYPE_PYTHON_METHOD,
        )
    except Exception:  # noqa: BLE001
        _log.debug("new_span_event failed in async view wrapper", exc_info=True)
        return await wrapped(*args, **kwargs)
    with span_event_scope(event, _is_control_flow_exception):
        try:
            return await wrapped(*args, **kwargs)
        finally:
            _capture_resolver_match(request, event)


def _is_control_flow_exception(exc) -> bool:
    """True for Django's ``Http404`` — the idiomatic "return a 404" signal,
    converted by Django to a 404 response: control flow, not a view failure
    (it carries no status attribute, so ``http_exception_status`` doesn't
    apply). The ``django`` module gate keeps an unrelated class merely named
    ``Http404`` from being swallowed."""
    return mro_has(exc, "Http404", module_prefix="django")


@safe_try
def _stash_buffered_hint(request, response) -> None:
    """Mark the environ when the response body is already materialized, so the
    shared WSGI root finalizes eagerly instead of handing the drain to an async
    child span — ``HttpResponse`` hides its buffered body behind an iterable
    without ``__len__``. ``StreamingHttpResponse`` (``streaming=True``) keeps
    the hand-off. Middleware outside ``_get_response`` could still swap the
    response for a streaming one; the hint then only costs that drain its
    tracing, never correctness. Sampled WSGI only: ``request.META`` is the
    original environ there (the ASGI transport never reads the hint)."""
    if getattr(response, "streaming", None) is False:
        meta = getattr(request, "META", None)
        if isinstance(meta, dict):
            meta["pinpoint.response_buffered"] = True


@safe_try
def _capture_resolver_match(request, event=None) -> None:
    """Stash the matched route template and (optionally) rename ``event``
    after the view's qualname, using the ``resolver_match`` Django set
    during its own URL resolution. No-op when resolution never happened
    (404 before routing, raw ASGI request, …)."""
    match = getattr(request, "resolver_match", None)
    if match is None:
        return
    url_pattern = getattr(match, "route", "") or ""
    if url_pattern:
        # WSGIRequest.META is the original environ; ASGIRequest.scope is the
        # original scope. Stash on both defensively because ASGIRequest.META is
        # a separate dict and cannot carry the route back to the outer layer.
        for transport in (
            getattr(request, "META", None),
            getattr(request, "scope", None),
        ):
            if isinstance(transport, dict):
                try:
                    transport["pinpoint.url_pattern"] = url_pattern
                except Exception:  # noqa: BLE001
                    pass
    if event is None:
        return
    callback = getattr(match, "func", None)
    if callback is None:
        return
    operation_name = cached_operation_name(
        callback, default=getattr(match, "url_name", None) or "")
    if operation_name:
        event.set_operation_name(operation_name)


def instrument() -> None:
    DjangoInstrumentor().instrument()


def instrument_wsgi(*_args: Any, **_kwargs: Any) -> None:
    """Autoload entry point that runs after Django's WSGI module is complete."""
    DjangoWSGIInstrumentor().instrument()


def instrument_asgi(*_args: Any, **_kwargs: Any) -> None:
    """Autoload entry point that runs after Django's ASGI module is complete."""
    DjangoASGIInstrumentor().instrument()
