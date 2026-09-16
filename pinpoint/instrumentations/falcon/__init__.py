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

"""Falcon instrumentation (WSGI + ASGI).

Falcon ships two App classes — ``falcon.app.App`` (WSGI) and
``falcon.asgi.app.App`` (ASGI subclass of the WSGI one). Both share the
same ``_get_responder`` resolver, which returns a 4-tuple of
``(responder, params, resource, uri_template)`` after routing. We hook:

- ``App.__call__`` on both WSGI and ASGI Apps to open the Pinpoint root
  span per request (separate wrappers — the two ``__call__`` methods are
  distinct).
- ``App._get_responder`` (WSGI only — the ASGI App inherits it, so one
  wrap covers both transports) to stash the matched URI template on
  ``req.env`` / ``req.scope`` (for url_stat aggregation per route) and
  to swap the responder with a span-event-emitting wrapper named after
  the user's resource method (``UserResource.on_get``, …).

The ASGI root-span lifecycle reuses the framework-agnostic ASGI runner rather
than duplicating the scope/header machinery — falcon adds nothing
transport-specific on top.
"""

from __future__ import annotations

import functools
import inspect
import wrapt  # type: ignore[import-not-found]

from ..._log import get_logger
from ...context import current_span
from ...errors import safe_try
from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_PYTHON_METHOD
from .._util import (
    callable_operation_name,
    control_flow_predicate,
    span_event_scope,
    wrap,
)
from ..asgi import asgi_entry_wrapper
from ..wsgi import wsgi_entry_wrapper

_log = get_logger("falcon")
_OPERATION_SERVER = "Falcon HTTP Server"


class FalconInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        wrap("falcon.app", "App.__call__", _wsgi_call_wrapper)
        # ``_get_responder`` is shared — the ASGI App subclasses the WSGI
        # App and inherits this method, so one wrap covers both transports.
        wrap("falcon.app", "App._get_responder", _get_responder_wrapper)
        # ``import falcon`` does not pull in ``falcon.asgi.app``, so hook it
        # post-import and wrap the ASGI ``__call__`` lazily.
        try:
            wrapt.register_post_import_hook(
                self._install_asgi_hook, "falcon.asgi.app"
            )
        except Exception:  # noqa: BLE001
            _log.debug("falcon.asgi post-import hook failed", exc_info=True)

    def _uninstrument(self) -> None:
        # The cache holds bound methods, i.e. the user's resource instances,
        # strongly; do not pin them past the integration's own lifetime.
        _responder_cache.clear()

    @safe_try
    def _install_asgi_hook(self, _module) -> None:
        self._wrap_target(
            "falcon.asgi.app", "App.__call__", _asgi_call_wrapper
        )


# ---- WSGI root span --------------------------------------------------------


_wsgi_call_wrapper = wsgi_entry_wrapper(_OPERATION_SERVER)


# ---- ASGI root span --------------------------------------------------------

# Falcon's ASGI App does the same scope/header/send shape as any other ASGI 3
# app — the only falcon-specific bit (URL template) is wired in by
# ``_get_responder_wrapper`` via ``scope['pinpoint.url_pattern']``.
_asgi_call_wrapper = asgi_entry_wrapper(_OPERATION_SERVER)


# ---- _get_responder wrap (shared WSGI/ASGI) --------------------------------


def _get_responder_wrapper(wrapped, instance, args, kwargs):
    """After falcon resolves a route:

    1. Stash the URI template (e.g. ``/items/{id}``) onto ``req.env`` /
       ``req.scope`` under ``pinpoint.url_pattern`` so the root span's
       url_stat aggregates per route template, not per concrete path.
    2. Swap the responder with a wrapper that emits a Python-method span
       event named after the user's responder (e.g.
       ``UserResource.on_get``), keeping the original behavior intact.
    """
    req = args[0] if args else kwargs.get("req")
    result = wrapped(*args, **kwargs)
    if not isinstance(result, tuple) or len(result) < 4:
        return result
    responder, params, resource, uri_template = (
        result[0], result[1], result[2], result[3],
    )
    _stash_url_pattern(req, uri_template)
    traced = _traced_responder_for(responder)
    if traced is None:
        return result
    return (traced, params, resource, uri_template) + tuple(result[4:])


# Routes are static after startup, so ``_wrap_responder``'s reflection + closure
# work is paid per responder, not per request. Bound methods hash by (instance,
# function), so an equal rebinding of the same resource method still hits the cache.
# Values hold strong refs (resources live for the process) under a size cap, and an
# overflow or unhashable responder falls back to wrapping per call.
_RESPONDER_CACHE_MAX = 2048
_responder_cache: dict = {}
# Cached "don't trace" marker: distinguishes a known-unwrappable responder
# (e.g. no resolvable name) from a cache miss, so it isn't re-reflected on
# every request either.
_RESPONDER_UNTRACED = False


def _traced_responder_for(responder):
    if responder is None:
        return None
    try:
        cached = _responder_cache.get(responder)
    except TypeError:
        # Unhashable responder — wrap it per call instead.
        return _wrap_responder(responder)
    if cached is not None:
        return cached or None
    traced = _wrap_responder(responder)
    if len(_responder_cache) < _RESPONDER_CACHE_MAX:
        _responder_cache[responder] = (
            traced if traced is not None else _RESPONDER_UNTRACED
        )
    return traced


@safe_try
def _stash_url_pattern(req, uri_template) -> None:
    if not uri_template or req is None:
        return
    # ``req.env`` (WSGI) / ``req.scope`` (ASGI). Explicit ``is not None``: an empty
    # dict is falsy and would fall through to the other attribute.
    target = getattr(req, "env", None)
    if target is None:
        target = getattr(req, "scope", None)
    if target is None:
        return
    try:
        target["pinpoint.url_pattern"] = uri_template
    except Exception:  # noqa: BLE001
        pass


def _is_async_responder(responder) -> bool:
    """Detect async responders robustly.

    Falcon calls responders as ``responder(req, resp, **params)`` — sync for
    WSGI, awaited for ASGI. A plain ``async def`` is caught by
    ``inspect.iscoroutinefunction``, but two valid shapes slip past a bare
    check on the responder object: a callable *object* whose ``__call__`` is
    async, and a ``functools.partial`` wrapping a coroutine function. Missing
    them routes an async responder down the sync path, where the returned
    coroutine is never awaited by us and its span event ends with zero
    duration (handler exceptions unrecorded). We unwrap ``partial`` and also
    inspect ``__call__`` so those shapes take the async trace path.
    """
    target = responder
    # Unwrap functools.partial chains down to the underlying callable.
    while isinstance(target, functools.partial):
        target = target.func
    if inspect.iscoroutinefunction(target):
        return True
    call = getattr(target, "__call__", None)
    return call is not None and inspect.iscoroutinefunction(call)


# falcon responders raise HTTPError/HTTPStatus (status on ``.status`` as a
# "400 Bad Request" string or an int) to produce non-2xx responses; the module
# gate keeps foreign HTTPError classes (urllib.error, requests) real errors.
_is_control_flow_exception = control_flow_predicate(
    ("HTTPError", "HTTPStatus"), module_prefix="falcon",
    status_attrs=("status", "status_code"))


def _wrap_responder(responder):
    """Return a transparent wrapper around ``responder`` that opens a span
    event on call. Falcon calls responders as ``responder(req, resp, **params)``
    (sync for WSGI, async for ASGI); we detect async shapes via
    ``_is_async_responder`` and emit the matching wrapper so awaitable returns
    stay awaitable.
    """
    if responder is None:
        return None
    op_name = callable_operation_name(responder)
    if not op_name:
        return None

    # Both closures replace the responder directly rather than via safe_wrapper, so
    # a native failure in new_span_event would escape into falcon's dispatch and 500
    # the request — guard and fall back untraced. ``functools.wraps`` (skipping any
    # attribute the responder lacks) keeps middleware and ``falcon.hooks``
    # introspection seeing the real responder, not an anonymous ``_traced``.
    if _is_async_responder(responder):
        @functools.wraps(responder)
        async def _traced(*a, **kw):
            span = current_span()
            if span is None:
                return await responder(*a, **kw)
            try:
                event = span.new_span_event(
                    op_name, service_type=SERVICE_TYPE_PYTHON_METHOD,
                )
            except Exception:  # noqa: BLE001
                return await responder(*a, **kw)
            with span_event_scope(event, _is_control_flow_exception):
                return await responder(*a, **kw)
        return _traced

    @functools.wraps(responder)
    def _traced_sync(*a, **kw):
        span = current_span()
        if span is None:
            return responder(*a, **kw)
        try:
            event = span.new_span_event(
                op_name, service_type=SERVICE_TYPE_PYTHON_METHOD,
            )
        except Exception:  # noqa: BLE001
            return responder(*a, **kw)
        with span_event_scope(event, _is_control_flow_exception):
            return responder(*a, **kw)
    return _traced_sync


def instrument() -> None:
    FalconInstrumentor().instrument()
