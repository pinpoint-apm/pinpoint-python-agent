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

"""Tornado server instrumentation.

Tornado has its own asyncio HTTP stack (no ASGI). The natural per-request
hook is ``tornado.web.RequestHandler._execute`` — the framework's coroutine
that resolves arguments, runs ``prepare()``, dispatches to ``get/post/...``,
and then calls ``finish()``. Wrapping ``_execute`` covers the entire request
lifecycle from a single point.

Tornado resolves routes with ``URLSpec`` regexes and never puts the matched
template on the handler, so url_stat is bucketed by ``handler.request.path``.
Each request also gets a Python-method span event named
``<HandlerClass>.<method>`` — the identifier a Tornado user recognises.
"""

from __future__ import annotations

import functools

from ..._log import get_logger
from ...agent import get_agent
from ...context import current_span, set_current_span
from ...errors import safe_try
from ...http_helper import (
    MultiDictHeaderReader,
    has_pinpoint_mapping,
    record_response_header_enabled,
)
from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_PYTHON_METHOD
from .._util import (
    annotate_server_request,
    control_flow_predicate,
    end_quietly,
    end_server_span,
    record_exception_on_span,
    reset_quietly,
    span_event_scope,
    wrap,
)

_log = get_logger("tornado")
_OPERATION_SERVER = "Tornado HTTP Server"
# Stamped on the per-request RequestHandler while our root span is open, so a
# stacked wrapper re-entering ``_execute`` for the *same* handler passes through.
# Keyed on the handler, not ``current_span()``, so an ambient/leaked context span
# can't disable tracing for every request.
_ROOT_SPAN_ACTIVE_ATTR = "_pinpoint_root_span_active"


class TornadoInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        # _execute is called once per request and returns a Future. We wrap
        # the bound coroutine so the span lifetime equals the request.
        wrap("tornado.web", "RequestHandler._execute", _execute_wrapper)
        # log_exception is the canonical sink Tornado uses for any exception
        # that escapes a handler — including HTTPError from RequestHandler.
        wrap("tornado.web", "RequestHandler.log_exception",
             _log_exception_wrapper)


# ---- per-request span ------------------------------------------------------

async def _execute_wrapper(wrapped, instance, args, kwargs):
    """Open a root span around RequestHandler._execute.

    `instance` is the RequestHandler. `instance.request` is the
    HTTPServerRequest carrying method/uri/headers."""
    request = getattr(instance, "request", None)
    if request is None:
        return await wrapped(*args, **kwargs)

    agent = get_agent()
    if agent is None or not agent.enabled:
        return await wrapped(*args, **kwargs)

    # A stacked wrapper (left by a re-instrument cycle) may re-enter ``_execute``
    # for THIS request, which would yield two root spans. Keyed on the per-request
    # handler marker, not ``current_span()``: asyncio copies the ambient context
    # into every request task, so a leaked span would make ``current_span()``
    # non-None for every request and silently drop tracing for the whole server.
    if _root_span_active(instance):
        _log.debug("tornado _execute re-entered for a request we already "
                   "traced; passing through")
        return await wrapped(*args, **kwargs)
    if current_span() is not None:
        # A span is visible in this task's context but no per-request marker is
        # set: it leaked in rather than being opened by us. Surface it and still
        # open our own root span below.
        _log.debug("tornado: span present in inherited context without a "
                   "per-request marker; opening a fresh root span "
                   "(possible context leak)")

    # A WebSocketHandler's ``_execute`` runs the receive-frame loop and only returns
    # at close, so a root span here would stay unfinished for the whole connection,
    # grow a span event per message, and report nothing until disconnect. Bypass it,
    # as the ASGI middleware does for non-``http`` scopes.
    if _is_websocket_handler(instance, request):
        return await wrapped(*args, **kwargs)

    # Unguarded by safe_wrapper (async body): an exception in the pre-dispatch setup
    # would escape into Tornado's connection machinery and, past ``set_current_span``,
    # leak the span into later keep-alive requests. Tear down, run untraced.
    span = None
    token = None
    try:
        # Tornado already normalizes request.method to uppercase, so it goes
        # to new_span as-is and the span-event name below lowercases it.
        method = getattr(request, "method", None) or "GET"
        path = getattr(request, "path", None) or "/"
        # The reader is a cheap lazy wrapper, but hand it to new_span only when an
        # upstream Pinpoint header is actually present.
        raw_headers = getattr(request, "headers", None) or {}
        headers = MultiDictHeaderReader(raw_headers)
        span = agent.new_span(
            _OPERATION_SERVER, path,
            headers=headers if has_pinpoint_mapping(raw_headers) else None,
            method=method,
        )
        sampled = span.sampled
        if sampled:
            _annotate_request(span, request, headers)
        # Stamp the per-request marker BEFORE handing control on, so a stacked
        # inner wrapper re-entering ``_execute`` for this same handler sees it
        # and passes through instead of opening a second root span.
        _mark_root_span_active(instance)
        token = set_current_span(span)

        # A Python-method event named after handler class+method (UserHandler.get)
        # so the UI shows what code ran, lasting the whole dispatch either way.
        op_name = f"{type(instance).__name__}.{method.lower()}" if sampled else ""
        event = span.new_span_event(op_name, service_type=SERVICE_TYPE_PYTHON_METHOD)
    except Exception:  # noqa: BLE001
        _log.debug("tornado span setup failed", exc_info=True)
        reset_quietly(token)
        end_quietly(span)
        return await wrapped(*args, **kwargs)

    try:
        # Same control-flow classifier as the span below: ``raise HTTPError(404)`` /
        # ``Finish`` is control flow, so only 5xx / non-HTTP exceptions flag it.
        with span_event_scope(event, _is_control_flow_exception):
            result = await wrapped(*args, **kwargs)
    except BaseException as exc:
        if sampled and not _is_control_flow_exception(exc):
            record_exception_on_span(span, exc)
        reset_quietly(token)
        _end_span(span, instance, request, sampled)
        raise

    reset_quietly(token)
    _end_span(span, instance, request, sampled)
    return result


def _log_exception_wrapper(wrapped, instance, args, kwargs):
    """Tornado routes every handler exception through ``log_exception`` —
    including ``HTTPError``/``Finish``, which handlers ``raise`` as the normal
    way to produce a non-2xx response. Recording those as span errors would
    inflate the transaction error rate with ordinary 4xx/redirect control flow,
    so we mirror only genuine failures (5xx / non-HTTP exceptions) onto the
    span. The response status code is still recorded separately by
    ``_end_span``, and the native recorder marks 5xx as failed on its own."""
    span = current_span()
    if span is not None and len(args) >= 2:
        exc = args[1]
        if not _is_control_flow_exception(exc):
            record_exception_on_span(span, exc)
    return wrapped(*args, **kwargs)


# ---- helpers ---------------------------------------------------------------

def _root_span_active(handler) -> bool:
    """True when our own wrapper already opened this request's root span.

    Read from a per-request marker on the handler, never from ``current_span()``
    — see the guard in ``_execute_wrapper`` for why the contextvar is unsafe
    here.
    """
    try:
        return bool(getattr(handler, _ROOT_SPAN_ACTIVE_ATTR, False))
    except Exception:  # noqa: BLE001
        return False


def _mark_root_span_active(handler) -> None:
    try:
        setattr(handler, _ROOT_SPAN_ACTIVE_ATTR, True)
    except Exception:  # noqa: BLE001
        # A handler that forbids attribute assignment simply won't carry the
        # marker; a stacked wrapper then at worst opens a duplicate root span —
        # far better than the current_span()-based guard dropping all traces.
        pass


# raise HTTPError(404) / Finish end a response as normal control flow; Finish
# carries no status, which http_exception_status reports as 0 — control flow.
# The tornado.web gate keeps foreign HTTPError classes real errors.
_is_control_flow_exception = control_flow_predicate(
    ("HTTPError", "Finish"), module_prefix="tornado.web")


@functools.lru_cache(maxsize=1024)
def _is_websocket_class(cls) -> bool:
    # Fixed per handler class (the app's route table), so the MRO walk is paid
    # once per class rather than per request.
    return any(klass.__name__ == "WebSocketHandler" for klass in cls.__mro__)


def _is_websocket_handler(handler, request) -> bool:
    """True for a Tornado ``WebSocketHandler`` (or a genuine websocket upgrade
    handshake).

    ``_execute`` stays pending for a websocket's entire connection lifetime, so
    a root span opened here would never close in time. The primary match is by
    class name across the MRO — this avoids importing ``tornado.websocket`` and
    still catches user subclasses.

    We also fall back to the request headers for any handler that speaks the
    handshake without subclassing ``WebSocketHandler`` — but *only* for a real
    handshake. Keying on the ``Upgrade`` header alone would also match a plain
    HTTP request that merely carries ``Upgrade: websocket`` (a client quirk or a
    misbehaving proxy); that request, routed to an ordinary handler that never
    upgrades, would silently lose its root span and — since ``current_span()``
    is then ``None`` — all of its downstream child calls too. Mirror aiohttp's
    ``_is_websocket_request`` gate: an upgrade requires BOTH ``Upgrade:
    websocket`` and a ``Connection`` header that carries the ``upgrade`` token
    (both case-insensitive; ``Connection`` is a comma-separated token list such
    as ``keep-alive, Upgrade``, so a substring match mirrors aiohttp exactly).
    """
    if _is_websocket_class(type(handler)):
        return True
    try:
        raw = getattr(request, "headers", None)
        if raw is not None:
            upgrade = raw.get("Upgrade") or raw.get("upgrade") or ""
            if str(upgrade).strip().lower() == "websocket":
                connection = raw.get("Connection") or raw.get("connection") or ""
                return "upgrade" in str(connection).lower()
    except Exception:  # noqa: BLE001
        pass
    return False


@safe_try
def _annotate_request(span, request, headers) -> None:
    remote = getattr(request, "remote_ip", "") or ""
    host = getattr(request, "host", "") or ""
    annotate_server_request(
        span, str(remote), str(host), headers,
        query_string=str(getattr(request, "query", "") or ""))


@safe_try
def _end_span(span, handler, request, sampled: bool = True) -> None:
    """Tornado RequestHandlers track status on themselves via
    ``handler.get_status()`` — this is the only authoritative place to read
    it once the handler has called ``set_status`` or ``finish``."""
    status = 0
    try:
        if handler is not None and hasattr(handler, "get_status"):
            status = int(handler.get_status() or 0)
    except Exception:  # noqa: BLE001
        status = 0
    method = (getattr(request, "method", None) or "").upper() if request else ""
    path = (getattr(request, "path", None) or "") if request else ""
    response_headers = (
        _response_headers(handler)
        if sampled and record_response_header_enabled(span) else ()
    )
    end_server_span(span, path or "/", method, status,
                    response_headers, sampled)


def _response_headers(handler):
    """Read ``handler._headers`` (tornado.httputil.HTTPHeaders) for response headers.

    Tornado does not expose a documented public accessor; the private
    ``_headers`` attribute has been stable for a decade and is what
    ``RequestHandler.set_header`` mutates.
    """
    if handler is None:
        return ()
    raw = getattr(handler, "_headers", None)
    if raw is None:
        return ()
    try:
        # Return raw pairs — HeadersReader stringifies them once downstream, so
        # a str-cast list here would just do it twice.
        if hasattr(raw, "get_all"):
            return list(raw.get_all())
        return list(raw.items())
    except Exception:  # noqa: BLE001
        return ()


def instrument() -> None:
    TornadoInstrumentor().instrument()
