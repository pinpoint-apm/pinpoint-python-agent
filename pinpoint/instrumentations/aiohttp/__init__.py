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

"""aiohttp server + client instrumentation.

aiohttp ships its own asyncio HTTP stack (no ASGI). The cleanest server hook
is ``aiohttp.web_protocol.RequestHandler._handle_request`` — the per-request
entry point that is called once per HTTP message after parsing. By wrapping
it we open the Pinpoint root span before any application middleware runs and
close it once the response is fully assembled, regardless of how the user
configured ``Application(middlewares=[...])``.

We also wrap ``aiohttp.web_urldispatcher.UrlDispatcher.resolve`` so that the
matched ``Resource`` template (e.g. ``/items/{id}``) ends up on the request
for url_stat aggregation.

Outbound ``aiohttp.ClientSession`` requests are instrumented by wrapping
``aiohttp.client.ClientSession._request`` — the single coroutine every
``session.get/post/...`` funnels through. Each outbound request becomes an
HTTP-client span event with the Pinpoint-* trace headers injected into a
per-request *copy* of the caller's headers, so cross-service traces stitch
through every aiohttp hop exactly as they do for httpx/requests/urllib3.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

import functools
import threading
import weakref

from ..._log import get_logger
from ...agent import get_agent
from ...context import current_span, set_current_span
from ...errors import safe_try
from ...http_helper import (
    MultiDictHeaderReader,
    has_pinpoint_mapping,
    record_response_header_enabled,
    trace_http_client_request,
)
from ...instrumentor import BaseInstrumentor
from ...propagator import inject_items
from ...service_type import (
    SERVICE_TYPE_PYTHON_HTTP_CLIENT,
    SERVICE_TYPE_PYTHON_METHOD,
)
from .._util import (
    annotate_client_response,
    annotate_server_request,
    callable_operation_name,
    control_flow_predicate,
    end_quietly,
    end_server_span,
    http_client_instrumentation_suppressed,
    mro_has,
    record_exception_on_event,
    record_exception_on_span,
    reset_quietly,
    span_event_scope,
    span_is_sampled,
    wrap,
)

_log = get_logger("aiohttp")
_OPERATION_SERVER = "aiohttp HTTP Server"
_OPERATION_CLIENT_REQUEST = "aiohttp.client.ClientSession._request"
# Stamped on the per-request Request while our root span is open, so a stacked
# wrapper re-entering ``_handle_request`` for the *same* request passes through.
# Keyed on the Request, not ``current_span()``, so an ambient/leaked context span
# can't disable tracing for every request. Shares the WSGI marker string.
_ROOT_SPAN_ACTIVE_KEY = "pinpoint.root_span_active"

# Routes whose ``_handler`` we swapped for a traced closure. The swap mutates
# *instances* the user owns (unlike the wrapt wrappers ``__wrapped__`` restores),
# so they must be remembered to undo it on shutdown. A ``WeakSet`` never keeps a
# route alive past the app's own lifetime.
_traced_routes: weakref.WeakSet = weakref.WeakSet()
# WeakSet add/iterate are Python-level and not atomic against each other: the
# loop thread registers routes while uninstrument (a signal handler, a test
# thread) snapshots them. One small lock instead of retrying a RuntimeError.
_traced_routes_lock = threading.Lock()


def _restore_traced_routes() -> None:
    # Restore every route handler we swapped so the user's route table is left
    # exactly as we found it — the traced closure must not survive shutdown as
    # a permanent (if inert) mutation.
    with _traced_routes_lock:
        routes = list(_traced_routes)
        _traced_routes.clear()
    for route in routes:
        _restore_route_handler(route)


class AiohttpServerInstrumentor(BaseInstrumentor):
    """Server-only instrumentor used by the ``web_protocol`` import hook."""


    def _instrument(self) -> None:
        wrap(
            "aiohttp.web_protocol",
            "RequestHandler._handle_request",
            _handle_request_wrapper,
        )
        wrap(
            "aiohttp.web_urldispatcher",
            "UrlDispatcher.resolve",
            _resolve_wrapper,
        )

    def _uninstrument(self) -> None:
        _restore_traced_routes()


class AiohttpClientInstrumentor(BaseInstrumentor):
    """Client-only instrumentor used by the ``client`` import hook."""


    def _instrument(self) -> None:
        wrap(
            "aiohttp.client",
            "ClientSession._request",
            _request_wrapper,
        )


def _restore_route_handler(route) -> None:
    """Put back the original ``route._handler`` if we replaced it with our
    traced closure (identified by the ``_pinpoint_traced`` stamp; the original
    is reachable via ``functools.wraps``' ``__wrapped__``)."""
    try:
        handler = getattr(route, "_handler", None)
        if not getattr(handler, "_pinpoint_traced", False):
            return
        original = getattr(handler, "__wrapped__", None)
        if original is not None:
            route._handler = original
    except Exception:  # noqa: BLE001
        _log.debug("failed to restore aiohttp route handler", exc_info=True)


# ---- per-request span ------------------------------------------------------

async def _handle_request_wrapper(wrapped, instance, args, kwargs):
    """Open a root span around the entire aiohttp request lifecycle.

    aiohttp calls ``_handle_request(request, start_time, request_handler)``
    which returns ``(resp, reset)``. We open the span, let the handler run,
    annotate status/exception from the result, then end.
    """
    request = args[0] if args else kwargs.get("request")
    if request is None:
        return await wrapped(*args, **kwargs)

    agent = get_agent()
    if agent is None or not agent.enabled:
        return await wrapped(*args, **kwargs)

    # A stacked wrapper (left by a re-instrument cycle) may re-enter
    # ``_handle_request`` for THIS request, which would yield two root spans.
    # Keyed on the per-request marker, not ``current_span()``: asyncio copies the
    # ambient context into every request task, so a leaked span would make
    # ``current_span()`` non-None for every request and silently drop tracing for
    # the whole server.
    if _root_span_active(request):
        _log.debug("aiohttp _handle_request re-entered for a request we "
                   "already traced; passing through")
        return await wrapped(*args, **kwargs)
    if current_span() is not None:
        # A span is visible in this task's context but no per-request marker is
        # set: it leaked in rather than being opened by us. Surface it and still
        # open our own root span below.
        _log.debug("aiohttp: span present in inherited context without a "
                   "per-request marker; opening a fresh root span "
                   "(possible context leak)")

    # A websocket upgrade makes the handler a long-lived receive loop, so a root
    # span here would stay unfinished for the whole connection, grow a span event
    # per message, and report nothing until disconnect. Bypass it, as the ASGI
    # middleware does for non-``http`` scopes.
    if _is_websocket_request(request):
        return await wrapped(*args, **kwargs)

    # Unguarded by safe_wrapper (async body): span setup must be guarded here, or an
    # exception escapes into aiohttp's connection machinery. Tear down, run untraced.
    span = None
    token = None
    try:
        path = _request_path(request)
        # The reader is a cheap lazy wrapper, but hand it to new_span only when an
        # upstream Pinpoint header is actually present.
        raw_headers = getattr(request, "headers", None) or {}
        headers = MultiDictHeaderReader(raw_headers)
        span = agent.new_span(
            _OPERATION_SERVER, path,
            headers=headers if has_pinpoint_mapping(raw_headers) else None,
            method=getattr(request, "method", "") or "",
        )
        sampled = span.sampled
        if sampled:
            _annotate_request(span, request, headers)
        # Stamp the per-request marker BEFORE handing control on, so a stacked
        # inner wrapper re-entering ``_handle_request`` for this same request
        # sees it and passes through instead of opening a second root span.
        _mark_root_span_active(request)
        token = set_current_span(span)
    except Exception:  # noqa: BLE001
        _log.debug("aiohttp span setup failed", exc_info=True)
        reset_quietly(token)
        end_quietly(span)
        return await wrapped(*args, **kwargs)
    status_code = 0

    try:
        resp_pair = await wrapped(*args, **kwargs)
    except BaseException as exc:
        if sampled:
            record_exception_on_span(span, exc)
        reset_quietly(token)
        # aiohttp normally turns a handler exception into a 500 response
        # itself; reaching here means it did not, so report the status the
        # client gets (see wsgi._failed_status).
        _end_span(span, request, 500 if isinstance(exc, Exception) else 0,
                  sampled=sampled)
        raise

    # aiohttp >=3 returns (StreamResponse, reset) from _handle_request; earlier
    # private versions returned just the response. Either way the response's
    # .status is settled by now.
    response = resp_pair[0] if isinstance(resp_pair, tuple) and resp_pair else resp_pair
    if response is not None:
        try:
            status_code = int(getattr(response, "status", 0) or 0)
        except Exception:  # noqa: BLE001
            pass

    reset_quietly(token)
    _end_span(span, request, status_code, response, sampled=sampled)
    return resp_pair


async def _resolve_wrapper(wrapped, instance, args, kwargs):
    """Stash the matched Resource pattern (e.g. ``/items/{id}``) on the
    request so the root span can use it for url_stat. ``UrlDispatcher.resolve``
    is an async method — we await the inner call and then annotate.

    Also idempotently wraps the matched route's underlying handler so that
    the user's coroutine shows up as its own span event named after the
    Python qualname (e.g. ``Handler.get``). ``UrlMappingMatchInfo.handler``
    is a property delegating to ``route._handler``; mutating the route
    once is the cleanest hook — every subsequent dispatch through the
    same route reuses the traced wrapper.
    """
    request = args[0] if args else kwargs.get("request")
    info = await wrapped(*args, **kwargs)
    if current_span() is None:
        # No root span, so neither the url_pattern stash nor the handler wrap
        # would be consumed. The wrap is idempotent and retried next request.
        return info
    _maybe_set_url_pattern(request, info)
    _instrument_matched_handler(info)
    return info


@safe_try
def _instrument_matched_handler(info) -> None:
    if info is None:
        return
    # ``info.route`` is the public attribute (since aiohttp 3.x); fall back
    # to the private ``_route`` slot if the API ever changes again.
    route = getattr(info, "route", None) or getattr(info, "_route", None)
    if route is None:
        return
    # Steady state first: an already-wrapped handler (every request on a route
    # after its first) is one getattr; the SystemRoute MRO walk below then only
    # runs for handlers not yet wrapped.
    handler = getattr(route, "_handler", None)
    if handler is None or getattr(handler, "_pinpoint_traced", False):
        return
    # A ``MatchInfoError`` carries a throwaway ``SystemRoute`` whose handler just
    # raises a control-flow ``HTTPException``, and aiohttp builds a fresh one per
    # unmatched request. Wrapping it would re-wrap per 404 and record the
    # control-flow exception as an error — telemetry noise.
    if mro_has(route, "SystemRoute"):  # the synthetic 404/405 route
        return
    op_name = callable_operation_name(handler)
    if not op_name:
        return

    @functools.wraps(handler)
    async def _traced(*a, **kw):
        span = current_span()
        if span is None:
            return await handler(*a, **kw)
        # Guarded: this closure isn't under safe_wrapper, so a native
        # failure would otherwise 500 the user's request.
        try:
            event = span.new_span_event(op_name, service_type=SERVICE_TYPE_PYTHON_METHOD)
        except Exception:  # noqa: BLE001
            return await handler(*a, **kw)
        try:
            return await handler(*a, **kw)
        except BaseException as exc:
            # ``raise web.HTTPNotFound()`` is aiohttp's normal way to send a non-2xx
            # response — control flow, not a failure. Record only 5xx / non-HTTP
            # exceptions; ``_end_span`` captures the status either way.
            if not _is_control_flow_exception(exc):
                record_exception_on_event(event, exc)
            raise
        finally:
            try:
                event.end()
            except Exception:  # noqa: BLE001
                pass

    _traced._pinpoint_traced = True  # type: ignore[attr-defined]
    try:
        route._handler = _traced
    except Exception:  # noqa: BLE001
        # Some custom routes may make ``_handler`` read-only; tracing the
        # handler is a nice-to-have so we simply skip on failure.
        return
    # Remember the mutated route so ``_uninstrument`` can restore its original
    # handler; ``WeakSet.add`` tolerates unhashable/weakref-incapable routes by
    # simply skipping them (they just won't be auto-restored).
    try:
        with _traced_routes_lock:
            _traced_routes.add(route)
    except Exception:  # noqa: BLE001
        pass


@safe_try
def _maybe_set_url_pattern(request, info) -> None:
    if request is None or info is None:
        return
    # info is a UrlMappingMatchInfo; .route exposes a Resource with a
    # canonical pattern attr. For dynamic resources this looks like
    # "/items/{id}". For plain resources it's the literal path.
    route = getattr(info, "route", None)
    resource = getattr(route, "resource", None) if route is not None else None
    pattern = (
        getattr(resource, "canonical", None)
        or getattr(resource, "raw_match_info", None)
    )
    if isinstance(pattern, str) and pattern:
        try:
            request["pinpoint.url_pattern"] = pattern
        except Exception:  # noqa: BLE001
            pass


# ---- helpers ---------------------------------------------------------------

def _root_span_active(request) -> bool:
    """True when our own wrapper already opened this request's root span.

    Read from a per-request marker on the Request mapping, never from
    ``current_span()`` — see the guard in ``_handle_request_wrapper`` for why
    the contextvar is unsafe here.
    """
    try:
        return bool(request.get(_ROOT_SPAN_ACTIVE_KEY))
    except Exception:  # noqa: BLE001
        return False


def _mark_root_span_active(request) -> None:
    try:
        request[_ROOT_SPAN_ACTIVE_KEY] = True
    except Exception:  # noqa: BLE001
        # A request that forbids item assignment simply won't carry the marker;
        # a stacked wrapper then at worst opens a duplicate root span — far
        # better than the current_span()-based guard dropping all traces.
        pass





# web.HTTPException doubles as a Response: handlers raise HTTPNotFound() /
# HTTPFound() to send non-2xx responses. The module gate keeps foreign
# HTTPException classes real errors.
_is_control_flow_exception = control_flow_predicate(
    "HTTPException", module_prefix="aiohttp")


def _is_websocket_request(request) -> bool:
    """True when the request is a genuine websocket upgrade handshake.

    aiohttp websocket handlers ``await ws.prepare(request)`` and then loop on
    incoming frames, so ``_handle_request`` stays pending for the connection
    lifetime. We must bypass root-span creation for those — but *only* for a
    real handshake. Keying on the ``Upgrade`` header alone would also match a
    plain HTTP request that merely carries ``Upgrade: websocket`` (a client
    quirk or a misbehaving proxy); that request, routed to an ordinary handler
    that never upgrades, would silently lose its root span and — since
    ``current_span()`` is then ``None`` — all of its downstream child calls too.

    Mirror the gate aiohttp itself applies in ``WebSocketResponse._handshake``:
    an upgrade requires BOTH ``Upgrade: websocket`` and a ``Connection`` header
    that carries the ``upgrade`` token (both case-insensitive; ``Connection`` is
    a comma-separated token list such as ``keep-alive, Upgrade``, so a substring
    match mirrors aiohttp exactly). A request missing either is served as
    ordinary HTTP — aiohttp rejects such an upgrade with 400 and the handler
    returns promptly — so it keeps its root span. (``Request.headers`` is a
    case-insensitive CIMultiDict; the extra lowercase lookup only matters for
    the plain-dict fakes in tests.)"""
    try:
        raw = getattr(request, "headers", None)
        if raw is None:
            return False
        upgrade = raw.get("Upgrade") or raw.get("upgrade") or ""
        if str(upgrade).strip().lower() != "websocket":
            return False
        connection = raw.get("Connection") or raw.get("connection") or ""
        return "upgrade" in str(connection).lower()
    except Exception:  # noqa: BLE001
        return False


def _request_path(request) -> str:
    path = getattr(request, "path", "/")
    return str(path) if path else "/"


@safe_try
def _annotate_request(span, request, headers) -> None:
    remote = getattr(request, "remote", "") or ""
    host = getattr(request, "host", "") or ""
    annotate_server_request(
        span, str(remote), str(host), headers,
        query_string=str(getattr(request, "query_string", "") or ""))


@safe_try
def _end_span(span, request, status_code: int, response=None,
              sampled: bool = True) -> None:
    method = getattr(request, "method", "") or ""
    url_pattern = ""
    try:
        url_pattern = request.get("pinpoint.url_pattern", "") or ""
    except Exception:  # noqa: BLE001
        pass
    if not url_pattern:
        url_pattern = _request_path(request)
    response_headers = (
        _response_headers(response)
        if sampled and record_response_header_enabled(span) else ()
    )
    end_server_span(span, url_pattern, method, status_code,
                    response_headers, sampled)


def _response_headers(response):
    if response is None:
        return ()
    raw = getattr(response, "headers", None)
    if raw is None:
        return ()
    try:
        # Return raw pairs — HeadersReader stringifies them once downstream, so
        # a str-cast list here would just do it twice.
        return list(raw.items())
    except Exception:  # noqa: BLE001
        return ()


# ---- client-side (outbound ClientSession) ----------------------------------

async def _request_wrapper(wrapped, instance, args, kwargs):
    """Trace one outbound ``ClientSession._request`` call.

    Every ``session.get/post/put/...`` funnels through this coroutine, so a
    single hook covers all client verbs. We open an HTTP-client span event,
    inject this request's Pinpoint context into a per-request *copy* of the
    caller's headers (never mutating a user-provided mapping — it may be shared
    across concurrent requests), await the real request, annotate status from
    the response, and end the event. A network error is recorded on the event
    and re-raised unchanged.
    """
    # A higher-level client (e.g. elasticsearch) already represents this HTTP
    # exchange — don't emit a duplicate generic event underneath it.
    if http_client_instrumentation_suppressed():
        return await wrapped(*args, **kwargs)
    span = current_span()
    if span is None:
        return await wrapped(*args, **kwargs)

    # Unguarded by safe_wrapper (async body): contain the pre-await setup here,
    # ending any event we opened and falling back to the untraced request. The user
    # ``await`` stays outside the try, so it is never swallowed nor awaited twice.
    event = None
    sampled = False
    request_kwargs = kwargs
    try:
        # Open the event before inject: the context written into the outbound headers
        # must carry this call's own depth/sequence.
        event = span.new_span_event(
            _OPERATION_CLIENT_REQUEST,
            service_type=SERVICE_TYPE_PYTHON_HTTP_CLIENT,
        )
        # ``headers`` is keyword-only on ``_request``, so it always lives in kwargs
        # — no positional-collision risk like urllib3's ``urlopen``. Inject into a
        # copy so a user-provided mapping is never mutated.
        headers = _headers_with_trace(span, kwargs.get("headers"))
        request_kwargs = dict(kwargs)
        request_kwargs["headers"] = headers
        sampled = span_is_sampled(span)
        if sampled:
            _annotate_client_request(event, _request_url(args, kwargs), headers)
    except Exception:  # noqa: BLE001
        _log.debug("aiohttp client instrumentation failed", exc_info=True)
        end_quietly(event)
        return await wrapped(*args, **kwargs)

    if not sampled:
        # Unsampled: the trace headers were still injected (downstream must see
        # the unsampled decision), but skip annotation and the event scope —
        # the event is the shared no-op one, whose end() does nothing.
        return await wrapped(*args, **request_kwargs)

    # span_event_scope records an ordinary exception (connection error, timeout,
    # cancellation surfaced as an Exception) on the event and always ends it,
    # then lets the exception propagate into the user's await unchanged.
    with span_event_scope(event):
        response = await wrapped(*args, **request_kwargs)
        annotate_client_response(event, response)
        return response


def _request_url(args, kwargs):
    """Target URL of ``_request(method, str_or_url, ...)``.

    ``str_or_url`` is the second positional (a ``str`` or ``yarl.URL``); accept
    the keyword form too for the rare caller that passes it by name."""
    if len(args) > 1:
        return args[1]
    return kwargs.get("str_or_url")


def _headers_with_trace(span, provided):
    """Build the per-request outbound headers carrying this request's trace
    context.

    Copies the caller's headers first — a user-provided mapping may be shared
    across concurrent ``session`` calls, so it is never mutated in place (the
    same rule the messaging producers follow) — then writes the Pinpoint-*
    pairs on top. A mapping is copied in-kind so a ``CIMultiDict``'s
    multi-valued entries survive, and ``__setitem__`` *replaces* the trace keys
    rather than appending (no duplicate ``Pinpoint-*`` lines). aiohttp's
    ``_prepare_headers`` still merges the result on top of the session defaults,
    so handing it an explicit mapping never drops a default header.
    """
    trace_headers = [
        (str(key), str(value)) for key, value in inject_items(span)
    ]
    if provided is None:
        headers: Any = {}
    elif hasattr(provided, "items") and callable(getattr(provided, "items")):
        # dict / CIMultiDict / CIMultiDictProxy: ``.copy()`` yields a mutable
        # same-kind (a proxy copies to a mutable multidict); dict() is the
        # fallback for an exotic mapping without ``.copy()``.
        headers = provided.copy() if hasattr(provided, "copy") else dict(provided)
    else:
        # LooseHeaders also permits (name, value) pairs: materialize a private list,
        # keep duplicate non-trace headers, and drop caller-supplied trace keys
        # case-insensitively so each propagation key occurs once.
        trace_names = {key.lower() for key, _value in trace_headers}
        headers = [
            (key, value)
            for key, value in provided
            if str(key).lower() not in trace_names
        ]
        headers.extend(trace_headers)
        return headers
    for key, value in trace_headers:
        headers[key] = value
    return headers


@safe_try
def _annotate_client_request(event, url, headers) -> None:
    url_str = str(url or "")
    # yarl.URL already parsed the authority C-side; urlparse only for the
    # plain-string fallback (urllib's tiny parse cache misses under URL
    # variety on high-fan-out clients).
    host = getattr(url, "authority", None)
    if not host:
        host = urlparse(url_str).netloc or ""
    trace_http_client_request(event, host, url_str, headers)


def instrument_server(*_args: Any, **_kwargs: Any) -> None:
    AiohttpServerInstrumentor().instrument()


def instrument_client(*_args: Any, **_kwargs: Any) -> None:
    AiohttpClientInstrumentor().instrument()


def instrument() -> None:
    """Manual entry point: enable both independent transports."""
    instrument_server()
    instrument_client()
