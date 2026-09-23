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

"""Generic ASGI instrumentation.

``PinpointASGIMiddleware`` turns every inbound ``http`` scope into a Pinpoint
root span, and is the foundation the FastAPI/Starlette/aiohttp integrations
delegate to or mirror. Lifespan and websocket scopes pass through untouched.

Status comes from the outbound ``http.response.start`` event — the only place
ASGI reliably exposes it to a middleware.

See ``README.md`` for the full behavior list and the wrapping snippet.
"""

from __future__ import annotations

from typing import Any
from collections.abc import Awaitable, Callable

from ..._log import get_logger
from ...agent import get_agent
from ...context import set_current_span
from ...errors import safe_try
from ...http_helper import (
    ASGIHeadersReader,
    has_pinpoint_pairs,
    record_response_header_enabled,
)
from .._util import (
    annotate_server_request,
    callable_operation_name,
    end_quietly,
    end_server_span,
    record_exception_on_span,
    reset_quietly,
)

_log = get_logger("asgi")
_DEFAULT_FRAMEWORK_NAME = "ASGI"
# Set on the per-request scope while a root span is open, so a nested Pinpoint
# layer (an autoload-instrumented app also wrapped by hand, a Falcon app mounted
# inside FastAPI) can't open a second root span for the same request.
SCOPE_SPAN_ACTIVE_KEY = "pinpoint.root_span_active"

Scope = dict[str, Any]
Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


class PinpointASGIMiddleware:
    """Drop-in ASGI middleware that opens a Pinpoint root span per request.

    Stays compatible with ASGI 2 (instance call) and ASGI 3 (single async
    callable) by always exposing the ASGI 3 shape — the standard for new code.
    """

    __slots__ = ("_app", "_operation", "_entry_event", "_fallback_event")

    def __init__(self, app: ASGIApp,
                 framework_name: str = _DEFAULT_FRAMEWORK_NAME,
                 entry_event: bool = True):
        self._app = app
        self._operation = f"{framework_name} HTTP Server"
        # A bare app has no framework layer to name the handler that ran, so
        # its root span would report an empty call tree — name one event after
        # the application callable, resolved once rather than per request.
        # ``entry_event=False`` is for an integration that opens its own
        # handler event (the Starlette wrapper below); a second, outer event
        # named after its middleware stack would be noise.
        self._entry_event = (
            callable_operation_name(app, default="asgi.app")
            if entry_event else "")
        # That handler event only exists when a handler actually runs. A
        # request matching no route reaches none — Starlette answers a 404 from
        # ``Router.default``, and a redirect_slashes hit from the router itself
        # — so name one after the outcome instead, opened only when the request
        # recorded nothing at all. Not ``callable_operation_name(app)`` like the
        # eager event above: ``app`` here is the framework's built middleware
        # stack, whose outermost layer is an internal ("ServerErrorMiddleware"
        # on a 404 reads as both wrong and alarming).
        self._fallback_event = (
            "" if entry_event else f"{framework_name.lower()}.no_route")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await _trace_asgi_request(
            self._app, scope, receive, send, self._operation,
            entry_event=self._entry_event,
            fallback_event=self._fallback_event,
        )


async def _trace_asgi_request(app: ASGIApp, scope: Scope, receive: Receive,
                              send: Send, operation: str,
                              entry_event: str = "",
                              fallback_event: str = "") -> None:
    """Run one ASGI request under a root span.

    Framework wrappers call this coroutine directly instead of allocating and
    caching a middleware object per application instance.
    """
    if scope.get("type") != "http":
        # Lifespan, websocket, etc — pass through untouched.
        await app(scope, receive, send)
        return

    agent = get_agent()
    if agent is None or not agent.enabled:
        await app(scope, receive, send)
        return

    if scope.get(SCOPE_SPAN_ACTIVE_KEY):
        # An enclosing Pinpoint layer already opened the root span for this
        # request — opening another would double transaction counts.
        await app(scope, receive, send)
        return

    # Async body: nothing shields the ASGI server from an exception here, so
    # span setup must be guarded — on failure, run the app untraced.
    span = None
    token = None
    try:
        path = scope.get("path", "/") or "/"
        # The reader is a cheap lazy wrapper, but hand it to new_span only when an
        # upstream Pinpoint header is actually present.
        raw_headers = scope.get("headers") or ()
        headers = ASGIHeadersReader(raw_headers)
        span = agent.new_span(
            operation, path,
            headers=headers if has_pinpoint_pairs(raw_headers) else None,
            method=scope.get("method", "") or "",
        )

        sampled = span.sampled
        if sampled:
            _annotate_request(span, scope, headers)
        scope[SCOPE_SPAN_ACTIVE_KEY] = True
        token = set_current_span(span)
    except Exception:  # noqa: BLE001
        _log.debug("ASGI span setup failed", exc_info=True)
        reset_quietly(token)
        end_quietly(span)
        await app(scope, receive, send)
        return

    # Captured via ``nonlocal`` instead of holder dicts so each request
    # allocates only the (unavoidable) send closure, not two scratch dicts.
    status_code = 0
    response_headers: Any = None

    # Skip the send wrapper when neither sampling nor URL-stat collection needs
    # the status, avoiding a coroutine hop on every streamed response chunk.
    needs_status = sampled or getattr(span, "_collect_url_stat", False)
    send_fn: Any = send
    if needs_status:
        async def _wrapped_send(message: dict[str, Any]) -> None:
            nonlocal status_code, response_headers
            if message.get("type") == "http.response.start":
                try:
                    status_code = int(message.get("status") or 0)
                except Exception:  # noqa: BLE001
                    pass
                if sampled and record_response_header_enabled(span):
                    try:
                        response_headers = ASGIHeadersReader(
                            message.get("headers") or (),
                        )
                    except Exception:  # noqa: BLE001
                        pass
            await send(message)
        send_fn = _wrapped_send

    event = None
    if entry_event and sampled:
        try:
            event = span.new_span_event(entry_event)
        except Exception:  # noqa: BLE001
            event = None
    try:
        await app(scope, receive, send_fn)
    except BaseException as exc:
        if sampled:
            record_exception_on_span(span, exc)
        # No ``http.response.start`` reached the wrapper, so nothing set the
        # status — see wsgi._failed_status for why this reports 500.
        if not status_code and isinstance(exc, Exception):
            status_code = 500
        raise
    finally:
        # LIFO: the app's own nested events are balanced by the time it
        # returns, so this closes cleanly before the span does.
        if event is not None:
            try:
                event.end()
            except Exception:  # noqa: BLE001
                pass
        elif (fallback_event and sampled
              and not getattr(span, "_event_sequence", 1)):
            # Nothing was recorded: no route matched, so the handler wrapper
            # that would have named this transaction never ran. Give the root
            # span the one event it is supposed to have instead of an
            # empty call tree. Asked of the span's monotonic event counter, not
            # of its record buffer — the buffer is drained at the span-end
            # flush, and the unit-test bridge drains it per event.
            try:
                span.new_span_event(fallback_event).end()
            except Exception:  # noqa: BLE001
                pass
        reset_quietly(token)
        _end_span(span, scope, status_code, response_headers, sampled)


# ---- helpers ---------------------------------------------------------------


def _client_address(scope: Scope) -> str:
    client = scope.get("client")
    if isinstance(client, (list, tuple)) and client:
        return str(client[0]) or ""
    return ""


def _host(scope: Scope, headers: ASGIHeadersReader) -> str:
    host = headers.get("host") or ""
    if host:
        return host
    server = scope.get("server")
    if isinstance(server, (list, tuple)) and server:
        host_part = server[0] or ""
        port = server[1] if len(server) > 1 else None
        if port:
            return f"{host_part}:{port}"
        return str(host_part)
    return ""


@safe_try
def _annotate_request(span, scope: Scope, headers: ASGIHeadersReader) -> None:
    annotate_server_request(
        span, _client_address(scope), _host(scope, headers), headers,
        cookie_keys=("cookie",),
        query_string=_query_string(scope),
    )


def _query_string(scope: Scope) -> str:
    raw = scope.get("query_string", b"")
    if isinstance(raw, bytes):
        return raw.decode("latin-1")
    return str(raw or "")


@safe_try
def _end_span(span, scope: Scope, status_code: int,
              response_headers: Any = None,
              sampled: bool = True) -> None:
    end_server_span(span, _url_pattern(scope), scope.get("method", "") or "",
                    status_code, response_headers, sampled)


def _url_pattern(scope: Scope) -> str:
    explicit = scope.get("pinpoint.url_pattern")
    if explicit:
        return str(explicit)

    route = scope.get("route")
    pattern = getattr(route, "path", None)
    if isinstance(pattern, str) and pattern:
        return pattern

    return str(scope.get("path", "") or "")


def asgi_entry_wrapper(operation: str):
    """wrapt-style wrapper for a framework's ASGI 3 entry point.

    The returned wrapper is a *sync* function that returns the traced
    coroutine rather than ``async def``: uvicorn's startup factory probe
    calls ``app()`` with no args and only intercepts ``TypeError`` to decide
    whether the app is a factory, so it must see the same TypeError an
    unwrapped ``__call__`` raises. An ``async def`` wrapper would silently
    hand it an un-awaited coroutine, and the app gets misclassified as ASGI 2.

    Non-``http`` scopes (factory probe / lifespan / websocket) and disabled
    agents go straight to the wrapped method so its native behavior governs.
    """
    def _wrapper(wrapped, instance, args, kwargs):
        scope = args[0] if args else kwargs.get("scope")
        if scope is None or scope.get("type") != "http":
            return wrapped(*args, **kwargs)
        agent = get_agent()
        if agent is None or not agent.enabled:
            return wrapped(*args, **kwargs)
        # The safe-wrapper sentinel only tracks synchronous target execution.
        # ASGI user code runs after this wrapper returns its coroutine, so pass
        # the real bound target to the shared runner.
        target = getattr(wrapped, "__wrapped__", wrapped)
        return _trace_asgi_request(target, *args, operation=operation, **kwargs)
    return _wrapper
