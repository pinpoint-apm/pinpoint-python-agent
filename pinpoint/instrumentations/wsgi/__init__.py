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

"""Generic WSGI instrumentation.

A WSGI app is a callable ``app(environ, start_response) -> iterable``. We
provide a thin middleware that turns every inbound request into a Pinpoint
root span — without depending on any specific framework. Use it directly
when you embed a hand-rolled or framework-less WSGI app, or as a fallback
when a framework-specific instrumentation isn't available.

Usage::

    from pinpoint.instrumentations.wsgi import PinpointWSGIMiddleware
    application = PinpointWSGIMiddleware(application)

The middleware:

- Honors upstream ``Pinpoint-*`` headers (case-insensitive) so the request
  links into the caller's trace.
- Annotates the span with HTTP URL / status / endpoint and records URL stats.
- Routes uncaught exceptions back into the span via ``set_error`` while still
  re-raising for the outer server to handle.
"""

from __future__ import annotations

import weakref
from typing import Any
from collections.abc import Callable, Iterable, Mapping

from ..._log import get_logger
from ...agent import get_agent
from ...context import (
    SpanActivation,
    current_span,
    set_current_span,
)
from ...errors import safe_try
from ...http_helper import (
    EnvironHeaderReader,
    has_pinpoint_environ,
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

_log = get_logger("wsgi")
_DEFAULT_FRAMEWORK_NAME = "WSGI"
_OPERATION_RESPONSE_STREAM = "wsgi.response.iteration"
# Set on the per-request environ while a root span is open, so a nested Pinpoint
# layer (a manually wrapped app whose framework autoload also instrumented) can't
# open a second root span. Shares the ASGI middleware's scope key.
ENVIRON_SPAN_ACTIVE_KEY = "pinpoint.root_span_active"

StartResponse = Callable[..., Any]
WSGIApp = Callable[[dict[str, Any], StartResponse], Iterable[bytes]]


class PinpointWSGIMiddleware:
    """A drop-in WSGI middleware that turns every request into a Pinpoint span.

    The wrapper is deliberately simple: a callable class so it composes with
    existing middleware stacks (gunicorn, uWSGI, Werkzeug, etc.) without
    requiring keyword arguments.
    """

    __slots__ = ("_app", "_operation", "_entry_event")

    def __init__(self, app: WSGIApp,
                 framework_name: str = _DEFAULT_FRAMEWORK_NAME):
        self._app = app
        self._operation = f"{framework_name} HTTP Server"
        # A bare app has no framework layer to name the handler that ran, so
        # its root span would report an empty call tree — name one event after
        # the application callable, resolved once rather than per request.
        self._entry_event = callable_operation_name(app, default="wsgi.app")

    def __call__(self, environ: dict[str, Any],
                 start_response: StartResponse) -> Iterable[bytes]:
        return _trace_wsgi_request(
            self._app, environ, start_response, self._operation,
            entry_event=self._entry_event,
        )


def _trace_wsgi_request(app: WSGIApp, environ: dict[str, Any],
                        start_response: StartResponse,
                        operation: str,
                        entry_event: str = "") -> Iterable[bytes]:
    """Run one WSGI request under a root span.

    Framework integrations delegate here with their own operation name. This
    keeps sampling, header capture, exception handling, streaming finalization,
    and URL-stat behavior identical across every WSGI entry point.
    """
    agent = get_agent()
    if agent is None or not agent.enabled:
        return app(environ, start_response)

    # Two independent signals cover every WSGI nesting shape. The per-request
    # marker is definitive: it catches WSGI-in-WSGI via the shared environ.
    if environ.get(ENVIRON_SPAN_ACTIVE_KEY):
        return app(environ, start_response)
    # The contextvar catches WSGI mounted in an instrumented ASGI app via
    # Starlette's ``WSGIMiddleware``, which builds a fresh CGI-only environ (the
    # marker can't cross) but runs the app in a context-copying threadpool. This
    # signal is ambiguous, though: a span leaked without a reset on a threaded
    # worker (gunicorn ``gthread``, Waitress, mod_wsgi) looks identical and would
    # suppress root spans for every later request there — so log, don't stay
    # silent. A standalone WSGI app sees neither signal.
    if current_span() is not None:
        _log.debug(
            "WSGI: active span in inherited context without a per-request "
            "marker; treating the request as nested and not opening a second "
            "root span (Starlette WSGIMiddleware mount, or a possible "
            "live-span context leak on this worker thread)"
        )
        return app(environ, start_response)

    # Nothing shields the WSGI server from an exception here, so span setup
    # must be guarded. If tracing setup fails, run the app untraced.
    span = None
    token = None
    try:
        path = environ.get("PATH_INFO", "/") or "/"
        # The reader is a cheap lazy wrapper, but hand it to new_span only when an
        # upstream Pinpoint header is actually present.
        headers = EnvironHeaderReader(environ)
        span = agent.new_span(
            operation, path,
            headers=headers if has_pinpoint_environ(environ) else None,
            method=environ.get("REQUEST_METHOD", "") or "",
        )
        sampled = span.sampled
        if sampled:
            _annotate_request(span, environ, headers)
        try:
            environ[ENVIRON_SPAN_ACTIVE_KEY] = True
        except Exception:  # noqa: BLE001
            pass
        token = set_current_span(span)
    except Exception:  # noqa: BLE001
        _log.debug("WSGI span setup failed", exc_info=True)
        if token is not None:
            reset_quietly(token)
        end_quietly(span)
        return app(environ, start_response)

    # [status_code, response_headers], shared with the response iterable.
    state: list = [0, []]
    needs_status = sampled or getattr(span, "_collect_url_stat", False)
    try:
        capture_response_headers = (
            sampled and record_response_header_enabled(span)
        )
    except Exception:  # noqa: BLE001
        capture_response_headers = False
    start_response_fn = start_response
    if needs_status:
        def _wrapped_start_response(status: str, response_headers,
                                    exc_info=None):
            try:
                state[0] = int(str(status).split(" ", 1)[0])
            except Exception:  # noqa: BLE001
                pass
            if capture_response_headers:
                try:
                    state[1] = list(response_headers or [])
                except Exception:  # noqa: BLE001
                    pass
            # Forward the arity the app used. PEP 3333 servers accept the third
            # argument, but a hand-written middleware further down the chain
            # commonly declares only ``(status, headers)`` — adding an explicit
            # ``exc_info=None`` would TypeError there, at the point of no return
            # for the response.
            if exc_info is None:
                return start_response(status, response_headers)
            return start_response(status, response_headers, exc_info)
        start_response_fn = _wrapped_start_response

    entry = None
    if entry_event and sampled:
        try:
            entry = span.new_span_event(entry_event)
        except Exception:  # noqa: BLE001
            entry = None
    try:
        result = app(environ, start_response_fn)
    except BaseException as exc:
        end_quietly(entry)
        if sampled:
            record_exception_on_span(span, exc)
        reset_quietly(token)
        _end_span(span, environ, _failed_status(state[0], exc), state[1],
                  sampled)
        raise
    # Closed at the app boundary, before either exit below: the app's own
    # nested events are balanced by now (LIFO), and the response-iteration
    # event of the streaming path is a sibling, not a child.
    end_quietly(entry)

    # Only a genuinely lazy sampled iterable can run traced application code after
    # ``app`` returns: eager bodies are already materialized, an unsampled span
    # can't create a recording child, and wrapping a server file-wrapper would
    # defeat its sendfile fast path. Finalize the root directly in all three.
    # ``pinpoint.response_buffered`` is the framework integrations' hint that the
    # body is already materialized behind an opaque wrapper (werkzeug's
    # ClosingIterator, Django's HttpResponse) — without it every such response
    # would pay the async-child hand-off below (two extra native crossings plus
    # a second reported span). The trade: the wrapper's ``close()`` teardown
    # (context pops, teardown callbacks) then runs after the root span ended,
    # so work there goes untraced — bookkeeping, for a buffered body.
    if (
        not sampled
        or type(result) in _BUFFERED_BODY_TYPES
        or environ.get("pinpoint.response_buffered")
        or _is_file_wrapper_response(result, environ)
    ):
        reset_quietly(token)
        _end_span(span, environ, state[0], state[1], sampled)
        return result

    # A response iterable can be drained by another server thread, which must
    # never get the request span itself. Create a linked async child while still on
    # the owner thread and end the root here.
    response_span = None
    try:
        with span.new_span_event(_OPERATION_RESPONSE_STREAM):
            response_span = span.new_async_span(_OPERATION_RESPONSE_STREAM)
    except Exception:  # noqa: BLE001
        _log.debug("WSGI response-span hand-off failed", exc_info=True)

    # Reset and finalize the root in the context/thread that created it.
    reset_quietly(token)
    _end_span(span, environ, state[0], state[1], sampled)

    if response_span is None:
        # Tracing setup failed after the app returned. Leave response iteration
        # untouched; the server still owns normal close/error behavior.
        return result
    # If start_response already ran the root has the full response metadata, so
    # don't duplicate it on the child; for lazy generator-style apps, let the child
    # capture the status at drain time instead.
    response_end = _end_response_span if state[0] else _end_span
    return finalize_wsgi_response(
        result,
        response_span,
        environ,
        state,
        getattr(response_span, "sampled", False),
        end_span=response_end,
    )


# ---- helpers ---------------------------------------------------------------


def _failed_status(status_code: int, exc: BaseException) -> int:
    """Status to report for a request whose app raised.

    An ordinary exception escaping the app means the server answers 500, but
    the middleware never saw a ``start_response`` to read it from — so report
    the 500 the client actually gets. Cancellation and interpreter teardown
    (a ``BaseException`` that is not an ``Exception``) are not application
    failures, so they keep the status the app did set, if any.
    """
    if status_code or not isinstance(exc, Exception):
        return status_code
    return 500


def _annotate_request(span, environ: Mapping[str, Any],
                      headers: Mapping[str, str]) -> None:
    host = environ.get("HTTP_HOST") or environ.get("SERVER_NAME", "") or ""
    annotate_server_request(
        span, environ.get("REMOTE_ADDR", "") or "", host, headers,
        cookie_keys=("COOKIE",),
        query_string=environ.get("QUERY_STRING", "") or "",
    )


@safe_try
def _end_span(span, environ: Mapping[str, Any], status_code: int,
              response_headers: Iterable[tuple[str, str]] = (),
              sampled: bool = True) -> None:
    method = environ.get("REQUEST_METHOD", "") or ""
    # Prefer a route template if a framework-specific layer stashed one.
    url_pattern = (
        environ.get("pinpoint.url_pattern")
        or environ.get("PATH_INFO", "")
        or ""
    )
    end_server_span(span, url_pattern, method, status_code,
                    response_headers, sampled)


@safe_try
def _end_response_span(span, _environ, _status_code, _response_headers=(),
                       _sampled=True) -> None:
    """End a response child after the request root captured HTTP metadata."""
    span.end()


class _ClosingIterable:
    """Iterable wrapper that ends a response async span after body consumption.

    PEP 3333 says servers MUST call ``close()`` on the response iterable if
    one exists — we hook into that so the span lifecycle matches the request
    lifecycle, even for streaming responses.

    The span passed here is the linked async child created by the request
    thread, never the request root itself. The drain context activates that
    child per iteration/close, so streamed user code remains traceable without
    sharing one native span between request and response threads.
    """

    __slots__ = ("_iter", "_inner", "_span", "_activation", "_environ",
                 "_state", "_sampled", "_end_span", "_done", "_reaper",
                 "__weakref__")

    def __init__(self, iterable, span, environ, state, sampled, end_span):
        self._iter = iter(iterable)
        self._inner = iterable  # keep ref so close() reaches the original
        self._span = span
        # Per-step set/reset with a reused binding: a long stream would
        # otherwise allocate a fresh binding (plus an asyncio task probe)
        # for every body chunk.
        self._activation = SpanActivation(span)
        self._environ = environ
        # [status_code, response_headers] — shared with the middleware's
        # start_response wrapper.
        self._state = state
        self._sampled = sampled
        self._end_span = end_span
        self._done = False
        # PEP 3333 obliges the server to call close(), but a server that
        # abandons a partially-drained iterable on an error/reset path leaks
        # the async child's native active-span registration forever. The
        # reaper fires only if this wrapper is collected un-finalized;
        # _finalize detaches it on every cooperative path. It must not
        # reference self (that would pin the object).
        self._reaper = weakref.finalize(
            self, _reap_abandoned_response,
            end_span, span, environ, state, sampled)

    def __iter__(self):
        return self

    def __next__(self):
        # Activate the span only for this step, in the drain context, so the
        # reset below always matches its own set (never a cross-thread token).
        token = self._activation.set()
        try:
            return next(self._iter)
        except StopIteration:
            self._finalize()
            raise
        except BaseException as exc:
            if self._sampled:
                record_exception_on_span(self._span, exc)
            self._finalize()
            raise
        finally:
            reset_quietly(token)

    def close(self):
        # Re-activate the span across body teardown (generator ``finally:``,
        # ``werkzeug`` on-close callbacks, ``stream_with_context``) so cleanup
        # attaches to the request span and outbound calls there carry context;
        # otherwise the inner ``close()`` sees ``current_span()`` as None. The
        # set/reset pair stays in this drain context, so the token is never
        # cross-thread — same guarantee as __next__.
        token = self._activation.set()
        try:
            inner_close = getattr(self._inner, "close", None)
            if inner_close is not None:
                inner_close()
        finally:
            # End the span exactly once (``_finalize`` is guarded by
            # ``_done``) even if the inner ``close()`` raised, then always
            # reset the contextvar in a ``finally`` so it runs regardless.
            try:
                self._finalize()
            finally:
                reset_quietly(token)

    def _finalize(self):
        if self._done:
            return
        self._done = True
        self._reaper.detach()
        self._end_span(
            self._span, self._environ, self._state[0],
            self._state[1], self._sampled,
        )


def _reap_abandoned_response(end_span, span, environ, state, sampled):
    # Runs from GC (or the weakref atexit hook); everything is guarded — a
    # reaper must never surface an exception into whatever triggered the
    # collection.
    try:
        end_span(span, environ, state[0], state[1], sampled)
    except Exception:  # noqa: BLE001
        pass


class _SizedClosingIterable(_ClosingIterable):
    """``_ClosingIterable`` that also forwards ``len()`` to the wrapped body.

    wsgiref/gunicorn infer ``Content-Length`` when ``len(result) == 1``; a
    wrapper without ``__len__`` silently downgrades such responses to chunked /
    connection-close framing. Used only when the wrapped body actually defines
    ``__len__``, so ``hasattr(wrapper, '__len__')`` mirrors the inner body.
    """

    __slots__ = ()

    def __len__(self):
        return len(self._inner)


def finalize_wsgi_response(result, span, environ, state, sampled, end_span):
    """Hand the server the response iterable, ending its async span on drain.

    The caller has already ended the request root on its owning thread. ``span``
    is a linked child dedicated to the response drain, so it can follow a
    streaming body onto a server worker without sharing the root. ``end_span``
    lets callers customize child finalization.

    Two server optimizations are preserved rather than masked by the wrapper:

    - ``wsgi.file_wrapper``: if the app returned the server's file_wrapper
      instance, wrapping it would defeat the ``isinstance`` sendfile check, so
      it is returned unwrapped and the span ends now (the app's work is done;
      the kernel-level file send isn't traced).
    - ``__len__``: a body exposing ``len()`` is wrapped in a length-preserving
      subclass so ``Content-Length`` inference still fires.

    The caller is expected to have already reset the current-span contextvar in
    its own context before calling this.
    """
    if _is_file_wrapper_response(result, environ):
        end_span(span, environ, state[0], state[1], sampled)
        return result
    try:
        if hasattr(result, "__len__"):
            return _SizedClosingIterable(
                result, span, environ, state, sampled, end_span)
        return _ClosingIterable(result, span, environ, state, sampled, end_span)
    except Exception:  # noqa: BLE001
        # e.g. the app returned a non-iterable — don't leak the span; hand the
        # result back unchanged and let the server surface its own error.
        end_span(span, environ, state[0], state[1], sampled)
        return result


# Runtime tuple built per request otherwise: list/tuple are name loads, not
# constants, so ``in (list, tuple)`` emits BUILD_TUPLE on every response.
_BUFFERED_BODY_TYPES = (list, tuple)


def _is_file_wrapper_response(result, environ) -> bool:
    """Whether ``result`` is the server's opaque ``wsgi.file_wrapper`` type.

    Kept non-raising because servers are allowed to expose unusual callables as
    the factory; an invalid second argument to ``isinstance`` must never break
    the host response path.
    """
    file_wrapper = environ.get("wsgi.file_wrapper")
    if file_wrapper is None:
        return False
    try:
        return isinstance(result, file_wrapper)
    except Exception:  # noqa: BLE001
        return False


def wsgi_entry_wrapper(operation: str):
    """wrapt-style wrapper for a framework's WSGI entry point.

    Extracts ``(environ, start_response)`` positionally or by keyword
    (falcon names the first parameter ``env``) and runs the request under
    ``_trace_wsgi_request`` with the framework's operation name, passing
    straight through when the pair isn't recognizably present.
    """
    def _wrapper(wrapped, instance, args, kwargs):
        environ = (args[0] if args
                   else kwargs.get("environ", kwargs.get("env")))
        start_response = (args[1] if len(args) > 1
                          else kwargs.get("start_response"))
        if environ is None or start_response is None:
            return wrapped(*args, **kwargs)
        return _trace_wsgi_request(wrapped, environ, start_response, operation)
    return _wrapper
