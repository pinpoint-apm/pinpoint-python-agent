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

"""gRPC instrumentation for both client and server via interceptors.

Server side: we wrap ``grpc.server(...)`` so we can prepend our
``PinpointServerInterceptor`` to user-supplied interceptors. All four
handler kinds (unary-unary, unary-stream, stream-unary, stream-stream)
are wrapped so the server span covers the full RPC including streaming
I/O.

Client side: we wrap ``grpc.{insecure,secure}_channel`` similarly and
intercept all four client RPC kinds; outbound metadata is augmented
with Pinpoint headers and a child span event is opened on the active
span. Non-blocking/streaming calls end that event at dispatch so a response
consumed on another thread never carries the parent's event with it.
"""

from __future__ import annotations

import functools
from typing import Any
from collections.abc import Callable, Iterable, Iterator

from ...agent import get_agent
from ...annotation import ANNOTATION_GRPC_CLIENT_STATUS, ANNOTATION_HTTP_URL
from ...context import current_span, set_current_span
from ...errors import safe_try
from ...http_helper import has_pinpoint_pairs
from ...instrumentor import BaseInstrumentor
from ...propagator import inject_items
from ...service_type import SERVICE_TYPE_GRPC, SERVICE_TYPE_GRPC_SERVER
from .._util import (
    close_span_scope,
    end_quietly,
    record_exception_on_event,
    record_exception_on_span,
    replace_arg,
    span_is_sampled,
    wrap,
)

# The server span's operation name; its rpc name is the full method, and each
# client event is named for the method it calls.
_OPERATION_SERVER_CALL = "gRPC Server"

# Recorded as the remote address when the peer is unreadable.
_PEER_FALLBACK = "127.0.0.1"


def _normalize_target(target: Any) -> str:
    """A gRPC dial target reduced to the host:port Pinpoint records.

    A unix socket has no meaningful host, and the default ``dns:///`` scheme is
    noise that would split one server-map node in two.
    """
    text = str(target or "")
    if text.startswith("unix:"):
        return "localhost"
    if text.startswith("dns:///"):
        return text[len("dns:///"):]
    return text


def _peer_host(context: Any) -> str:
    """The calling client's host, for the server span's remote address.

    grpc's ``peer()`` is scheme-prefixed and port-suffixed
    (``ipv4:127.0.0.1:54321``, ``ipv6:[::1]:54321``, ``unix:/run/x.sock``)
    where Pinpoint wants the bare host. Unix sockets and unreadable peers fall
    back to ``127.0.0.1``, as any other non-ip scheme does.
    """
    try:
        peer_fn = getattr(context, "peer", None)
        peer = peer_fn() if callable(peer_fn) else ""
    except Exception:  # noqa: BLE001
        peer = ""
    if not peer:
        return _PEER_FALLBACK
    if peer.startswith("ipv6:"):
        rest = peer[len("ipv6:"):]
        # Brackets delimit the address when a port follows: ipv6:[::1]:54321.
        host = rest[1:].split("]", 1)[0] if rest.startswith("[") else rest
        return host or _PEER_FALLBACK
    if peer.startswith("ipv4:"):
        return peer[len("ipv4:"):].rsplit(":", 1)[0] or _PEER_FALLBACK
    return _PEER_FALLBACK


class GrpcInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        wrap("grpc", "server", _grpc_server_wrapper)
        wrap("grpc", "insecure_channel", _grpc_channel_wrapper)
        wrap("grpc", "secure_channel", _grpc_channel_wrapper)


# ---------------------------------------------------------------- shared utils
def _is_grpc_abort(exc: BaseException, context: Any) -> bool:
    """True when ``exc`` is the control-flow sentinel raised by
    ``ServicerContext.abort()`` rather than a real server error.

    ``abort()`` is the idiomatic way to return NOT_FOUND / INVALID_ARGUMENT
    etc.; grpc's ``_server.py`` implements it by setting the RPC's status code
    and then raising a bare ``Exception()`` (no args). Recording that as a span
    error would paint a service that legitimately aborts (e.g. a lookup miss)
    with a permanent error rate — every HTTP framework here already classifies
    such sub-500 control flow. The bare-``Exception()`` signature is abort()'s
    fingerprint (a real handler failure is a specific subclass, or carries a
    message); when the context also exposes the status we confirm it is non-OK.
    """
    if type(exc) is not Exception or exc.args:
        return False
    code = None
    try:
        code_fn = getattr(context, "code", None)
        if callable(code_fn):
            code = code_fn()
    except Exception:  # noqa: BLE001
        return True  # unreadable code: trust the bare-Exception sentinel
    if code is None:
        return True
    try:
        return code is not _grpc().StatusCode.OK
    except Exception:  # noqa: BLE001
        return True


def _open_handler_event(span: Any, method: str) -> Any:
    """One span event covering the handler, named for the RPC method.

    Without it a server span whose handler calls nothing downstream reports an
    empty call tree — the span's own rpc name is all the UI has to show.
    """
    try:
        return span.new_span_event(method)
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------- server side

_SERVER_INTERCEPTORS_POS = 2  # index of ``interceptors`` in grpc.server(
                              # thread_pool, handlers, interceptors, ...).


def _grpc_server_wrapper(wrapped, instance, args, kwargs):
    # ``grpc.server`` takes ``interceptors`` positionally, so prepend into whichever
    # of ``args[2]`` / ``kwargs['interceptors']`` the caller used — writing the kwarg
    # unconditionally would crash with "got multiple values".
    current = (args[_SERVER_INTERCEPTORS_POS]
               if len(args) > _SERVER_INTERCEPTORS_POS
               else kwargs.get("interceptors"))
    interceptors = [_PinpointServerInterceptor()] + list(current or [])
    new_args, new_kwargs = replace_arg(
        args, kwargs, _SERVER_INTERCEPTORS_POS, "interceptors", interceptors)
    return wrapped(*new_args, **new_kwargs)


@functools.cache
def _grpc():
    import grpc  # type: ignore[import-not-found]
    return grpc


def _grpc_interceptor_bases(*names: str):
    """Bases for one of our interceptor classes: the named grpc protocols, or
    ``object`` when grpc isn't importable — the class body must still execute
    at import time (it is simply never instantiated without grpc)."""
    try:
        grpc = _grpc()
    except Exception:  # noqa: BLE001
        return (object,)
    return tuple(getattr(grpc, name) for name in names)


def _stream_with_server_span(
    make_iter: Callable[[], Iterable[Any]],
    method: str,
    context: Any = None,
) -> Iterator[Any]:
    """Open and own a server-streaming span on the iterator thread.

    The generator body starts only at the first ``next()``. Creating the span
    here makes the same serving thread create, record, and end it. If an RPC is
    cancelled before the first pull, no span exists, so no channel-thread
    termination callback or arbitrary-thread GC finalizer is needed.

    Known dependency: on a client cancel mid-stream grpc's adapter breaks out
    of the loop *without* ``close()``-ing this iterator, so the ``finally``
    below runs when the suspended generator is finalized — immediately on
    CPython's refcount drop, but only at the next cycle collection when the
    generator sits in a reference cycle (or on PyPy). Until then the span and
    the serving thread's contextvar binding stay open. Ending the span from a
    termination callback instead would drive a native span from a second
    thread, which is worse; the root ``Span.end()`` is idempotent, so a late
    finalize is harmless.
    """
    span = _open_server_span(method, context)
    if span is None:
        yield from make_iter()
        return
    sampled = span.sampled
    token = set_current_span(span)
    event = _open_handler_event(span, method) if sampled else None
    try:
        # Not ``yield from``: that would delegate close()/throw() into the
        # handler's iterator, and the cancel note above relies on this frame
        # alone seeing them.
        for item in make_iter():  # noqa: UP028
            yield item
    except BaseException as exc:
        if sampled and not _is_grpc_abort(exc, context):
            record_exception_on_span(span, exc)
        raise
    finally:
        close_span_scope((span, event, token))


def _open_server_span(method: str, context):
    """Open the root span for one server RPC, or ``None`` when the agent is
    down/disabled (the caller then runs the handler untraced).

    Upstream trace context is read from ``context.invocation_metadata()``
    — the same client metadata ``handler_call_details`` carries, but
    available at handler-execution time so the traced handler can be built
    once per method instead of once per RPC. The metadata dict is only
    materialized when an upstream Pinpoint header is present; a brand-new
    trace's local sampling decision needs no header extraction, so
    ``new_span(headers=None)`` skips it. Annotation is
    sampled-only — for an unsampled span ``set_service_type`` would just
    cross pybind11 for no gain.
    """
    agent = get_agent()
    if agent is None or not agent.enabled:
        return None
    # dict(pairs) and agent.new_span (a pybind11 crossing) can raise when the agent
    # is in an error state, so return None and let the caller run the RPC untraced
    # rather than terminating an inbound RPC with UNKNOWN.
    try:
        pairs: Any = ()
        try:
            invocation_metadata = getattr(context, "invocation_metadata", None)
            if callable(invocation_metadata):
                pairs = invocation_metadata() or ()
        except Exception:  # noqa: BLE001
            pairs = ()
        headers = dict(pairs) if has_pinpoint_pairs(pairs) else None
        span = agent.new_span(_OPERATION_SERVER_CALL, method, headers=headers)
    except Exception:  # noqa: BLE001
        return None
    if span.sampled:
        try:
            span.set_service_type(SERVICE_TYPE_GRPC_SERVER)
            # endPoint and acceptorHost come from the client's Pinpoint-Host
            # header, and the native remote address falls back to that same
            # value — which is this server's own address, not the caller's — so
            # set the peer explicitly.
            span.set_remote_address(_peer_host(context))
        except Exception:  # noqa: BLE001
            pass
    return span


def _build_traced_handler(handler, method: str):
    """Wrap one RPC method handler with span lifecycle handling.

    Returns ``handler`` unchanged when there is nothing to wrap (unknown
    handler kind, grpc unimportable, handler-factory failure).
    """
    try:
        grpc = _grpc()
    except Exception:  # noqa: BLE001
        return handler

    # The four handler kinds differ only in which attribute holds the inner
    # callable and whether the response streams; the request/iterator flows
    # through untouched either way.
    for kind, response_streams in (
        ("unary_unary", False),
        ("unary_stream", True),
        ("stream_unary", False),
        ("stream_stream", True),
    ):
        inner = getattr(handler, kind, None)
        if inner is None:
            continue

        if response_streams:
            def _wrapped(request, context):
                return _stream_with_server_span(
                    lambda: inner(request, context), method, context,
                )
        else:
            def _wrapped(request, context):
                span = _open_server_span(method, context)
                if span is None:
                    return inner(request, context)
                sampled = span.sampled
                token = set_current_span(span)
                event = _open_handler_event(span, method) if sampled else None
                try:
                    return inner(request, context)
                except BaseException as exc:
                    # context.abort() raises a bare Exception() for control
                    # flow (NOT_FOUND etc.) — don't inflate the error rate.
                    if sampled and not _is_grpc_abort(exc, context):
                        record_exception_on_span(span, exc)
                    raise
                finally:
                    close_span_scope((span, event, token))

        try:
            return getattr(grpc, f"{kind}_rpc_method_handler")(
                _wrapped,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        except Exception:  # noqa: BLE001
            return handler

    return handler


class _PinpointServerInterceptor(*_grpc_interceptor_bases("ServerInterceptor")):
    # Method handlers are stable per method on real servers, so the traced wrapper
    # (2 closures, an RpcMethodHandler namedtuple, 4-kind reflection) is built once
    # per method rather than per RPC. Method names come from the client, so the cache
    # is bounded and overflow falls back to building per RPC. The identity check on
    # the original handler rebuilds when a dynamic service swaps it. Races are
    # benign: equivalent wrappers, one wins.
    _HANDLER_CACHE_MAX = 1024

    def __init__(self) -> None:
        self._handler_cache: dict = {}

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return handler

        method = getattr(handler_call_details, "method", "") or ""
        cached = self._handler_cache.get(method)
        if cached is not None and cached[0] is handler:
            return cached[1]

        traced = _build_traced_handler(handler, method)
        if len(self._handler_cache) < self._HANDLER_CACHE_MAX:
            self._handler_cache[method] = (handler, traced)
        return traced


# ---------------------------------------------------------------- client side
def _grpc_channel_wrapper(wrapped, instance, args, kwargs):
    channel = wrapped(*args, **kwargs)
    # grpc.{insecure,secure}_channel(target, ...) — the dial target is what the
    # span event records as its destination, so capture it once per channel
    # rather than per RPC (the interceptor never sees it).
    target = _normalize_target(args[0] if args else kwargs.get("target"))
    try:
        return _grpc().intercept_channel(
            channel, _PinpointClientInterceptor(target))
    except Exception:  # noqa: BLE001
        return channel


def _inject_into_call_details(span: Any, client_call_details: Any) -> Any:
    """Return a copy of ``client_call_details`` with Pinpoint trace headers
    appended to its ``metadata``. Must run regardless of sampling so the
    server side can stitch into the trace."""
    new_metadata = list(getattr(client_call_details, "metadata", None) or [])
    try:
        for key, value in inject_items(span):
            new_metadata.append((str(key).lower(), str(value)))
    except Exception:  # noqa: BLE001
        pass
    try:
        return client_call_details._replace(metadata=new_metadata)
    except Exception:  # noqa: BLE001
        return client_call_details


def _finish_unary_client_event(event, response) -> None:
    """Annotate the RPC status and end the client span event for a unary
    response.

    Blocking invocations reach here with an already-terminated outcome
    (grpc's ``_interceptor.py`` waits out the RPC inside the continuation),
    so status extraction and the end run synchronously on the calling thread
    — ``code()`` on a terminated call does not block.

    ``.future()`` invocations route through the same unary interceptors, but
    for those the continuation returns a *live* rendezvous, whose status is
    only observable by blocking (serializing the caller's fan-out) or from a
    done callback on a channel thread. A span and its events are
    single-threaded for their lifetime, so neither is an option: the event ends
    here, on the owning thread, covering the dispatch only — a ``.future()``
    call's eventual status and error are not recorded.
    """
    try:
        done_fn = getattr(response, "done", None)
        if callable(done_fn) and not done_fn():
            end_quietly(event)
            return
    except Exception:  # noqa: BLE001
        pass
    # ``code()`` takes the rendezvous condition lock; extract once and share
    # it between the annotation and the failure check below.
    status = _extract_grpc_status(response)
    _annotate_grpc_status(event, status)
    # grpc's blocking interceptor path does NOT raise RPC failures out of the
    # continuation: ``_interceptor.py`` catches ``grpc.RpcError`` and *returns* it as
    # the call object, re-raised by ``call.result()`` after this interceptor has
    # returned. So failures arrive through the success path, and the event must be
    # flagged here (set_error before end) or a downstream outage leaves client
    # events non-errored and error-rate alerting green. The reliable failure
    # signal is the resolved status code, not the Python type: every live
    # rendezvous is a ``grpc.RpcError`` subclass regardless of success, so
    # ``isinstance`` would mark successful futures as errors; a non-OK status
    # cannot. Only invoked on a terminated outcome, so it never blocks.
    if status and status != "OK":
        record_exception_on_event(event, response)
    end_quietly(event)


class _PinpointClientInterceptor(*_grpc_interceptor_bases(
        "UnaryUnaryClientInterceptor", "UnaryStreamClientInterceptor",
        "StreamUnaryClientInterceptor", "StreamStreamClientInterceptor")):
    # All four gRPC protocols share the interceptor signature
    # (continuation, client_call_details, request_or_iterator) and the same
    # span-event lifecycle, so one body serves every kind.
    def __init__(self, target: str = "") -> None:
        self._target = target

    def _intercept(self, continuation, client_call_details, request):
        span = current_span()
        if span is None:
            return continuation(client_call_details, request)
        method = getattr(client_call_details, "method", "") or ""
        # Open the event before inject: the context written into the gRPC metadata
        # must carry this call's own depth/sequence. ``new_span_event`` crosses
        # pybind11 and can raise when the agent is in an error state; raising here
        # would abort the RPC before the request is sent, so degrade to an untraced
        # call — every helper below (annotate/end/error) is a no-op on ``None``.
        try:
            event = span.new_span_event(
                method, service_type=SERVICE_TYPE_GRPC,
            )
        except Exception:  # noqa: BLE001
            event = None
        # Annotate before inject, not after: the Pinpoint-Host header the
        # server reads as its acceptorHost is the event's destination, so a
        # destination set afterwards propagates as an empty host.
        sampled = span_is_sampled(span)
        if sampled:
            _annotate_client(event, self._target, method)
        new_details = _inject_into_call_details(span, client_call_details)
        if not sampled:
            return continuation(new_details, request)
        try:
            response = continuation(new_details, request)
        except BaseException as exc:
            _annotate_grpc_status(event, _extract_grpc_status(exc))
            record_exception_on_event(event, exc)
            end_quietly(event)
            raise
        # A streaming rendezvous may be iterated/cancelled on another thread.
        # Keep the event dispatch-scoped and return the original response so
        # no SpanEvent is retained across that ownership boundary.
        _finish_unary_client_event(event, response)
        return response

    intercept_unary_unary = _intercept
    intercept_unary_stream = _intercept
    intercept_stream_unary = _intercept
    intercept_stream_stream = _intercept


@safe_try
def _annotate_client(event, target: str, method: str) -> None:
    """Stamp the call target on the client event.

    Both destination and endPoint are the dialled host:port — the destination
    is what the server map links on and what rides the Pinpoint-Host header,
    so a logical name here would strand the two applications as separate
    nodes. The method keeps its place in the httpUrl annotation, as
    ``grpc://<target><method>``.
    """
    event.set_destination(target)
    event.set_end_point(target)
    event.annotate_string(ANNOTATION_HTTP_URL, "grpc://" + target + method)


@safe_try
def _annotate_grpc_status(event, status: str) -> None:
    if status:
        event.annotate_string(ANNOTATION_GRPC_CLIENT_STATUS, status)


def _extract_grpc_status(source) -> str:
    """Resolved status name, or "": a broken/absent/blocking ``code()`` must
    never raise into user code (``code()`` is read at most once)."""
    try:
        return _status_name(source)
    except Exception:  # noqa: BLE001
        return ""


def _status_name(source) -> str:
    if source is None:
        return ""
    code = getattr(source, "code", None)
    if callable(code):
        code = code()
    if code is None:
        return ""
    name = getattr(code, "name", None)
    if name:
        return str(name)
    text = str(code)
    if "." in text:
        return text.rsplit(".", 1)[-1]
    return text


def instrument() -> None:
    GrpcInstrumentor().instrument()
