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

"""gRPC instrumentation.

Exercises the server interceptor (`_PinpointServerInterceptor`) and the
client interceptor (`_PinpointClientInterceptor`) against in-memory
fakes — no real grpc channel is opened. Covers all four RPC kinds
(unary-unary, unary-stream, stream-unary, stream-stream) so the
streaming wrappers don't regress.
"""

from __future__ import annotations

import contextvars
from collections import namedtuple

import pytest

grpc = pytest.importorskip("grpc")

from _fakes import FakeAgent as _FakeAgent, UnsampledNative, fake_inject_items

import pinpoint
from pinpoint import context as ppctx
from pinpoint.annotation import ANNOTATION_GRPC_CLIENT_STATUS, ANNOTATION_HTTP_URL
from pinpoint.instrumentations import grpc as grpc_instr
from pinpoint.instrumentations.grpc import (
    _normalize_target,
    _peer_host,
    _PinpointClientInterceptor,
    _PinpointServerInterceptor,
)
from pinpoint.propagator import HEADER_SAMPLED


# ---------------------------------------------------------------------------
# Server-side handler call details / handlers
# ---------------------------------------------------------------------------

_HandlerCallDetails = namedtuple(
    "HandlerCallDetails", ["method", "invocation_metadata"],
)


def _details(method="/svc/Method", metadata=()):
    return _HandlerCallDetails(method=method, invocation_metadata=metadata)


def _make_handler(*, unary_unary=None, unary_stream=None,
                  stream_unary=None, stream_stream=None):
    if unary_unary is not None:
        return grpc.unary_unary_rpc_method_handler(unary_unary)
    if unary_stream is not None:
        return grpc.unary_stream_rpc_method_handler(unary_stream)
    if stream_unary is not None:
        return grpc.stream_unary_rpc_method_handler(stream_unary)
    if stream_stream is not None:
        return grpc.stream_stream_rpc_method_handler(stream_stream)
    raise AssertionError("no handler given")


# ---------------------------------------------------------------------------
# Server-side tests
# ---------------------------------------------------------------------------

def test_server_unary_unary_opens_and_ends_span(fake_agent):
    seen_span = []

    def handler(req, ctx):
        seen_span.append(ppctx.current_span())
        return f"echo:{req}"

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(unary_unary=handler), _details(),
    )

    out = wrapped.unary_unary("hello", object())
    assert out == "echo:hello"
    # span was current during the handler and is now ended
    assert seen_span and seen_span[0] is not None
    assert fake_agent.native_spans[0].end_called
    assert ppctx.current_span() is None


def test_server_unary_unary_records_handler_exception(fake_agent):
    def handler(req, ctx):
        raise ValueError("boom")

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(unary_unary=handler), _details(),
    )
    with pytest.raises(ValueError):
        wrapped.unary_unary("x", object())
    span = fake_agent.native_spans[0]
    assert span.errors  # set_error was called
    assert span.end_called


def test_is_grpc_abort_classifies_abort_sentinel():
    """``context.abort()`` raises a bare ``Exception()`` after setting a non-OK
    code — that is control flow, not a server error. A real handler failure
    (a typed exception, or one carrying a message) is not classified as abort."""
    class _Ctx:
        def code(self):
            return grpc.StatusCode.NOT_FOUND

    assert grpc_instr._is_grpc_abort(Exception(), _Ctx()) is True
    # Bare Exception with an OK code is not an abort.

    class _OkCtx:
        def code(self):
            return grpc.StatusCode.OK
    assert grpc_instr._is_grpc_abort(Exception(), _OkCtx()) is False
    # Typed / message-bearing exceptions are real errors, not abort sentinels.
    assert grpc_instr._is_grpc_abort(ValueError("boom"), _Ctx()) is False
    assert grpc_instr._is_grpc_abort(Exception("msg"), _Ctx()) is False


def test_server_unary_unary_abort_is_not_recorded_as_error(fake_agent):
    """A handler that calls ``context.abort(NOT_FOUND)`` (idiomatic control
    flow) must not inflate the transaction error rate; the span still ends and
    the exception still propagates."""
    class _Ctx:
        def code(self):
            return grpc.StatusCode.NOT_FOUND

    def handler(req, ctx):
        raise Exception()  # exactly what grpc's context.abort() raises

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(unary_unary=handler), _details(),
    )
    with pytest.raises(Exception):
        wrapped.unary_unary("x", _Ctx())
    span = fake_agent.native_spans[0]
    assert not span.errors           # abort is control flow, not an error
    assert span.end_called


def test_server_unary_stream_traces_full_iteration(fake_agent):
    seen_span_per_yield = []

    def handler(req, ctx):
        for i in range(3):
            seen_span_per_yield.append(ppctx.current_span())
            yield f"{req}#{i}"

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(unary_stream=handler), _details(),
    )

    span = fake_agent.native_spans[0] if fake_agent.native_spans else None
    # The span is opened lazily inside _stream_with_server_span; iterate to
    # drive it.
    out = list(wrapped.unary_stream("greet", object()))
    assert out == ["greet#0", "greet#1", "greet#2"]
    span = fake_agent.native_spans[0]
    assert span.end_called
    assert ppctx.current_span() is None
    # span must be the current_span during every yield
    assert all(s is not None for s in seen_span_per_yield)


def test_server_unary_stream_records_streaming_error(fake_agent):
    def handler(req, ctx):
        yield "ok"
        raise RuntimeError("mid-stream boom")

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(unary_stream=handler), _details(),
    )

    it = wrapped.unary_stream("x", object())
    assert next(it) == "ok"
    with pytest.raises(RuntimeError):
        next(it)
    span = fake_agent.native_spans[0]
    assert span.errors
    assert span.end_called


def test_server_stream_unary_traces_request_consumption(fake_agent):
    captured = []

    def handler(req_iter, ctx):
        for r in req_iter:
            captured.append((r, ppctx.current_span()))
        return "joined:" + ",".join(r for r, _ in captured)

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(stream_unary=handler), _details(),
    )

    out = wrapped.stream_unary(iter(["a", "b"]), object())
    assert out == "joined:a,b"
    span = fake_agent.native_spans[0]
    assert span.end_called
    assert all(s is not None for _, s in captured)


def test_server_stream_stream_traces_full_iteration(fake_agent):
    def handler(req_iter, ctx):
        for r in req_iter:
            yield f"echo:{r}"

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(stream_stream=handler), _details(),
    )

    out = list(wrapped.stream_stream(iter(["a", "b", "c"]), object()))
    assert out == ["echo:a", "echo:b", "echo:c"]
    span = fake_agent.native_spans[0]
    assert span.end_called


def test_server_passes_through_when_agent_disabled(monkeypatch):
    agent = _FakeAgent(enabled=False)
    monkeypatch.setattr(pinpoint.agent, "_instance", agent)

    sentinel = object()

    interceptor = _PinpointServerInterceptor()
    result = interceptor.intercept_service(
        lambda d: sentinel, _details(),
    )
    assert result is sentinel
    assert not agent.native_spans


class _FakeServicerContext:
    """Upstream trace context is read from ``context.invocation_metadata()``
    (the same client metadata ``handler_call_details`` carries), so traced
    handlers can be cached per method instead of rebuilt per RPC."""

    def __init__(self, metadata=()):
        self._metadata = tuple(metadata)

    def invocation_metadata(self):
        return self._metadata


def test_server_skips_reader_without_pinpoint_metadata(fake_agent):
    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(unary_unary=lambda req, ctx: "ok"),
        _details(),
    )
    wrapped.unary_unary("hi", _FakeServicerContext([("x-custom", "v")]))
    # No upstream Pinpoint metadata -> reader-less new_span.
    assert fake_agent.native_spans[-1].headers is None


def test_server_builds_reader_with_upstream_metadata(fake_agent):
    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(unary_unary=lambda req, ctx: "ok"),
        _details(),
    )
    wrapped.unary_unary("hi", _FakeServicerContext([("pinpoint-traceid", "T-1")]))
    # Upstream context present -> metadata dict handed to new_span.
    assert fake_agent.native_spans[-1].headers is not None


def test_server_handler_cached_per_method(fake_agent):
    """Same method + same handler object -> the traced wrapper is reused
    instead of being rebuilt on every RPC."""
    handler = _make_handler(unary_unary=lambda req, ctx: "ok")

    interceptor = _PinpointServerInterceptor()
    first = interceptor.intercept_service(lambda d: handler, _details())
    second = interceptor.intercept_service(lambda d: handler, _details())
    assert first is second

    # A different handler under the same method must trigger a rebuild.
    other = _make_handler(unary_unary=lambda req, ctx: "other")
    third = interceptor.intercept_service(lambda d: other, _details())
    assert third is not first
    assert third.unary_unary("x", _FakeServicerContext()) == "other"


# ---------------------------------------------------------------------------
# Server-side: grpc.server(...) wrapper argument handling
# ---------------------------------------------------------------------------

def test_grpc_server_wrapper_positional_interceptors_no_collision():
    """``grpc.server(thread_pool, handlers, interceptors)`` passes
    ``interceptors`` positionally (index 2). The wrapper must prepend into
    that positional slot instead of also setting ``kwargs['interceptors']``,
    which would raise ``TypeError: got multiple values for 'interceptors'``."""
    captured = {}

    def wrapped(thread_pool, handlers=None, interceptors=None, **kwargs):
        captured["interceptors"] = interceptors
        captured["kwargs"] = kwargs
        return "server"

    user_interceptor = object()
    out = grpc_instr._grpc_server_wrapper(
        wrapped, instance=None,
        args=("pool", ("handler",), [user_interceptor]),
        kwargs={},
    )
    assert out == "server"
    assert "interceptors" not in captured["kwargs"]
    # Pinpoint interceptor prepended; user interceptor preserved after it.
    interceptors = captured["interceptors"]
    assert isinstance(interceptors[0], _PinpointServerInterceptor)
    assert interceptors[-1] is user_interceptor


def test_grpc_server_wrapper_keyword_interceptors():
    """``interceptors`` passed as a keyword is still prepended into."""
    captured = {}

    def wrapped(thread_pool, handlers=None, interceptors=None, **kwargs):
        captured["interceptors"] = interceptors
        return "server"

    user_interceptor = object()
    grpc_instr._grpc_server_wrapper(
        wrapped, instance=None, args=("pool",),
        kwargs={"interceptors": [user_interceptor]},
    )
    interceptors = captured["interceptors"]
    assert isinstance(interceptors[0], _PinpointServerInterceptor)
    assert interceptors[-1] is user_interceptor


def test_grpc_server_wrapper_does_not_mutate_original_args():
    """Works on copies so a fallback retry sees the caller's original list."""
    original = [object()]

    def wrapped(thread_pool, handlers=None, interceptors=None, **kwargs):
        return "server"

    grpc_instr._grpc_server_wrapper(
        wrapped, instance=None,
        args=("pool", None, original), kwargs={},
    )
    # Our interceptor was not inserted into the caller's list.
    assert len(original) == 1
    assert not isinstance(original[0], _PinpointServerInterceptor)


# ---------------------------------------------------------------------------
# Client-side tests
# ---------------------------------------------------------------------------

_ClientCallDetails = namedtuple(
    "ClientCallDetails", ["method", "timeout", "metadata", "credentials"],
)


def _ccd(method="/svc/Method", metadata=None):
    return _ClientCallDetails(
        method=method, timeout=None, metadata=metadata, credentials=None,
    )


def _root_span(agent):
    """Set a root span as current and return it (caller resets via fixture)."""
    span = agent.new_span("root", "/root")
    ppctx.set_current_span(span)
    return span


def test_client_unary_unary_records_event_and_injects_metadata(fake_agent):
    _root_span(fake_agent)
    captured = {}

    def continuation(call_details, request):
        captured["call_details"] = call_details
        captured["request"] = request
        return "RESP"

    interceptor = _PinpointClientInterceptor()
    seeded = [("x-user", "v")]
    out = interceptor.intercept_unary_unary(
        continuation, _ccd(metadata=seeded), "REQ",
    )
    assert out == "RESP"
    operation = "/svc/Method"
    assert ("event_start", "root", operation) in fake_agent.events
    assert ("event_end", "root", operation) in fake_agent.events
    # The wrapper hands a list to grpc (never a tuple/None) and preserves
    # any pre-seeded metadata; Pinpoint headers are appended by `inject_items`
    # when the native agent is loaded (no-op in unit tests).
    md = captured["call_details"].metadata
    assert isinstance(md, list)
    assert ("x-user", "v") in md


def test_client_unary_unary_records_grpc_status_when_available(fake_agent):
    span = _root_span(fake_agent)

    class _Status:
        name = "OK"

    class _Response:
        def code(self):
            return _Status()

    interceptor = _PinpointClientInterceptor()
    out = interceptor.intercept_unary_unary(
        lambda _d, _r: _Response(), _ccd(), "REQ",
    )
    assert isinstance(out, _Response)
    ev = span._native.all_events[-1]
    assert (
        "str",
        ANNOTATION_GRPC_CLIENT_STATUS,
        "OK",
    ) in ev.annotations.entries


def test_client_unary_unary_future_ends_event_at_dispatch(fake_agent):
    """``stub.Method.future()`` routes through the same unary interceptor but
    the continuation returns a live call. The interceptor must return it
    without touching ``code()`` (which blocks) and end the event on the
    calling thread right away: deferring to the call's done callback would
    finish the event from a gRPC channel thread while the application thread
    keeps using the parent span — a Span/SpanEvent is single-threaded for its
    lifetime. The event covers the dispatch; the resolved status is not
    recorded for futures."""
    span = _root_span(fake_agent)
    call = _FakeResolvableRpcCall(grpc.StatusCode.OK)

    interceptor = _PinpointClientInterceptor()
    out = interceptor.intercept_unary_unary(lambda _d, _r: call, _ccd(), "REQ")

    assert out is call
    operation = "/svc/Method"
    assert ("event_start", "root", operation) in fake_agent.events
    # Ended synchronously at dispatch, on the owning thread.
    assert ("event_end", "root", operation) in fake_agent.events
    # No done callback was registered: nothing may touch the event later.
    assert call._callbacks == []

    call.terminate()

    ends = [e for e in fake_agent.events
            if e == ("event_end", "root", operation)]
    assert len(ends) == 1
    ev = span._native.all_events[-1]
    assert not any(
        entry[1] == ANNOTATION_GRPC_CLIENT_STATUS
        for entry in ev.annotations.entries
    )


def test_client_stream_unary_future_ends_event_at_dispatch(fake_agent):
    """Same dispatch-scoped event for ``.future()`` on stream-unary methods."""
    _root_span(fake_agent)
    call = _FakeResolvableRpcCall(grpc.StatusCode.OK)

    interceptor = _PinpointClientInterceptor()
    out = interceptor.intercept_stream_unary(
        lambda _d, _r: call, _ccd(), iter(["a"]),
    )

    assert out is call
    operation = "/svc/Method"
    assert ("event_end", "root", operation) in fake_agent.events
    assert call._callbacks == []


def test_client_unary_unary_already_done_call_ends_synchronously(fake_agent):
    """The blocking path returns an already-terminated outcome; the event
    must end synchronously inside the interceptor (no deferral needed)."""
    _root_span(fake_agent)
    call = _FakeResolvableRpcCall(grpc.StatusCode.OK, done=True)

    interceptor = _PinpointClientInterceptor()
    out = interceptor.intercept_unary_unary(lambda _d, _r: call, _ccd(), "REQ")

    assert out is call
    operation = "/svc/Method"
    assert ("event_end", "root", operation) in fake_agent.events


class _FakeResolvableRpcCall(grpc.RpcError):
    """A unary outcome / ``.future()`` call that terminates with a fixed
    status code.

    It subclasses ``grpc.RpcError`` because *every* grpc rendezvous is an
    ``RpcError`` — on success and failure alike (``_MultiThreadedRendezvous``
    inherits it) — which is exactly why failure detection must key off the
    status code, not ``isinstance(..., RpcError)``. ``code()`` asserts the call
    has terminated, mirroring real grpc where it would otherwise block."""

    def __init__(self, code, *, done=False):
        self._code = code
        self._done = done
        self._callbacks = []

    def done(self):
        return self._done

    def add_done_callback(self, fn):
        if self._done:
            fn(self)
        else:
            self._callbacks.append(fn)

    def code(self):
        assert self._done, "code() on a live call would block in real grpc"
        return self._code

    def terminate(self):
        self._done = True
        for fn in list(self._callbacks):
            fn(self)


def test_client_unary_unary_blocking_rpc_error_marks_event_error(fake_agent):
    """grpc's blocking interceptor path does not raise RPC failures out of the
    continuation — ``_interceptor.py`` catches ``grpc.RpcError`` and *returns*
    it as the call object — so a failed RPC reaches the interceptor's success
    path. The event must still be marked errored (and its status annotated),
    otherwise a downstream outage is recorded as a healthy call."""
    span = _root_span(fake_agent)
    failed = _FakeResolvableRpcCall(grpc.StatusCode.UNAVAILABLE, done=True)

    interceptor = _PinpointClientInterceptor()
    out = interceptor.intercept_unary_unary(lambda _d, _r: failed, _ccd(), "REQ")

    assert out is failed
    operation = "/svc/Method"
    assert ("event_end", "root", operation) in fake_agent.events
    assert any(e[0] == "event_error" for e in fake_agent.events)
    ev = span._native.all_events[-1]
    assert (
        "str",
        ANNOTATION_GRPC_CLIENT_STATUS,
        "UNAVAILABLE",
    ) in ev.annotations.entries


def test_client_unary_unary_future_termination_does_not_touch_event(fake_agent):
    """A ``.future()`` call resolving — successfully or not — must not amend
    the already-ended dispatch event: the resolution is observed on a gRPC
    channel thread, and touching the event from there would share the parent
    span across threads."""
    span = _root_span(fake_agent)
    call = _FakeResolvableRpcCall(grpc.StatusCode.UNAVAILABLE)

    interceptor = _PinpointClientInterceptor()
    out = interceptor.intercept_unary_unary(lambda _d, _r: call, _ccd(), "REQ")

    assert out is call
    operation = "/svc/Method"
    assert ("event_end", "root", operation) in fake_agent.events
    assert call._callbacks == []

    call.terminate()

    assert not any(e[0] == "event_error" for e in fake_agent.events)
    ev = span._native.all_events[-1]
    assert not any(
        entry[1] == ANNOTATION_GRPC_CLIENT_STATUS
        for entry in ev.annotations.entries
    )


def test_client_unary_unary_blocking_success_is_not_marked_error(fake_agent):
    """A *successful* blocking outcome is still a ``grpc.RpcError`` subclass
    (all rendezvous are), so failure detection keys off the OK status code, not
    the type — otherwise every call would be falsely flagged as an error."""
    span = _root_span(fake_agent)
    call = _FakeResolvableRpcCall(grpc.StatusCode.OK, done=True)

    interceptor = _PinpointClientInterceptor()
    interceptor.intercept_unary_unary(lambda _d, _r: call, _ccd(), "REQ")

    operation = "/svc/Method"
    assert ("event_end", "root", operation) in fake_agent.events
    assert not any(e[0] == "event_error" for e in fake_agent.events)
    ev = span._native.all_events[-1]
    assert (
        "str",
        ANNOTATION_GRPC_CLIENT_STATUS,
        "OK",
    ) in ev.annotations.entries


def test_client_stream_event_ends_at_dispatch_and_ignores_later_failure(fake_agent):
    """The response can be handed to another thread, so the parent's event is
    ended before returning it and later iteration never touches that event."""
    span = _root_span(fake_agent)

    class _FailingRendezvous(grpc.RpcError):
        def __init__(self):
            self._done = False

        def __iter__(self):
            def _gen():
                yield "r0"
                assert self._done  # error surfaces only after termination
                raise self

            return _gen()

        def done(self):
            return self._done

        def code(self):
            assert self._done, "code() on a live call would block in real grpc"
            return grpc.StatusCode.UNAVAILABLE

        def terminate(self):
            self._done = True

    call = _FailingRendezvous()
    interceptor = _PinpointClientInterceptor()
    resp = interceptor.intercept_unary_stream(
        lambda _d, _r: call, _ccd(), "REQ")

    operation = "/svc/Method"
    assert ("event_end", "root", operation) in fake_agent.events
    iterator = iter(resp)
    assert next(iterator) == "r0"
    call.terminate()

    with pytest.raises(grpc.RpcError):
        next(iterator)

    assert not any(event[0] == "event_error" for event in fake_agent.events)
    event = span._native.all_events[-1]
    assert not any(
        entry[1] == ANNOTATION_GRPC_CLIENT_STATUS
        for entry in event.annotations.entries
    )


def test_client_unary_unary_unsampled_inject_writes_s0_marker(fake_agent, monkeypatch):
    """An unsampled parent must still propagate ``Pinpoint-Sampled: s0`` so
    the server short-circuits its own sampling decision."""
    from pinpoint.agent import UnSampledSpan  # type: ignore[attr-defined]

    monkeypatch.setattr(grpc_instr, "inject_items", fake_inject_items)

    sp = UnSampledSpan(UnsampledNative())
    ppctx.set_current_span(sp)

    captured: dict = {}

    def continuation(call_details, request):
        captured["metadata"] = list(call_details.metadata or [])
        return "RESP"

    interceptor = _PinpointClientInterceptor()
    out = interceptor.intercept_unary_unary(continuation, _ccd(), "REQ")
    assert out == "RESP"
    md = dict(captured["metadata"])
    # gRPC metadata keys are conventionally lowercased by the wrapper.
    assert md.get(HEADER_SAMPLED.lower()) == "s0"


def test_client_unary_unary_no_event_when_no_current_span(fake_agent):
    ppctx.set_current_span(None)
    interceptor = _PinpointClientInterceptor()
    out = interceptor.intercept_unary_unary(
        lambda d, r: "RESP", _ccd(), "REQ",
    )
    assert out == "RESP"
    assert fake_agent.native_spans == []


def test_client_unary_stream_event_is_dispatch_scoped(fake_agent):
    span = _root_span(fake_agent)

    class _ResponseIter:
        def __iter__(self):
            return iter(["r0", "r1", "r2"])

        def code(self):
            return "StatusCode.OK"

    def continuation(call_details, request):
        # Pretend the server returns 3 responses
        return _ResponseIter()

    interceptor = _PinpointClientInterceptor()
    resp = interceptor.intercept_unary_stream(continuation, _ccd(), "REQ")

    # The original response is returned and the parent event is already ended.
    operation = "/svc/Method"
    assert ("event_start", "root", operation) in fake_agent.events
    assert ("event_end", "root", operation) in fake_agent.events

    assert list(resp) == ["r0", "r1", "r2"]
    ev = span._native.all_events[-1]
    assert (
        "str",
        ANNOTATION_GRPC_CLIENT_STATUS,
        "OK",
    ) in ev.annotations.entries



def test_client_unary_stream_iteration_error_does_not_reopen_dispatch_event(fake_agent):
    _root_span(fake_agent)

    def gen():
        yield "ok"
        raise RuntimeError("server stream boom")

    interceptor = _PinpointClientInterceptor()
    resp = interceptor.intercept_unary_stream(
        lambda d, r: gen(), _ccd(), "REQ",
    )
    assert next(resp) == "ok"
    with pytest.raises(RuntimeError):
        next(resp)
    # The original exception propagates, but the already-ended event is not
    # retained and amended from the iterator's potentially different thread.
    event_log = fake_agent.events
    assert not any(e[0] == "event_error" for e in event_log)
    assert ("event_end", "root", "/svc/Method") in event_log


def test_client_stream_unary_ends_event_after_continuation(fake_agent):
    _root_span(fake_agent)

    def continuation(call_details, request_iterator):
        list(request_iterator)  # drain
        return "RESP"

    interceptor = _PinpointClientInterceptor()
    out = interceptor.intercept_stream_unary(continuation, _ccd(), iter(["a"]))
    assert out == "RESP"
    assert ("event_end", "root", "/svc/Method") in fake_agent.events


def test_client_stream_stream_event_is_dispatch_scoped(fake_agent):
    _root_span(fake_agent)

    def continuation(call_details, request_iterator):
        return iter(["x", "y"])

    interceptor = _PinpointClientInterceptor()
    resp = interceptor.intercept_stream_stream(continuation, _ccd(), iter(["a"]))

    operation = "/svc/Method"
    assert ("event_end", "root", operation) in fake_agent.events
    assert list(resp) == ["x", "y"]


def test_client_streaming_response_proxies_unknown_attrs(fake_agent):
    _root_span(fake_agent)

    class _FakeCall:
        def __iter__(self): return iter([1, 2])
        def trailing_metadata(self): return (("k", "v"),)

    interceptor = _PinpointClientInterceptor()
    resp = interceptor.intercept_unary_stream(
        lambda d, r: _FakeCall(), _ccd(), "REQ",
    )
    # The real response is returned unchanged, so grpc-specific methods stay.
    assert resp.trailing_metadata() == (("k", "v"),)
    list(resp)


# ---------------------------------------------------------------------------
# Tear-down hygiene
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_current_span():
    yield
    ppctx.set_current_span(None)


def test_client_unary_stream_partial_consumption_retains_no_event(fake_agent):
    """Partial consumption needs no callback/finalizer because the event was
    ended synchronously at dispatch."""

    _root_span(fake_agent)

    class _Call:
        """Iterable call object with done-callback support, like
        grpc._MultiThreadedRendezvous."""

        def __init__(self):
            self._cbs = []

        def __iter__(self):
            return iter(["r0", "r1", "r2"])

        def add_done_callback(self, cb):
            self._cbs.append(cb)

        def terminate(self):
            for cb in self._cbs:
                cb(self)

    call = _Call()
    interceptor = _PinpointClientInterceptor()
    resp = interceptor.intercept_unary_stream(lambda d, r: call, _ccd(), "REQ")

    it = iter(resp)
    assert next(it) == "r0"  # partial consumption, then abandon
    operation = "/svc/Method"
    assert ("event_end", "root", operation) in fake_agent.events

    # The instrumentation must not have hooked the done callback.
    assert call._cbs == []
    call.terminate()
    ends = [e for e in fake_agent.events if e == ("event_end", "root", operation)]
    assert len(ends) == 1


def test_client_unary_stream_abandonment_needs_no_gc_fallback(fake_agent):
    """Dropping the raw response cannot affect the dispatch event."""
    import gc

    _root_span(fake_agent)
    interceptor = _PinpointClientInterceptor()
    resp = interceptor.intercept_unary_stream(
        lambda d, r: iter(["r0", "r1"]), _ccd(), "REQ",
    )
    assert next(iter(resp)) == "r0"
    operation = "/svc/Method"
    assert ("event_end", "root", operation) in fake_agent.events

    del resp
    gc.collect()
    assert ("event_end", "root", operation) in fake_agent.events


# ---------------------------------------------------------------------------
# Stability: agent/native failures must never fail the RPC (M2 / M3)
# ---------------------------------------------------------------------------

class _RaisingAgent(_FakeAgent):
    """Agent whose ``new_span`` blows up crossing into native — mimics an
    error state (shutdown race, pybind failure)."""

    def new_span(self, operation, rpc_point, headers=None, method=""):
        raise RuntimeError("native new_span exploded")


def test_server_agent_failure_runs_unary_rpc_untraced(monkeypatch):
    """``_open_server_span`` swallowing an ``agent.new_span`` failure must
    degrade to running the handler untraced, not terminate the RPC."""
    monkeypatch.setattr(pinpoint.agent, "_instance", _RaisingAgent())

    seen = []

    def handler(req, ctx):
        seen.append(ppctx.current_span())
        return f"echo:{req}"

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(unary_unary=handler), _details(),
    )
    out = wrapped.unary_unary("hi", _FakeServicerContext())
    assert out == "echo:hi"
    # Ran untraced: no span was installed on the contextvar.
    assert seen == [None]
    assert ppctx.current_span() is None


def test_server_agent_failure_runs_streaming_rpc_untraced(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _RaisingAgent())

    def handler(req, ctx):
        for i in range(2):
            yield f"{req}#{i}"

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(unary_stream=handler), _details(),
    )
    out = list(wrapped.unary_stream("greet", _FakeServicerContext()))
    assert out == ["greet#0", "greet#1"]


def _raise_new_span_event(*_args, **_kwargs):
    raise RuntimeError("native new_span_event exploded")


def test_client_event_failure_runs_rpc_untraced(fake_agent, monkeypatch):
    """A native ``new_span_event`` failure in the client interceptor must not
    abort the call before the request is sent — the RPC proceeds untraced."""
    span = fake_agent.new_span("root", "/root")
    monkeypatch.setattr(span._native, "new_span_event", _raise_new_span_event)
    ppctx.set_current_span(span)

    captured = {}

    def continuation(call_details, request):
        captured["called"] = True
        captured["request"] = request
        return "RESP"

    interceptor = _PinpointClientInterceptor()
    out = interceptor.intercept_unary_unary(continuation, _ccd(), "REQ")
    assert out == "RESP"
    assert captured.get("called") is True
    assert captured["request"] == "REQ"


def test_client_streaming_event_failure_runs_rpc_untraced(fake_agent, monkeypatch):
    span = fake_agent.new_span("root", "/root")
    monkeypatch.setattr(span._native, "new_span_event", _raise_new_span_event)
    ppctx.set_current_span(span)

    def continuation(call_details, request):
        return iter(["r0", "r1"])

    interceptor = _PinpointClientInterceptor()
    resp = interceptor.intercept_unary_stream(continuation, _ccd(), "REQ")
    # No event was opened, but iteration still works end to end.
    assert list(resp) == ["r0", "r1"]


# ---------------------------------------------------------------------------
# Stability: server-streaming cancellation must not leak the span (M5)
# ---------------------------------------------------------------------------

def test_server_stream_reset_in_different_context_still_ends_span(fake_agent):
    """A client cancelling a server-streaming RPC finalizes the response
    generator in a different context than the first ``next()``. The
    cross-context ``reset_current_span`` raises ``ValueError`` — the guard
    must swallow it so ``span.end()`` still runs and the native root span
    isn't leaked."""
    def handler(req, ctx):
        yield "a"
        yield "b"

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(unary_stream=handler), _details(),
    )

    gen = wrapped.unary_stream("x", _FakeServicerContext())
    # Run the first next() inside a *copied* context so set_current_span's
    # token is bound to that context, not the outer one.
    ctx = contextvars.copy_context()
    assert ctx.run(lambda: next(gen)) == "a"

    # Finalize in the outer context: reset_current_span(token) would raise
    # "Token was created in a different Context". close() must not propagate
    # that ValueError...
    gen.close()
    # ...and the span must still have been ended (no leak).
    assert fake_agent.native_spans[0].end_called


# ---------------------------------------------------------------------------
# Stability: server-streaming span must not leak when the response iterator
# is never started (RPC cancelled/deadline-expired before first next()) (M6)
# ---------------------------------------------------------------------------

class _FakeStreamContext(_FakeServicerContext):
    """Servicer context that records RPC-termination callbacks, mirroring
    ``grpc.ServicerContext.add_callback``. ``add_callback`` returns True (the
    callback is registered and fires on ``terminate()``)."""

    def __init__(self, metadata=()):
        super().__init__(metadata)
        self._callbacks = []

    def add_callback(self, callback):
        self._callbacks.append(callback)
        return True

    def terminate(self):
        for cb in list(self._callbacks):
            cb()


def test_server_unary_stream_never_iterated_creates_no_span_or_callback(fake_agent):
    """Span creation is lazy at first next(), so pre-iteration cancellation
    has no foreign-thread cleanup work and cannot leak a span."""
    started = []

    def handler(req, ctx):
        started.append(True)  # only runs once the generator is iterated
        yield "a"

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(unary_stream=handler), _details(),
    )

    ctx = _FakeStreamContext()
    it = wrapped.unary_stream("x", ctx)  # generator returned, never iterated
    assert fake_agent.native_spans == []
    assert ctx._callbacks == []
    assert started == []  # handler body has not run

    ctx.terminate()  # RPC terminates before the first next()
    assert fake_agent.native_spans == []
    assert started == []
    assert it is not None  # keep the iterator alive: proves callback, not GC


def test_server_unary_stream_termination_mid_iteration_defers_to_generator(fake_agent):
    """No termination callback owns the span; the iterator thread's finally
    ends the lazily created span."""
    def handler(req, ctx):
        yield "a"
        yield "b"

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(unary_stream=handler), _details(),
    )

    ctx = _FakeStreamContext()
    it = wrapped.unary_stream("x", ctx)
    assert next(it) == "a"  # iterator thread created and owns the span

    ctx.terminate()  # client cancels mid-stream
    span = fake_agent.native_spans[0]
    assert ctx._callbacks == []
    assert not span.end_called

    it.close()  # gRPC finalizes the abandoned generator
    assert span.end_called


def test_server_unary_stream_gc_before_iteration_has_no_span(fake_agent):
    """Dropping a never-started response generator has nothing to finalize."""
    import gc

    def handler(req, ctx):
        yield "a"

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(unary_stream=handler), _details(),
    )

    # _FakeServicerContext has no add_callback -> only the finalizer applies.
    it = wrapped.unary_stream("x", _FakeServicerContext())
    assert fake_agent.native_spans == []

    del it
    gc.collect()
    assert fake_agent.native_spans == []


def test_server_unary_stream_already_terminated_creates_no_span(fake_agent):
    """A never-pulled, already-terminated RPC does not create a span."""

    class _TerminatedContext(_FakeServicerContext):
        def add_callback(self, callback):
            return False  # already terminated -> callback won't be called

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(unary_stream=lambda req, ctx: iter(["a"])),
        _details(),
    )

    it = wrapped.unary_stream("x", _TerminatedContext())
    assert fake_agent.native_spans == []
    assert it is not None


def test_server_stream_stream_gc_before_iteration_has_no_span(fake_agent):
    """Stream-stream uses the same lazy ownership rule."""
    import gc

    def handler(req_iter, ctx):
        for r in req_iter:
            yield f"echo:{r}"

    interceptor = _PinpointServerInterceptor()
    wrapped = interceptor.intercept_service(
        lambda d: _make_handler(stream_stream=handler), _details(),
    )

    it = wrapped.stream_stream(iter(["a"]), _FakeServicerContext())
    assert fake_agent.native_spans == []

    del it
    gc.collect()
    assert fake_agent.native_spans == []


# ---------------------------------------------------------------------------
# Stability: returning the raw streaming response preserves grpc type checks
# ---------------------------------------------------------------------------

class _FakeRendezvous(grpc.RpcError, grpc.Call, grpc.Future):
    """Stand-in for grpc's ``_MultiThreadedRendezvous`` which implements the
    ``RpcError`` / ``Call`` / ``Future`` interfaces user code type-checks."""

    def __iter__(self):
        return iter(["r0", "r1"])

    def code(self):
        return grpc.StatusCode.OK

    # grpc.Call abstract surface
    def add_callback(self, callback): return True
    def cancel(self): return False
    def details(self): return ""
    def initial_metadata(self): return ()
    def is_active(self): return False
    def time_remaining(self): return None
    def trailing_metadata(self): return ()

    # grpc.Future abstract surface
    def add_done_callback(self, fn): pass
    def cancelled(self): return False
    def done(self): return True
    def exception(self, timeout=None): return None
    def result(self, timeout=None): return None
    def running(self): return False
    def traceback(self, timeout=None): return None


def test_client_streaming_response_forwards_isinstance(fake_agent):
    """Returning the raw rendezvous preserves grpc interface checks."""
    _root_span(fake_agent)
    interceptor = _PinpointClientInterceptor()
    resp = interceptor.intercept_unary_stream(
        lambda d, r: _FakeRendezvous(), _ccd(), "REQ",
    )
    assert isinstance(resp, grpc.RpcError)
    assert isinstance(resp, grpc.Call)
    assert isinstance(resp, grpc.Future)
    # The event was already ended before the raw iterator was returned.
    assert list(resp) == ["r0", "r1"]
    operation = "/svc/Method"
    assert ("event_end", "root", operation) in fake_agent.events


def test_client_streaming_response_propagates_rpc_error(fake_agent):
    """The raw grpc error escapes iteration unchanged; the dispatch event is
    not retained to record it from a possibly different thread."""
    _root_span(fake_agent)

    class _ErroringRendezvous(grpc.RpcError):
        def __iter__(self):
            def _gen():
                yield "r0"
                raise self
            return _gen()

        def code(self):
            return grpc.StatusCode.UNAVAILABLE

        def done(self):
            return False

    interceptor = _PinpointClientInterceptor()
    resp = interceptor.intercept_unary_stream(
        lambda d, r: _ErroringRendezvous(), _ccd(), "REQ",
    )
    it = iter(resp)
    assert next(it) == "r0"
    with pytest.raises(grpc.RpcError):
        next(it)
    operation = "/svc/Method"
    assert ("event_end", "root", operation) in fake_agent.events
    assert not any(e[0] == "event_error" for e in fake_agent.events)


# ---------------------------------------------------------------------------
# Call-target reporting
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("target,expected", [
    ("localhost:50051", "localhost:50051"),
    ("dns:///localhost:8080", "localhost:8080"),
    ("unix:/run/grpc.sock", "localhost"),
    ("unix:///run/grpc.sock", "localhost"),
    ("", ""),
])
def test_normalize_target_strips_dns_scheme_and_maps_unix(target, expected):
    assert _normalize_target(target) == expected


@pytest.mark.parametrize("peer,expected", [
    ("ipv4:127.0.0.1:54321", "127.0.0.1"),
    ("ipv4:10.0.0.7:9", "10.0.0.7"),
    ("ipv6:[::1]:54321", "::1"),
    ("unix:/run/grpc.sock", "127.0.0.1"),
    ("", "127.0.0.1"),
    (None, "127.0.0.1"),
])
def test_peer_host_strips_scheme_and_port(peer, expected):
    class _Ctx:
        def peer(self):
            if peer is None:
                raise RuntimeError("no peer")
            return peer

    assert _peer_host(_Ctx()) == expected


def test_client_event_reports_the_dial_target_not_a_literal(fake_agent):
    """destination and endPoint are the dialled host:port, and the method
    rides the httpUrl annotation.

    destination is also what the Pinpoint-Host header carries, so a literal
    here would strand client and server as separate server-map nodes."""
    span = _root_span(fake_agent)
    interceptor = _PinpointClientInterceptor("localhost:50051")
    interceptor.intercept_unary_unary(
        lambda _d, _r: "RESP", _ccd(method="/grpcdemo.Hello/UnaryCall"), "REQ",
    )
    ev = span._native.all_events[-1]
    assert ev.destination == "localhost:50051"
    assert ev.endpoint == "localhost:50051"
    assert (
        "str",
        ANNOTATION_HTTP_URL,
        "grpc://localhost:50051/grpcdemo.Hello/UnaryCall",
    ) in ev.annotations.entries


def test_client_host_header_carries_the_target(fake_agent):
    """Pinpoint-Host is built from the event's destination, and the server
    reads it as its acceptorHost — so the destination has to be stamped
    before inject runs, not after."""
    _root_span(fake_agent)
    captured = {}

    def continuation(call_details, request):
        captured["metadata"] = call_details.metadata
        return "RESP"

    _PinpointClientInterceptor("localhost:50051").intercept_unary_unary(
        continuation, _ccd(), "REQ",
    )
    assert ("pinpoint-host", "localhost:50051") in captured["metadata"]


def test_server_span_records_the_caller_not_its_own_acceptor_host(fake_agent):
    """The native remote address falls back to acceptorHost (the client's
    Pinpoint-Host), which names this server — so the peer is set explicitly,
    as Go's ppgrpc does."""
    class _Ctx:
        def invocation_metadata(self):
            return ()

        def peer(self):
            return "ipv4:10.1.2.3:44444"

    interceptor = _PinpointServerInterceptor()
    traced = interceptor.intercept_service(
        lambda _d: _make_handler(unary_unary=lambda req, ctx: "ok"), _details(),
    )
    traced.unary_unary("REQ", _Ctx())
    assert fake_agent.native_spans[0].remote_address == "10.1.2.3"
