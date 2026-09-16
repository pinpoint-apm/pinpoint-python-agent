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

"""gRPC client + server over a real in-process grpc channel.

Both sides of the instrumentation are exercised at once: the client
interceptor (installed by wrapping ``grpc.insecure_channel``) injects the
Pinpoint metadata and records a span event per RPC; the server interceptor
(prepended by wrapping ``grpc.server``) opens a root span per RPC and stitches
it to the client trace from the invocation metadata. A generic bytes-in/
bytes-out service keeps the module protoc-free. No Docker service is needed.
"""

from __future__ import annotations

import time
from concurrent import futures

import pytest

grpc = pytest.importorskip("grpc")

from pinpoint.annotation import (
    ANNOTATION_GRPC_CLIENT_STATUS,
    ANNOTATION_HTTP_URL,
)
from pinpoint.service_type import (
    SERVICE_TYPE_GRPC,
    SERVICE_TYPE_GRPC_SERVER,
    SERVICE_TYPE_PYTHON_METHOD,
)

pytestmark = pytest.mark.no_docker

_OP_SERVER = "gRPC Server"
_METHOD_UNARY = "/it.Echo/Unary"
_METHOD_STREAM = "/it.Echo/ServerStream"
_METHOD_CLIENT_STREAM = "/it.Echo/ClientStream"
_METHOD_BIDI = "/it.Echo/BidiStream"

# host:port the module's channel dialled, filled in by the ``channel`` fixture.
_DIALED: dict = {}


@pytest.fixture(scope="module", autouse=True)
def _instrument():
    from pinpoint.instrumentations import grpc as grpc_instr

    grpc_instr.instrument()


def _unary(request: bytes, context) -> bytes:
    if request == b"boom":
        raise RuntimeError("boom-it")
    if request == b"abort":
        context.abort(grpc.StatusCode.NOT_FOUND, "missing-it")
    return b"echo:" + request


def _server_stream(request: bytes, context):
    for i in range(3):
        yield b"chunk-%d" % i


def _client_stream(request_iterator, context):
    return b"joined:" + b"|".join(request_iterator)


def _bidi_stream(request_iterator, context):
    for request in request_iterator:
        if request == b"boom":
            raise RuntimeError("stream-boom-it")
        yield b"echo:" + request


@pytest.fixture(scope="module")
def channel(_instrument):
    """One real server + intercepted client channel for the module.

    Both ``grpc.server`` and ``grpc.insecure_channel`` are called *after*
    ``instrument()`` so the wrapt wrappers actually splice the interceptors
    in.
    """
    handlers = {
        "Unary": grpc.unary_unary_rpc_method_handler(_unary),
        "ServerStream": grpc.unary_stream_rpc_method_handler(_server_stream),
        "ClientStream": grpc.stream_unary_rpc_method_handler(_client_stream),
        "BidiStream": grpc.stream_stream_rpc_method_handler(_bidi_stream),
    }
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    server.add_generic_rpc_handlers(
        (grpc.method_handlers_generic_handler("it.Echo", handlers),))
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    _DIALED["target"] = f"127.0.0.1:{port}"
    channel = grpc.insecure_channel(_DIALED["target"])
    yield channel
    channel.close()
    server.stop(grace=None)


def _wait_until(predicate, timeout: float = 10.0) -> None:
    """The server span is ended on a serving thread; the client can observe
    its response a hair before the wrapper's ``finally`` runs. Poll briefly
    instead of asserting a cross-thread ordering grpc does not guarantee."""
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert predicate()


def _target() -> str:
    """The host:port the module's channel dialled — what the client event
    records as its destination and endPoint (Go's ppgrpc does the same)."""
    return _DIALED["target"]


def _server_spans(recorder):
    return [s for s in recorder.spans if s.operation == _OP_SERVER]


def test_unary_records_client_event_and_server_root_span(
        channel, traced, consumer_agent):
    reply = channel.unary_unary(_METHOD_UNARY)(b"ping", timeout=10)
    assert reply == b"echo:ping"

    ev = traced.single(_METHOD_UNARY, SERVICE_TYPE_GRPC)
    assert ev.ended
    assert ev.service_type == SERVICE_TYPE_GRPC
    assert ev.destination == _target()
    assert ev.endpoint == _target()
    assert ev.ann(ANNOTATION_HTTP_URL) == [
        "grpc://" + _target() + _METHOD_UNARY]
    assert ev.ann(ANNOTATION_GRPC_CLIENT_STATUS) == ["OK"]
    assert ev.error is None

    _wait_until(lambda: _server_spans(traced) and _server_spans(traced)[0].ended)
    span = _server_spans(traced)[0]
    assert span.rpc == _METHOD_UNARY
    assert span.service_type == SERVICE_TYPE_GRPC_SERVER
    assert span.error is None

    # The invocation metadata carried the client trace across the wire
    # (grpc lowercases metadata keys).
    assert span.headers is not None
    assert span.headers.get("pinpoint-traceid") == traced.spans[0].trace_id
    # Sampled transactions omit Pinpoint-Sampled: the marker is written only
    # for drops.
    assert span.headers.get("pinpoint-sampled") is None


def test_server_streaming_client_event_is_dispatch_scoped(
        channel, traced, consumer_agent):
    stream = channel.unary_stream(_METHOD_STREAM)(b"go", timeout=10)

    chunks = list(stream)
    assert chunks == [b"chunk-0", b"chunk-1", b"chunk-2"]

    ev = traced.single(_METHOD_STREAM, SERVICE_TYPE_GRPC)
    assert ev.ended
    assert ev.endpoint == _target()
    # The response may be iterated on another application thread. The client
    # event ends at dispatch and therefore does not retain/record final status.
    assert ev.ann(ANNOTATION_GRPC_CLIENT_STATUS) == []

    _wait_until(lambda: _server_spans(traced) and _server_spans(traced)[0].ended)
    span = _server_spans(traced)[0]
    assert span.rpc == _METHOD_STREAM
    assert span.error is None


def test_client_streaming_records_both_sides(
        channel, traced, consumer_agent):
    reply = channel.stream_unary(_METHOD_CLIENT_STREAM)(
        iter((b"a", b"b", b"c")), timeout=10)
    assert reply == b"joined:a|b|c"

    event = traced.single(_METHOD_CLIENT_STREAM, SERVICE_TYPE_GRPC)
    assert event.ended
    assert event.endpoint == _target()
    assert event.ann(ANNOTATION_GRPC_CLIENT_STATUS) == ["OK"]
    assert event.error is None

    _wait_until(lambda: _server_spans(traced) and _server_spans(traced)[0].ended)
    span = _server_spans(traced)[0]
    assert span.rpc == _METHOD_CLIENT_STREAM
    assert span.error is None
    assert span.headers is not None
    assert span.headers.get("pinpoint-traceid") == traced.spans[0].trace_id


def test_bidirectional_streaming_records_both_sides(
        channel, traced, consumer_agent):
    stream = channel.stream_stream(_METHOD_BIDI)(
        iter((b"one", b"two")), timeout=10)
    assert list(stream) == [b"echo:one", b"echo:two"]

    event = traced.single(_METHOD_BIDI, SERVICE_TYPE_GRPC)
    assert event.ended
    assert event.endpoint == _target()
    assert event.ann(ANNOTATION_GRPC_CLIENT_STATUS) == []
    assert event.error is None

    _wait_until(lambda: _server_spans(traced) and _server_spans(traced)[0].ended)
    span = _server_spans(traced)[0]
    assert span.rpc == _METHOD_BIDI
    assert span.error is None


def test_bidirectional_stream_error_recorded_on_server_only(
        channel, traced, consumer_agent):
    stream = channel.stream_stream(_METHOD_BIDI)(
        iter((b"before", b"boom")), timeout=10)
    iterator = iter(stream)
    assert next(iterator) == b"echo:before"
    with pytest.raises(grpc.RpcError) as excinfo:
        next(iterator)
    assert excinfo.value.code() == grpc.StatusCode.UNKNOWN

    event = traced.single(_METHOD_BIDI, SERVICE_TYPE_GRPC)
    assert event.ended
    assert event.error is None
    assert event.ann(ANNOTATION_GRPC_CLIENT_STATUS) == []

    _wait_until(lambda: _server_spans(traced) and _server_spans(traced)[0].ended)
    span = _server_spans(traced)[0]
    assert span.rpc == _METHOD_BIDI
    assert span.error is not None
    assert span.error[0] == "RuntimeError"


def test_server_error_recorded_on_both_sides(channel, traced, consumer_agent):
    with pytest.raises(grpc.RpcError) as excinfo:
        channel.unary_unary(_METHOD_UNARY)(b"boom", timeout=10)
    assert excinfo.value.code() == grpc.StatusCode.UNKNOWN

    ev = traced.single(_METHOD_UNARY, SERVICE_TYPE_GRPC)
    assert ev.ended
    assert ev.error is not None
    assert ev.ann(ANNOTATION_GRPC_CLIENT_STATUS) == ["UNKNOWN"]

    _wait_until(lambda: _server_spans(traced) and _server_spans(traced)[0].ended)
    span = _server_spans(traced)[0]
    assert span.error is not None
    assert span.error[0] == "RuntimeError"


def test_abort_is_control_flow_not_server_error(channel, traced,
                                                consumer_agent):
    """``context.abort(NOT_FOUND)`` is the idiomatic lookup miss — the server
    span must not count it as an error, while the client still sees the non-OK
    status on its span event."""
    with pytest.raises(grpc.RpcError) as excinfo:
        channel.unary_unary(_METHOD_UNARY)(b"abort", timeout=10)
    assert excinfo.value.code() == grpc.StatusCode.NOT_FOUND

    ev = traced.single(_METHOD_UNARY, SERVICE_TYPE_GRPC)
    assert ev.ended
    assert ev.ann(ANNOTATION_GRPC_CLIENT_STATUS) == ["NOT_FOUND"]

    _wait_until(lambda: _server_spans(traced) and _server_spans(traced)[0].ended)
    span = _server_spans(traced)[0]
    assert span.error is None


def test_no_current_span_passes_through(channel, recorder, consumer_agent):
    """Client side without an active span: the RPC must still work and emit
    no client event; the server still opens its own root span (a fresh
    transaction) with its handler event, so only those appear."""
    reply = channel.unary_unary(_METHOD_UNARY)(b"solo", timeout=10)
    assert reply == b"echo:solo"

    assert recorder.events_named(_METHOD_UNARY, SERVICE_TYPE_GRPC) == []
    # The server's own handler event is the only one recorded.
    assert [e.service_type for e in recorder.events] == [
        SERVICE_TYPE_PYTHON_METHOD]
    _wait_until(lambda: _server_spans(recorder)
                and _server_spans(recorder)[0].ended)
    span = _server_spans(recorder)[0]
    # No upstream Pinpoint metadata — a brand-new trace, headers not captured.
    assert span.headers is None
