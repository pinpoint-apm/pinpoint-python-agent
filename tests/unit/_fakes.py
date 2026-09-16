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

"""Shared fake of the native layer (src/_native.cpp) for the unit tests.

One full-surface fake instead of a per-file copy. Tests that need extra
behaviour subclass these instead of redefining them.

Recorder event shapes (``recorder.events``):

    ("span_start", op, rpc)          span created
    ("span_end", op)                 span ended
    ("span_error", op, args)         Span.set_error
    ("event_start", span_op, op)     span event created
    ("event_end", span_op, op)       span event ended (LIFO)
    ("event_error", op, args)        SpanEvent.set_error

Annotation entry shapes (``annotations.entries``):

    ("int" | "long" | "str", key, value)
    ("strstr", key, (s1, s2))
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from pinpoint.propagator import (HEADER_SAMPLED, HEADER_SPAN_ID,
                                 HEADER_TRACE_ID)
from pinpoint.agent import UnSampledSpan
from pinpoint.tracer import Span


class Recorder:
    """Order-preserving log of span/event lifecycle tuples."""

    def __init__(self):
        self.events: List[tuple] = []
        self.last_async_native: Optional["FakeNativeSpan"] = None


class FakeAnnotation:
    def __init__(self):
        self.entries: List[Tuple] = []

    def append_int(self, k, v):
        self.entries.append(("int", k, v))

    def append_long(self, k, v):
        self.entries.append(("long", k, v))

    def append_string(self, k, v):
        self.entries.append(("str", k, v))

    def append_string_string(self, k, s1, s2):
        self.entries.append(("strstr", k, (s1, s2)))

    def append_long_iibbs(self, k, long_value, i1, i2, b1, b2, s):
        self.entries.append(("proxy", k, (long_value, i1, i2, b1, b2, s)))


class FakeNativeSpanEvent:
    def __init__(self, op, recorder):
        self.op = op
        self.recorder = recorder
        self.service_type = None
        self.operation_name = None
        self.destination = None
        self.endpoint = None
        self.next_span_id = None
        self.errors: List[tuple] = []
        self.sqls: List[tuple] = []
        self.annotations = FakeAnnotation()

    def set_service_type(self, st):
        self.service_type = st
        return self

    def set_operation_name(self, name):
        self.operation_name = name
        return self

    def set_destination(self, dest):
        self.destination = dest
        return self

    def set_end_point(self, ep):
        self.endpoint = ep
        return self

    def set_next_span_id(self, span_id):
        self.next_span_id = span_id
        return self

    def set_sql_query(self, sql, args=""):
        self.sqls.append((sql, args))
        return self

    def set_error(self, *args):
        self.errors.append(args)
        self.recorder.events.append(("event_error", self.op, args))
        return self

    def get_annotations(self):
        return self.annotations


class FakeNativeSpan:
    """Full-surface fake native span. Pass constructor args by keyword past
    ``op``."""

    def __init__(self, op="root", rpc="", recorder=None, headers=None,
                 sampled=True):
        self.op = op
        self.rpc = rpc
        self.recorder = recorder if recorder is not None else Recorder()
        self.headers = headers
        self.sampled = sampled
        self._events: List[FakeNativeSpanEvent] = []
        self.all_events: List[FakeNativeSpanEvent] = []
        self.errors: List[tuple] = []
        self.url_stats: List[tuple] = []
        self.status_code = 0
        self.end_called = False
        self.service_type = None
        self.remote_address = None
        self.endpoint = None
        self.acceptor_host = None
        self.logging = False
        self.annotations = FakeAnnotation()
        self.recorder.events.append(("span_start", op, rpc))

    # ---- propagation -------------------------------------------------------
    def inject_context_pairs(self):
        return (("Pinpoint-TraceID", "trace-id"), ("Pinpoint-SpanID", "1"))

    # ---- setters ----------------------------------------------------------
    def set_service_type(self, st):
        self.service_type = st

    def set_remote_address(self, addr):
        self.remote_address = addr

    def set_end_point(self, ep):
        self.endpoint = ep

    def set_acceptor_host(self, host):
        self.acceptor_host = host

    def set_status_code(self, code):
        self.status_code = code

    def set_url_stat(self, *args):
        self.url_stats.append(args)

    def set_error(self, *args):
        self.errors.append(args)
        self.recorder.events.append(("span_error", self.op, args))

    def mark_error(self, name, message):
        self.errors.append(("verdict", name, message))
        self.recorder.events.append(
            ("span_error_verdict", self.op, (name, message)))

    def get_annotations(self):
        return self.annotations

    # ---- events / children -------------------------------------------------
    def new_span_event(self, operation, service_type=0):
        ev = FakeNativeSpanEvent(operation, self.recorder)
        self._events.append(ev)
        self.all_events.append(ev)
        self.recorder.events.append(("event_start", self.op, operation))
        return ev

    def end_span_event(self):
        ev = self._events.pop()
        self.recorder.events.append(("event_end", self.op, ev.op))

    def new_async_span(self, operation, *_args):
        child = type(self)(f"async:{operation}", recorder=self.recorder)
        self.recorder.last_async_native = child
        return child

    # ---- finalizers (mirror src/_native.cpp) -------------------------------
    def end_span(self, url_pattern="", method="", status_code=0,
                 error_verdicts=()):
        # The unsampled path's finalize: status/url_stat only when cached.
        for name, message in error_verdicts:
            self.mark_error(name, message)
        if status_code != 0:
            self.set_status_code(status_code)
        if url_pattern:
            self.set_url_stat(url_pattern, method, status_code)
        self.end_called = True
        self.recorder.events.append(("span_end", self.op))

    def record_span_events(self, span_events):
        for record in span_events:
            replay_span_event(self, record)

    def end_span_with_data(self, service_type, remote_addr, endpoint,
                           acceptor_host, status_code, url_pattern, method,
                           error_verdicts, annotations, span_events, logging):
        # No defaults: the pybind def requires every one, so a wrapper that
        # stopped passing one has to fail here too.
        # Usually empty here: the conftest ``SpanEvent._finalize`` patch
        # replays each event eagerly. Covers records that skipped that patch.
        for record in span_events:
            replay_span_event(self, record)
        for name, message in error_verdicts:
            self.mark_error(name, message)
        replay_annotations(self, annotations)
        if service_type != 0:
            self.set_service_type(service_type)
        if remote_addr:
            self.set_remote_address(remote_addr)
        if endpoint:
            self.set_end_point(endpoint)
        if acceptor_host:
            self.set_acceptor_host(acceptor_host)
        if status_code != 0:
            self.set_status_code(status_code)
        if url_pattern:
            self.set_url_stat(url_pattern, method, status_code)
        self.logging = bool(logging)
        self.end_span()


class SqlFakeNativeSpanEvent(FakeNativeSpanEvent):
    """Delta over the base fake: the SQL instrumentation tests read
    ("sql", ...) / ("destination", ...) / ("end_point", ...) tuples off
    ``rec.events``, so those setters are mirrored into the recorder log."""

    def set_destination(self, dest):
        self.recorder.events.append(("destination", self.op, dest))
        return super().set_destination(dest)

    def set_end_point(self, ep):
        self.recorder.events.append(("end_point", self.op, ep))
        return super().set_end_point(ep)

    def set_sql_query(self, sql, args=""):
        self.recorder.events.append(("sql", self.op, sql, args))
        return super().set_sql_query(sql, args)


class SqlFakeNativeSpan(FakeNativeSpan):
    def new_span_event(self, operation, service_type=0):
        ev = SqlFakeNativeSpanEvent(operation, self.recorder)
        self._events.append(ev)
        self.all_events.append(ev)
        self.recorder.events.append(("event_start", self.op, operation))
        return ev


def fake_inject_items(span):
    """Deterministic ``propagator.inject_items`` over fake native spans."""
    if span is None or getattr(span, "_native", None) is None:
        return ()
    try:
        return span._native.inject_context_pairs()
    except Exception:
        return ()


def stub_inject(monkeypatch, *modules):
    """Route ``propagator.inject_items`` — and the name each instrumentation
    ``module`` imported by value — through :func:`fake_inject_items`."""
    from pinpoint import propagator

    for module in (propagator, *modules):
        monkeypatch.setattr(module, "inject_items", fake_inject_items)


class UnsampledAgent:
    """Agent stand-in whose every root span is the real ``UnSampledSpan`` over
    ``native`` — drives a transport's unsampled path."""

    enabled = True

    def __init__(self, native, collect_url_stat=True):
        self._native = native
        self._collect_url_stat = collect_url_stat

    def new_span(self, operation, rpc_point, headers=None, method=""):
        return UnSampledSpan(self._native, collect_url_stat=self._collect_url_stat)


class FakeAgent:
    """Doubles as its own recorder: spans it creates log into ``.events``."""

    def __init__(self, enabled=True):
        self.enabled = enabled
        self.events: List[tuple] = []
        self.native_spans: List[FakeNativeSpan] = []
        self.last_native: Optional[FakeNativeSpan] = None
        self.last_async_native: Optional[FakeNativeSpan] = None
        self.last_method = ""

    def new_span(self, operation, rpc_point, headers=None, method=""):
        native = FakeNativeSpan(operation, rpc_point, recorder=self,
                                headers=headers)
        # Kept so transport tests can assert the HTTP method reaches span
        # creation — the native Http.Server.ExcludeMethod filter is skipped
        # for an empty one.
        self.last_method = method
        self.last_native = native
        self.native_spans.append(native)
        # The real binding returns the identity from the creation call.
        return Span(native, trace_id="trace-id", span_id=1)


# ---------------------------------------------------------------------------
# Replay of Python-buffered annotation / span-event records onto a fake —
# the finalize logic src/_native.cpp runs on the real native span.
# hasattr-guarded so minimal local fakes can opt out of parts of the surface.
# ---------------------------------------------------------------------------

def replay_annotations(native_obj, items):
    """Mirror src/_native.cpp's ``marshal_annotations`` + ``apply_marshalled``:
    replay the Python-buffered ``(tag, key, *values)`` batch. Tags match
    tracer.py's ``_ANN_*``."""
    if not items:
        return
    ann = (native_obj.get_annotations()
           if hasattr(native_obj, "get_annotations") else None)
    for item in items:
        tag = item[0]
        if tag == 4:
            # _ANN_SQL -> SetSqlQuery(sql, binds): (tag, sql, args), no key.
            if hasattr(native_obj, "set_sql_query"):
                native_obj.set_sql_query(item[1], item[2])
            continue
        if tag == 5:
            # _ANN_ERROR -> SetError: (tag, message) or
            # (tag, name, message[, frames]), no key.
            if hasattr(native_obj, "set_error"):
                native_obj.set_error(*item[1:])
            continue
        if ann is None:
            continue
        key = item[1]
        if tag == 0:
            ann.append_int(key, item[2])
        elif tag == 1:
            ann.append_string(key, item[2])
        elif tag == 2 and hasattr(ann, "append_string_string"):
            ann.append_string_string(key, item[2], item[3])
        elif tag == 3 and hasattr(ann, "append_long"):
            ann.append_long(key, item[2])
        elif tag == 6 and hasattr(ann, "append_long_iibbs"):
            # _ANN_LONG_IIBBS, the proxy-header payload: Span only, and the
            # binding compiles it out of the SpanEvent instantiation.
            ann.append_long_iibbs(key, *item[2:])


# Record tuple layout — see SpanEvent._finalize in tracer.py. Declared here,
# next to the replay that decodes it, so a layout change is one edit.
_REC_SEQUENCE = 0
_REC_DEPTH = 1
_REC_START = 2
_REC_END = 3
_REC_SERVICE_TYPE = 4
_REC_OPERATION = 5
_REC_DESTINATION = 6
_REC_END_POINT = 7
_REC_NEXT_SPAN_ID = 8
_REC_ASYNC_ID = 9
_REC_ANNOTATIONS = 10


def replay_span_event(native_span, record):
    """Mirror ``replay_marshalled_events`` in src/_native.cpp: create the
    event on the fake native span, apply the buffered fields and annotations,
    then finalize it. Like the binding, one failing event is dropped without
    raising into ``Span.end()``."""
    (_seq, _depth, _start_ms, _end_ms, service_type, operation,
     destination, endpoint, next_span_id, _async_id, annotations) = record
    try:
        ev = native_span.new_span_event(operation, service_type)
        if ev is None:
            return
        replay_annotations(ev, annotations)
        if next_span_id and hasattr(ev, "set_next_span_id"):
            ev.set_next_span_id(next_span_id)
        if service_type != 0 and hasattr(ev, "set_service_type"):
            ev.set_service_type(service_type)
        if operation and hasattr(ev, "set_operation_name"):
            ev.set_operation_name(operation)
        if destination and hasattr(ev, "set_destination"):
            ev.set_destination(destination)
        if endpoint and hasattr(ev, "set_end_point"):
            ev.set_end_point(endpoint)
        if hasattr(native_span, "end_span_event"):
            native_span.end_span_event()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Tracer-layer stubs shared by the HTTP client wrapper tests (httpx /
# requests / urllib3).
#
# These fake ``pinpoint.tracer.Span``/``SpanEvent`` rather than the native
# layer above: those wrappers are pinned against the Span contract they call
# (new_span_event / inject_context_items / .sampled), not against a real
# span's record buffering.
# ---------------------------------------------------------------------------


class ClientEvent:
    def __init__(self):
        self.ended = False
        self.errors = []

    def set_error(self, *args):
        self.errors.append(args)
        return self

    def end(self):
        self.ended = True


class ClientSpan:
    _native = None

    def __init__(self, sampled=True):
        self.sampled = sampled
        self.events = []

    def inject_context_items(self):
        # Delegates to the fake natives below — the wrapper builds these
        # pairs itself in production (Span.inject_context_items).
        native = self._native
        return () if native is None else native.inject_context_pairs()

    def new_span_event(self, *_args, **_kwargs):
        event = ClientEvent()
        self.events.append(event)
        return event


class UnsampledNative:
    """Native behavior on an unsampled span: context injection returns only
    the ``Pinpoint-Sampled: s0`` marker, so a downstream server
    short-circuits its own sampling decision."""

    def inject_context_pairs(self):
        return ((HEADER_SAMPLED, "s0"),)


class UnsampledClientSpan(ClientSpan):
    def __init__(self):
        super().__init__(sampled=False)
        self._native = UnsampledNative()


class _SampledNative:
    """Injects a realistic set of ``Pinpoint-*`` trace headers, as the native
    context writer does for a sampled span."""

    def __init__(self, trace_id="app^1700000000000^1", span_id="42"):
        self.trace_id = trace_id
        self.span_id = span_id

    def inject_context_pairs(self):
        return (
            (HEADER_TRACE_ID, self.trace_id),
            (HEADER_SPAN_ID, self.span_id),
            (HEADER_SAMPLED, "s1"),
        )


class SampledClientSpan(ClientSpan):
    def __init__(self, trace_id="app^1700000000000^1", span_id="42"):
        super().__init__(sampled=True)
        self._native = _SampledNative(trace_id, span_id)


class ClientRequest:
    """Shaped after an HTTPX Request — the richest of the clients. Its pickle
    hooks deliberately detach the stream, so a wrapper that copied the request
    with plain ``copy.copy`` fails here whichever client is under test."""

    url = "http://example.test/items/1"
    method = "GET"

    def __init__(self):
        self.headers = {}
        self.stream = object()
        self.extensions = {"timeout": object()}

    def __getstate__(self):
        return {
            name: value
            for name, value in self.__dict__.items()
            if name not in ("extensions", "stream")
        }

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.extensions = {}
        self.stream = object()


class ClientResponse:
    """``status_code`` is the requests/httpx attribute (urllib3 uses
    ``status`` — see the urllib3 stub in test_http_client_dedupe.py)."""

    status_code = 200
    headers = {}


def pinpoint_keys(headers):
    return {k for k in headers if str(k).startswith("Pinpoint-")}


# ---------------------------------------------------------------------------
# Request-envelope builders shared by the HTTP transport tests.
#
# ``trace_id`` is explicit rather than a default key: whether a request
# arrives with an inbound Pinpoint context decides which branch of the
# transport runs, so each caller states it.
# ---------------------------------------------------------------------------


def wsgi_environ(path="/items/42", method="GET", trace_id=None, **overrides):
    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "HTTP_HOST": "example.test",
        "SERVER_NAME": "example.test",
        "wsgi.url_scheme": "http",
        "REMOTE_ADDR": "10.0.0.1",
        "HTTP_X_TRACE": "abc",
    }
    if trace_id:
        environ["HTTP_PINPOINT_TRACEID"] = trace_id
    environ.update(overrides)
    return environ


def http_scope(path="/items/42", method="GET", trace_id=None, **overrides):
    headers = [(b"host", b"example.test"), (b"x-trace", b"abc")]
    if trace_id:
        headers.append((b"pinpoint-traceid", trace_id.encode()))
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"q=1",
        "headers": headers,
        "client": ("10.0.0.1", 5555),
        "server": ("example.test", 80),
    }
    scope.update(overrides)
    return scope
