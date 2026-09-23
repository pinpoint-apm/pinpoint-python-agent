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

"""In-memory recording stand-ins for the ``_native`` Span/SpanEvent/Agent.

The integration suite exercises the *real* instrumentation wrappers against
*real* client libraries talking to *real* servers (testcontainers). The one
seam that stays in-process is the native span sink: the native agent has no
in-memory exporter (finished spans drain straight into the gRPC sender), so
these recorders implement the exact surface ``pinpoint.tracer.Span`` /
``SpanEvent`` call on their native handles and keep everything inspectable.

They are consumed through the **real** Python wrappers::

    rec = Recorder()
    span = RecordingNativeSpan(rec).span()
    token = pinpoint.context.set_current_span(span)
    ... run instrumented client code ...
    rec.events  # finished span events, in creation order

so the full wrapper pipeline — annotation buffering, one-shot end latches,
the Python-side event stack — runs unmodified. Span events are pure Python
and reach native only as the completed records batched into
``end_span_with_data``; the shared tests/conftest.py bridge replays each
record here as soon as its event ends (via ``new_span_event`` + setters +
``end_span_event``), so assertions can run before the span itself ends.
"""

from __future__ import annotations

import itertools
import threading
from dataclasses import dataclass, field
from typing import Any

from pinpoint.tracer import Span

# Annotation buffer tags — must mirror pinpoint/tracer.py (_ANN_*).
_TAG_NAMES = {0: "int", 1: "str", 2: "strstr", 3: "long", 6: "proxy"}

# Constant identity pairs of every outbound injection — what
# pinpoint.agent.Agent builds from Config in production (Span.inject_base).
IT_INJECT_BASE = (
    ("Pinpoint-pAppName", "it-test"),
    ("Pinpoint-pAppType", "1700"),
    ("Pinpoint-pAppNamespace", ""),
)
_TAG_SQL = 4  # (tag, sql, args) — decoded into RecordedEvent.sql, not annotations
_TAG_ERROR = 5  # (tag, message) | (tag, name, message[, cs]) — decoded into .error

_ids = itertools.count(1)


def _decode_annotations(annotations) -> list[tuple]:
    """Normalize the buffered ``(tag, key, *values)`` tuples the tracer flushes
    at end() into ``("str"|"int"|"strstr"|"long", key, *values)`` — the same
    shape the unit-test fakes use, so assertions read identically."""
    out: list[tuple] = []
    for item in annotations or ():
        tag, key = item[0], item[1]
        out.append((_TAG_NAMES.get(tag, str(tag)), key) + tuple(item[2:]))
    return out


@dataclass
class RecordedEvent:
    """One span event, replayed from its completed record (see module doc)."""
    operation: str
    service_type: int
    span: RecordingNativeSpan
    destination: str = ""
    endpoint: str = ""
    annotations: list[tuple] = field(default_factory=list)
    sql: list[tuple[str, str]] = field(default_factory=list)
    error: tuple | None = None
    next_span_id: int = 0
    ended: bool = False

    # -- assertion sugar ----------------------------------------------------
    def ann(self, key: int) -> list[Any]:
        """All annotation values recorded under ``key`` (any tag)."""
        return [a[2] for a in self.annotations if a[1] == key]


@dataclass
class RecordedSpan:
    """Root/async span bookkeeping, finalized by ``end_span_with_data`` /
    ``end_span``."""
    operation: str
    rpc: str = ""
    headers: dict[str, str] | None = None
    trace_id: str = ""
    service_type: int = 0
    remote_address: str = ""
    endpoint: str = ""
    acceptor_host: str = ""
    status_code: int = 0
    url_pattern: str = ""
    method: str = ""
    annotations: list[tuple] = field(default_factory=list)
    error: tuple | None = None
    logging: bool = False
    ended: bool = False

    def ann(self, key: int) -> list[Any]:
        return [a[2] for a in self.annotations if a[1] == key]


class Recorder:
    """Shared sink. ``events`` collects every span event across all spans in
    creation order; ``spans`` collects root/async spans in creation order."""

    def __init__(self):
        self.events: list[RecordedEvent] = []
        self.spans: list[RecordedSpan] = []
        self._lock = threading.Lock()

    # -- assertion sugar ----------------------------------------------------
    def events_named(self, operation: str,
                     service_type: int = 0) -> list[RecordedEvent]:
        """Events with this operation name, optionally narrowed by service
        type — a gRPC client event and the server handler event it reaches
        both carry the RPC method as their name."""
        return [e for e in self.events
                if e.operation == operation
                and (not service_type or e.service_type == service_type)]

    def single(self, operation: str, service_type: int = 0) -> RecordedEvent:
        matches = self.events_named(operation, service_type)
        assert len(matches) == 1, (
            f"expected exactly one event {operation!r}"
            f"{f' of service type {service_type}' if service_type else ''}, "
            f"got {[(e.operation, e.service_type) for e in self.events]!r}")
        return matches[0]


class _Annotations:
    """`get_annotations()` surface — used by the http-server helper stubs."""

    def __init__(self, sink: list[tuple]):
        self._sink = sink

    def append_int(self, key, value):
        self._sink.append(("int", key, value))

    def append_long(self, key, value):
        self._sink.append(("long", key, value))

    def append_string(self, key, value):
        self._sink.append(("str", key, value))

    def append_string_string(self, key, v1, v2):
        self._sink.append(("strstr", key, v1, v2))


class RecordingNativeSpanEvent:
    """Implements the native SpanEvent surface ``tracer.SpanEvent`` touches."""

    def __init__(self, record: RecordedEvent, recorder: Recorder):
        self._record = record
        self._recorder = recorder

    # live-recorded calls (bypass the wrapper's buffered fields)
    def set_error(self, name, message="", _frames=None):
        self._record.error = (str(name), str(message))

    def set_sql_query(self, sql, args=""):
        self._record.sql.append((sql, args))

    def get_annotations(self):
        return _Annotations(self._record.annotations)

    # setters the http-helper stubs may drive on the native handle
    def set_end_point(self, ep):
        self._record.endpoint = str(ep)
        return self

    def set_destination(self, dest):
        self._record.destination = str(dest)
        return self

    def set_next_span_id(self, next_span_id):
        self._record.next_span_id = int(next_span_id)
        return self


class RecordingNativeSpan:
    """Implements the native Span surface ``tracer.Span`` + the http-helper
    stubs touch. Root spans register a :class:`RecordedSpan` on the recorder."""

    def __init__(self, recorder: Recorder, operation: str = "it-root",
                 rpc: str = "", headers: dict[str, str] | None = None):
        self._recorder = recorder
        self._span_id = next(_ids)
        self._trace_id = f"it-agent^100^{self._span_id}"
        self._open_events: list[RecordedEvent] = []
        self.record = RecordedSpan(operation=operation, rpc=rpc,
                                   headers=headers, trace_id=self._trace_id)
        with recorder._lock:
            recorder.spans.append(self.record)

    def span(self, **kwargs) -> Span:
        """The real wrapper over this native, carrying the identity the real
        binding hands back from the creation call."""
        return Span(self, trace_id=self._trace_id, span_id=self._span_id,
                    **kwargs)

    # -- span events ----------------------------------------------------------
    # The wrapper never calls new_span_event/end_span_event itself (events are
    # Python-side, flushed as records at span end); these are the replay
    # surface the tests/conftest.py bridge drives per finished record.
    def new_span_event(self, operation, service_type=0):
        record = RecordedEvent(operation=operation,
                               service_type=int(service_type), span=self)
        with self._recorder._lock:
            self._recorder.events.append(record)
            self._open_events.append(record)
        return RecordingNativeSpanEvent(record, self._recorder)

    def end_span_event(self):
        with self._recorder._lock:
            self._open_events.pop().ended = True

    def new_async_span(self, operation, _async_id=0, _async_sequence=0):
        return RecordingNativeSpan(self._recorder, operation=operation)

    # -- live setters (http server helper stubs, error path) ------------------
    def set_error(self, name, message=""):
        self.record.error = (str(name), str(message))

    def mark_error(self, name, message):
        verdicts = getattr(self, "error_verdicts", None)
        if verdicts is None:
            verdicts = self.error_verdicts = []
        verdicts.append((str(name), str(message)))

    def set_remote_address(self, addr):
        self.record.remote_address = str(addr)

    def set_end_point(self, ep):
        self.record.endpoint = str(ep)

    def set_acceptor_host(self, host):
        self.record.acceptor_host = str(host)

    def set_status_code(self, code):
        self.record.status_code = int(code)

    def set_url_stat(self, url_pattern, method, status_code):
        self.record.url_pattern = str(url_pattern)
        self.record.method = str(method)
        self.record.status_code = int(status_code)

    def get_annotations(self):
        return _Annotations(self.record.annotations)

    # -- finalize --------------------------------------------------------------
    def end_span(self, url_pattern="", method="", status_code=0,
                 error_verdicts=()):
        for name, message in error_verdicts:
            self.mark_error(name, message)
        if url_pattern:
            self.set_url_stat(url_pattern, method, status_code)
        self.record.ended = True

    def end_span_with_data(self, service_type, remote_addr, endpoint,
                           acceptor_host, status_code, url_pattern, method,
                           error_verdicts=(), annotations=(), span_events=(),
                           logging=False):
        # span_events is normally empty here: the tests/conftest.py bridge
        # replays each record eagerly at event end. Records that skipped the
        # bridge are replayed now, mirroring the native batch replay.
        for record in span_events or ():
            (_seq, _depth, _start, _end, ev_service_type, operation,
             destination, ev_endpoint, next_span_id, _async_id,
             ev_annotations) = record
            ev = self.new_span_event(operation, ev_service_type)
            if destination:
                ev.set_destination(destination)
            if ev_endpoint:
                ev.set_end_point(ev_endpoint)
            if next_span_id:
                ev.set_next_span_id(next_span_id)
            for item in ev_annotations or ():
                if item[0] == _TAG_SQL:
                    ev.set_sql_query(item[1], item[2])
                elif item[0] == _TAG_ERROR:
                    ev.set_error(*item[1:])
                else:
                    ev._record.annotations.extend(_decode_annotations([item]))
            self.end_span_event()
        for name, message in error_verdicts:
            self.mark_error(name, message)
        r = self.record
        r.service_type = int(service_type)
        if remote_addr:
            r.remote_address = remote_addr
        if endpoint:
            r.endpoint = endpoint
        if acceptor_host:
            r.acceptor_host = acceptor_host
        if status_code:
            r.status_code = int(status_code)
        if url_pattern:
            r.url_pattern = url_pattern
        if method:
            r.method = method
        for item in annotations or ():
            if item[0] == _TAG_ERROR:
                self.set_error(*item[1:])
            else:
                r.annotations.extend(_decode_annotations([item]))
        r.logging = bool(logging)
        r.ended = True


class RecordingAgent:
    """Minimal ``pinpoint.agent.Agent`` stand-in for consumer-side wrappers
    (kafka / rabbitmq), which create *root* spans via
    ``get_agent().new_span(operation, rpc, headers=...)``.

    Install it as ``pinpoint.agent._instance`` (the ``consumer_agent``
    fixture does this) and every consumer invocation lands in ``recorder``.
    """

    enabled = True

    def __init__(self, recorder: Recorder, config=None):
        from types import SimpleNamespace

        self.recorder = recorder
        self.config = config or SimpleNamespace()

    def new_span(self, operation: str, rpc_point: str, headers=None, method=""):
        recorded_headers = None
        if headers is not None:
            try:
                recorded_headers = dict(headers)
            except (TypeError, ValueError):
                # Server instrumentations pass native HeaderReader adapters,
                # not mappings. Capture the propagation fields the integration
                # assertions care about without requiring the reader to expose
                # iteration.
                recorded_headers = {}
                for key in (
                    "Pinpoint-TraceID", "Pinpoint-SpanID",
                    "Pinpoint-pAppName", "Pinpoint-Sampled",
                ):
                    value = headers.get(key)
                    if value is not None:
                        recorded_headers[key] = value
        return RecordingNativeSpan(
            self.recorder, operation=operation, rpc=rpc_point,
            headers=recorded_headers,
        ).span(inject_base=IT_INJECT_BASE)
