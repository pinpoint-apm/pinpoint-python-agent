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

"""HTTP helper tests."""

from __future__ import annotations

from types import SimpleNamespace
from typing import List, Tuple

import pytest

from _fakes import FakeAnnotation as _FakeAnnotation
from pinpoint import http_helper
from pinpoint.http_helper import (
    ASGIHeadersReader,
    EnvironHeaderReader,
    HeadersReader,
    MultiDictHeaderReader,
    get_remote_addr,
    has_pinpoint_environ,
    has_pinpoint_mapping,
    has_pinpoint_pairs,
    parse_cookie_header,
    trace_http_client_request,
    trace_http_client_response,
    trace_http_server_request,
    trace_http_server_response,
)
from pinpoint.annotation import (
    ANNOTATION_HTTP_COOKIE,
    ANNOTATION_HTTP_PROXY_HEADER,
    ANNOTATION_HTTP_REQUEST_HEADER,
    ANNOTATION_HTTP_RESPONSE_HEADER,
)
from pinpoint.service_type import SERVICE_TYPE_PYTHON_HTTP_CLIENT


@pytest.fixture(autouse=True)
def enable_header_recording(monkeypatch):
    """Give the direct ``_record_header`` helper paths an enabled config."""
    from pinpoint import agent as agent_mod

    cfg = SimpleNamespace(
        http_server_record_request_cookie=["session_id"],
        http_server_record_response_header=["Content-Type"],
        http_client_record_request_header=["User-Agent"],
        http_client_record_request_cookie=["session_id"],
        http_client_record_response_header=["Content-Type"],
    )
    monkeypatch.setattr(agent_mod, "_instance", SimpleNamespace(config=cfg))


# ---------------------------------------------------------------------------
# Fakes — mirror the public surface the helpers touch on Span / SpanEvent.
# These fake the *Python wrapper* layer (chainable annotate_* methods), which
# _fakes models only at the native layer, so they stay local; the annotation
# sink is the shared _fakes.FakeAnnotation.
# ---------------------------------------------------------------------------


class _FakeSpan:
    def __init__(self) -> None:
        self.remote_addr = None
        self.endpoint = None
        self.status_code = None
        self.url_stats: List[Tuple[str, str, int]] = []
        # (key, name, value) triples from Python-side header recording.
        self.header_annotations: List[Tuple[int, str, str]] = []
        # (key, long, int, int, byte, byte, str) from proxy-header recording.
        self.proxy_annotations: List[tuple] = []

    def set_remote_address(self, addr: str) -> None:
        self.remote_addr = addr

    def set_end_point(self, ep: str) -> None:
        self.endpoint = ep

    def set_status_code(self, code: int) -> None:
        self.status_code = code

    def set_url_stat(self, pattern: str, method: str, status: int) -> None:
        self.url_stats.append((pattern, method, status))

    def annotate_string_string(self, k: int, s1: str, s2: str) -> "_FakeSpan":
        self.header_annotations.append((k, s1, s2))
        return self

    def annotate_long_iibbs(self, k, l, i1, i2, b1, b2, s) -> "_FakeSpan":
        self.proxy_annotations.append((k, l, i1, i2, b1, b2, s))
        return self


class _FakeSpanEvent:
    def __init__(self) -> None:
        self._ended = False  # live: the client helpers guard on _ended
        self._anno = _FakeAnnotation()
        self.service_type = None
        self.destination = None
        self.endpoint = None
        self.header_annotations: List[Tuple[int, str, str]] = []

    def set_service_type(self, t: int) -> "_FakeSpanEvent":
        self.service_type = t
        return self

    def set_destination(self, d: str) -> "_FakeSpanEvent":
        self.destination = d
        return self

    def set_end_point(self, ep: str) -> "_FakeSpanEvent":
        self.endpoint = ep
        return self

    def annotate_int(self, k: int, v: int) -> "_FakeSpanEvent":
        self._anno.append_int(k, v)
        return self

    def annotate_string(self, k: int, v: str) -> "_FakeSpanEvent":
        self._anno.append_string(k, v)
        return self

    def annotate_string_string(self, k: int, s1: str, s2: str) -> "_FakeSpanEvent":
        self.header_annotations.append((k, s1, s2))
        return self


# ---------------------------------------------------------------------------
# HeadersReader
# ---------------------------------------------------------------------------


def test_headers_reader_get_is_case_insensitive():
    r = HeadersReader({"User-Agent": "curl/8.1", "X-Request-ID": "abc"})
    assert r.get("user-agent") == "curl/8.1"
    assert r.get("USER-AGENT") == "curl/8.1"
    assert r.get("x-request-id") == "abc"
    assert r.get("missing") is None


def test_headers_reader_iterates_all_entries():
    r = HeadersReader([("A", "1"), ("B", "2"), ("C", "3")])
    seen: List[Tuple[str, str]] = []
    r.for_each(lambda k, v: (seen.append((k, v)), True)[1])
    assert seen == [("A", "1"), ("B", "2"), ("C", "3")]


def test_headers_reader_for_each_stops_when_callback_returns_false():
    r = HeadersReader([("A", "1"), ("B", "2"), ("C", "3")])
    seen: List[Tuple[str, str]] = []

    def cb(k: str, v: str) -> bool:
        seen.append((k, v))
        return k != "B"  # stop after B

    r.for_each(cb)
    assert seen == [("A", "1"), ("B", "2")]


def test_asgi_headers_reader_decodes_byte_pairs_case_insensitively():
    r = ASGIHeadersReader([
        (b"host", b"example.test"),
        (b"X-Request-ID", b"abc"),
    ])
    assert r.get("HOST") == "example.test"
    assert r.get("x-request-id") == "abc"

    seen: List[Tuple[str, str]] = []
    r.for_each(lambda k, v: (seen.append((k, v)), True)[1])
    assert seen == [("host", "example.test"), ("X-Request-ID", "abc")]


@pytest.mark.parametrize("environ, expected", [
    ({"REQUEST_METHOD": "GET", "HTTP_PINPOINT_TRACEID": "T-1"}, True),
    ({"HTTP_HOST": "x", "HTTP_PINPOINT_SAMPLED": "s0"}, True),
    ({"REQUEST_METHOD": "GET", "HTTP_X_TRACE": "abc"}, False),
    ({}, False),
])
def test_has_pinpoint_environ(environ, expected):
    assert has_pinpoint_environ(environ) is expected


@pytest.mark.parametrize("headers, expected", [
    ({"host": "x", "pinpoint-traceid": "T-1"}, True),
    ({"Host": "x", "Pinpoint-SpanID": "5"}, True),   # mixed case keys
    ({"host": "x", "x-trace": "abc"}, False),
    ({}, False),
])
def test_has_pinpoint_mapping(headers, expected):
    assert has_pinpoint_mapping(headers) is expected


@pytest.mark.parametrize("pairs, expected", [
    ([("topic", b"orders"), ("pinpoint-traceid", b"T-1")], True),  # str keys
    ([(b"Pinpoint-SpanID", b"5")], True),                          # bytes keys
    ([("x-custom", "v"), ("Pinpoint-TraceID", "T-1")], True),      # mixed case
    ([("x-custom", b"v"), ("other", b"y")], False),
    ([], False),
])
def test_has_pinpoint_pairs(pairs, expected):
    assert has_pinpoint_pairs(pairs) is expected


def test_asgi_headers_reader_get_returns_last_value_for_duplicates():
    r = ASGIHeadersReader([
        (b"x-forwarded-for", b"1.1.1.1"),
        (b"x-forwarded-for", b"2.2.2.2"),
    ])
    assert r.get("x-forwarded-for") == "2.2.2.2"


def test_asgi_headers_reader_decodes_unmatched_values_only_when_iterated():
    decoded: List[str] = []

    class LazyValue:
        def __str__(self) -> str:
            decoded.append("x-lazy")
            return "late"

    r = ASGIHeadersReader([
        (b"host", b"example.test"),
        (b"x-lazy", LazyValue()),
    ])

    assert decoded == []
    assert r.get("HOST") == "example.test"
    assert decoded == []

    seen: List[Tuple[str, str]] = []
    r.for_each(lambda k, v: (seen.append((k, v)), True)[1])
    assert seen == [("host", "example.test"), ("x-lazy", "late")]
    assert decoded == ["x-lazy"]


def test_trace_http_server_request_records_from_asgi_reader(monkeypatch):
    from pinpoint import agent as agent_mod
    cfg = SimpleNamespace(http_server_record_request_header=["host"])
    monkeypatch.setattr(agent_mod, "_instance", SimpleNamespace(config=cfg))
    span = _FakeSpan()
    reader = ASGIHeadersReader([(b"host", b"example.test")])
    trace_http_server_request(
        span, "10.0.0.1:54321", "example.test", request_headers=reader,
    )
    # Extracted from the lazy reader interpreter-side, buffered as a
    # two-string annotation under the configured name.
    assert span.header_annotations == [
        (ANNOTATION_HTTP_REQUEST_HEADER, "host", "example.test")]


# ---------------------------------------------------------------------------
# get_remote_addr — port of HttpTracerUtil::getRemoteAddr's fallback tail
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "remote_addr, expected",
    [
        ("10.0.0.1:54321", "10.0.0.1"),
        ("[::1]:54321", "[::1]"),
        ("fe80::1", "fe80::1"),
        ("", ""),
    ],
)
def test_get_remote_addr_strips_port(remote_addr, expected):
    assert get_remote_addr(remote_addr) == expected


# ---------------------------------------------------------------------------
# Server-side helpers — everything buffers Python-side, proxy headers included.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "headers, remote_addr, expected",
    [
        # X-Forwarded-For wins; first entry, whitespace-trimmed.
        ({"X-Forwarded-For": "1.2.3.4, 10.0.0.1"}, "9.9.9.9:1", "1.2.3.4"),
        ({"x-forwarded-for": " 1.2.3.4 "}, "9.9.9.9:1", "1.2.3.4"),
        # Empty first entry falls through to the next source.
        ({"X-Forwarded-For": " , "}, "9.9.9.9:1", "9.9.9.9"),
        ({"X-Real-Ip": "5.6.7.8"}, "9.9.9.9:1", "5.6.7.8"),
        ({"X-Forwarded-For": "", "X-Real-Ip": "5.6.7.8"}, "9.9.9.9:1", "5.6.7.8"),
        # No proxy headers: socket address, port-stripped.
        ({"User-Agent": "curl/8.1"}, "9.9.9.9:1234", "9.9.9.9"),
        ({"User-Agent": "curl/8.1"}, "[::1]:8080", "[::1]"),
    ],
)
def test_trace_http_server_request_resolves_remote_addr(
    headers, remote_addr, expected,
):
    span = _FakeSpan()
    trace_http_server_request(span, remote_addr, "example.test", headers)
    assert span.remote_addr == expected
    assert span.endpoint == "example.test"


def test_trace_http_server_request_skips_native_when_recording_off():
    # Request-header recording unconfigured (the default): the helper makes no
    # native call at all — nothing delegated, nothing recorded.
    span = _FakeSpan()
    trace_http_server_request(
        span, "10.0.0.1:54321", "example.test",
        request_headers={"User-Agent": "curl/8.1"},
    )
    assert span.header_annotations == []


def test_trace_http_server_request_records_cookies_when_configured():
    # The autouse config enables http_server_record_request_cookie for
    # "session_id" only — "token" is filtered by the allow-list.
    span = _FakeSpan()
    trace_http_server_request(
        span, "127.0.0.1", "example.test",
        request_headers={"Cookie": "session_id=abc; token=xyz"},
        cookie_reader={"session_id": "abc", "token": "xyz"},
    )
    assert span.header_annotations == [
        (ANNOTATION_HTTP_COOKIE, "session_id", "abc")]


def test_trace_http_server_request_with_no_headers_sets_endpoint_only():
    """When no request reader is supplied the wrapper still stamps
    remote/endpoint so the span isn't blank."""
    span = _FakeSpan()
    trace_http_server_request(span, "10.0.0.1", "example.test", None)
    assert span.remote_addr == "10.0.0.1"
    assert span.endpoint == "example.test"


@pytest.mark.parametrize(
    "header, value, expected",
    [
        # Apache: t is epoch µs -> ms, D/i/b ints, code 3.
        ("Pinpoint-ProxyApache", "t=1234567890 D=100 i=5 b=10",
         (1234567, 3, 100, 5, 10, "")),
        # Nginx: exact sec.mmm -> ms; D sec.mmm -> microseconds.
        ("Pinpoint-ProxyNginx", "t=1234567890.500 D=0.250",
         (1234567890500, 2, 250000, -1, -1, "")),
        # App: t is epoch ms as-is, app name, code 1.
        ("Pinpoint-ProxyApp", "t=1234567890123 app=front",
         (1234567890123, 1, -1, -1, -1, "front")),
    ],
)
def test_trace_http_server_request_records_proxy_annotation(
    header, value, expected,
):
    """Pinpoint-Proxy* parsing runs interpreter-side: the payload is buffered
    as one composite annotation and remote/endpoint resolve like any other
    request."""
    span = _FakeSpan()
    trace_http_server_request(
        span, "10.0.0.1:54321", "example.test",
        request_headers={header: value, "X-Forwarded-For": "1.2.3.4"},
    )
    assert span.remote_addr == "1.2.3.4"
    assert span.endpoint == "example.test"
    assert span.proxy_annotations == [
        (ANNOTATION_HTTP_PROXY_HEADER,) + expected]


def test_proxy_records_all_valid_headers():
    """Every valid proxy hop is recorded in Apache/Nginx/App order."""
    span = _FakeSpan()
    trace_http_server_request(
        span, "10.0.0.1", "ep",
        request_headers={"Pinpoint-ProxyApp": "t=1 app=x",
                         "Pinpoint-ProxyApache": "t=2000 D=1"},
    )
    assert span.proxy_annotations == [
        (ANNOTATION_HTTP_PROXY_HEADER, 2, 3, 1, -1, -1, ""),
        (ANNOTATION_HTTP_PROXY_HEADER, 1, 1, -1, -1, -1, "x")]


def test_no_proxy_header_records_no_proxy_annotation():
    span = _FakeSpan()
    trace_http_server_request(
        span, "10.0.0.1", "ep", request_headers={"User-Agent": "curl/8.1"})
    assert span.proxy_annotations == []


def test_trace_http_server_response_buffers_status_and_url_stat():
    span = _FakeSpan()
    trace_http_server_response(
        span, "/items/{id}", "GET", 200,
        response_headers=[("Content-Type", "application/json")],
    )
    assert span.status_code == 200
    assert span.url_stats == [("/items/{id}", "GET", 200)]
    # The autouse config enables http_server_record_response_header.
    assert span.header_annotations == [
        (ANNOTATION_HTTP_RESPONSE_HEADER, "Content-Type", "application/json")]


def test_trace_http_server_response_skips_status_zero():
    span = _FakeSpan()
    trace_http_server_response(
        span, "/foo", "GET", 0, response_headers=[("Content-Type", "text/plain")],
    )
    # No status yet (early disconnect): nothing buffered — but the configured
    # allow-list entries of the response headers still land on the span.
    assert span.status_code is None
    assert span.url_stats == []
    assert span.header_annotations == [
        (ANNOTATION_HTTP_RESPONSE_HEADER, "Content-Type", "text/plain")]


# ---------------------------------------------------------------------------
# Client-side helpers
# ---------------------------------------------------------------------------


def test_trace_http_client_request_caches_metadata_and_records_headers():
    event = _FakeSpanEvent()
    trace_http_client_request(
        event,
        host="api.example.com",
        url="https://api.example.com/users/42",
        request_headers={"User-Agent": "demo/1.0"},
    )
    assert event.service_type == SERVICE_TYPE_PYTHON_HTTP_CLIENT
    assert event.endpoint == "api.example.com"
    assert event.destination == "api.example.com"
    assert event._anno.entries == [("str", 40, "https://api.example.com/users/42")]
    # The autouse config enables http_client_record_request_header.
    assert event.header_annotations == [
        (ANNOTATION_HTTP_REQUEST_HEADER, "User-Agent", "demo/1.0")]


def test_trace_http_client_request_with_no_headers_still_stamps_service_type():
    event = _FakeSpanEvent()
    trace_http_client_request(event, "host", "/u", None)
    assert event.service_type == SERVICE_TYPE_PYTHON_HTTP_CLIENT
    assert event.endpoint == "host"
    assert event.destination == "host"
    # ANNOTATION_HTTP_URL == 40.
    assert event._anno.entries == [("str", 40, "/u")]


def test_trace_http_client_response_records_status_and_headers():
    event = _FakeSpanEvent()
    trace_http_client_response(
        event, 503, response_headers=[("Content-Type", "text/plain")],
    )
    assert event._anno.entries == [("int", 46, 503)]
    assert event.header_annotations == [
        (ANNOTATION_HTTP_RESPONSE_HEADER, "Content-Type", "text/plain")]


def test_trace_http_client_response_with_no_status_records_headers():
    """Status not known yet — configured RecordResponseHeader entries still
    land on the span."""
    event = _FakeSpanEvent()
    trace_http_client_response(
        event, None, response_headers=[("Content-Type", "text/plain")],
    )
    assert event.header_annotations == [
        (ANNOTATION_HTTP_RESPONSE_HEADER, "Content-Type", "text/plain")]


def test_record_header_skips_when_config_empty(monkeypatch):
    from pinpoint import agent as agent_mod

    cfg = SimpleNamespace(
        http_server_record_response_header=[],
        http_client_record_request_header=[],
        http_client_record_request_cookie=[],
        http_client_record_response_header=[],
    )
    monkeypatch.setattr(agent_mod, "_instance", SimpleNamespace(config=cfg))

    event = _FakeSpanEvent()
    trace_http_client_response(
        event, None, response_headers=[("Content-Type", "text/plain")],
    )

    assert event.header_annotations == []


def test_record_header_dumps_all_headers_when_configured(monkeypatch):
    """A single case-insensitive HEADERS-ALL entry records every header under
    its wire-case name — the recorder's dump-all mode."""
    from pinpoint import agent as agent_mod

    cfg = SimpleNamespace(http_client_record_response_header=["headers-all"])
    monkeypatch.setattr(agent_mod, "_instance", SimpleNamespace(config=cfg))

    event = _FakeSpanEvent()
    trace_http_client_response(
        event, 200,
        response_headers=[("Content-Type", "text/plain"), ("X-A", "1")],
    )

    assert event.header_annotations == [
        (ANNOTATION_HTTP_RESPONSE_HEADER, "Content-Type", "text/plain"),
        (ANNOTATION_HTTP_RESPONSE_HEADER, "X-A", "1"),
    ]


def test_config_configured_client_header_is_recorded(monkeypatch):
    # Exercises the Config fallback for targets without a native snapshot;
    # env-var configuration reaches production spans via the native snapshot
    # instead (the env vars are not mirrored into the Python Config).
    from pinpoint import agent as agent_mod
    from pinpoint.config import Config

    cfg = Config(
        application_name="env-header",
        http_client_record_request_header=["Authorization"],
    )
    monkeypatch.setattr(agent_mod, "_instance", SimpleNamespace(config=cfg))

    event = _FakeSpanEvent()
    trace_http_client_request(
        event,
        "api.example.com",
        "/users",
        request_headers={"Authorization": "Bearer token"},
    )

    assert event.header_annotations == [
        (ANNOTATION_HTTP_REQUEST_HEADER, "Authorization", "Bearer token")]


def test_helpers_are_no_op_for_none_span():
    # Should not raise — the helpers guard against missing native handles.
    trace_http_server_request(None, "addr", "endpoint", {})
    trace_http_server_response(None, "/", "GET", 200, [])
    trace_http_client_request(None, "host", "/", {})
    trace_http_client_response(None, 200, [])


# ---------------------------------------------------------------------------
# Dict-backed reader (HeadersReader factory)
# ---------------------------------------------------------------------------


def test_headers_reader_returns_dict_reader():
    r = HeadersReader({"A": "1"})
    assert isinstance(r, http_helper.DictHeaderReader)
    # An already-built reader passes straight through without re-wrapping.
    assert HeadersReader(r) is r


def test_headers_reader_skips_none_keys_and_values():
    r = HeadersReader([("A", "1"), (None, "x"), ("B", None), ("C", "3")])
    assert r.get("a") == "1"
    assert r.get("c") == "3"
    seen: List[Tuple[str, str]] = []
    r.for_each(lambda k, v: (seen.append((k, v)), True)[1])
    assert seen == [("A", "1"), ("C", "3")]


# ---------------------------------------------------------------------------
# Server cookie / response recording gates (Issue 2)
# ---------------------------------------------------------------------------


def _set_server_config(monkeypatch, **flags):
    from pinpoint import agent as agent_mod
    monkeypatch.setattr(
        agent_mod, "_instance", SimpleNamespace(config=SimpleNamespace(**flags)),
    )


def test_cookie_reader_gated_off_records_nothing(monkeypatch):
    _set_server_config(monkeypatch, http_server_record_request_cookie=[])
    span = _FakeSpan()
    trace_http_server_request(
        span, "127.0.0.1", "ex",
        request_headers={"Cookie": "a=b"}, cookie_reader={"a": "b"},
    )
    assert span.header_annotations == []


def test_cookie_recorded_when_recording_on(monkeypatch):
    _set_server_config(monkeypatch, http_server_record_request_cookie=["a"])
    span = _FakeSpan()
    trace_http_server_request(
        span, "127.0.0.1", "ex",
        request_headers={"Cookie": "a=b"}, cookie_reader={"a": "b"},
    )
    assert span.header_annotations == [(ANNOTATION_HTTP_COOKIE, "a", "b")]


def test_response_reader_gated_off_records_nothing(monkeypatch):
    _set_server_config(monkeypatch, http_server_record_response_header=[])
    span = _FakeSpan()
    trace_http_server_response(
        span, "/p", "GET", 200,
        response_headers=[("Content-Type", "text/plain")],
    )
    assert span.header_annotations == []


def test_response_header_recorded_when_recording_on(monkeypatch):
    _set_server_config(monkeypatch, http_server_record_response_header=["Content-Type"])
    span = _FakeSpan()
    # Wire header arrives lower-cased: the allow-list match is
    # case-insensitive and the annotation carries the configured name.
    trace_http_server_response(
        span, "/p", "GET", 200,
        response_headers=[("content-type", "text/plain")],
    )
    assert span.header_annotations == [
        (ANNOTATION_HTTP_RESPONSE_HEADER, "Content-Type", "text/plain")]


def test_span_snapshot_overrides_stale_python_header_config(monkeypatch):
    # The config file replaces kwargs. A stale Python HEADERS-ALL must not win
    # over the native span's resolved empty list (privacy-safe direction).
    _set_server_config(
        monkeypatch,
        http_server_record_request_header=["HEADERS-ALL"],
    )
    disabled = _FakeSpan()
    disabled._config_snapshot = (
        "file-app", 1000, "", 64, 5000,
        (), (), (), (), (), (),
    )
    trace_http_server_request(
        disabled, "127.0.0.1", "ex",
        request_headers={"Authorization": "secret"},
    )
    assert disabled.header_annotations == []

    # The inverse also holds: a file/hot-reload allow-list is honored even
    # when the initial Python Config has recording disabled.
    _set_server_config(
        monkeypatch,
        http_server_record_request_header=[],
    )
    enabled = _FakeSpan()
    enabled._config_snapshot = (
        "file-app", 1000, "", 64, 5000,
        ("X-Allowed",), (), (), (), (), (),
    )
    trace_http_server_request(
        enabled, "127.0.0.1", "ex",
        request_headers={"X-Allowed": "yes", "Authorization": "secret"},
    )
    assert enabled.header_annotations == [
        (ANNOTATION_HTTP_REQUEST_HEADER, "X-Allowed", "yes"),
    ]


def test_span_snapshot_drives_the_sql_bind_value_gate(monkeypatch):
    """The bind-value gate reads the span's resolved snapshot, not the kwargs.

    This is what lets a config file (and a hot reload) enable or disable
    bind-value capture: the native agent formats its own bind values off the
    same ``Sql.TraceBindValue``, so a Python gate that only saw ``init()``
    kwargs would either drop values the operator asked for or, worse, ship
    values they did not.
    """
    from pinpoint.instrumentations._util import sql_bind_values_enabled
    from pinpoint.http_helper import sql_trace_bind_values_enabled

    # Python Config says off; the resolved snapshot (index 12) says on.
    _set_server_config(monkeypatch, sql_trace_bind_values=False)
    enabled = _FakeSpan()
    enabled._config_snapshot = (
        "file-app", 1000, "", 64, 5000,
        (), (), (), (), (), (),
        1, True,
    )
    assert sql_trace_bind_values_enabled(enabled) is True

    # And the inverse: kwargs on, the file turned it off.
    _set_server_config(monkeypatch, sql_trace_bind_values=True)
    disabled = _FakeSpan()
    disabled._config_snapshot = (
        "file-app", 1000, "", 64, 5000,
        (), (), (), (), (), (),
        1, False,
    )
    assert sql_trace_bind_values_enabled(disabled) is False

    # A target without a snapshot (unsampled span, test double, direct helper
    # call) falls back to the Python Config.
    _set_server_config(monkeypatch, sql_trace_bind_values=True)
    assert sql_trace_bind_values_enabled(_FakeSpan()) is True
    _set_server_config(monkeypatch, sql_trace_bind_values=False)
    assert sql_trace_bind_values_enabled(_FakeSpan()) is False

    # Nothing resolves at all -> do not capture.
    monkeypatch.setattr("pinpoint.agent._instance", None)
    assert sql_trace_bind_values_enabled(_FakeSpan()) is False
    assert sql_bind_values_enabled() is False


def test_header_recording_cache_not_poisoned_by_concurrent_reinit(monkeypatch):
    """A re-init interleaved with a header check must not leave the new agent
    serving the old agent's flag.

    Reproduces the race deterministically: while agent1's entry is being
    computed (recording OFF), a *concurrent* ``shutdown()``+``init()`` swaps in
    agent2 (recording ON) and a header check repopulates the cache under agent2.
    The original agent1 computation then completes its store. The cache must
    still report agent2's live config, not agent1's stale entry.

    Before the fix, agent1's store landed in the dict now keyed to agent2, so
    the final assertion sees agent1's ``False`` served under agent2.
    """
    attr = "http_server_record_response_header"

    live = {"agent": None}
    monkeypatch.setattr(http_helper, "get_agent", lambda: live["agent"])

    reinit = {"done": False}

    class _Cfg:
        def __init__(self, value, on_read=None):
            self._value = value
            self._on_read = on_read

        @property
        def http_server_record_response_header(self):
            if self._on_read is not None:
                self._on_read()
            return self._value

    agent2 = SimpleNamespace(config=_Cfg(["Content-Type"]))

    def concurrent_reinit():
        # Stand-in for another thread running shutdown()+init() (-> agent2) and
        # servicing one header check under it, part-way through agent1's own
        # compute. Fires once so agent2's read doesn't recurse.
        if reinit["done"]:
            return
        reinit["done"] = True
        live["agent"] = agent2
        http_helper._header_recording_configured(attr)

    agent1 = SimpleNamespace(config=_Cfg([], on_read=concurrent_reinit))

    live["agent"] = agent1
    # agent1's real config: response-header recording OFF.
    assert http_helper._header_recording_configured(attr) is False

    # The live agent is now agent2 (recording ON) after the interleaving.
    assert http_helper._header_recording_configured(attr) is True


# ---------------------------------------------------------------------------
# EnvironHeaderReader / MultiDictHeaderReader (Issue 3 lazy readers)
# ---------------------------------------------------------------------------


def test_environ_header_reader_get_translates_keys():
    environ = {
        "HTTP_X_FORWARDED_FOR": "1.2.3.4",
        "HTTP_PINPOINT_TRACEID": "T-1",
        "CONTENT_TYPE": "application/json",
        "REQUEST_METHOD": "GET",  # a CGI var, not an HTTP header
    }
    r = EnvironHeaderReader(environ)
    assert r.get("X-Forwarded-For") == "1.2.3.4"
    assert r.get("x-forwarded-for") == "1.2.3.4"
    assert r.get("Pinpoint-TraceID") == "T-1"
    assert r.get("Content-Type") == "application/json"
    assert r.get("Request-Method") is None
    assert r.get("missing") is None


def test_environ_header_reader_for_each_yields_only_headers():
    environ = {
        "HTTP_HOST": "ex",
        "CONTENT_LENGTH": "10",
        "REQUEST_METHOD": "GET",
        "wsgi.input": object(),
    }
    seen: List[Tuple[str, str]] = []
    EnvironHeaderReader(environ).for_each(
        lambda k, v: (seen.append((k, v)), True)[1]
    )
    keys = {k for k, _ in seen}
    # WSGI environ keys are upper-cased (HTTP_HOST), so the reader hands back
    # the flattened, dash-separated header names.
    assert ("HOST", "ex") in seen
    assert ("CONTENT-LENGTH", "10") in seen
    assert "REQUEST_METHOD" not in keys and "wsgi.input" not in keys


def test_environ_header_reader_get_does_not_scan_whole_environ():
    class CountingDict(dict):
        items_calls = 0

        def items(self):
            type(self).items_calls += 1
            return super().items()

    environ = CountingDict({"HTTP_HOST": "ex", "HTTP_X_FORWARDED_FOR": "1.2.3.4"})
    r = EnvironHeaderReader(environ)
    assert r.get("Host") == "ex"
    assert r.get("X-Forwarded-For") == "1.2.3.4"
    # Lookups translate to a single dict.get — never a full-environ walk.
    assert CountingDict.items_calls == 0


def test_multidict_header_reader_get_and_for_each():
    class CIMultiDict(dict):
        def get(self, key, default=None):
            for k, v in self.items():
                if k.lower() == str(key).lower():
                    return v
            return default

    raw = CIMultiDict({"Host": "ex", "X-Forwarded-For": "1.2.3.4"})
    r = MultiDictHeaderReader(raw)
    assert r.get("host") == "ex"
    assert r.get("X-FORWARDED-FOR") == "1.2.3.4"
    assert r.get("missing") is None
    seen: List[Tuple[str, str]] = []
    r.for_each(lambda k, v: (seen.append((k, v)), True)[1])
    assert ("Host", "ex") in seen


def test_multidict_header_reader_contains_broken_container():
    class BrokenHeaders:
        def get(self, _key):
            raise RuntimeError("broken get")

        def items(self):
            raise RuntimeError("broken iteration")

    reader = MultiDictHeaderReader(BrokenHeaders())
    assert reader.get("Host") is None
    reader.for_each(lambda _key, _value: pytest.fail("must not be called"))


def test_header_readers_stop_when_callback_raises():
    reader = EnvironHeaderReader({"HTTP_A": "1", "HTTP_B": "2"})
    calls = []

    def broken_callback(key, value):
        calls.append((key, value))
        raise RuntimeError("consumer failed")

    reader.for_each(broken_callback)
    assert len(calls) == 1


def test_parse_cookie_header_skips_malformed_and_empty_names():
    assert parse_cookie_header(
        "session=abc; malformed; =empty; spaced = value ; token=x=y"
    ) == {
        "session": "abc",
        "spaced": "value",
        "token": "x=y",
    }


# ---------------------------------------------------------------------------
# ASGIHeadersReader adaptive decode (Issue 3)
# ---------------------------------------------------------------------------


def test_asgi_reader_switches_to_dict_after_threshold():
    decoded: List[str] = []

    class LazyValue:
        def __init__(self, s: str):
            self._s = s

        def __str__(self) -> str:
            decoded.append(self._s)
            return self._s

    r = ASGIHeadersReader([
        (b"a", LazyValue("1")),
        (b"b", LazyValue("2")),
        (b"c", LazyValue("3")),
    ])
    # First two probes stay on the raw reverse-scan path.
    assert r.get("a") == "1"
    assert r.get("b") == "2"
    # The third probe crosses the threshold and decodes the whole list once.
    assert r.get("c") == "3"
    assert set(decoded) == {"1", "2", "3"}
    # Further lookups are served from the O(1) dict without re-decoding.
    snapshot = list(decoded)
    assert r.get("a") == "1"
    assert decoded == snapshot


@pytest.mark.parametrize("header,value", [
    ("Pinpoint-ProxyApache", "D=100"),
    ("Pinpoint-ProxyApache", "t=0"),
    ("Pinpoint-ProxyApache", "t=-1000000"),
    ("Pinpoint-ProxyApache", "t=abc D=10"),
    ("Pinpoint-ProxyNginx", "t=123.4"),
    ("Pinpoint-ProxyNginx", "t=123.4567"),
    ("Pinpoint-ProxyNginx", "t=NaN"),
    ("Pinpoint-ProxyNginx", "t=1e3.000"),
    ("Pinpoint-ProxyNginx", "t=１２３.000"),
    ("Pinpoint-ProxyApp", "t=100 app=" + "x" * 31),
    ("Pinpoint-ProxyApp", "t=100 app=<script>"),
])
def test_proxy_rejects_invalid_timestamp_or_app(header, value):
    span = _FakeSpan()
    trace_http_server_request(span, "127.0.0.1", "ep", {header: value})
    assert span.proxy_annotations == []


def test_bad_proxy_does_not_hide_other_hops():
    span = _FakeSpan()
    trace_http_server_request(span, "127.0.0.1", "ep", {
        "Pinpoint-ProxyApache": "t=bad",
        "Pinpoint-ProxyNginx": "t=.123 D=0.123",
        "Pinpoint-ProxyApp": "t=123 app=valid.name-1",
    })
    assert span.proxy_annotations == [
        (ANNOTATION_HTTP_PROXY_HEADER, 123, 2, 123000, -1, -1, ""),
        (ANNOTATION_HTTP_PROXY_HEADER, 123, 1, -1, -1, -1, "valid.name-1"),
    ]


def test_proxy_apache_cuts_before_int64_parse():
    span = _FakeSpan()
    trace_http_server_request(span, "127.0.0.1", "ep", {
        "Pinpoint-ProxyApache": "t=9223372036854775807000 D=2147483648",
    })
    assert span.proxy_annotations == [
        (ANNOTATION_HTTP_PROXY_HEADER, 2 ** 63 - 1, 3, -1, -1, -1, ""),
    ]


def _user_proxy_span(names):
    span = _FakeSpan()
    span._config_snapshot = (
        "app", 1000, "", 64, 5000, (), (), (), (), (), (), 1, False, names,
    )
    return span


@pytest.mark.parametrize("timestamp,expected", [
    ("1504230492763", 1504230492763),
    ("1504230492763123", 1504230492763),
    ("1504230492.763", 1504230492763),
    ("0000000000001", 1),
    ("150423049276312", 150423049276312),
    ("9223372036854775807000", 2**63 - 1),
    # Native drops the trailing three bytes before validating the prefix.
    ("1504230492763bad", 1504230492763),
    ("1504230492763가", 1504230492763),
    # >=16 bytes takes the microsecond branch before looking for a dot.
    ("1504230492763.123", 0),
    ("", 0), ("150423049276", 0), ("0000000000000", 0),
    ("-1504230492763", 0), ("+1504230492763", 0),
    ("１５０４２３０４９２７６３", 0), ("1504230492.76", 0),
    ("1504230492.7631", 0), ("150423049.763", 0),
    ("1504230492e003", 0), ("1504230492.NaN", 0),
    ("9223372036854775808000", 0),
])
def test_user_proxy_timestamp_inference(timestamp, expected):
    span = _user_proxy_span(["X-Proxy"])
    trace_http_server_request(span, "ip", "ep", {"x-proxy": f"t={timestamp}"})
    assert span.proxy_annotations == (
        [(ANNOTATION_HTTP_PROXY_HEADER, expected, 4, -1, -1, -1, "X-Proxy")]
        if expected else [])


@pytest.mark.parametrize("duration,expected", [
    ("123", 123), ("0.123", 123000), (".123", 123000),
    ("2147483647", 2147483647), ("2147483648", -1),
    ("2147.483", 2147483000), ("2147.484", -1),
    ("", -1), ("0", -1), ("-1", -1), ("+1", -1), ("NaN", -1), ("1e3", -1),
    ("0.12", -1), ("0.1234", -1), ("１", -1), ("1.2.123", -1),
])
def test_user_proxy_duration(duration, expected):
    span = _user_proxy_span(["X-Proxy"])
    trace_http_server_request(span, "ip", "ep", {
        "X-Proxy": f"t=1504230492763 D={duration} i=80 b=10 app=ignored",
    })
    assert span.proxy_annotations == [
        (ANNOTATION_HTTP_PROXY_HEADER, 1504230492763, 4, expected, -1, -1, "X-Proxy"),
    ]


@pytest.mark.parametrize("name,app", [
    ("X-" + "a" * 40, "X-" + "a" * 30),
    ("X-" + "가" * 11, "X-" + "가" * 10),
    ("X" + "가" * 11, "X" + "가" * 10),
    ("X-" + "😀" * 8, "X-" + "😀" * 7),
])
def test_user_proxy_name_utf8_byte_cap(name, app):
    span = _user_proxy_span([name])
    trace_http_server_request(span, "ip", "ep", {name: "t=1504230492763"})
    assert span.proxy_annotations[0][-1] == app


def test_user_proxy_named_lazy_lookups_and_all_hops():
    class Reader(http_helper.HeaderReader):
        def __init__(self):
            self.lookups = []

        def get(self, name):
            self.lookups.append(name)
            return {
                "Pinpoint-ProxyApp": "t=1504230492763 app=builtin",
                "X-Bad": "t=bad", "X-Empty": "", "X-Missing": None,
                "X-One": "malformed t=bad t=1504230492763 D=9 D=10",
                "HEADERS-ALL": "t=1504230492763",
            }.get(name)

        def items(self):
            raise AssertionError("proxy recording must not enumerate headers")

    names = ["", "X-Bad", "X-Empty", "X-Missing", "X-One", "HEADERS-ALL", "X-One"]
    span = _user_proxy_span(names)
    reader = Reader()
    trace_http_server_request(span, "ip", "ep", reader)
    assert reader.lookups[-6:] == names[1:]
    assert "" not in reader.lookups
    assert span.proxy_annotations == [
        (ANNOTATION_HTTP_PROXY_HEADER, 1504230492763, 1, -1, -1, -1, "builtin"),
        (ANNOTATION_HTTP_PROXY_HEADER, 1504230492763, 4, 10, -1, -1, "X-One"),
        (ANNOTATION_HTTP_PROXY_HEADER, 1504230492763, 4, -1, -1, -1, "HEADERS-ALL"),
        (ANNOTATION_HTTP_PROXY_HEADER, 1504230492763, 4, 10, -1, -1, "X-One"),
    ]


def test_user_proxy_snapshot_wins_over_kwargs(monkeypatch):
    _set_server_config(monkeypatch, http_server_proxy_user_header_names=["X-Stale"])
    for names in ([], ["X-Resolved"]):
        span = _user_proxy_span(names)
        trace_http_server_request(span, "ip", "ep", {
            "X-Stale": "t=1504230492763", "X-Resolved": "t=1504230492763",
        })
        assert [a[-1] for a in span.proxy_annotations] == names
    # Compatibility fallback for a helper target with no native snapshot.
    span = _FakeSpan()
    trace_http_server_request(span, "ip", "ep", {"X-Stale": "t=1504230492763"})
    assert span.proxy_annotations[0][-1] == "X-Stale"


def test_proxy_header_enable_disables_all_proxy_parsers(monkeypatch):
    _set_server_config(monkeypatch, http_server_proxy_header_enable=False,
                       http_server_proxy_user_header_names=["X-Proxy"])
    span = _FakeSpan()
    trace_http_server_request(span, "ip", "ep", {
        "Pinpoint-ProxyApp": "t=1504230492763 app=builtin",
        "X-Proxy": "t=1504230492763",
    })
    assert span.proxy_annotations == []


@pytest.mark.parametrize("idle,busy,expected", [
    ("0", "100", (0, 100)),
    ("-1", "101", (-1, -1)),
    ("+1", "9" * 5000, (-1, -1)),
])
def test_apache_proxy_percent_range(idle, busy, expected):
    span = _FakeSpan()
    trace_http_server_request(span, "ip", "ep", {
        "Pinpoint-ProxyApache": f"t=2000 i={idle} b={busy}",
    })
    assert span.proxy_annotations[0][4:6] == expected


# ---------------------------------------------------------------------------
# Request parameters (server) and URL query string (client): both off by default.
# ---------------------------------------------------------------------------


def _set_config(monkeypatch, **cfg):
    from pinpoint import agent as agent_mod
    monkeypatch.setattr(agent_mod, "_instance",
                        SimpleNamespace(config=SimpleNamespace(**cfg)))


def test_client_url_query_stripped_by_default():
    event = _FakeSpanEvent()
    trace_http_client_request(event, "h", "https://h/p?token=secret&x=1", None)
    assert event._anno.entries == [("str", 40, "https://h/p")]


def test_client_url_query_kept_when_configured(monkeypatch):
    _set_config(monkeypatch, http_client_record_url_query=True)
    event = _FakeSpanEvent()
    trace_http_client_request(event, "h", "https://h/p?x=1", None)
    assert event._anno.entries == [("str", 40, "https://h/p?x=1")]


def test_server_request_param_not_recorded_by_default():
    span = _FakeSpan()
    span.param_annotations = []
    span.annotate_string = lambda k, v: span.param_annotations.append((k, v))
    trace_http_server_request(span, "1.2.3.4", "h", {}, query_string="a=1")
    assert span.param_annotations == []


def test_server_request_param_recorded_when_configured(monkeypatch):
    _set_config(monkeypatch, http_server_record_request_param=True)
    span = _FakeSpan()
    span.param_annotations = []
    span.annotate_string = lambda k, v: span.param_annotations.append((k, v))
    trace_http_server_request(span, "1.2.3.4", "h", {},
                              query_string="a=1&b=x%20y&empty=")
    assert span.param_annotations == [(41, "a=1&b=x y&empty=")]


def test_format_request_params_applies_java_limits():
    from pinpoint.http_helper import format_request_params
    out = format_request_params("k=" + "v" * 100)
    assert out == "k=" + "v" * 64 + "..."
    many = "&".join(f"k{i}=vvvvvvvvvv" for i in range(100))
    out = format_request_params(many)
    assert out.endswith("&...")
    assert len(out) <= 512 + len("&...")


# ---------------------------------------------------------------------------
# Real-IP header resolution follows the configured header list.
# ---------------------------------------------------------------------------


def _remote(monkeypatch, headers, remote="9.9.9.9:1", **cfg):
    if cfg:
        _set_config(monkeypatch, **cfg)
    span = _FakeSpan()
    trace_http_server_request(span, remote, "h", headers)
    return span.remote_addr


def test_real_ip_custom_header_order(monkeypatch):
    hdrs = {"X-Forwarded-For": "1.1.1.1", "CF-Connecting-IP": "2.2.2.2"}
    assert _remote(monkeypatch, hdrs,
                   http_server_real_ip_header=["CF-Connecting-IP"]) == "2.2.2.2"


def test_real_ip_empty_list_trusts_no_header(monkeypatch):
    assert _remote(monkeypatch, {"X-Forwarded-For": "1.1.1.1"},
                   http_server_real_ip_header=[]) == "9.9.9.9"


def test_real_ip_placeholder_value_is_skipped(monkeypatch):
    hdrs = {"X-Forwarded-For": "Unknown, 3.3.3.3", "X-Real-Ip": "4.4.4.4"}
    assert _remote(monkeypatch, hdrs,
                   http_server_real_ip_header=["X-Forwarded-For", "X-Real-Ip"],
                   http_server_real_ip_empty_value="unknown") == "4.4.4.4"


@pytest.mark.parametrize("value, expected", [
    ('for=1.2.3.4;proto=https, for=10.0.0.1', "1.2.3.4"),
    ('for="[2001:db8::1]:4711"', "[2001:db8::1]"),
    ('For=192.0.2.60:8080', "192.0.2.60"),
    ('proto=https', "9.9.9.9"),
])
def test_real_ip_forwarded_header(monkeypatch, value, expected):
    assert _remote(monkeypatch, {"Forwarded": value},
                   http_server_real_ip_header=["Forwarded"]) == expected


def _snapshot(url_query=False, request_param=False,
              real_ip=("X-Forwarded-For", "X-Real-Ip"), empty_value=""):
    """A resolved native snapshot tuple, as to_python_config builds it."""
    return (
        "file-app", 1000, "", 64, 5000,
        (), (), (), (), (), (),
        1, False, (), False,
        url_query, request_param, real_ip, empty_value,
    )


def test_span_snapshot_drives_the_request_param_gate(monkeypatch):
    """A config file (and a hot reload) reaches the Python-side recorder."""
    _set_config(monkeypatch, http_server_record_request_param=False)
    span = _FakeSpan()
    span.param_annotations = []
    span.annotate_string = lambda k, v: span.param_annotations.append((k, v))
    span._config_snapshot = _snapshot(request_param=True)
    trace_http_server_request(span, "1.2.3.4", "h", {}, query_string="a=1")
    assert span.param_annotations == [(41, "a=1")]

    # The inverse: kwargs on, the resolved config off.
    _set_config(monkeypatch, http_server_record_request_param=True)
    off = _FakeSpan()
    off.param_annotations = []
    off.annotate_string = lambda k, v: off.param_annotations.append((k, v))
    off._config_snapshot = _snapshot(request_param=False)
    trace_http_server_request(off, "1.2.3.4", "h", {}, query_string="a=1")
    assert off.param_annotations == []


def test_span_snapshot_drives_the_client_url_query_gate(monkeypatch):
    _set_config(monkeypatch, http_client_record_url_query=False)
    event = _FakeSpanEvent()
    event._span = SimpleNamespace(_config_snapshot=_snapshot(url_query=True))
    trace_http_client_request(event, "h", "https://h/p?x=1", None)
    assert event._anno.entries == [("str", 40, "https://h/p?x=1")]

    _set_config(monkeypatch, http_client_record_url_query=True)
    off = _FakeSpanEvent()
    off._span = SimpleNamespace(_config_snapshot=_snapshot(url_query=False))
    trace_http_client_request(off, "h", "https://h/p?x=1", None)
    assert off._anno.entries == [("str", 40, "https://h/p")]


def test_span_snapshot_drives_real_ip_resolution(monkeypatch):
    _set_config(monkeypatch, http_server_real_ip_header=["X-Forwarded-For"])
    span = _FakeSpan()
    span._config_snapshot = _snapshot(real_ip=("CF-Connecting-IP",),
                                      empty_value="unknown")
    trace_http_server_request(span, "9.9.9.9:1", "h", {
        "X-Forwarded-For": "1.1.1.1", "CF-Connecting-IP": "2.2.2.2"})
    assert span.remote_addr == "2.2.2.2"

    # The placeholder comes from the snapshot too, and an empty resolved list
    # is an explicit "trust no header" -- not a fall back to the XFF default.
    skipped = _FakeSpan()
    skipped._config_snapshot = _snapshot(real_ip=("CF-Connecting-IP",),
                                         empty_value="unknown")
    trace_http_server_request(skipped, "9.9.9.9:1", "h",
                              {"CF-Connecting-IP": "Unknown"})
    assert skipped.remote_addr == "9.9.9.9"

    none_trusted = _FakeSpan()
    none_trusted._config_snapshot = _snapshot(real_ip=())
    trace_http_server_request(none_trusted, "9.9.9.9:1", "h",
                              {"X-Forwarded-For": "1.1.1.1"})
    assert none_trusted.remote_addr == "9.9.9.9"


def test_pre_index_snapshot_falls_back_to_the_python_config(monkeypatch):
    """A shorter tuple (older binding, external test double) must not read as
    "everything off" -- the Python Config still decides."""
    _set_config(monkeypatch, http_server_record_request_param=True,
                http_server_real_ip_header=["CF-Connecting-IP"],
                http_server_real_ip_empty_value="")
    span = _FakeSpan()
    span.param_annotations = []
    span.annotate_string = lambda k, v: span.param_annotations.append((k, v))
    span._config_snapshot = ("file-app", 1000, "", 64, 5000,
                             (), (), (), (), (), (), 1)
    trace_http_server_request(span, "9.9.9.9:1", "h",
                              {"CF-Connecting-IP": "2.2.2.2"},
                              query_string="a=1")
    assert span.param_annotations == [(41, "a=1")]
    assert span.remote_addr == "2.2.2.2"


def test_real_ip_env_list_is_parsed(monkeypatch):
    from pinpoint.config import Config, apply_native_env_overrides
    monkeypatch.setenv("PINPOINT_PY_HTTP_SERVER_REAL_IP_HEADER", "Forwarded, X-Real-Ip")
    monkeypatch.setenv("PINPOINT_PY_HTTP_SERVER_REAL_IP_EMPTY_VALUE", "unknown")
    cfg = apply_native_env_overrides(Config())
    assert cfg.http_server_real_ip_header == ["Forwarded", "X-Real-Ip"]
    assert cfg.http_server_real_ip_empty_value == "unknown"
