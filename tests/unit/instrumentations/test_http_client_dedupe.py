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

"""HTTP client wrapper de-duplication tests: the requests-over-urllib3 nesting
suppression, plus urllib3's own ``urlopen`` header handling (positional slot,
pool defaults, HTTPHeaderDict). The ``_send_wrapper`` rules requests shares
with httpx are pinned once, for both, in test_http_client_send_wrapper.py."""

import pytest

from _fakes import (ClientRequest, ClientResponse, ClientSpan,
                    SampledClientSpan, UnsampledClientSpan, pinpoint_keys)

from pinpoint.context import reset_current_span, set_current_span
from pinpoint.instrumentations import requests as requests_instr
from pinpoint.instrumentations import urllib3 as urllib3_instr
from pinpoint.instrumentations._util import suppress_http_client_instrumentation
from pinpoint.propagator import HEADER_SAMPLED, HEADER_SPAN_ID, HEADER_TRACE_ID


class _Urllib3Response:
    status = 200
    headers = {}


class _Pool:
    scheme = "http"
    host = "example.test"
    port = 80


class _PoolWithDefaults:
    """A pool carrying default headers, mirroring
    ``HTTPConnectionPool(host, headers={...})``. urllib3 only applies these
    when the per-call ``headers`` is ``None``."""

    scheme = "http"
    host = "example.test"
    port = 80

    def __init__(self):
        self.headers = {"Authorization": "Bearer token"}


def test_requests_suppresses_nested_urllib3_span_event():
    span = ClientSpan()
    token = set_current_span(span)
    try:
        def _urllib3_send(*_args, **_kwargs):
            return _Urllib3Response()

        def _requests_send(*_args, **_kwargs):
            urllib3_instr._urlopen_wrapper(
                _urllib3_send,
                _Pool(),
                ("GET", "/items/1"),
                {"headers": {}},
            )
            return ClientResponse()

        response = requests_instr._send_wrapper(
            _requests_send, None, (ClientRequest(),), {},
        )

        assert isinstance(response, ClientResponse)
        assert len(span.events) == 1
        assert span.events[0].ended is True
    finally:
        reset_current_span(token)


def test_requests_suppressed_opens_no_event_and_injects_no_headers():
    """Regression: an elasticsearch client using ``RequestsHttpNode`` rides on
    ``requests`` and opens a ``suppress_http_client_instrumentation()`` scope so
    the underlying HTTP client emits no nested event and injects no trace
    headers into the ES request. ``Session.send`` must honor that scope exactly
    like httpx/urllib3 — otherwise it duplicates the event and pollutes the ES
    request with ``Pinpoint-*`` headers."""
    span = SampledClientSpan()
    request = ClientRequest()
    captured: dict = {}
    token = set_current_span(span)
    try:
        def _send(*send_args, **send_kwargs):
            sent = send_args[0] if send_args else send_kwargs["request"]
            captured["sent"] = sent
            captured["headers"] = dict(sent.headers)
            return ClientResponse()

        with suppress_http_client_instrumentation():
            response = requests_instr._send_wrapper(_send, None, (request,), {})

        assert isinstance(response, ClientResponse)
        # No span event opened under the suppression scope.
        assert span.events == []
        # The original request was forwarded unchanged — no private copy, no
        # injected trace headers on the request the ES client owns.
        assert captured["sent"] is request
        assert pinpoint_keys(captured["headers"]) == set()
        assert pinpoint_keys(request.headers) == set()
    finally:
        reset_current_span(token)


def test_requests_without_suppression_still_traces(monkeypatch):
    """Counterpart to the suppression regression: outside a suppression scope
    ``Session.send`` still opens/ends the event and injects trace headers into
    the private per-send request copy (existing behavior)."""
    span = SampledClientSpan()
    request = ClientRequest()
    monkeypatch.setattr(
        requests_instr, "trace_http_client_request", lambda *a, **k: None
    )
    captured: dict = {}
    token = set_current_span(span)
    try:
        def _send(*send_args, **send_kwargs):
            sent = send_args[0] if send_args else send_kwargs["request"]
            captured["headers"] = dict(sent.headers)
            return ClientResponse()

        response = requests_instr._send_wrapper(_send, None, (request,), {})

        assert isinstance(response, ClientResponse)
        assert len(span.events) == 1
        assert span.events[0].ended is True
        assert captured["headers"][HEADER_TRACE_ID] == "app^1700000000000^1"
        assert captured["headers"][HEADER_SPAN_ID] == "42"
    finally:
        reset_current_span(token)


def test_urllib3_direct_call_still_emits_span_event():
    span = ClientSpan()
    token = set_current_span(span)
    try:
        def _send(*_args, **_kwargs):
            return _Urllib3Response()

        response = urllib3_instr._urlopen_wrapper(
            _send,
            _Pool(),
            ("GET", "/items/1"),
            {"headers": {}},
        )

        assert isinstance(response, _Urllib3Response)
        assert len(span.events) == 1
        assert span.events[0].ended is True
    finally:
        reset_current_span(token)


def test_urllib3_unsampled_inject_writes_s0_marker():
    """Same as above for urllib3 — the marker is injected into the headers
    handed down to ``urlopen``. The wrapper works on a private copy, so the
    caller's original ``kwargs['headers']`` mapping is left untouched."""
    span = UnsampledClientSpan()
    kwargs: dict = {"headers": {}}
    captured: dict = {}
    token = set_current_span(span)
    try:
        def _send(*_args, **kw):
            captured["headers"] = kw.get("headers")
            return _Urllib3Response()

        urllib3_instr._urlopen_wrapper(
            _send, _Pool(), ("GET", "/items/1"), kwargs,
        )
        assert captured["headers"].get(HEADER_SAMPLED) == "s0"
        # Caller's original mapping is not polluted with trace headers.
        assert kwargs["headers"] == {}
    finally:
        reset_current_span(token)


def test_urllib3_positional_headers_no_collision():
    """``urlopen(method, url, body, headers)`` passes ``headers``
    positionally (index 3). The wrapper must inject into that positional slot
    instead of also setting ``kwargs['headers']`` — otherwise
    ``wrapped(*args, **kwargs)`` raises
    ``TypeError: got multiple values for 'headers'``."""
    span = UnsampledClientSpan()
    original_headers = {"x-custom": "v"}
    captured: dict = {}
    token = set_current_span(span)
    try:
        def _send(method, url, body=None, headers=None, **kwargs):
            # Real signature: a double-passed ``headers`` raises here.
            captured["headers"] = headers
            captured["kwargs"] = kwargs
            return _Urllib3Response()

        # method, url, body, headers — all positional.
        urllib3_instr._urlopen_wrapper(
            _send, _Pool(),
            ("GET", "/items/1", b"body", original_headers), {},
        )
        assert "headers" not in captured["kwargs"]
        assert captured["headers"]["x-custom"] == "v"
        assert captured["headers"].get(HEADER_SAMPLED) == "s0"
        # Caller's original mapping is left untouched.
        assert original_headers == {"x-custom": "v"}
    finally:
        reset_current_span(token)


def test_urllib3_no_headers_preserves_pool_defaults():
    """When ``urlopen`` is called with no per-call ``headers``, the wrapper
    must not suppress urllib3's ``headers is None -> self.headers`` fallback.
    The outbound call must carry the pool's default headers (e.g.
    ``Authorization``) plus the injected ``Pinpoint-*`` headers."""
    span = UnsampledClientSpan()
    pool = _PoolWithDefaults()
    captured: dict = {}
    token = set_current_span(span)
    try:
        def _send(*_args, **kw):
            captured["headers"] = kw.get("headers")
            return _Urllib3Response()

        # No ``headers`` positionally and none in kwargs.
        urllib3_instr._urlopen_wrapper(_send, pool, ("GET", "/items/1"), {})

        assert captured["headers"]["Authorization"] == "Bearer token"
        assert captured["headers"].get(HEADER_SAMPLED) == "s0"
        # Pool's own defaults must not be mutated with trace headers.
        assert pool.headers == {"Authorization": "Bearer token"}
    finally:
        reset_current_span(token)


def test_urllib3_positional_none_headers_preserves_pool_defaults():
    """``urlopen(method, url, body, None)`` passes ``headers=None``
    positionally. urllib3 falls back to ``self.headers`` in that case too, so
    the wrapper must seed from the pool defaults rather than an empty dict."""
    span = UnsampledClientSpan()
    pool = _PoolWithDefaults()
    captured: dict = {}
    token = set_current_span(span)
    try:
        def _send(method, url, body=None, headers=None, **kwargs):
            captured["headers"] = headers
            captured["kwargs"] = kwargs
            return _Urllib3Response()

        urllib3_instr._urlopen_wrapper(
            _send, pool, ("GET", "/items/1", b"body", None), {},
        )

        assert "headers" not in captured["kwargs"]
        assert captured["headers"]["Authorization"] == "Bearer token"
        assert captured["headers"].get(HEADER_SAMPLED) == "s0"
        assert pool.headers == {"Authorization": "Bearer token"}
    finally:
        reset_current_span(token)


def test_urllib3_explicit_headers_do_not_merge_pool_defaults():
    """A caller-supplied ``headers`` dict wins outright: urllib3 does not
    merge ``self.headers`` on top of it, and neither do we."""
    span = UnsampledClientSpan()
    pool = _PoolWithDefaults()
    captured: dict = {}
    token = set_current_span(span)
    try:
        def _send(*_args, **kw):
            captured["headers"] = kw.get("headers")
            return _Urllib3Response()

        urllib3_instr._urlopen_wrapper(
            _send, pool, ("GET", "/items/1"), {"headers": {"x-custom": "v"}},
        )

        assert captured["headers"]["x-custom"] == "v"
        # Pool defaults are NOT merged onto an explicit caller dict.
        assert "Authorization" not in captured["headers"]
        assert captured["headers"].get(HEADER_SAMPLED) == "s0"
    finally:
        reset_current_span(token)


def test_urllib3_unsampled_span_skips_annotation_but_opens_event(monkeypatch):
    """An unsampled span still opens the span event so the trace context
    written by ``inject`` reflects this RPC's depth/sequence, but the
    sampled-only annotation (``trace_http_client_request``, ``event.end``)
    is skipped."""
    span = ClientSpan(sampled=False)
    annotated: dict = {"called": False}

    def _trace(*_a, **_kw):
        annotated["called"] = True

    monkeypatch.setattr(urllib3_instr, "trace_http_client_request", _trace)
    token = set_current_span(span)
    try:
        def _send(*_args, **_kwargs):
            return _Urllib3Response()

        response = urllib3_instr._urlopen_wrapper(
            _send,
            _Pool(),
            ("GET", "/items/1"),
            {"headers": {}},
        )

        assert isinstance(response, _Urllib3Response)
        # Event opened (one) but neither annotated nor ended.
        assert len(span.events) == 1
        assert span.events[0].ended is False
        assert annotated["called"] is False
    finally:
        reset_current_span(token)


def test_urllib3_httpheaderdict_preserves_multivalue_headers():
    """A caller-supplied ``urllib3.HTTPHeaderDict`` carrying repeated fields
    (two ``Cookie`` / two ``X-Multi`` lines) must reach ``urlopen`` with those
    fields still separate. Seeding via ``dict(provided)`` would route through
    ``HTTPHeaderDict.__getitem__`` and comma-join them into a single line —
    wrong on the wire, and semantically broken for ``Cookie`` (whose delimiter
    is ``;``, not ``,``). The Pinpoint trace headers must still be injected via
    set-semantics, and the caller's own HTTPHeaderDict must be left untouched
    (the wrapper works on a private copy)."""
    HTTPHeaderDict = pytest.importorskip("urllib3").HTTPHeaderDict
    span = SampledClientSpan()
    provided = HTTPHeaderDict()
    provided.add("X-Multi", "a")
    provided.add("X-Multi", "b")
    provided.add("Cookie", "c1=1")
    provided.add("Cookie", "c2=2")
    captured: dict = {}
    token = set_current_span(span)
    try:
        def _send(*_args, **kw):
            captured["headers"] = kw.get("headers")
            return _Urllib3Response()

        urllib3_instr._urlopen_wrapper(
            _send, _Pool(), ("GET", "/items/1"), {"headers": provided},
        )

        forwarded = captured["headers"]
        # Repeated fields survive as separate entries (not comma-joined).
        assert isinstance(forwarded, HTTPHeaderDict)
        assert forwarded.getlist("X-Multi") == ["a", "b"]
        assert forwarded.getlist("Cookie") == ["c1=1", "c2=2"]
        # Trace context was still injected onto the forwarded headers.
        assert forwarded[HEADER_TRACE_ID] == "app^1700000000000^1"
        assert forwarded[HEADER_SPAN_ID] == "42"
        assert HEADER_SAMPLED in forwarded
        # The caller's own HTTPHeaderDict is not polluted or collapsed.
        assert pinpoint_keys(provided) == set()
        assert provided.getlist("X-Multi") == ["a", "b"]
        assert provided.getlist("Cookie") == ["c1=1", "c2=2"]
    finally:
        reset_current_span(token)


def test_urllib3_pool_default_httpheaderdict_preserves_multivalue_headers():
    """The pool-default seeding branch must preserve a multi-valued
    ``HTTPHeaderDict`` too — not only the caller-supplied one.

    When ``urlopen`` gets no per-call headers, urllib3 falls back to the pool's
    ``self.headers``. If those defaults are an ``HTTPHeaderDict`` with repeated
    fields, seeding via ``dict(...)`` routes each field through
    ``HTTPHeaderDict.__getitem__`` and comma-joins them (wrong on the wire, and
    semantically broken for ``Cookie``). The pool-default branch of
    ``_seed_headers`` must copy in-kind like the caller-supplied branch does,
    and must not mutate the pool's own default headers."""
    HTTPHeaderDict = pytest.importorskip("urllib3").HTTPHeaderDict
    span = SampledClientSpan()
    pool = _Pool()
    defaults = HTTPHeaderDict()
    defaults.add("X-Multi", "a")
    defaults.add("X-Multi", "b")
    defaults.add("Cookie", "c1=1")
    defaults.add("Cookie", "c2=2")
    pool.headers = defaults
    captured: dict = {}
    token = set_current_span(span)
    try:
        def _send(*_args, **kw):
            captured["headers"] = kw.get("headers")
            return _Urllib3Response()

        # No per-call headers → seeds from the pool's HTTPHeaderDict defaults.
        urllib3_instr._urlopen_wrapper(_send, pool, ("GET", "/items/1"), {})

        forwarded = captured["headers"]
        # Repeated pool-default fields survive as separate entries.
        assert isinstance(forwarded, HTTPHeaderDict)
        assert forwarded.getlist("X-Multi") == ["a", "b"]
        assert forwarded.getlist("Cookie") == ["c1=1", "c2=2"]
        # Trace context is still injected onto the forwarded headers.
        assert forwarded[HEADER_TRACE_ID] == "app^1700000000000^1"
        assert HEADER_SAMPLED in forwarded
        # The pool's own defaults are left untouched (wrapper uses a copy).
        assert pinpoint_keys(defaults) == set()
        assert defaults.getlist("Cookie") == ["c1=1", "c2=2"]
    finally:
        reset_current_span(token)
