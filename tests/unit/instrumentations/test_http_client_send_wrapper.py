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

"""The ``_send_wrapper`` contract httpx and requests both implement.

Both wrap a ``send(request)`` taking the request as arg 0 and inject into a
private per-send copy, so every rule below has to hold for both. Client-only
behaviour lives in the per-client modules: httpx's async wrapper in
test_httpx_instrumentation.py, urllib3's positional-``headers`` handling and
the requests-over-urllib3 nesting suppression in test_http_client_dedupe.py.
"""

import threading

import pytest

from _fakes import (ClientRequest, ClientResponse, ClientSpan,
                    SampledClientSpan, UnsampledClientSpan, pinpoint_keys)

from pinpoint.context import reset_current_span, set_current_span
from pinpoint.instrumentations import _util
from pinpoint.instrumentations import httpx as httpx_instr
from pinpoint.instrumentations import requests as requests_instr
from pinpoint.propagator import HEADER_SAMPLED, HEADER_SPAN_ID, HEADER_TRACE_ID

CLIENTS = [
    pytest.param(httpx_instr, id="httpx"),
    pytest.param(requests_instr, id="requests"),
]



@pytest.mark.parametrize("client", CLIENTS)
def test_unsampled_span_skips_annotation_but_opens_event(client, monkeypatch):
    """An unsampled span still opens the span event so the trace context
    written by ``inject`` reflects this RPC's depth/sequence, but the
    sampled-only annotation (``trace_http_client_request``, ``event.end``)
    is skipped."""
    span = ClientSpan(sampled=False)
    request = ClientRequest()
    annotated: dict = {"called": False}

    def _trace(*_a, **_kw):
        annotated["called"] = True

    monkeypatch.setattr(client, "trace_http_client_request", _trace)
    token = set_current_span(span)
    try:
        def _send(*_args, **_kwargs):
            return ClientResponse()

        response = client._send_wrapper(_send, None, (request,), {})

        assert isinstance(response, ClientResponse)
        # Event opened (one) but neither annotated nor ended.
        assert len(span.events) == 1
        assert span.events[0].ended is False
        assert annotated["called"] is False
    finally:
        reset_current_span(token)


@pytest.mark.parametrize("client", CLIENTS)
def test_unsampled_inject_writes_s0_marker(client):
    """An unsampled parent must still propagate ``Pinpoint-Sampled: s0`` so
    the downstream server short-circuits its own sampling decision. The marker
    rides the outgoing call, then is stripped from the reusable request."""
    span = UnsampledClientSpan()
    request = ClientRequest()
    captured: dict = {}
    token = set_current_span(span)
    try:
        def _send(*send_args, **send_kwargs):
            sent = send_args[0] if send_args else send_kwargs["request"]
            captured["sampled"] = sent.headers.get(HEADER_SAMPLED)
            return ClientResponse()

        client._send_wrapper(_send, None, (request,), {})
        assert captured["sampled"] == "s0"
        assert pinpoint_keys(request.headers) == set()
    finally:
        reset_current_span(token)


@pytest.mark.parametrize("client", CLIENTS)
def test_passes_private_header_mapping(client, monkeypatch):
    span = ClientSpan()
    request = ClientRequest()
    captured = {}
    token = set_current_span(span)
    try:
        def _trace(_event, _host, _url, headers):
            captured["headers"] = headers

        monkeypatch.setattr(client, "trace_http_client_request", _trace)

        def _send(*_args, **_kwargs):
            return ClientResponse()

        client._send_wrapper(_send, None, (request,), {})
        assert captured["headers"] is not request.headers
        assert request.headers == {}
    finally:
        reset_current_span(token)


@pytest.mark.parametrize("client", CLIENTS)
def test_setup_failure_still_sends_no_leak(client, monkeypatch):
    """Regression (native span-event leak): a failure in the pre-send
    instrumentation must be contained — the request still goes out exactly
    once, and the span event opened before the failure is ended, not leaked."""
    span = ClientSpan()
    request = ClientRequest()
    calls = {"n": 0}

    def _boom(*_a, **_kw):
        raise RuntimeError("pre-send instrumentation failed")

    # Fails *after* the span event is opened (inside _util.open_client_send),
    # so the event.end() cleanup path is what's exercised.
    monkeypatch.setattr(_util, "span_is_sampled", _boom)
    token = set_current_span(span)
    try:
        def _send(*_args, **_kwargs):
            calls["n"] += 1
            return ClientResponse()

        response = client._send_wrapper(_send, None, (request,), {})

        # The request still ran, exactly once.
        assert isinstance(response, ClientResponse)
        assert calls["n"] == 1
        # Event opened, then ended on failure — no leaked span event.
        assert len(span.events) == 1
        assert span.events[0].ended is True
    finally:
        reset_current_span(token)


@pytest.mark.parametrize("client", CLIENTS)
def test_send_carries_headers_on_private_request(client, monkeypatch):
    """The outgoing request must carry the trace headers for *this* call, but
    the user-owned request remains unchanged throughout the send."""
    span = SampledClientSpan()
    request = ClientRequest()
    monkeypatch.setattr(
        client, "trace_http_client_request", lambda *a, **k: None
    )
    captured: dict = {}
    token = set_current_span(span)
    try:
        def _send(*send_args, **send_kwargs):
            sent = send_args[0] if send_args else send_kwargs["request"]
            captured["request"] = sent
            captured["headers"] = dict(sent.headers)
            assert request.headers == {}
            return ClientResponse()

        client._send_wrapper(_send, None, (request,), {})

        # Outgoing carried the trace context...
        assert captured["headers"][HEADER_TRACE_ID] == "app^1700000000000^1"
        assert captured["headers"][HEADER_SPAN_ID] == "42"
        assert captured["request"] is not request
        # ...on a shallow copy: one-shot payloads stay shared, never cloned.
        assert captured["request"].stream is request.stream
        assert captured["request"].extensions is request.extensions
        assert pinpoint_keys(request.headers) == set()
    finally:
        reset_current_span(token)


@pytest.mark.parametrize("client", CLIENTS)
def test_keyword_send_rebinds_response_to_original_request(
        client, monkeypatch):
    """The returned response keeps the caller-visible request identity."""
    span = SampledClientSpan()
    request = ClientRequest()
    captured = {}
    monkeypatch.setattr(
        client, "trace_http_client_request", lambda *a, **k: None
    )
    token = set_current_span(span)
    try:
        def _send(*send_args, **send_kwargs):
            sent = send_args[0] if send_args else send_kwargs["request"]
            captured["sent"] = sent
            response = ClientResponse()
            response.request = sent
            return response

        response = client._send_wrapper(
            _send, None, (), {"request": request}
        )

        assert captured["sent"] is not request
        assert response.request is request
        assert request.headers == {}
    finally:
        reset_current_span(token)


@pytest.mark.parametrize("client", CLIENTS)
def test_concurrent_reuse_isolates_trace_headers(client, monkeypatch):
    """Overlapping sends of one request cannot share or restore each other's
    trace IDs."""
    request = ClientRequest()
    spans = {
        "trace-a": SampledClientSpan("trace-a", "span-a"),
        "trace-b": SampledClientSpan("trace-b", "span-b"),
    }
    entered = {trace_id: threading.Event() for trace_id in spans}
    release = {trace_id: threading.Event() for trace_id in spans}
    sent_requests = {}
    errors = []
    monkeypatch.setattr(
        client, "trace_http_client_request", lambda *a, **k: None
    )

    def _send(*send_args, **send_kwargs):
        sent = send_args[0] if send_args else send_kwargs["request"]
        trace_id = sent.headers[HEADER_TRACE_ID]
        sent_requests[trace_id] = sent
        entered[trace_id].set()
        if not release[trace_id].wait(timeout=2):
            raise TimeoutError(f"send {trace_id} was not released")
        return ClientResponse()

    def _run(trace_id):
        token = set_current_span(spans[trace_id])
        try:
            client._send_wrapper(_send, None, (request,), {})
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            reset_current_span(token)

    first = threading.Thread(target=_run, args=("trace-a",))
    second = threading.Thread(target=_run, args=("trace-b",))
    first.start()
    assert entered["trace-a"].wait(timeout=2)
    second.start()
    assert entered["trace-b"].wait(timeout=2)

    original_during_overlap = dict(request.headers)
    release["trace-a"].set()
    first.join(timeout=2)
    release["trace-b"].set()
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert original_during_overlap == {}
    assert request.headers == {}
    assert sent_requests["trace-a"] is not request
    assert sent_requests["trace-b"] is not request
    assert sent_requests["trace-a"] is not sent_requests["trace-b"]
    assert sent_requests["trace-a"].headers[HEADER_SPAN_ID] == "span-a"
    assert sent_requests["trace-b"].headers[HEADER_SPAN_ID] == "span-b"


@pytest.mark.parametrize("client", CLIENTS)
def test_reused_request_has_no_stale_trace_ids(client, monkeypatch):
    """A request reused across sends must not keep stale trace IDs."""
    span = SampledClientSpan()
    request = ClientRequest()
    monkeypatch.setattr(
        client, "trace_http_client_request", lambda *a, **k: None
    )
    token = set_current_span(span)
    try:
        def _send(*_args, **_kwargs):
            return ClientResponse()

        client._send_wrapper(_send, None, (request,), {})
        client._send_wrapper(_send, None, (request,), {})
        assert pinpoint_keys(request.headers) == set()
    finally:
        reset_current_span(token)


@pytest.mark.parametrize("client", CLIENTS)
def test_private_copy_preserves_user_header_of_same_name(client, monkeypatch):
    """Injection must not overwrite a same-named header on the original."""
    span = SampledClientSpan()
    request = ClientRequest()
    request.headers[HEADER_TRACE_ID] = "user-value"
    monkeypatch.setattr(
        client, "trace_http_client_request", lambda *a, **k: None
    )
    token = set_current_span(span)
    try:
        def _send(*_args, **_kwargs):
            return ClientResponse()

        client._send_wrapper(_send, None, (request,), {})
        # The user's own value is restored, not deleted.
        assert request.headers[HEADER_TRACE_ID] == "user-value"
        # The injected-only key is gone.
        assert HEADER_SPAN_ID not in request.headers
    finally:
        reset_current_span(token)


@pytest.mark.parametrize("client", CLIENTS)
def test_keeps_original_headers_when_send_raises(client, monkeypatch):
    """A failed send propagates, user-owned request left untouched."""
    span = SampledClientSpan()
    request = ClientRequest()
    monkeypatch.setattr(
        client, "trace_http_client_request", lambda *a, **k: None
    )
    token = set_current_span(span)
    try:
        def _send(*send_args, **send_kwargs):
            sent = send_args[0] if send_args else send_kwargs["request"]
            assert pinpoint_keys(sent.headers)
            assert request.headers == {}
            raise RuntimeError("connect timeout")

        raised = False
        try:
            client._send_wrapper(_send, None, (request,), {})
        except RuntimeError:
            raised = True
        assert raised, "the send exception must propagate to the caller"
        assert pinpoint_keys(request.headers) == set()
    finally:
        reset_current_span(token)
