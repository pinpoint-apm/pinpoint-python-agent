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

"""httpx instrumentation fast-path tests — the async wrapper and the
httpx-only paths. The sync ``_send_wrapper`` rules httpx shares with
requests are pinned once, for both, in test_http_client_send_wrapper.py."""

import asyncio

from _fakes import (ClientEvent, ClientRequest, ClientResponse, ClientSpan,
                    SampledClientSpan, pinpoint_keys)

from pinpoint.context import reset_current_span, set_current_span
from pinpoint.instrumentations import _util
from pinpoint.instrumentations import httpx as httpx_instr
from pinpoint.propagator import HEADER_SPAN_ID, HEADER_TRACE_ID


def test_httpx_async_send_emits_event(monkeypatch):
    """The async wrapper opens/ends the span event and returns the response."""
    span = ClientSpan()
    request = ClientRequest()
    monkeypatch.setattr(httpx_instr, "trace_http_client_request", lambda *a, **k: None)
    token = set_current_span(span)
    try:
        async def _send(*_args, **_kwargs):
            return ClientResponse()

        response = asyncio.run(
            httpx_instr._async_send_wrapper(_send, None, (request,), {})
        )
        assert isinstance(response, ClientResponse)
        assert len(span.events) == 1
        assert span.events[0].ended is True
    finally:
        reset_current_span(token)


def test_httpx_async_setup_failure_still_sends_no_leak(monkeypatch):
    """A native/parse error in the async pre-await instrumentation must be
    contained: the request still goes out (once) and the span event opened
    before the failure is ended, not leaked."""
    span = ClientSpan()
    request = ClientRequest()
    calls = {"n": 0}

    # Force a failure *after* the span event is opened to exercise the
    # event.end() cleanup path.
    def _boom(_span):
        raise RuntimeError("native sampled check failed")

    monkeypatch.setattr(_util, "span_is_sampled", _boom)
    token = set_current_span(span)
    try:
        async def _send(*_args, **_kwargs):
            calls["n"] += 1
            return ClientResponse()

        response = asyncio.run(
            httpx_instr._async_send_wrapper(_send, None, (request,), {})
        )
        # The request still ran, exactly once.
        assert isinstance(response, ClientResponse)
        assert calls["n"] == 1
        # Event opened, then ended on failure — no leaked span event.
        assert len(span.events) == 1
        assert span.events[0].ended is True
    finally:
        reset_current_span(token)


def test_httpx_async_send_carries_headers_on_private_request(monkeypatch):
    span = SampledClientSpan()
    request = ClientRequest()
    monkeypatch.setattr(httpx_instr, "trace_http_client_request", lambda *a, **k: None)
    captured: dict = {}
    token = set_current_span(span)
    try:
        async def _send(*send_args, **send_kwargs):
            sent = send_args[0] if send_args else send_kwargs["request"]
            captured["request"] = sent
            captured["headers"] = dict(sent.headers)
            assert request.headers == {}
            return ClientResponse()

        asyncio.run(httpx_instr._async_send_wrapper(_send, None, (request,), {}))

        assert captured["headers"][HEADER_TRACE_ID] == "app^1700000000000^1"
        assert captured["request"] is not request
        assert pinpoint_keys(request.headers) == set()
    finally:
        reset_current_span(token)


def test_httpx_async_concurrent_reuse_isolates_trace_headers(monkeypatch):
    """Overlapping async sends cannot share or restore each other's trace IDs."""
    request = ClientRequest()
    spans = {
        "trace-a": SampledClientSpan("trace-a", "span-a"),
        "trace-b": SampledClientSpan("trace-b", "span-b"),
    }
    sent_requests = {}
    monkeypatch.setattr(
        httpx_instr, "trace_http_client_request", lambda *a, **k: None
    )

    async def _scenario():
        entered = {trace_id: asyncio.Event() for trace_id in spans}
        release = {trace_id: asyncio.Event() for trace_id in spans}

        async def _send(*send_args, **send_kwargs):
            sent = send_args[0] if send_args else send_kwargs["request"]
            trace_id = sent.headers[HEADER_TRACE_ID]
            sent_requests[trace_id] = sent
            entered[trace_id].set()
            await release[trace_id].wait()
            return ClientResponse()

        async def _run(trace_id):
            token = set_current_span(spans[trace_id])
            try:
                return await httpx_instr._async_send_wrapper(
                    _send, None, (request,), {}
                )
            finally:
                reset_current_span(token)

        first = asyncio.create_task(_run("trace-a"))
        await asyncio.wait_for(entered["trace-a"].wait(), timeout=2)
        second = asyncio.create_task(_run("trace-b"))
        await asyncio.wait_for(entered["trace-b"].wait(), timeout=2)

        original_during_overlap = dict(request.headers)
        release["trace-a"].set()
        await asyncio.wait_for(first, timeout=2)
        release["trace-b"].set()
        await asyncio.wait_for(second, timeout=2)
        return original_during_overlap

    original_during_overlap = asyncio.run(_scenario())

    assert original_during_overlap == {}
    assert request.headers == {}
    assert sent_requests["trace-a"] is not request
    assert sent_requests["trace-b"] is not request
    assert sent_requests["trace-a"] is not sent_requests["trace-b"]
    assert sent_requests["trace-a"].headers[HEADER_SPAN_ID] == "span-a"
    assert sent_requests["trace-b"].headers[HEADER_SPAN_ID] == "span-b"


def test_httpx_async_keeps_original_headers_when_send_raises(monkeypatch):
    """A failed async send leaves the user-owned request untouched."""
    span = SampledClientSpan()
    request = ClientRequest()
    monkeypatch.setattr(httpx_instr, "trace_http_client_request", lambda *a, **k: None)
    token = set_current_span(span)
    try:
        async def _send(*send_args, **send_kwargs):
            sent = send_args[0] if send_args else send_kwargs["request"]
            assert pinpoint_keys(sent.headers)
            assert request.headers == {}
            raise RuntimeError("connect timeout")

        raised = False
        try:
            asyncio.run(httpx_instr._async_send_wrapper(_send, None, (request,), {}))
        except RuntimeError:
            raised = True
        assert raised, "the send exception must propagate to the caller"
        assert pinpoint_keys(request.headers) == set()
    finally:
        reset_current_span(token)


def test_httpx_annotate_request_swallows_bad_url():
    """``_annotate_request`` is now @safe_try: a urlparse ValueError on a
    malformed URL must not escape (it would otherwise break the user request)."""
    event = ClientEvent()

    class _BadRequest:
        # An unterminated IPv6 literal makes urlparse raise ValueError.
        url = "http://[::1"
        headers = {}

    # Must not raise.
    httpx_instr._annotate_request(event, _BadRequest())
