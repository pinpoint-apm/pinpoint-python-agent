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

"""HTTP clients (requests / urllib3 / httpx) against a real echo container.

The echo server reflects the request headers back as JSON, so these tests
assert the ``Pinpoint-*`` propagation headers actually crossed the wire —
not merely that the wrapper attempted an injection.
"""

from __future__ import annotations

import asyncio
import json

import pytest

requests = pytest.importorskip("requests")

from pinpoint.annotation import (
    ANNOTATION_HTTP_STATUS_CODE,
    ANNOTATION_HTTP_URL,
)
from pinpoint.service_type import SERVICE_TYPE_PYTHON_HTTP_CLIENT

_OP_REQUESTS = "requests.sessions.Session.send"
_OP_URLLIB3 = "urllib3.connectionpool.HTTPConnectionPool.urlopen"


@pytest.fixture(scope="module", autouse=True)
def _instrument():
    from pinpoint.instrumentations import requests as requests_instr
    from pinpoint.instrumentations import urllib3 as urllib3_instr

    requests_instr.instrument()
    urllib3_instr.instrument()


def test_requests_get_records_event_and_propagates(http_echo_container,
                                                   traced):
    url = http_echo_container["url"] + "/it/path"
    resp = requests.get(url)
    assert resp.status_code == 200

    # Dedupe: requests is the top layer; the nested urllib3 call is silent.
    events = [e for e in traced.events if e.ended]
    assert [e.operation for e in events] == [_OP_REQUESTS]
    ev = events[0]
    netloc = f"{http_echo_container['host']}:{http_echo_container['port']}"
    assert ev.service_type == SERVICE_TYPE_PYTHON_HTTP_CLIENT
    assert ev.endpoint == netloc
    assert ev.destination == netloc
    assert ev.ann(ANNOTATION_HTTP_URL) == [url]
    assert ev.ann(ANNOTATION_HTTP_STATUS_CODE) == [200]

    # The echo body proves the Pinpoint headers went out on the wire.
    echoed = {k.lower(): v for k, v in resp.json()["headers"].items()}
    assert echoed.get("pinpoint-traceid") == traced.spans[0].trace_id
    # Sampled transactions omit Pinpoint-Sampled: the marker is written only
    # for drops, and the identity pairs go out instead.
    assert "pinpoint-sampled" not in echoed
    assert echoed.get("pinpoint-pappname") == "it-test"
    # ... and were stripped from the caller-visible request afterwards.
    assert not any(k.lower().startswith("pinpoint-")
                   for k in resp.request.headers)


def test_urllib3_direct_call_records_event(http_echo_container, traced):
    urllib3 = pytest.importorskip("urllib3")

    http = urllib3.PoolManager()
    resp = http.request("GET", http_echo_container["url"] + "/direct")
    assert resp.status == 200

    ev = traced.single(_OP_URLLIB3)
    assert ev.ended
    assert ev.service_type == SERVICE_TYPE_PYTHON_HTTP_CLIENT
    urls = ev.ann(ANNOTATION_HTTP_URL)
    assert urls and urls[0].endswith("/direct")
    assert ev.ann(ANNOTATION_HTTP_STATUS_CODE) == [200]

    echoed = {k.lower(): v
              for k, v in json.loads(resp.data)["headers"].items()}
    assert echoed.get("pinpoint-traceid") == traced.spans[0].trace_id


def test_httpx_sync_and_async(http_echo_container, traced):
    httpx = pytest.importorskip("httpx")
    from pinpoint.instrumentations import httpx as httpx_instr

    httpx_instr.instrument()

    url = http_echo_container["url"] + "/httpx"
    resp = httpx.get(url)
    assert resp.status_code == 200
    sync_ev = traced.single("httpx.Client.send")
    assert sync_ev.ended
    assert sync_ev.ann(ANNOTATION_HTTP_URL) == [url]
    assert sync_ev.ann(ANNOTATION_HTTP_STATUS_CODE) == [200]
    echoed = {k.lower(): v for k, v in resp.json()["headers"].items()}
    assert echoed.get("pinpoint-traceid") == traced.spans[0].trace_id

    async def main():
        async with httpx.AsyncClient() as client:
            return await client.get(url)

    resp = asyncio.run(main())
    assert resp.status_code == 200
    async_ev = traced.single("httpx.AsyncClient.send")
    assert async_ev.ended
    assert async_ev.ann(ANNOTATION_HTTP_STATUS_CODE) == [200]


def test_aiohttp_client_records_event_and_propagates(http_echo_container,
                                                     traced):
    aiohttp = pytest.importorskip("aiohttp")
    from pinpoint.instrumentations import aiohttp as aiohttp_instr

    aiohttp_instr.instrument_client()

    url = http_echo_container["url"] + "/aiohttp"

    async def main():
        async with aiohttp.ClientSession() as session:
            # A caller-provided headers mapping must survive un-mutated: the
            # wrapper injects the Pinpoint pairs into a per-request copy.
            caller_headers = {"X-It": "keep"}
            async with session.get(url, headers=caller_headers) as resp:
                body = await resp.json()
            assert caller_headers == {"X-It": "keep"}
            return resp.status, body

    status, body = asyncio.run(main())
    assert status == 200

    ev = traced.single("aiohttp.client.ClientSession._request")
    assert ev.ended
    assert ev.service_type == SERVICE_TYPE_PYTHON_HTTP_CLIENT
    netloc = f"{http_echo_container['host']}:{http_echo_container['port']}"
    assert ev.endpoint == netloc
    assert ev.destination == netloc
    assert ev.ann(ANNOTATION_HTTP_URL) == [url]
    assert ev.ann(ANNOTATION_HTTP_STATUS_CODE) == [200]

    # The echo body proves the Pinpoint headers went out on the wire,
    # alongside the caller's own header.
    echoed = {k.lower(): v for k, v in body["headers"].items()}
    assert echoed.get("pinpoint-traceid") == traced.spans[0].trace_id
    # Sampled transactions omit Pinpoint-Sampled: the marker is written only
    # for drops, and the identity pairs go out instead.
    assert "pinpoint-sampled" not in echoed
    assert echoed.get("pinpoint-pappname") == "it-test"
    assert echoed.get("x-it") == "keep"


def test_aiohttp_client_connection_error_recorded(traced):
    aiohttp = pytest.importorskip("aiohttp")
    from pinpoint.instrumentations import aiohttp as aiohttp_instr

    aiohttp_instr.instrument_client()

    # Bind-then-close guarantees a refused port without racing another
    # process for it.
    import socket

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    async def main():
        async with aiohttp.ClientSession() as session:
            await session.get(f"http://127.0.0.1:{port}/refused")

    with pytest.raises(aiohttp.ClientConnectorError):
        asyncio.run(main())

    ev = traced.single("aiohttp.client.ClientSession._request")
    assert ev.ended
    assert ev.error is not None
    assert ev.ann(ANNOTATION_HTTP_STATUS_CODE) == []


def test_no_current_span_no_trace_no_headers(http_echo_container, recorder):
    resp = requests.get(http_echo_container["url"] + "/untraced")
    assert resp.status_code == 200
    assert recorder.events == []
    echoed = {k.lower(): v for k, v in resp.json()["headers"].items()}
    assert not any(k.startswith("pinpoint-") for k in echoed)
