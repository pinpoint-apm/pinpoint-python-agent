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

"""elasticsearch-py sync/async clients against a real Elasticsearch 8.

urllib3 and aiohttp are instrumented alongside on purpose: the ES wrapper must
suppress the nested generic HTTP-client event for its own sync and async
transport calls, so each ES call yields exactly one span event.
"""

from __future__ import annotations

import asyncio

import pytest

elasticsearch = pytest.importorskip("elasticsearch")

from pinpoint.annotation import (
    ANNOTATION_ELASTICSEARCH_DSL,
    ANNOTATION_HTTP_STATUS_CODE,
)
from pinpoint.service_type import SERVICE_TYPE_ELASTICSEARCH

_OP = "elastic_transport.Transport.perform_request"
_OP_ASYNC = "elastic_transport.AsyncTransport.perform_request"
_OP_URLLIB3 = "urllib3.connectionpool.HTTPConnectionPool.urlopen"
_OP_AIOHTTP = "aiohttp.client.ClientSession._request"


@pytest.fixture(scope="module", autouse=True)
def _instrument():
    from pinpoint.instrumentations import aiohttp as aiohttp_instr
    from pinpoint.instrumentations import elasticsearch as es_instr
    from pinpoint.instrumentations import urllib3 as urllib3_instr

    es_instr.instrument()
    urllib3_instr.instrument()
    aiohttp_instr.instrument_client()


@pytest.fixture(scope="module")
def client(elasticsearch_container):
    es = elasticsearch.Elasticsearch(
        elasticsearch_container["url"], request_timeout=30)
    # Connection warm-up outside any traced block.
    assert es.ping()
    yield es
    es.close()


def test_index_and_search_record_events(client, elasticsearch_container,
                                        traced):
    client.index(index="it-idx", id="1", document={"color": "red"},
                 refresh=True)
    result = client.search(index="it-idx", query={"match": {"color": "red"}})
    assert result["hits"]["total"]["value"] == 1

    events = [e for e in traced.events_named(_OP) if e.ended]
    assert len(events) == 2
    for ev in events:
        assert ev.service_type == SERVICE_TYPE_ELASTICSEARCH
        assert ev.destination == "ElasticSearch"
        assert ev.endpoint.endswith(
            str(elasticsearch_container["url"].rsplit(":", 1)[-1]))
        statuses = ev.ann(ANNOTATION_HTTP_STATUS_CODE)
        assert statuses and statuses[0] in (200, 201)

    search_ev = events[-1]
    dsl = search_ev.ann(ANNOTATION_ELASTICSEARCH_DSL)
    assert dsl and "color" in dsl[0]

    # The ES transport rides on urllib3 — suppressed, no nested client event.
    assert traced.events_named(_OP_URLLIB3) == []


def test_async_index_and_search_record_events(elasticsearch_container,
                                              traced):
    async def main():
        client = elasticsearch.AsyncElasticsearch(
            elasticsearch_container["url"], request_timeout=30)
        try:
            await client.index(
                index="it-async-idx",
                id="1",
                document={"color": "blue"},
                refresh=True,
            )
            return await client.search(
                index="it-async-idx", query={"match": {"color": "blue"}})
        finally:
            await client.close()

    result = asyncio.run(main())
    assert result["hits"]["total"]["value"] == 1

    events = [event for event in traced.events_named(_OP_ASYNC)
              if event.ended]
    assert len(events) == 2
    for event in events:
        assert event.service_type == SERVICE_TYPE_ELASTICSEARCH
        assert event.destination == "ElasticSearch"
        assert event.endpoint.endswith(
            str(elasticsearch_container["url"].rsplit(":", 1)[-1]))
        statuses = event.ann(ANNOTATION_HTTP_STATUS_CODE)
        assert statuses and statuses[0] in (200, 201)

    dsl = events[-1].ann(ANNOTATION_ELASTICSEARCH_DSL)
    assert dsl and "blue" in dsl[0]
    # AsyncTransport uses aiohttp underneath; its generic event is suppressed.
    assert traced.events_named(_OP_AIOHTTP) == []


def test_api_error_annotates_status_without_event_error(client, traced):
    """A 404 from the cluster is raised by the *client* layer above the
    instrumented transport (``Elasticsearch.perform_request`` converts the
    response to ``NotFoundError`` after ``Transport.perform_request``
    returns) — so the transport event completes normally with the response
    status annotated, and no event error."""
    with pytest.raises(elasticsearch.NotFoundError):
        client.search(index="it-no-such-index",
                      query={"match_all": {}})

    events = traced.events_named(_OP)
    assert events and events[-1].ended
    assert events[-1].error is None
    assert events[-1].ann(ANNOTATION_HTTP_STATUS_CODE) == [404]


def test_transport_error_recorded_on_event(traced):
    """A genuine transport failure (connection refused) does raise through
    the instrumented layer and must land on the span event."""
    import socket

    from elastic_transport import TransportError

    # Bind-then-close guarantees a refused port without racing another
    # process for it.
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    es = elasticsearch.Elasticsearch(
        f"http://127.0.0.1:{port}", request_timeout=2, max_retries=0)
    try:
        with pytest.raises(TransportError):
            es.info()
    finally:
        es.close()

    events = traced.events_named(_OP)
    assert events and events[-1].ended
    assert events[-1].error is not None


def test_no_current_span_passes_through(client, recorder):
    assert client.ping()
    assert recorder.events == []
