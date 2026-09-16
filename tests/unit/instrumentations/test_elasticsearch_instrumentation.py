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

"""elasticsearch instrumentation.

Drives the perform_request wrapper against in-memory fakes — no
elasticsearch / elastic_transport dependency required. Validates span
event lifecycle, DSL extraction (body / ``q`` URL param), endpoint
resolution against both v7-style connection_pool and v8-style
node_pool, status-code annotation, error capture, and truncation.
"""

from __future__ import annotations

import asyncio
import json

import pytest

import _fakes
from pinpoint import context as ppctx
from pinpoint.annotation import (
    ANNOTATION_ELASTICSEARCH_DSL,
    ANNOTATION_ELASTICSEARCH_VERSION,
    ANNOTATION_HTTP_STATUS_CODE,
)
from pinpoint.instrumentations import elasticsearch as es_instr
from pinpoint.service_type import SERVICE_TYPE_ELASTICSEARCH


# ---------------------------------------------------------------------------
# Fakes (span fakes + ``push_span`` fixture come from _fakes / conftest)
# ---------------------------------------------------------------------------

# ---- transport / node stand-ins -------------------------------------------

class _V8Node:
    def __init__(self, host, port):
        self.host = host
        self.port = port


class _V8NodePool:
    def __init__(self, nodes):
        self._nodes = list(nodes)

    def all(self):
        return list(self._nodes)


class _V8Transport:
    def __init__(self, nodes=(("es.test", 9200),)):
        self.node_pool = _V8NodePool(_V8Node(h, p) for h, p in nodes)


class _V7Connection:
    def __init__(self, host):
        self.host = host  # already "host:port" in elasticsearch v7


class _V7ConnectionPool:
    def __init__(self, hosts):
        self.connections = [_V7Connection(h) for h in hosts]


class _V7Transport:
    def __init__(self, hosts=("es.test:9200",)):
        self.connection_pool = _V7ConnectionPool(hosts)


class _V8ResponseMeta:
    def __init__(self, status):
        self.status = status


class _V8Response:
    def __init__(self, status, body=None):
        self.meta = _V8ResponseMeta(status)
        self.body = body or {}


# ---------------------------------------------------------------------------
# Happy path — sync wrapper
# ---------------------------------------------------------------------------

def test_sync_wrapper_emits_event_with_destination_and_service_type(push_span):
    sp, rec = push_span

    def wrapped(*args, **kwargs):
        return _V8Response(200, {"hits": {"total": 0}})

    result = es_instr._sync_perform_request_wrapper(
        wrapped,
        instance=_V8Transport(),
        args=("GET", "/idx/_search"),
        kwargs={"body": {"query": {"match_all": {}}}},
    )
    assert isinstance(result, _V8Response)

    operation = "elastic_transport.Transport.perform_request"
    assert ("event_start", "root", operation) in rec.events
    assert ("event_end", "root", operation) in rec.events

    ev = sp._native.all_events[-1]
    assert ev.service_type == SERVICE_TYPE_ELASTICSEARCH
    assert ev.destination == "ElasticSearch"
    assert ev.endpoint == "es.test:9200"

    # Body must show up as a DSL annotation (truncated to 256 chars).
    dsl_entries = [e for e in ev.annotations.entries
                   if e[0] == "str" and e[1] == ANNOTATION_ELASTICSEARCH_DSL]
    assert dsl_entries
    rendered = dsl_entries[-1][2]
    assert "match_all" in rendered
    assert len(rendered) <= 256

    # The HTTP status from the v8 ApiResponse should be annotated.
    status_entries = [e for e in ev.annotations.entries
                      if e[0] == "int" and e[1] == ANNOTATION_HTTP_STATUS_CODE]
    assert status_entries == [("int", ANNOTATION_HTTP_STATUS_CODE, 200)]


def test_sync_wrapper_suppresses_http_client_even_on_event_setup_failure(
        push_span, monkeypatch):
    """The underlying ES call must run with HTTP-client instrumentation
    suppressed on EVERY path — including when opening the span event fails, so
    it never falls through to an un-suppressed retry that emits a stray HTTP
    event and injects Pinpoint headers into the ES request."""
    from pinpoint.instrumentations._util import (
        http_client_instrumentation_suppressed,
    )
    sp, rec = push_span

    def boom(*_a, **_kw):
        raise RuntimeError("native event alloc failed")
    monkeypatch.setattr(es_instr, "_open_event", boom)

    seen = {}

    def wrapped(*args, **kwargs):
        seen["suppressed"] = http_client_instrumentation_suppressed()
        return _V8Response(200, {})

    result = es_instr._sync_perform_request_wrapper(
        wrapped, instance=_V8Transport(), args=("GET", "/idx/_search"), kwargs={},
    )
    assert isinstance(result, _V8Response)
    assert seen["suppressed"] is True   # ran inside the suppression scope
    # No event opened (setup failed), so tracing degraded silently.
    assert not [e for e in rec.events if e[0] == "event_start"]


def test_sync_wrapper_v7_endpoint_pulled_from_connection_pool(push_span):
    sp, rec = push_span

    def wrapped(*_a, **_kw):
        return {"acknowledged": True}  # v7 returns the decoded body directly

    es_instr._sync_perform_request_wrapper(
        wrapped,
        instance=_V7Transport(hosts=("primary.es:9201",)),
        args=("PUT", "/idx"),
        kwargs={"params": None, "body": {"settings": {}}},
    )
    ev = sp._native.all_events[-1]
    assert ev.endpoint == "primary.es:9201"
    assert ev.destination == "ElasticSearch"


def test_sync_wrapper_records_cluster_info_when_response_exposes_it(push_span):
    sp, _rec = push_span

    def wrapped(*_a, **_kw):
        return _V8Response(
            200,
            {
                "cluster_name": "es-cluster",
                "version": {"number": "8.11.0"},
            },
        )

    es_instr._sync_perform_request_wrapper(
        wrapped,
        instance=_V8Transport(),
        args=("GET", "/"),
        kwargs={},
    )
    ev = sp._native.all_events[-1]
    assert (
        "str",
        ANNOTATION_ELASTICSEARCH_VERSION,
        "es-cluster/8.11.0",
    ) in ev.annotations.entries


def test_sync_wrapper_records_exception(push_span):
    sp, rec = push_span

    def boom(*_a, **_kw):
        raise RuntimeError("connection refused")

    with pytest.raises(RuntimeError, match="connection refused"):
        es_instr._sync_perform_request_wrapper(
            boom,
            instance=_V8Transport(),
            args=("GET", "/idx/_search"),
            kwargs={},
        )
    assert any(e[0] == "event_error" for e in rec.events)
    assert (
        "event_end",
        "root",
        "elastic_transport.Transport.perform_request",
    ) in rec.events


def test_sync_wrapper_no_active_span_passes_through():
    def wrapped(*_a, **_kw):
        return "ok"

    out = es_instr._sync_perform_request_wrapper(
        wrapped, instance=_V8Transport(),
        args=("GET", "/"), kwargs={},
    )
    assert out == "ok"


def test_sync_wrapper_unsampled_span_passes_through_without_event():
    from pinpoint.agent import UnSampledSpan  # type: ignore[attr-defined]

    rec = _fakes.Recorder()
    sp = UnSampledSpan(object())
    token = ppctx.set_current_span(sp)
    try:
        def wrapped(*_a, **_kw):
            return "ok"

        out = es_instr._sync_perform_request_wrapper(
            wrapped,
            instance=_V8Transport(),
            args=("GET", "/idx/_search"),
            kwargs={"body": {"query": {"match": {"body": "x" * 5000}}}},
        )
        assert out == "ok"
        assert not [e for e in rec.events if e[0] == "event_start"]
    finally:
        ppctx.reset_current_span(token)


# ---------------------------------------------------------------------------
# Async wrapper
# ---------------------------------------------------------------------------

def _run(coro):
    return asyncio.run(coro)


def test_async_wrapper_emits_event(push_span):
    sp, rec = push_span

    async def wrapped(*_a, **_kw):
        return _V8Response(201)

    out = _run(es_instr._async_perform_request_wrapper(
        wrapped, instance=_V8Transport(),
        args=("POST", "/idx/_doc"),
        kwargs={"body": {"name": "alice"}},
    ))
    assert isinstance(out, _V8Response)
    operation = "elastic_transport.AsyncTransport.perform_request"
    assert ("event_start", "root", operation) in rec.events
    assert ("event_end", "root", operation) in rec.events


def test_async_wrapper_records_exception(push_span):
    sp, rec = push_span

    async def boom(*_a, **_kw):
        raise ValueError("nope")

    with pytest.raises(ValueError, match="nope"):
        _run(es_instr._async_perform_request_wrapper(
            boom, instance=_V8Transport(),
            args=("GET", "/idx/_search"), kwargs={},
        ))
    assert any(e[0] == "event_error" for e in rec.events)


# ---------------------------------------------------------------------------
# DSL extraction
# ---------------------------------------------------------------------------

def test_extract_dsl_prefers_dict_body():
    body = {"query": {"term": {"status": "active"}}}
    dsl = es_instr._extract_dsl("/idx/_search", {"body": body})
    assert "term" in dsl
    assert "active" in dsl
    # Verify it parses as JSON.
    json.loads(dsl)


def test_extract_dsl_handles_bytes_body():
    raw = b'{"query": {"match_all": {}}}'
    dsl = es_instr._extract_dsl("/idx/_search", {"body": raw})
    assert "match_all" in dsl


def test_extract_dsl_handles_bulk_list_body():
    body = [
        {"index": {"_index": "idx", "_id": "1"}},
        {"name": "alice"},
        {"index": {"_index": "idx", "_id": "2"}},
        {"name": "bob"},
    ]
    dsl = es_instr._extract_dsl("/_bulk", {"body": body})
    # NDJSON serialization: one JSON doc per line.
    assert "alice" in dsl
    assert dsl.count("\n") >= 1


def test_extract_dsl_falls_back_to_q_param_in_kwargs():
    dsl = es_instr._extract_dsl(
        "/idx/_search", {"body": None, "params": {"q": "name:alice"}},
    )
    assert dsl == "name:alice"


def test_extract_dsl_falls_back_to_q_param_in_url():
    dsl = es_instr._extract_dsl("/idx/_search?q=color:red", {})
    assert dsl == "color:red"


def test_extract_dsl_truncates_to_256_chars():
    big = {"query": {"match": {"body": "x" * 1000}}}
    dsl = es_instr._extract_dsl("/idx/_search", {"body": big})
    assert len(dsl) == 256


def test_extract_dsl_limits_bulk_body_before_stringifying_all_items():
    class _BadString:
        def __str__(self):
            raise AssertionError("should not stringify truncated items")

    body = [{"index": {"_id": str(i)}} for i in range(32)]
    body.append({"bad": _BadString()})
    dsl = es_instr._extract_dsl("/_bulk", {"body": body})
    assert len(dsl) <= 256
    assert "bad" not in dsl


def test_json_preview_uses_one_global_budget_for_deep_wide_body():
    calls = [0]

    class _CountedLongKey:
        def __init__(self, depth, index):
            self.depth = depth
            self.index = index

        def __str__(self):
            calls[0] += 1
            return f"{self.depth}-{self.index}-" + ("k" * 300)

    # Share each child across the next level: a per-value limit would walk
    # every path in this 8**4 shape and convert 4,680 keys before truncating
    # the final JSON to 256 characters.
    body = "leaf"
    for depth in range(4):
        body = {
            _CountedLongKey(depth, index): body
            for index in range(8)
        }

    dsl = es_instr._json_preview(body, 256)

    assert len(dsl) <= 256
    assert calls[0] <= 8


def test_json_preview_node_budget_bounds_empty_text_shape():
    calls = [0]

    class _CountedEmptyKey:
        def __str__(self):
            calls[0] += 1
            return ""

    # Empty keys and values spend no character budget. A separate node budget
    # must still prevent the multiplicative item/depth walk.
    body = ""
    for _depth in range(4):
        body = {_CountedEmptyKey(): body for _index in range(8)}

    dsl = es_instr._json_preview(body, 32)

    assert len(dsl) <= 32
    assert calls[0] <= 32


@pytest.mark.parametrize("max_len", [0, 1, 2, 3])
def test_json_preview_never_exceeds_tiny_limit(max_len):
    assert len(es_instr._json_preview({"key": "value"}, max_len)) <= max_len


# ---------------------------------------------------------------------------
# Endpoint resolution
# ---------------------------------------------------------------------------

def test_resolve_endpoint_v8_node_pool():
    assert es_instr._resolve_endpoint(_V8Transport(nodes=(("h", 9200),))) == "h:9200"


def test_resolve_endpoint_v7_connection_pool():
    assert es_instr._resolve_endpoint(_V7Transport(hosts=("h:9201",))) == "h:9201"


def test_resolve_endpoint_missing_returns_empty():
    class _Bare: pass
    assert es_instr._resolve_endpoint(_Bare()) == ""


def test_resolve_endpoint_follows_node_pool_after_sniff():
    # Sniffing (sniff_on_start / sniff_on_connection_fail) rewrites the node
    # pool at runtime: the first node can be dropped or replaced. The resolved
    # endpoint must track the live pool, not a value memoized from the first
    # request. Before the fix, the first result was cached on the instance and
    # every later span kept reporting the dead/first node.
    transport = _V8Transport(nodes=(("nodeA", 9200),))
    assert es_instr._resolve_endpoint(transport) == "nodeA:9200"

    # nodeA leaves the pool (replaced by a sniffed node).
    transport.node_pool._nodes = [_V8Node("nodeB", 9200)]
    assert es_instr._resolve_endpoint(transport) == "nodeB:9200"


# ---------------------------------------------------------------------------
# Op-name shape
# ---------------------------------------------------------------------------

def test_op_name_falls_back_when_url_missing(push_span):
    sp, rec = push_span

    def wrapped(*_a, **_kw):
        return _V8Response(200)

    es_instr._sync_perform_request_wrapper(
        wrapped, instance=_V8Transport(),
        args=("HEAD",), kwargs={},
    )
    assert (
        "event_start",
        "root",
        "elastic_transport.Transport.perform_request",
    ) in rec.events


def test_sync_wrapper_suppresses_underlying_http_client(push_span):
    """ES transports ride on urllib3/httpx — the generic HTTP-client
    wrappers must be suppressed during perform_request so each ES call
    doesn't also emit a nested HTTP event (or inject Pinpoint headers
    into the ES request)."""
    from pinpoint.instrumentations._util import (
        http_client_instrumentation_suppressed,
    )

    sp, rec = push_span
    seen = {}

    def wrapped(*args, **kwargs):
        seen["suppressed"] = http_client_instrumentation_suppressed()
        return _V8Response(200, {})

    es_instr._sync_perform_request_wrapper(
        wrapped, instance=_V8Transport(),
        args=("GET", "/idx/_search"), kwargs={},
    )
    assert seen["suppressed"] is True
    # Suppression is scoped to the call — cleared afterwards.
    assert http_client_instrumentation_suppressed() is False
