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

"""elasticsearch (official Python client) instrumentation.

Wraps the transport-layer ``perform_request`` rather than every client method,
which covers search/index/get/bulk/... with one hook and stays stable across
versions. Two lineages expose it: 7.x ships ``elasticsearch.Transport`` in the
package, 8.x split it into ``elastic_transport`` (``Transport`` /
``AsyncTransport``). Whichever is importable gets wrapped.

The recorded DSL is truncated to 256 characters so the annotation stays small
on the wire.
"""

from __future__ import annotations

import functools
import json
from urllib.parse import parse_qs, urlparse

from ..._log import get_logger
from ...annotation import (
    ANNOTATION_ELASTICSEARCH_DSL,
    ANNOTATION_ELASTICSEARCH_VERSION,
    ANNOTATION_HTTP_STATUS_CODE,
)
from ...context import current_span
from ...errors import safe_try
from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_ELASTICSEARCH
from .._util import (
    limited_structure,
    span_event_scope,
    span_is_sampled,
    suppress_http_client_instrumentation,
    truncate_text,
    wrap,
)

_log = get_logger("elasticsearch")
_OPERATION_ELASTIC_TRANSPORT = "elastic_transport.Transport.perform_request"
_OPERATION_ELASTIC_ASYNC_TRANSPORT = "elastic_transport.AsyncTransport.perform_request"
_OPERATION_ES_TRANSPORT = "elasticsearch.Transport.perform_request"

# Characters of DSL recorded per request; the rest is elided.
_MAX_DSL_LENGTH = 256
_MAX_JSON_ITEMS = 8
_MAX_JSON_DEPTH = 4


class ElasticsearchInstrumentor(BaseInstrumentor):
    def _instrument(self) -> None:
        # v8+: transport moved out into a standalone package. Wrap both
        # sync and async transports — the client picks one per session.
        # The wrappers' ``operation`` defaults already name these targets.
        # ``wrap`` never raises: a missing module/attr logs debug and no-ops.
        wrap(
            "elastic_transport", "Transport.perform_request",
            _sync_perform_request_wrapper,
        )
        wrap(
            "elastic_transport", "AsyncTransport.perform_request",
            _async_perform_request_wrapper,
        )
        # v7.x: transport lives at ``elasticsearch.Transport``. Present
        # both as a fallback (older deployments) and alongside v8 (some
        # users keep v7 pinned via a vendor wrapper).
        wrap(
            "elasticsearch", "Transport.perform_request",
            _v7_sync_perform_request_wrapper,
        )


def _sync_perform_request_wrapper(
    wrapped, instance, args, kwargs, *,
    operation: str = _OPERATION_ELASTIC_TRANSPORT,
):
    span = current_span()
    if span is None:
        return wrapped(*args, **kwargs)
    # ES transports ride on urllib3/requests, so suppress the nested HTTP-client
    # instrumentation for the WHOLE call — unsampled path and event-setup-failure
    # fallback included. Opening the event outside this scope would let a
    # ``_open_event`` failure reach safe_wrapper's un-suppressed retry, emitting a
    # stray HTTP event and injecting headers into the ES request.
    with suppress_http_client_instrumentation():
        if not span_is_sampled(span):
            return wrapped(*args, **kwargs)
        try:
            event = _open_event(span, instance, args, kwargs, operation)
        except Exception:  # noqa: BLE001
            return wrapped(*args, **kwargs)
        with span_event_scope(event):
            result = wrapped(*args, **kwargs)
            _annotate_cluster_info(event, instance, result)
            _annotate_response(event, result)
            return result


async def _async_perform_request_wrapper(
    wrapped, instance, args, kwargs, *,
    operation: str = _OPERATION_ELASTIC_ASYNC_TRANSPORT,
):
    span = current_span()
    if span is None:
        return await wrapped(*args, **kwargs)
    # See the sync wrapper: hold the suppression across the whole call so every path
    # to ``wrapped()`` is covered. Unguarded by safe_wrapper (async body), so the
    # ``_open_event`` failure is handled here, not by an un-suppressed retry.
    with suppress_http_client_instrumentation():
        if not span_is_sampled(span):
            return await wrapped(*args, **kwargs)
        try:
            event = _open_event(span, instance, args, kwargs, operation)
        except Exception:  # noqa: BLE001
            return await wrapped(*args, **kwargs)
        with span_event_scope(event):
            result = await wrapped(*args, **kwargs)
            _annotate_cluster_info(event, instance, result)
            _annotate_response(event, result)
            return result


# v7's transport reuses the sync wrapper with only the operation name bound.
# safe_wrapper logs ``fn.__qualname__`` on failure, and partials have none.
_v7_sync_perform_request_wrapper = functools.partial(
    _sync_perform_request_wrapper, operation=_OPERATION_ES_TRANSPORT,
)
_v7_sync_perform_request_wrapper.__qualname__ = "_v7_sync_perform_request_wrapper"


def _open_event(span, instance, args, kwargs, operation: str):
    url = _extract_url(args, kwargs)
    event = span.new_span_event(operation, service_type=SERVICE_TYPE_ELASTICSEARCH)
    event.set_destination("ElasticSearch")
    _annotate_endpoint(event, instance)
    _annotate_dsl(event, url, kwargs)
    return event


def _extract_url(args, kwargs) -> str:
    """``perform_request`` takes ``(method, url_or_target, ...)`` in both
    v7 (positional ``params``/``body``) and v8 (keyword-only after the
    target). Pulling the second positional — falling back to keyword
    names — covers either shape without importing the libraries here."""
    if len(args) >= 2 and args[1] is not None:
        return str(args[1])
    if "target" in kwargs and kwargs["target"] is not None:
        return str(kwargs["target"])
    if "url" in kwargs and kwargs["url"] is not None:
        return str(kwargs["url"])
    return ""


@safe_try
def _annotate_endpoint(event, instance) -> None:
    endpoint = _resolve_endpoint(instance)
    if endpoint:
        event.set_end_point(endpoint)


def _resolve_endpoint(instance) -> str:
    """Best-effort host:port lookup off the transport instance.

    v8 ``elastic_transport.Transport`` exposes a ``node_pool`` whose
    nodes carry ``host``/``port``. v7 ``elasticsearch.Transport`` keeps
    a ``connection_pool.connections`` list where each connection
    stringifies host:port on the ``host`` attribute already.

    Resolve from the live node/connection pool on every call — do NOT memoize
    on the instance. With client-side sniffing (``sniff_on_start`` /
    ``sniff_on_connection_fail``) the pool is rewritten at runtime: the first
    node can be dropped or replaced by sniffed nodes. A cached endpoint would
    pin every later span to that first (possibly dead) node. The walk stops at
    the first usable node, so the per-call cost is bounded and small.
    """
    node_pool = getattr(instance, "node_pool", None)
    if node_pool is not None:
        nodes = _iter_node_pool(node_pool)
        for node in nodes:
            host = getattr(node, "host", None) or ""
            port = getattr(node, "port", None)
            if host and port:
                return f"{host}:{port}"
            if host:
                return str(host)

    pool = getattr(instance, "connection_pool", None)
    if pool is not None:
        connections = getattr(pool, "connections", None) or []
        for conn in connections:
            host = getattr(conn, "host", None)
            if host:
                return str(host)
    return ""


def _iter_node_pool(node_pool):
    """``NodePool.all()`` (elastic_transport 8.x) returns an iterable of
    nodes; older snapshots expose the nodes via ``__iter__``. Try the
    canonical accessor first, then fall back.

    Returns the accessor's iterable as-is — the caller only reads until
    the first usable node, so copying the whole pool into a list per
    request would be waste. A lazily-raised iteration error surfaces in
    the caller, which sits under ``@safe_try``.
    """
    for attempt in ("all", "alive"):
        accessor = getattr(node_pool, attempt, None)
        if callable(accessor):
            try:
                nodes = accessor()
            except Exception:  # noqa: BLE001
                continue
            if nodes is not None:
                return nodes
    return node_pool


@safe_try
def _annotate_dsl(event, url: str, kwargs) -> None:
    dsl = _extract_dsl(url, kwargs)
    if dsl:
        event.annotate_string(ANNOTATION_ELASTICSEARCH_DSL, dsl)


@safe_try
def _annotate_cluster_info(event, instance, result) -> None:
    cluster_info = _extract_cluster_info(instance, result)
    if cluster_info:
        event.annotate_string(ANNOTATION_ELASTICSEARCH_VERSION, cluster_info)


def _extract_cluster_info(instance, result) -> str:
    # Constant per transport: parsed once from the ``GET /`` info body and
    # stamped on the instance so later requests skip the body walk.
    stamped = getattr(instance, "_pinpoint_cluster_info", None)
    if stamped:
        return stamped
    resolved = _cluster_info_from_body(result)
    if resolved:
        try:
            instance._pinpoint_cluster_info = resolved
        except Exception:  # noqa: BLE001
            pass
    return resolved


def _cluster_info_from_body(result) -> str:
    body = getattr(result, "body", result)
    if not isinstance(body, dict):
        return ""
    cluster_name = body.get("cluster_name") or body.get("clusterName") or ""
    version = body.get("version") or {}
    if isinstance(version, dict):
        version = version.get("number") or version.get("build_flavor") or ""
    if cluster_name and version:
        return f"{cluster_name}/{version}"
    if cluster_name:
        return str(cluster_name)
    if version:
        return str(version)
    return ""


def _extract_dsl(url: str, kwargs) -> str:
    """Pick a representative DSL string for the trace.

    Prefer the request body — that's where the query/aggregation lives for
    search/index/update/bulk. Fall back to the ``q`` URL parameter, which is
    all a URI-style search like ``/index/_search?q=foo`` carries."""
    body = kwargs.get("body", None)
    text = _stringify_body(body)
    if not text:
        text = _q_param(url, kwargs)
    if not text:
        return ""
    return truncate_text(text, _MAX_DSL_LENGTH)


def _stringify_body(body, max_len: int = _MAX_DSL_LENGTH) -> str:
    if body is None:
        return ""
    if isinstance(body, dict):
        return _json_preview(body, max_len)
    if isinstance(body, (list, tuple)):
        # _bulk and friends pass a list of dicts which the client
        # serializes as NDJSON. Render the same shape for the trace.
        lines = []
        used = 0
        for item in body:
            remaining = max_len - used
            if remaining <= 0:
                break
            line = (
                _json_preview(item, remaining)
                if isinstance(item, dict)
                else _decode(item, remaining)
            )
            if not line:
                continue
            lines.append(line)
            used += len(line) + 1
        return truncate_text("\n".join(lines), max_len)
    return _decode(body, max_len)


def _decode(value, max_len: int = _MAX_DSL_LENGTH) -> str:
    if isinstance(value, (bytes, bytearray)):
        return truncate_text(
            bytes(value[: max_len * 4]).decode("utf-8", "replace"), max_len)
    return truncate_text(str(value), max_len)


def _json_preview(value, max_len: int) -> str:
    if max_len <= 0:
        return ""
    try:
        rendered = json.dumps(
            limited_structure(
                value, max_chars=max_len, max_depth=_MAX_JSON_DEPTH,
                max_items=_MAX_JSON_ITEMS, max_string=max_len,
            ),
            ensure_ascii=False,
            default=str,
        )
        return truncate_text(rendered, max_len)
    except Exception:  # noqa: BLE001
        return ""


def _q_param(url: str, kwargs) -> str:
    params = kwargs.get("params") or {}
    if isinstance(params, dict):
        q = params.get("q")
        if q:
            return str(q)
    if url and "?" in url:
        try:
            qs = parse_qs(urlparse(url).query)
        except Exception:  # noqa: BLE001
            return ""
        val = qs.get("q")
        if val:
            return val[0] if isinstance(val, list) and val else str(val)
    return ""


@safe_try
def _annotate_response(event, result) -> None:
    """v8 returns an ``ApiResponse`` with ``meta.status``; v7 returns the
    decoded body directly and exposes no status from here."""
    meta = getattr(result, "meta", None)
    if meta is None:
        return
    status = getattr(meta, "status", None)
    if status is None:
        return
    try:
        event.annotate_int(ANNOTATION_HTTP_STATUS_CODE, int(status))
    except (TypeError, ValueError):
        pass


def instrument() -> None:
    ElasticsearchInstrumentor().instrument()
