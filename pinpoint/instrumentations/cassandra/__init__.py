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

"""cassandra-driver instrumentation.

The DataStax Python driver for Apache Cassandra exposes its query API on
``cassandra.cluster.Session``:

- ``execute(query, parameters=None, ...)`` — synchronous, blocking.
- ``execute_async(query, parameters=None, ...)`` — returns a
  ``ResponseFuture``. We wrap the call to record a span event around
  the dispatch; the actual result delivery happens later in the driver's
  IO thread, but the span event itself lives only as long as the call
  to ``execute_async`` (instantaneous in practice — it just queues).
- ``execute_concurrent`` / ``execute_concurrent_with_args`` are helpers
  in ``cassandra.concurrent`` that ultimately call ``Session.execute_async``,
  so the wrapper above covers them transitively.

Connection metadata: the Session is bound to a Cluster, and the cluster's
``contact_points`` list and ``port`` give the destination endpoint. The
``Session.keyspace`` attribute is the equivalent of a database name.

Records against ``SERVICE_TYPE_CASSANDRA = 2601``, the query-execution type;
2600 is the destination type the UI draws the node with, not what a statement
event carries.
"""

from __future__ import annotations

import contextvars
from typing import Any

from ...context import current_span
from ...errors import safe_try
from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_CASSANDRA
from .._util import (
    cached_endpoint,
    limited_repr,
    span_event_scope,
    span_is_sampled,
    sql_bind_values_enabled,
    truncate_text,
    wrap,
)

# Cap on recorded CQL text. Statement reprs can embed bound values (including
# multi-MB blob params), so every branch below routes through this bound rather
# than copying an unbounded string into ``set_sql_query`` on the hot path.
_CQL_MAX_LEN = 1024

_OPERATION_EXECUTE = "cassandra.cluster.Session.execute"
_OPERATION_EXECUTE_ASYNC = "cassandra.cluster.Session.execute_async"

# The driver implements ``Session.execute()`` as ``execute_async(...).result()``, so
# without suppression every sync query emits two nested events with the same
# CQL/params/endpoint annotations.
_in_sync_execute: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "pinpoint_cassandra_in_sync_execute", default=False,
)


class CassandraInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        wrap("cassandra.cluster", "Session.execute", _execute_wrapper)
        wrap(
            "cassandra.cluster", "Session.execute_async",
            _execute_async_wrapper,
        )


def _execute_wrapper(wrapped, instance, args, kwargs):
    """Synchronous query — span event lives for the full network round-trip."""
    span = current_span()
    if span is None:
        return wrapped(*args, **kwargs)
    if not span_is_sampled(span):
        return wrapped(*args, **kwargs)

    cql = _extract_cql(args, kwargs)
    event = span.new_span_event(
        _OPERATION_EXECUTE, service_type=SERVICE_TYPE_CASSANDRA,
    )
    _annotate(event, instance, cql, args, kwargs)

    token = _in_sync_execute.set(True)
    try:
        with span_event_scope(event):
            return wrapped(*args, **kwargs)
    finally:
        _in_sync_execute.reset(token)


def _execute_async_wrapper(wrapped, instance, args, kwargs):
    """Async dispatch — the span event covers the *call* (which queues
    the request), not the eventual result. The Cassandra driver runs its
    own IO thread; tracing the future's resolution requires hooking the
    callback chain, which is out of scope for the initial integration."""
    if _in_sync_execute.get():
        # Called from inside the wrapped Session.execute — that wrapper
        # already recorded this query; a second event would be a duplicate.
        return wrapped(*args, **kwargs)
    span = current_span()
    if span is None:
        return wrapped(*args, **kwargs)
    if not span_is_sampled(span):
        return wrapped(*args, **kwargs)

    cql = _extract_cql(args, kwargs)
    event = span.new_span_event(
        _OPERATION_EXECUTE_ASYNC, service_type=SERVICE_TYPE_CASSANDRA,
    )
    _annotate(event, instance, cql, args, kwargs)

    with span_event_scope(event):
        return wrapped(*args, **kwargs)


# ---- helpers ---------------------------------------------------------------


def _extract_cql(args, kwargs) -> Any:
    """``Session.execute(query, parameters=None, ...)`` — the first
    positional/keyword arg is the CQL statement (or a Statement subclass)."""
    if args:
        return args[0]
    # Membership, not truthiness: ``BatchStatement`` defines ``__len__``, so
    # an empty batch passed by keyword would otherwise fall through to None.
    if "query" in kwargs:
        return kwargs["query"]
    return kwargs.get("statement")


@safe_try
def _annotate(event, session, cql, args, kwargs) -> None:
    # contact_points + port are fixed for a session's cluster, so the joined
    # endpoint is memoized on the session (this runs per statement).
    endpoint = cached_endpoint(session, _resolve_session_endpoint) or ""
    # keyspace can change over a session (``USE``), so read it fresh.
    keyspace = str(getattr(session, "keyspace", "") or "")
    if endpoint:
        event.set_end_point(endpoint)
    if keyspace:
        event.set_destination(keyspace)
    elif endpoint:
        event.set_destination(endpoint)

    cql_text = _cql_text(cql)
    params = args[1] if len(args) > 1 else kwargs.get("parameters")
    params_text = ""
    if params and sql_bind_values_enabled(event):
        # Bound values may hold PII/secrets — capture only when enabled
        # (default off; see Config.sql_trace_bind_values).
        params_text = limited_repr(params, 1024)
    event.set_sql_query(cql_text, params_text)


def _resolve_session_endpoint(session):
    """``(contact_points, port)`` of the session's cluster, joined for
    ``cached_endpoint``; ``("", 0)`` when the session exposes no cluster."""
    cluster = getattr(session, "cluster", None)
    contact_points = getattr(cluster, "contact_points", None) or ()
    return ",".join(str(cp) for cp in contact_points if cp), getattr(cluster, "port", 0) or 0


def _cql_text(cql) -> str:
    """``cql`` may be a plain string or a ``Statement``: SimpleStatement carries
    the CQL on ``.query_string``, a BoundStatement on its prepared statement's.
    Always capped — statement text can embed bound values."""
    text = cql if isinstance(cql, str) else (
        getattr(cql, "query_string", None)
        or getattr(getattr(cql, "prepared_statement", None), "query_string", None))
    if isinstance(text, str):
        return truncate_text(text, _CQL_MAX_LEN)
    # limited_repr, never bare repr(): a BoundStatement's repr embeds its bound
    # values, so a large blob parameter would materialize a multi-MB transient.
    return "" if cql is None else limited_repr(cql, _CQL_MAX_LEN)


def instrument() -> None:
    CassandraInstrumentor().instrument()
