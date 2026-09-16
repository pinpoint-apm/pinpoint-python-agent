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

"""Pyramid instrumentation.

Two hooks:

1. ``pyramid.router.Router.__call__`` — top-level WSGI entry; opens the
   Pinpoint root span for the full request lifecycle.
2. ``_call_view`` — Pyramid's internal helper that resolves and invokes
   the matched view callable. We wrap it to (a) emit a Python-method
   span event named after the view qualname and (b) lift the matched
   route pattern onto ``request.environ['pinpoint.url_pattern']`` for
   url_stat aggregation.

The ``_call_view`` wrap is installed on **both** the ``pyramid.router``
and ``pyramid.view`` module bindings. ``pyramid.router`` does
``from pyramid.view import _call_view`` at import time and
``Router.handle_request`` resolves the name through its own module
globals, so wrapping only ``pyramid.view`` would miss every routed
request.
"""

from __future__ import annotations

from ...context import current_span
from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_PYTHON_METHOD
from .._util import (
    cached_operation_name,
    control_flow_predicate,
    span_event_scope,
    span_is_sampled,
    wrap,
)
from ..wsgi import wsgi_entry_wrapper

_OPERATION_SERVER = "Pyramid HTTP Server"


class PyramidInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        wrap("pyramid.router", "Router.__call__", _router_call_wrapper)
        # ``_call_view`` is the helper that invokes the user's view callable, but
        # ``pyramid.router`` imports it by value at module load and resolves it
        # through its own globals — so wrapping only ``pyramid.view._call_view`` would
        # miss the routing path. Wrap both bindings to cover normal request flow and
        # direct ``render_view_to_response()`` callers.
        wrap("pyramid.router", "_call_view", _call_view_wrapper)
        wrap("pyramid.view", "_call_view", _call_view_wrapper)


# ---- root WSGI span --------------------------------------------------------

_router_call_wrapper = wsgi_entry_wrapper(_OPERATION_SERVER)


# ---- per-view span event ---------------------------------------------------

def _call_view_wrapper(wrapped, instance, args, kwargs):
    """``pyramid.view._call_view`` is the internal helper that dispatches
    to a registered view callable. We bracket it with a span event named
    after the view + lift the matched route pattern onto the environ."""
    span = current_span()
    if span is None:
        return wrapped(*args, **kwargs)

    request = _extract_request(args, kwargs)
    sampled = span_is_sampled(span)
    # Unsampled: only the route template matters (URL-stat bucketing), and
    # only while URL stats are collected.
    if not sampled and not span._collect_url_stat:
        return wrapped(*args, **kwargs)

    op_name, route_pattern = _resolve_view_name_and_route(request)
    if route_pattern and request is not None:
        try:
            request.environ["pinpoint.url_pattern"] = route_pattern
        except Exception:  # noqa: BLE001
            pass

    if not sampled or op_name is None:
        return wrapped(*args, **kwargs)

    event = span.new_span_event(op_name, service_type=SERVICE_TYPE_PYTHON_METHOD)
    # ``raise HTTPNotFound()`` doubles as a WSGI response, so a sub-500 one is
    # control flow, not a view error. Only 5xx / non-HTTP exceptions flag the event;
    # the root span captures the status either way.
    with span_event_scope(event, _is_control_flow_exception):
        return wrapped(*args, **kwargs)


# pyramid views raise HTTPNotFound() and friends (status on ``.code``) as the
# normal way to send a non-2xx response; the module gate keeps foreign
# HTTPException classes real errors.
_is_control_flow_exception = control_flow_predicate(
    "HTTPException", module_prefix="pyramid", status_attrs=("code",))


# ---- helpers ---------------------------------------------------------------


def _extract_request(args, kwargs):
    """``_call_view``'s signature varies across Pyramid versions, so find the
    request by the ``request`` keyword or by the positional carrying
    ``matched_route`` / ``environ``."""
    if "request" in kwargs:
        return kwargs["request"]
    for cand in args:
        if hasattr(cand, "matched_route") or hasattr(cand, "environ"):
            return cand
    return None


def _resolve_view_name_and_route(request):
    """Return (operation_name, route_pattern). Either may be None."""
    if request is None:
        return None, None
    op_name = None
    route_pattern = None
    matched = getattr(request, "matched_route", None)
    if matched is not None:
        # Pyramid Route exposes .name (logical name) and .pattern (template).
        route_pattern = getattr(matched, "pattern", None)
        op_name = getattr(matched, "name", None)
    # A more user-meaningful name is the view callable's qualname when
    # Pyramid attaches it to the request as `_view_callable_` (>=2.0) or
    # via registry introspection. Fall back to route name.
    view = getattr(request, "_view_callable_", None) or getattr(
        request, "view", None
    )
    candidate = cached_operation_name(view, default=None)
    if candidate:
        op_name = candidate
    return op_name, route_pattern


def instrument() -> None:
    PyramidInstrumentor().instrument()
