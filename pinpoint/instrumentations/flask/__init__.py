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

"""Flask WSGI instrumentation.

Hooks `Flask.wsgi_app` as the outermost entry so every request becomes a root
Pinpoint transaction. Upstream `Pinpoint-*` headers are honored; responses
don't write them back (distributed tracing propagates only outbound).
"""

from __future__ import annotations

from ...context import current_span
from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_PYTHON_METHOD
from .._util import (
    cached_operation_name,
    control_flow_predicate,
    no_current_span,
    span_event_scope,
    span_is_sampled,
    wrap,
)
from ..wsgi import wsgi_entry_wrapper

_OPERATION_SERVER = "Flask HTTP Server"

# flask.request is one stable LocalProxy for the process's life; bound once at
# instrument time (flask is imported — the post-import hook fired) instead of
# re-imported two to three times per request.
_flask_request = None


class FlaskInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        global _flask_request
        # Import from ``flask.globals``, not ``flask``: the post-import hook fires
        # while ``flask/__init__.py`` is still executing ``from .app import
        # Flask``, so ``flask.request`` is not bound yet and ``from flask import
        # request`` raises ImportError (partially initialized module) — which
        # _safe_install would swallow, silently leaving Flask uninstrumented.
        # ``flask.app`` imports ``flask.globals`` itself, so it is complete here.
        from flask.globals import request as _flask_request
        wrap("flask.app", "Flask.wsgi_app", _wsgi_app_wrapper)
        wrap("flask.app", "Flask.dispatch_request", _dispatch_request_wrapper)
        # Sees the finished Response (normal and error-handler paths both funnel
        # through it), where werkzeug still knows whether the body is buffered —
        # by the time the WSGI layer gets the ClosingIterator, it doesn't.
        wrap("flask.app", "Flask.finalize_request", _finalize_request_wrapper,
             precheck=no_current_span)


_wsgi_app_wrapper = wsgi_entry_wrapper(_OPERATION_SERVER)


def _dispatch_request_wrapper(wrapped, instance, args, kwargs):
    """Wrap `Flask.dispatch_request` so the resolved view function shows up
    as its own span event named after the Python method actually executed.

    Also stashes the matched URL rule (e.g. ``/items/<int:item_id>``) on the
    request environ so the shared WSGI finalizer can hand it to `set_url_stat`
    — that way URL-stat aggregation buckets per route template, not per
    concrete path.
    """
    span = current_span()
    if span is None:
        return wrapped(*args, **kwargs)

    if not span_is_sampled(span):
        # Unsampled: no span event, so only the route template matters — and only
        # when URL stats are collected. The operation name ``_resolve_view`` also
        # returns is discarded (one dict lookup, not worth a second resolver).
        if span._collect_url_stat:
            url_pattern = _resolve_view(instance)[1]
            if url_pattern is not None:
                _stash_url_pattern(url_pattern)
        return wrapped(*args, **kwargs)

    operation_name, url_pattern = _resolve_view(instance)
    if url_pattern is not None:
        _stash_url_pattern(url_pattern)

    if operation_name is None:
        return wrapped(*args, **kwargs)

    event = span.new_span_event(operation_name, service_type=SERVICE_TYPE_PYTHON_METHOD)
    # ``flask.abort(404)`` raises a werkzeug HTTPException out of dispatch_request;
    # a sub-500 one is control flow, not a view error. Only 5xx / non-HTTP exceptions
    # flag the event; the root span captures the status either way.
    with span_event_scope(event, _is_control_flow_exception):
        return wrapped(*args, **kwargs)


# flask.abort(404) raises a werkzeug HTTPException subclass with the status
# on ``.code``; the module gate keeps foreign HTTPException classes real errors.
_is_control_flow_exception = control_flow_predicate(
    "HTTPException", module_prefix="werkzeug", status_attrs=("code",))


def _finalize_request_wrapper(wrapped, instance, args, kwargs):
    response = wrapped(*args, **kwargs)
    _stash_buffered_hint(response)
    return response


def _stash_buffered_hint(response) -> None:
    """Mark the environ when the response body is already materialized, so the
    shared WSGI root finalizes eagerly instead of handing the drain to an async
    child span — werkzeug always hides the body behind a ClosingIterator, which
    the WSGI layer cannot classify. Streamed (``is_streamed``) and passthrough
    (``send_file``) bodies keep the hand-off. The trade: the ClosingIterator's
    teardown callbacks then run after the root span ended — bookkeeping, for a
    buffered body."""
    try:
        span = current_span()
        if span is None or not span_is_sampled(span):
            return
        if response.is_streamed or response.direct_passthrough:
            return
        _flask_request.environ["pinpoint.response_buffered"] = True
    except Exception:  # noqa: BLE001
        pass


def _stash_url_pattern(url_pattern: str) -> None:
    """Stash the route template for shared WSGI URL-stat finalization."""
    try:
        _flask_request.environ["pinpoint.url_pattern"] = url_pattern
    except Exception:  # noqa: BLE001
        pass


def _resolve_view(app):
    """Return (operation_name, url_pattern) for the currently-matched route.

    Returns (None, None) if no rule matched (404 etc.).
    """
    try:
        rule = _flask_request.url_rule
        if rule is None:
            return None, None
        endpoint = rule.endpoint
        url_pattern = rule.rule
        view_func = app.view_functions.get(endpoint)
        if view_func is None:
            return endpoint, url_pattern
        return cached_operation_name(view_func, default=endpoint), url_pattern
    except Exception:  # noqa: BLE001
        return None, None


def instrument() -> None:
    """Module-level entry point (called by autoload post-import hook)."""
    FlaskInstrumentor().instrument()
