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

"""`requests` HTTP client instrumentation.

Wraps `requests.sessions.Session.send` so every outbound request becomes a
span event on the current transaction, with Pinpoint-* headers injected.
"""

from __future__ import annotations

from urllib.parse import urlsplit

from ...context import current_span
from ...http_helper import (
    trace_http_client_request,
)
from ...instrumentor import BaseInstrumentor
from ...errors import safe_try
from .._util import (
    annotate_client_response,
    http_client_instrumentation_suppressed,
    open_client_send,
    rebind_http_response_request,
    span_event_scope,
    suppress_http_client_instrumentation,
    wrap,
)

_OPERATION_SEND = "requests.sessions.Session.send"


class RequestsInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        wrap("requests.sessions", "Session.send", _send_wrapper)


def _send_wrapper(wrapped, instance, args, kwargs):
    # A higher-level client (elasticsearch's RequestsHttpNode) already represents
    # this exchange and opened a suppress_http_client scope: no duplicate
    # Session.send event underneath it, and no header injection into its request.
    if http_client_instrumentation_suppressed():
        return wrapped(*args, **kwargs)
    span = current_span()
    request = args[0] if args else kwargs.get("request")
    if span is None or request is None:
        return wrapped(*args, **kwargs)
    try:
        event, send_request, send_args, send_kwargs, sampled = open_client_send(
            span, _OPERATION_SEND, request, args, kwargs, _annotate_request)
    except Exception:  # noqa: BLE001
        # Untraced fallback, still suppressed: the urllib3 layer underneath
        # would otherwise emit the event this one failed to open.
        with suppress_http_client_instrumentation():
            return wrapped(*args, **kwargs)

    # Session.send rides on urllib3, so suppress its instrumentation for the
    # send: one HTTP node per call in the UI.
    if not sampled:
        with suppress_http_client_instrumentation():
            response = wrapped(*send_args, **send_kwargs)
    else:
        with span_event_scope(event):
            with suppress_http_client_instrumentation():
                response = wrapped(*send_args, **send_kwargs)
            annotate_client_response(event, response)
    rebind_http_response_request(response, request, send_request)
    return response


@safe_try
def _annotate_request(event, request) -> None:
    url = getattr(request, "url", "") or ""
    trace_http_client_request(
        event, urlsplit(url).netloc, url, getattr(request, "headers", None))


def instrument() -> None:
    RequestsInstrumentor().instrument()
