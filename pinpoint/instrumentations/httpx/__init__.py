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

"""`httpx` HTTP client instrumentation.

Wraps both ``httpx.Client.send`` and ``httpx.AsyncClient.send`` so every
outbound request becomes a span event on the current transaction, with
Pinpoint-* headers injected. The two ``send`` methods both live in
``httpx._client`` and both receive a fully-built ``Request`` — the same
hook point requests uses.
"""

from __future__ import annotations

from urllib.parse import urlparse

from ..._log import get_logger
from ...context import current_span
from ...errors import safe_try
from ...http_helper import (
    trace_http_client_request,
)
from ...instrumentor import BaseInstrumentor
from .._util import (
    annotate_client_response,
    http_client_instrumentation_suppressed,
    open_client_send,
    rebind_http_response_request,
    span_event_scope,
    wrap,
)

_log = get_logger("httpx")
_OPERATION_CLIENT_SEND = "httpx.Client.send"
_OPERATION_ASYNC_CLIENT_SEND = "httpx.AsyncClient.send"


class HttpxInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        wrap("httpx._client", "Client.send", _send_wrapper)
        wrap("httpx._client", "AsyncClient.send", _async_send_wrapper)


def _send_wrapper(wrapped, instance, args, kwargs):
    # A higher-level client (e.g. elasticsearch) already represents this
    # HTTP exchange — don't emit a duplicate generic event underneath it.
    if http_client_instrumentation_suppressed():
        return wrapped(*args, **kwargs)
    span = current_span()
    request = args[0] if args else kwargs.get("request")
    if span is None or request is None:
        return wrapped(*args, **kwargs)
    try:
        event, send_request, send_args, send_kwargs, sampled = open_client_send(
            span, _OPERATION_CLIENT_SEND, request, args, kwargs, _annotate_request)
    except Exception:  # noqa: BLE001
        _log.debug("httpx instrumentation failed", exc_info=True)
        return wrapped(*args, **kwargs)

    if not sampled:
        response = wrapped(*send_args, **send_kwargs)
    else:
        with span_event_scope(event):
            response = wrapped(*send_args, **send_kwargs)
            annotate_client_response(event, response)
    rebind_http_response_request(response, request, send_request)
    return response


async def _async_send_wrapper(wrapped, instance, args, kwargs):
    # See _send_wrapper — suppressed under higher-level client wrappers. The
    # user ``await`` stays outside the setup guard, so it is never swallowed
    # nor awaited twice.
    if http_client_instrumentation_suppressed():
        return await wrapped(*args, **kwargs)
    span = current_span()
    request = args[0] if args else kwargs.get("request")
    if span is None or request is None:
        return await wrapped(*args, **kwargs)
    try:
        event, send_request, send_args, send_kwargs, sampled = open_client_send(
            span, _OPERATION_ASYNC_CLIENT_SEND, request, args, kwargs,
            _annotate_request)
    except Exception:  # noqa: BLE001
        _log.debug("httpx async instrumentation failed", exc_info=True)
        return await wrapped(*args, **kwargs)

    if not sampled:
        response = await wrapped(*send_args, **send_kwargs)
    else:
        with span_event_scope(event):
            response = await wrapped(*send_args, **send_kwargs)
            annotate_client_response(event, response)
    rebind_http_response_request(response, request, send_request)
    return response


@safe_try
def _annotate_request(event, request) -> None:
    url = getattr(request, "url", None)
    url_str = str(url or "")
    # httpx.URL already parsed host/port; urlparse only for the plain-string
    # fallback.
    host = getattr(url, "host", None)
    if host:
        port = getattr(url, "port", None)
        host = f"{host}:{port}" if port else host
    else:
        host = urlparse(url_str).netloc or ""
    trace_http_client_request(
        event, host, url_str, getattr(request, "headers", None))


def instrument() -> None:
    HttpxInstrumentor().instrument()
