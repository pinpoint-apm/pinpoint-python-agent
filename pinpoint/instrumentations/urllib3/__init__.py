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

"""`urllib3` instrumentation (also covers requests' transport).

For most deployments `requests` is the user-facing client; but users who use
urllib3 directly still benefit. Wraps `urllib3.connectionpool.HTTPConnectionPool.urlopen`.
"""

from __future__ import annotations

from ...context import current_span
from ...http_helper import (
    trace_http_client_request,
)
from ..._log import get_logger
from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_PYTHON_HTTP_CLIENT
from .._util import (
    annotate_client_response,
    http_client_instrumentation_suppressed,
    inject_http_headers,
    replace_arg,
    span_event_scope,
    span_is_sampled,
    wrap,
)

_log = get_logger("urllib3")
_OPERATION_URLOPEN = "urllib3.connectionpool.HTTPConnectionPool.urlopen"

_URLOPEN_HEADERS_POS = 3  # index of ``headers`` in HTTPConnectionPool.urlopen(
                          # method, url, body, headers, ...).


class Urllib3Instrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        wrap("urllib3.connectionpool", "HTTPConnectionPool.urlopen", _urlopen_wrapper)


def _urlopen_wrapper(wrapped, instance, args, kwargs):
    if http_client_instrumentation_suppressed():
        return wrapped(*args, **kwargs)

    span = current_span()
    if span is None:
        return wrapped(*args, **kwargs)

    # A private ``headers`` dict to inject into: inject must write trace headers
    # regardless of sampling so downstream servers can stitch into the trace.
    #
    # ``urlopen`` takes ``headers`` positionally, so the slot is whichever of
    # ``args[3]`` / ``kwargs['headers']`` the caller used — writing the kwarg
    # unconditionally would crash with "got multiple values". The copies also keep a
    # ``safe_wrapper`` retry from seeing the injected headers.
    #
    # With no caller headers, ``urlopen`` falls back to the pool's ``self.headers``;
    # passing an explicit dict would suppress that and silently drop pool defaults
    # like ``Authorization``. So seed from the pool's own defaults in that case,
    # preserving urllib3's ``headers is None -> self.headers`` semantics.
    current = (args[_URLOPEN_HEADERS_POS]
               if len(args) > _URLOPEN_HEADERS_POS
               else kwargs.get("headers"))
    headers = _seed_headers(current, instance)
    new_args, new_kwargs = replace_arg(
        args, kwargs, _URLOPEN_HEADERS_POS, "headers", headers)

    # Open the span event first so the trace context written into outbound
    # headers reflects this RPC's position (depth/sequence) in the trace. An
    # unsampled span hands back the shared no-op event.
    event = span.new_span_event(
        _OPERATION_URLOPEN,
        service_type=SERVICE_TYPE_PYTHON_HTTP_CLIENT,
    )
    inject_http_headers(span, headers)

    if not span_is_sampled(span):
        return wrapped(*new_args, **new_kwargs)

    # endpoint/full_url are consumed only by the sampled trace call below, so
    # defer their construction (+ the host/port/scheme getattrs) until here
    # instead of paying it on every span-carrying but unsampled call.
    # Guarded: a failure here would reach safe_wrapper's untraced retry with
    # the event still open on the span's stack. A thinner event is fine.
    try:
        url = args[1] if len(args) > 1 else kwargs.get("url", "")
        host = getattr(instance, "host", "") or ""
        port = getattr(instance, "port", None)
        endpoint = f"{host}:{port}" if port else host
        full_url = f"{getattr(instance, 'scheme', 'http')}://{endpoint}{url}"
        trace_http_client_request(event, endpoint, full_url, headers)
    except Exception:  # noqa: BLE001
        _log.debug("urllib3 request annotation failed", exc_info=True)

    with span_event_scope(event):
        response = wrapped(*new_args, **new_kwargs)

        annotate_client_response(event, response)
        return response


def _seed_headers(provided, instance):
    """Build the private headers mapping to inject into.

    Copy the caller's headers when supplied; otherwise seed from the pool's
    default headers (``instance.headers``) so urllib3's ``headers is None ->
    self.headers`` fallback is preserved. Never mutates ``instance.headers``.

    An ``urllib3.HTTPHeaderDict`` — whether caller-supplied or the pool's
    default — is copied *in-kind* rather than coerced through ``dict(...)``:
    ``dict(HTTPHeaderDict)`` routes each field through ``__getitem__``, which
    comma-joins repeated values (two ``Cookie`` lines collapse into one
    wrong-delimiter line). ``.copy()`` keeps the multi-valued structure so
    urllib3 emits the original header lines, and :func:`inject_http_headers`
    writes trace keys via ``__setitem__`` on either container type (replace,
    not append — no duplicate ``Pinpoint-*`` lines).
    """
    source = provided if provided is not None else getattr(instance, "headers",
                                                            None)
    if source is None:
        return {}
    if hasattr(source, "getlist") and hasattr(source, "copy"):
        return source.copy()
    return dict(source)


def instrument() -> None:
    Urllib3Instrumentor().instrument()
