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

"""Distributed-tracing header propagation.

- `extract_pinpoint_headers(source)` — dumps the upstream `Pinpoint-*`
  propagation headers out of a header mapping or reader into the plain
  ``{canonical-name: value}`` dict `Agent.new_span` hands to the native
  ``NewSpan`` overload. Native never reads a Python object per key.
- `inject_items(span)` — returns the outgoing propagation header pairs,
  built entirely on the Python wrappers; the generated child span id flushes
  to native as the event's ``nextSpanId`` when the event ends.

Instrumentations call these — users rarely touch them directly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from collections.abc import Iterable, Mapping

if TYPE_CHECKING:
    from .tracer import Span


def extract_pinpoint_headers(source) -> dict[str, str]:
    """Dump the Pinpoint propagation headers into ``{canonical-name: value}``.

    ``source`` is either a header ``Mapping`` (any key case — HTTP/2 and
    message-queue carriers lowercase them) or one of http_helper's lazy
    readers (case-insensitive ``get``). A reader is asked only for the ten
    :data:`PINPOINT_HEADERS` names, so it stays lazy.
    """
    extracted: dict[str, str] = {}
    if isinstance(source, Mapping):
        for key, value in source.items():
            name = PINPOINT_HEADERS_BY_LOWER.get(str(key).lower())
            if name is not None and value is not None:
                extracted[name] = str(value)
        return extracted
    for name in PINPOINT_HEADERS:
        value = source.get(name)
        if value is not None:
            extracted[name] = str(value)
    return extracted


def inject_items(span: Span) -> Iterable[tuple[str, str]]:
    """Return the span's distributed-tracing headers as ``(key, value)`` pairs.

    Built wrapper-side by :meth:`pinpoint.tracer.Span.inject_context_items`
    (sampled: the full header set for the innermost open span event;
    unsampled: the ``s0`` drop marker; noop: nothing) — no native call.
    """
    if span is None:
        return ()
    return span.inject_context_items()


# The exact Pinpoint header names, so callers don't need the native module
# loaded to know them.
HEADER_TRACE_ID = "Pinpoint-TraceID"
HEADER_SPAN_ID = "Pinpoint-SpanID"
HEADER_PARENT_SPAN_ID = "Pinpoint-pSpanID"
HEADER_SAMPLED = "Pinpoint-Sampled"
HEADER_FLAG = "Pinpoint-Flags"
HEADER_PARENT_APP_NAME = "Pinpoint-pAppName"
HEADER_PARENT_APP_TYPE = "Pinpoint-pAppType"
HEADER_PARENT_APP_NAMESPACE = "Pinpoint-pAppNamespace"
HEADER_PARENT_SERVICE_NAME = "Pinpoint-pServiceName"
HEADER_HOST = "Pinpoint-Host"

# Every propagation header the native context extraction reads, canonical case.
PINPOINT_HEADERS = (
    HEADER_TRACE_ID,
    HEADER_SPAN_ID,
    HEADER_PARENT_SPAN_ID,
    HEADER_SAMPLED,
    HEADER_FLAG,
    HEADER_PARENT_APP_NAME,
    HEADER_PARENT_APP_TYPE,
    HEADER_PARENT_APP_NAMESPACE,
    HEADER_PARENT_SERVICE_NAME,
    HEADER_HOST,
)

# Shared with the Kafka carrier, which resolves the same canonical names out of
# application-supplied byte keys.
PINPOINT_HEADERS_BY_LOWER = {name.lower(): name for name in PINPOINT_HEADERS}
