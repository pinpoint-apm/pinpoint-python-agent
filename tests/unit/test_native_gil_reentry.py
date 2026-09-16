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

"""Real-binding coverage for GIL-released span creation paths.

``new_span`` releases the GIL around the native admission, context extraction
and active-span work — the header dict is materialized into a C++ map during
argument conversion, so extraction never re-enters the interpreter. Async-span
creation also releases the GIL around API-cache work; its returned shared
handle must keep its lifecycle. Span events never cross the binding
individually: they are replayed as completed records inside
``end_span_with_data``.
"""

import time

import pytest

from pinpoint.propagator import HEADER_TRACE_ID
from tests.integration._collector import start_native_agent

_EXTRA_YAML = """Span:
  MaxEventDepth: 7
  MaxEventSequence: 11
Http:
  Server:
    RecordRequestHeader: ["X-Test"]
    RecordRequestParam: true
    RealIpHeader: ["CF-Connecting-IP"]
    RealIpEmptyValue: "unknown"
  Client:
    RecordUrlQuery: true
Sql:
  TraceBindValue: false
"""


@pytest.fixture(scope="module")
def native_agent():
    with start_native_agent("unit-native-gil", _EXTRA_YAML) as agent:
        yield agent


def test_new_span_extracts_inbound_context(native_agent):
    span, sampled, trace_id, span_id, revision = native_agent.new_span(
        "op", "/rpc", {HEADER_TRACE_ID: "upstream-agent^1700000000^7",
                       "Pinpoint-SpanID": "42", "Pinpoint-pSpanID": "41"}, "")

    assert sampled is True
    assert trace_id == "upstream-agent^1700000000^7"
    assert span_id != 0
    # The creation call carries only the config revision; the resolved
    # snapshot is served by agent/span and keeps revision at index 11, SQL at
    # 12, user proxy names at 13, callstack capture at 14, and the four HTTP
    # settings the Python layer records itself at 15-18.
    assert revision == 1
    assert native_agent.get_config_snapshot() == (
        "unit-native-gil", 1700, "", 7, 11,
        ["X-Test"], [], [], [], [], [],
        1,
        # Sql.TraceBindValue as resolved above. Note the native default is
        # true, so this only reads False because the fixture config says so.
        False,
        [],  # Http.Server.ProxyUserHeaderNames
        False,  # EnableCallstackTrace
        # Resolved off the fixture YAML, i.e. what a config file gives the
        # Python recorders in http_helper.
        True,  # Http.Client.RecordUrlQuery
        True,  # Http.Server.RecordRequestParam
        ["CF-Connecting-IP"],  # Http.Server.RealIpHeader
        "unknown",  # Http.Server.RealIpEmptyValue
    )
    assert span.get_config_snapshot() == native_agent.get_config_snapshot()
    span.end_span()


def test_new_span_with_empty_headers_starts_fresh_trace(native_agent):
    span, sampled, _trace_id, _span_id, _revision = native_agent.new_span(
        "op", "/rpc", {}, "")

    assert sampled is True
    span.end_span()


def test_new_span_with_malformed_trace_id_starts_fresh_trace(native_agent):
    span, sampled, trace_id, span_id, revision = native_agent.new_span(
        "op", "/rpc", {HEADER_TRACE_ID: "this-is-not-a-valid-trace-id"}, "")

    assert sampled is True
    # Invalid inbound context now starts a new sampled transaction.
    assert trace_id and trace_id != "this-is-not-a-valid-trace-id"
    assert span_id not in (0, -1)
    assert revision == 1
    span.end_span()


def test_async_span_handle_survives_gil_released_creation(native_agent):
    span, sampled, _trace_id, _span_id, _revision = native_agent.new_span(
        "root", "/rpc", {}, "")
    assert sampled is True

    # Span events live Python-side now; the async link ids are wrapper-assigned
    # and arrive as arguments (the parent event is flushed with async_id=7 in
    # the span_events batch below).
    async_span = span.new_async_span("background", 7, 1)

    async_span.end_span()
    now = int(time.time() * 1000)
    span.end_span_with_data(
        1700, "", "", "", 200, "", "", [], [],
        [(0, 1, now - 5, now, 1400, "handoff", "", "", 0, 7, [])],
        False,
    )
