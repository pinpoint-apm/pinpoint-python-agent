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

"""The ``logging`` slot of ``_native.Span.end_span_with_data``.

The ids never cross into native (the wrapper already holds them); only the
flag does, as part of the one end() payload. These tests pin that the slot is
required and accepted on a live native span.
"""

import pytest

from tests.integration._collector import start_native_agent


@pytest.fixture(scope="module")
def native_agent():
    with start_native_agent("unit-native-logging", "") as agent:
        yield agent


def _new_span(agent):
    span, sampled, _trace_id, _span_id, _revision = agent.new_span(
        "op", "/rpc", {}, "")
    assert sampled is True
    return span


def test_logging_flag_is_accepted_in_end_payload(native_agent):
    _new_span(native_agent).end_span_with_data(
        1700, "", "", "", 200, "", "", [], [], [], True)


def test_logging_slot_is_required(native_agent):
    span = _new_span(native_agent)
    with pytest.raises(TypeError):
        span.end_span_with_data(1700, "", "", "", 200, "", "", [], [], [])
    span.end_span_with_data(1700, "", "", "", 200, "", "", [], [], [], False)


def test_wrapper_end_passes_the_flag_through(native_agent):
    from pinpoint.tracer import Span

    span = Span(_new_span(native_agent), trace_id="t", span_id=1)
    span.set_logging()
    span.end()  # must not raise through the flush path
