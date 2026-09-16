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

"""Inbound ``Pinpoint-Flags`` parsing and its outbound re-emission.

The header arrives from an untrusted edge and the span re-emits it verbatim on
every outbound call of the transaction, so the value must be bounded exactly
where native context extraction bounds it: an out-of-int32 value is rejected
there and leaves the flags at 0.
"""

import _fakes
from pinpoint.agent import Agent
from pinpoint.config import Config
from pinpoint.propagator import HEADER_FLAG, HEADER_TRACE_ID, HEADER_SPAN_ID, HEADER_PARENT_SPAN_ID

_CONFIG_SNAPSHOT = (
    "app", 1000, "", 64, 5000,
    (), (), (), (), (), (),
    1,  # config revision
    False,  # Sql.TraceBindValue
)


class _FakeNativeAgent:
    def __init__(self, native_span):
        self.native_span = native_span

    def enable(self):
        return True

    def get_config_snapshot(self):
        return _CONFIG_SNAPSHOT

    def new_span(self, *_args):
        return (self.native_span, True, "agent^1^1", 42, _CONFIG_SNAPSHOT[11])


def _outbound_flags(raw_flags):
    """The ``Pinpoint-Flags`` value a span built from ``raw_flags`` injects."""
    agent = Agent(
        _FakeNativeAgent(_fakes.FakeNativeSpan(sampled=True)),
        Config(application_name="app"),
    )
    span = agent.new_span("GET /", "/", headers={HEADER_FLAG: raw_flags, HEADER_TRACE_ID: "agent^1^1",
                                                 HEADER_SPAN_ID: "42", HEADER_PARENT_SPAN_ID: "41"})
    span.new_span_event("outbound")
    return dict(span.inject_context_items())[HEADER_FLAG]


def test_in_range_flags_are_propagated():
    assert _outbound_flags("2") == "2"
    assert _outbound_flags("-2") == "-2"
    assert _outbound_flags(str(2 ** 31 - 1)) == str(2 ** 31 - 1)
    assert _outbound_flags(str(-(2 ** 31))) == str(-(2 ** 31))


def test_out_of_int32_flags_are_dropped_like_the_native_extraction():
    # Native rejects these, so the flags stay 0 there — and a value this
    # size must never reach the outbound header set.
    assert _outbound_flags(str(2 ** 31)) == "0"
    assert _outbound_flags(str(-(2 ** 31) - 1)) == "0"
    assert _outbound_flags("1" + "0" * 300) == "0"


def test_unparsable_flags_stay_zero():
    assert _outbound_flags("not-a-number") == "0"
    assert _outbound_flags("") == "0"


def test_fresh_trace_does_not_adopt_inbound_flags_or_host():
    from pinpoint.propagator import HEADER_HOST
    agent = Agent(_FakeNativeAgent(_fakes.FakeNativeSpan()), Config(application_name="app"))
    for headers in (
        {HEADER_FLAG: "7", HEADER_HOST: "peer"},
        {HEADER_TRACE_ID: "agent^1^1", HEADER_FLAG: "7"},
        {HEADER_TRACE_ID: "other^1^1", HEADER_SPAN_ID: "42",
         HEADER_PARENT_SPAN_ID: "41", HEADER_FLAG: "7", HEADER_HOST: "peer"},
    ):
        span = agent.new_span("GET /", "/", headers=headers)
        with span.new_span_event("outbound"):
            assert dict(span.inject_context_items())[HEADER_FLAG] == "0"
        assert not span._acceptor_host
        assert span._parent_span_id == -1
        span.end()


def test_flags_reject_python_specific_integer_syntax():
    assert _outbound_flags("1_000") == "0"
    assert _outbound_flags("１２") == "0"
