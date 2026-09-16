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

"""``Http.Server.Exclude{Url,Method}`` against the real native agent.

Both filters run inside native span admission, so a Python-side double proves
nothing: the only way to know the config reaches them is to admit spans
through a started agent. ``ExcludeMethod`` is skipped natively when the method
is empty, so the method argument on both creators exists solely to reach this
filter.

A filtered request comes back as the native noop span: unsampled, with an
empty trace id and span id 0, where an admitted one carries a real identity.
"""

import pytest

from tests.integration._collector import start_native_agent

_APP = "unit-native-http-filters"

_EXTRA_YAML = """Http:
  Server:
    ExcludeUrl: ["/health", "/assets/**"]
    ExcludeMethod: ["options", "HEAD"]
"""


@pytest.fixture(scope="module")
def native_agent():
    with start_native_agent(_APP, _EXTRA_YAML) as agent:
        yield agent


def _admitted(agent, rpc_point, method="", headers=None):
    """True when the agent recorded a real span for this request."""
    span, sampled, trace_id, span_id, _rev = agent.new_span(
        "HTTP Server", rpc_point, headers or {}, method)
    try:
        return bool(sampled and trace_id and span_id)
    finally:
        span.end_span()


def test_unfiltered_request_is_admitted(native_agent):
    assert _admitted(native_agent, "/orders/42", "GET") is True


@pytest.mark.parametrize("method", ["OPTIONS", "options", "head"])
def test_excluded_method_is_filtered(native_agent, method):
    """Matching is case-insensitive in both directions — the config holds
    "options" lowercase and "HEAD" uppercase."""
    assert _admitted(native_agent, "/orders/42", method) is False


def test_excluded_method_filters_the_headers_creator_too(native_agent):
    """A request carrying upstream trace context takes the other creator; it
    must reach the same filter."""
    headers = {"Pinpoint-TraceID": "agent^1^1", "Pinpoint-SpanID": "7",
               "Pinpoint-Sampled": "s1"}
    assert _admitted(native_agent, "/orders/42", "GET", headers) is True
    assert _admitted(native_agent, "/orders/42", "OPTIONS", headers) is False


def test_empty_method_skips_the_filter(native_agent):
    """Non-HTTP entry points (messaging consumers, gRPC) pass no method and
    must never be filtered by it."""
    assert _admitted(native_agent, "/orders/42", "") is True


@pytest.mark.parametrize("path", ["/health", "/assets/img/logo.png"])
def test_excluded_url_is_filtered(native_agent, path):
    assert _admitted(native_agent, path, "GET") is False


# ---------------------------------------------------------------------------
# What the Python wrapper hands out for each of the three native outcomes
# ---------------------------------------------------------------------------

def _wrapper_agent(native_agent):
    from pinpoint.agent import Agent
    from pinpoint.config import Config

    return Agent(native_agent, Config(application_name=_APP))


def test_filtered_request_is_wrapped_as_a_pure_null_span(native_agent):
    """The three native outcomes must reach three different wrappers.

    A filtered request is a native noop span: no active-span registration, no
    URL stat, nothing to propagate. Wrapping it like a genuinely unsampled
    transaction would claim a native lifetime it does not have and pay an
    ``end_span()`` crossing on the health-check path.
    """
    from pinpoint.agent import UnSampledSpan, _NullSpan
    from pinpoint.propagator import HEADER_SAMPLED
    from pinpoint.tracer import Span

    agent = _wrapper_agent(native_agent)

    filtered = agent.new_span("HTTP Server", "/health", method="GET")
    assert type(filtered) is _NullSpan
    assert filtered.span_id == 0
    assert filtered._native is None
    assert tuple(filtered.inject_context_items()) == ()
    filtered.end()

    unsampled = agent.new_span(
        "HTTP Server", "/orders/42",
        headers={"Pinpoint-TraceID": "agent^1^1", "Pinpoint-SpanID": "7",
                 HEADER_SAMPLED: "s0"},
        method="GET")
    assert type(unsampled) is UnSampledSpan
    assert unsampled.span_id != 0
    assert tuple(unsampled.inject_context_items()) == ((HEADER_SAMPLED, "s0"),)
    unsampled.end()

    admitted = agent.new_span("HTTP Server", "/orders/42", method="GET")
    assert type(admitted) is Span
    admitted.end()
