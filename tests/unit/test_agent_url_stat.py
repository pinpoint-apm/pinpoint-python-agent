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

import _fakes
from pinpoint.agent import Agent
from pinpoint.config import Config
from pinpoint.propagator import (
    HEADER_PARENT_APP_NAME,
    HEADER_PARENT_APP_TYPE,
    HEADER_PARENT_SERVICE_NAME,
)


_CONFIG_SNAPSHOT = (
    "app", 1000, "", 64, 5000,
    (), (), (), (), (), (),
    1,  # config revision
    False,  # Sql.TraceBindValue
)


class _FakeNativeSpan(_fakes.FakeNativeSpan):
    """Shared fake plus an ``is_sampled`` crossing counter."""

    def __init__(self, sampled=True):
        super().__init__(sampled=sampled)
        self.sampled_calls = 0

    def is_sampled(self):
        self.sampled_calls += 1
        return self.sampled


class _FakeNativeAgent:
    def __init__(self, native_span, config_snapshot=_CONFIG_SNAPSHOT):
        self.native_span = native_span
        self.config_snapshot = config_snapshot
        self.calls = []
        # Counted apart from ``calls`` so the span-creation assertions below
        # stay unaffected by the snapshot fetch in Agent.__init__.
        self.snapshot_fetches = 0

    def enable(self):
        return True

    def get_config_snapshot(self):
        self.snapshot_fetches += 1
        return self.config_snapshot

    def _revision(self):
        return self.config_snapshot[11] if self.native_span.sampled else 0

    def new_span(self, *_args):
        self.calls.append(("new_span", _args))
        return (self.native_span, self.native_span.sampled,
                "agent^1^1", 42, self._revision())


def _new_agent(native_span, collect_url_stat):
    return Agent(
        _FakeNativeAgent(native_span),
        Config(application_name="app", http_collect_url_stat=collect_url_stat),
    )


def test_agent_sampled_span_skips_url_stat_when_config_disabled():
    native = _FakeNativeSpan(sampled=True)
    span = _new_agent(native, collect_url_stat=False).new_span("GET /", "/")

    span.set_url_stat("/", "GET", 200)

    assert native.url_stats == []


def test_agent_sampled_span_records_url_stat_when_config_enabled():
    native = _FakeNativeSpan(sampled=True)
    span = _new_agent(native, collect_url_stat=True).new_span("GET /", "/")

    span.set_url_stat("/", "GET", 200)

    assert native.url_stats == []
    span.end()

    assert native.url_stats == [("/", "GET", 200)]
    assert native.end_called is True


def test_agent_unsampled_span_skips_url_stat_when_config_disabled():
    native = _FakeNativeSpan(sampled=False)
    span = _new_agent(native, collect_url_stat=False).new_span("GET /", "/")

    span.set_url_stat("/", "GET", 200)
    span.end()

    assert native.url_stats == []
    assert native.end_called is True


def test_agent_unsampled_span_records_url_stat_when_config_enabled():
    native = _FakeNativeSpan(sampled=False)
    span = _new_agent(native, collect_url_stat=True).new_span("GET /", "/")

    span.set_url_stat("/", "GET", 200)
    span.end()

    assert native.url_stats == [("/", "GET", 200)]
    assert native.end_called is True


def test_agent_unsampled_span_does_not_retry_when_url_stat_end_helper_fails(monkeypatch):
    native = _FakeNativeSpan(sampled=False)
    span = _new_agent(native, collect_url_stat=True).new_span("GET /", "/")

    def fail_end_span(*_args):
        raise RuntimeError("boom")

    monkeypatch.setattr(native, "end_span", fail_end_span)

    span.set_url_stat("/", "GET", 200)
    span.end()

    assert native.url_stats == []
    assert native.end_called is False


def test_agent_uses_sampled_flag_from_new_span_without_is_sampled_call():
    native = _FakeNativeSpan(sampled=False)
    native_agent = _FakeNativeAgent(native)
    agent = Agent(
        native_agent,
        Config(application_name="app", http_collect_url_stat=True),
    )

    span = agent.new_span("GET /", "/")

    assert span.sampled is False
    assert native.sampled_calls == 0
    assert native_agent.calls == [("new_span", ("GET /", "/", {}, ""))]


def test_agent_uses_sampled_flag_from_new_span_with_headers():
    native = _FakeNativeSpan(sampled=True)
    native_agent = _FakeNativeAgent(native)
    agent = Agent(
        native_agent,
        Config(application_name="app", http_collect_url_stat=True),
    )

    span = agent.new_span("GET /", "/", headers={"Pinpoint-TraceID": "T-1"})

    assert span.sampled is True
    assert native.sampled_calls == 0
    assert native_agent.calls == [
        ("new_span", ("GET /", "/", {"Pinpoint-TraceID": "T-1"}, ""))]


def test_agent_uses_native_span_snapshot_for_propagation_and_event_limits():
    native = _FakeNativeSpan(sampled=True)
    snapshot = (
        "resolved-app", 7777, "resolved-service", 3, 9,
        (), (), (), (), (), (),
        1,
        False,
    )
    agent = Agent(
        _FakeNativeAgent(native, snapshot),
        Config(
            application_name="stale-app",
            application_type=1000,
            service_name="stale-service",
            span_max_event_depth=64,
            span_max_event_sequence=5000,
        ),
    )

    span = agent.new_span("GET /", "/")
    first = span.new_span_event("first")
    second = span.new_span_event("second")
    span.new_span_event("third")
    span.new_span_event("fourth")
    overflow = span.new_span_event("overflow")
    headers = dict(span.inject_context_items())

    assert headers[HEADER_PARENT_APP_NAME] == "resolved-app"
    assert headers[HEADER_PARENT_APP_TYPE] == "7777"
    assert headers[HEADER_PARENT_SERVICE_NAME] == "resolved-service"
    assert first._sequence == 0
    assert second._sequence == 1
    assert overflow._sequence is None


def test_agent_refreshes_config_snapshot_when_span_reports_new_revision():
    native = _FakeNativeSpan(sampled=True)
    native_agent = _FakeNativeAgent(native)
    agent = Agent(
        native_agent,
        Config(application_name="app", http_collect_url_stat=True),
    )
    assert native_agent.snapshot_fetches == 1  # fetched once at startup

    # Same revision: spans reuse the cached snapshot without a native fetch.
    span = agent.new_span("GET /", "/")
    assert span._max_event_depth == 64
    assert native_agent.snapshot_fetches == 1

    # A hot reload bumps the native revision; the next span triggers exactly
    # one re-fetch and picks up the reloaded values.
    native_agent.config_snapshot = (
        "app", 1000, "", 3, 9,
        ("X-Reloaded",), (), (), (), (), (),
        2,
        False,
    )
    reloaded = agent.new_span("GET /", "/")
    assert native_agent.snapshot_fetches == 2
    assert reloaded._max_event_depth == 3
    assert reloaded._max_event_sequence == 9
    assert reloaded._config_snapshot[5] == ("X-Reloaded",)

    agent.new_span("GET /", "/")
    assert native_agent.snapshot_fetches == 2  # revision stable again


def test_file_and_profile_url_stats_are_left_to_native():
    for config in (
        Config(application_name="app", config_file_path="config.yaml"),
        Config(application_name="app", profiles={"prod": {"Http": {"CollectUrlStat": True}}}),
        Config(application_name="app", enable_callstack_trace=True),
    ):
        native = _FakeNativeSpan()
        span = Agent(_FakeNativeAgent(native), config).new_span("GET /", "/")
        span.set_url_stat("/users/{id}", "GET", 200)
        span.end()
        assert native.url_stats == [("/users/{id}", "GET", 200)]


def test_empty_url_stat_pattern_uses_native_null_bucket():
    for sampled in (True, False):
        native = _FakeNativeSpan(sampled=sampled)
        span = _new_agent(native, collect_url_stat=True).new_span("GET /", "/")
        span.set_url_stat("", "GET", 404)
        span.end()
        assert native.url_stats == [("/NULL", "GET", 404)]


def test_agent_reload_reads_admitted_span_generation():
    """Reload after admission must not attach the latest agent config to it."""
    old = _CONFIG_SNAPSHOT + (("X-Old",),)
    current = old[:11] + (2, False, ("X-New",))
    newest = old[:11] + (3, True, ("X-Newest",))
    native = _FakeNativeSpan()
    native_agent = _FakeNativeAgent(native, old)
    agent = Agent(native_agent, Config(application_name="app"))
    native.get_config_snapshot = lambda: current
    native_agent.config_snapshot = newest
    native_agent._revision = lambda: 2
    span = agent.new_span("admitted-before-reload", "/")
    assert span._config_snapshot == current
    assert native_agent.snapshot_fetches == 1
    assert span._config_snapshot[12] is False  # SQL index is unchanged.
    span.end()

    # A delayed admission from an older revision must also get its own names.
    native.get_config_snapshot = lambda: old
    native_agent._revision = lambda: 1
    span = agent.new_span("straggler", "/")
    assert span._config_snapshot == old
    span.end()


def test_agent_resolves_callstack_flag_per_snapshot_and_legacy_falls_back_off():
    native = _FakeNativeSpan()
    disabled = _CONFIG_SNAPSHOT + ((), False)
    native_agent = _FakeNativeAgent(native, disabled)
    agent = Agent(native_agent, Config(application_name="app",
                                       enable_callstack_trace=True))

    old_span = agent.new_span("old", "/")
    assert old_span._enable_callstack_trace is False

    enabled = disabled[:11] + (2, False, (), True)
    native_agent.config_snapshot = enabled
    reloaded = agent.new_span("new", "/")
    assert reloaded._enable_callstack_trace is True
    assert old_span._enable_callstack_trace is False

    legacy_agent = Agent(
        _FakeNativeAgent(_FakeNativeSpan(), _CONFIG_SNAPSHOT),
        Config(application_name="app", enable_callstack_trace=True),
    )
    assert legacy_agent.new_span("legacy", "/")._enable_callstack_trace is False
