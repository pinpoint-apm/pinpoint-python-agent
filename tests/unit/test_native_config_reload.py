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

"""``_native.reload_agent_config()`` against the real native agent.

The reload itself runs natively — YAML parsing, validation, the
reloadable/non-reloadable split, the config revision — so a Python double
proves nothing about it: start an agent against the mock collector, reload,
and read the resolved snapshot back through ``get_config_snapshot()``.
"""

from __future__ import annotations

import time

import pytest

from pinpoint.agent import Agent
from pinpoint.config import Config
from tests.integration._collector import MockCollector

_APP = "unit-native-config-reload"

# Snapshot indices (src/_native.cpp to_python_config).
_MAX_EVENT_DEPTH = 3
_REVISION = 11

_APPLIED, _NO_AGENT, _CONFIG_FILE_ACTIVE, _INVALID_YAML, _INVALID_CONFIG, _ERROR = range(6)


def _config(port, extra=""):
    return f"""
Enable: true
ApplicationName: "{_APP}"
Collector:
  Host: "127.0.0.1"
  AgentPort: {port}
  SpanPort: {port}
  StatPort: {port}
  AgentInfo:
    SendRetryIntervalMs: 100
Sampling:
  Type: "PERCENT"
  PercentRate: 100.0
Stat:
  Enable: false
{extra}"""


@pytest.fixture(scope="module")
def started():
    """``(native_agent, collector_port)``: one agent for the module, so the
    tests below assert on revision *deltas*, never absolute values."""
    from pinpoint import _native
    collector = MockCollector().start()
    agent = _native.start_agent("", _config(collector.port), "", 1700, "unit-test", [], [])
    try:
        collector.wait_agent_info_for_application(_APP)
        deadline = time.monotonic() + 5
        while not agent.enable():
            if time.monotonic() >= deadline:
                raise AssertionError("native agent did not become enabled")
            time.sleep(0.01)
        yield agent, collector.port
    finally:
        agent.shutdown()
        collector.stop()


def _reload(yaml):
    from pinpoint import _native
    status, restart_required, message = _native.reload_agent_config(yaml)
    return status, list(restart_required), message


def test_no_running_agent_is_refused_as_no_agent():
    """Runs first, before the module fixture starts the agent: StartAgent()
    is one-shot per process, so there is no way to start a second one to shut
    down. Every earlier native test module shut its agent down at module
    teardown, which native also reports as no agent."""
    status, restart_required, message = _reload("ApplicationName: x")
    assert (status, restart_required) == (_NO_AGENT, [])
    assert "no agent" in message


def test_applied_reload_moves_the_revision_and_the_reloadable_setting(started):
    agent, port = started
    before = tuple(agent.get_config_snapshot())

    status, restart_required, message = _reload(
        _config(port, "Span:\n  MaxEventDepth: 7\n"))

    assert (status, restart_required, message) == (_APPLIED, [], "")
    after = tuple(agent.get_config_snapshot())
    assert after[_REVISION] == before[_REVISION] + 1
    assert after[_MAX_EVENT_DEPTH] == 7
    assert before[_MAX_EVENT_DEPTH] != 7


def test_omitted_key_falls_back_to_its_default_not_its_running_value(started):
    """The YAML is the complete configuration."""
    agent, port = started
    _reload(_config(port, "Span:\n  MaxEventDepth: 9\n"))
    assert tuple(agent.get_config_snapshot())[_MAX_EVENT_DEPTH] == 9

    status, _restart, _msg = _reload(_config(port))

    assert status == _APPLIED
    assert tuple(agent.get_config_snapshot())[_MAX_EVENT_DEPTH] == 64  # the default


def test_non_reloadable_change_is_applied_around_and_reported(started):
    agent, port = started
    before = tuple(agent.get_config_snapshot())

    status, restart_required, _msg = _reload(
        _config(port + 1, "Span:\n  MaxEventDepth: 11\n"))

    assert status == _APPLIED
    # By YAML path, in doc/config.md order: the three ports differ.
    assert restart_required == [
        "Collector.AgentPort", "Collector.SpanPort", "Collector.StatPort"]
    after = tuple(agent.get_config_snapshot())
    assert after[_REVISION] == before[_REVISION] + 1
    assert after[_MAX_EVENT_DEPTH] == 11  # the reloadable part still applied


@pytest.mark.parametrize("yaml, expected", [
    ("just a scalar", _INVALID_YAML),
    ("- a\n- sequence\n", _INVALID_YAML),
    ("Collector: {Host: [unclosed", _INVALID_YAML),
    ("Sampling:\n  PercentRate: 50\n", _INVALID_CONFIG),  # no ApplicationName / Collector.Host
])
def test_refused_reload_changes_nothing(started, yaml, expected):
    agent, _port = started
    before = tuple(agent.get_config_snapshot())

    status, restart_required, message = _reload(yaml)

    assert status == expected
    assert restart_required == []
    assert message  # the reason, for the host's log
    assert tuple(agent.get_config_snapshot()) == before


def test_wrong_value_type_is_invalid_config(started):
    agent, port = started
    before = tuple(agent.get_config_snapshot())
    status, _restart, message = _reload(
        _config(port, "Span:\n  MaxEventDepth: [1, 2]\n"))
    assert status == _INVALID_CONFIG
    assert message
    assert tuple(agent.get_config_snapshot()) == before


def test_python_wrapper_sees_the_new_generation(started):
    """The whole path: ``Agent.reload_config`` over the real binding refreshes
    the cached per-span config the wrapper hands to new spans."""
    native, port = started
    wrapper = Agent(native, Config(application_name=_APP))
    try:
        revision_before = wrapper._span_config[0]

        result = wrapper.reload_config(_config(port, "Span:\n  MaxEventDepth: 5\n"))

        assert result.ok and result.restart_required == ()
        revision, snapshot, _inject, max_depth, _max_seq = wrapper._span_config
        assert revision == revision_before + 1
        assert max_depth == 5
        assert snapshot[_MAX_EVENT_DEPTH] == 5

        refused = wrapper.reload_config("just a scalar")
        assert refused.status == "invalid_yaml" and refused.message
        assert wrapper._span_config[0] == revision
    finally:
        # The module fixture owns the native agent; drop the wrapper without
        # letting its __del__ shut the shared agent down.
        object.__setattr__(wrapper, "_shutdown", True)
