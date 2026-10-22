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

"""``pinpoint.reload_config()`` / ``Agent.reload_config()`` over a native
double.

Contract under test: the wrapper turns the native ``(status, restart_required,
message)`` tuple into a ``ConfigReloadResult``, never raises, refreshes its
cached config snapshot only on an applied reload, and answers ``no_agent``
itself whenever there is nothing to reload. The native call's own semantics
are covered against the real extension in ``test_native_config_reload.py``.
"""

from __future__ import annotations

import logging

import pytest

import pinpoint
from pinpoint import agent as agent_mod
from pinpoint.agent import Agent, ConfigReloadResult, _NullAgent
from pinpoint.config import Config


def _snapshot(revision, max_event_depth=64):
    return (
        "app", 1700, "", max_event_depth, 5000,
        (), (), (), (), (), (),
        revision,
        False, (), False, False, False, "", "",
    )


class _NativeAgent:
    """Native agent double whose config snapshot moves on every applied
    reload. ``reload_agent_config`` is a *module* function in the real
    binding (it addresses the process-wide agent), so the double of that
    lives on :class:`_NativeModule`, installed as ``agent._native``."""

    def __init__(self):
        self.snapshot_calls = 0
        self.revision = 1
        self.depth = 64

    def get_config_snapshot(self):
        self.snapshot_calls += 1
        return _snapshot(self.revision, self.depth)

    def shutdown(self):
        pass


class _NativeModule:
    def __init__(self, agent, replies):
        self.agent = agent
        self.replies = list(replies)
        self.reload_calls = []

    def reload_agent_config(self, config_yaml):
        self.reload_calls.append(config_yaml)
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        if reply[0] == 0:
            self.agent.revision += 1
            self.agent.depth = 7
        return reply


@pytest.fixture
def make_agent(monkeypatch):
    """``make_agent(*replies) -> (native_module, agent)``: an ``Agent`` over
    the doubles, with ``agent._native`` swapped for the module double."""
    def _make(*replies):
        native_agent = _NativeAgent()
        module = _NativeModule(native_agent, replies)
        module.native_agent = native_agent
        monkeypatch.setattr(agent_mod, "_native", module)
        return module, Agent(native_agent, Config(application_name="app"))
    return _make


# ---------------------------------------------------------------------------
# ConfigReloadResult
# ---------------------------------------------------------------------------

def test_result_truthiness_follows_ok():
    assert ConfigReloadResult("applied").ok is True
    assert bool(ConfigReloadResult("applied", ("ApplicationName",))) is True
    for status in ("no_agent", "config_file_active", "invalid_yaml",
                   "invalid_config", "error"):
        result = ConfigReloadResult(status, message="why")
        assert result.ok is False
        assert not result
    with pytest.raises(Exception):
        ConfigReloadResult("applied").status = "error"  # frozen


# ---------------------------------------------------------------------------
# Agent.reload_config
# ---------------------------------------------------------------------------

def test_applied_reload_refreshes_the_cached_snapshot(caplog, make_agent):
    native, agent = make_agent((0, [], ""))
    assert agent._span_config[0] == 1
    before = native.native_agent.snapshot_calls

    with caplog.at_level(logging.WARNING, logger="pinpoint"):
        result = agent.reload_config("Sampling: {PercentRate: 1}")

    assert result == ConfigReloadResult("applied", (), "")
    assert native.reload_calls == ["Sampling: {PercentRate: 1}"]
    assert native.native_agent.snapshot_calls == before + 1
    revision, _snap, _inject, max_depth, _max_seq = agent._span_config
    assert (revision, max_depth) == (2, 7)
    assert not [r for r in caplog.records if "reload" in r.getMessage()]


def test_restart_required_is_reported_and_warned(caplog, make_agent):
    native, agent = make_agent((0, ["ApplicationName", "Collector.Host"], ""))

    with caplog.at_level(logging.WARNING, logger="pinpoint"):
        result = agent.reload_config("ApplicationName: other")

    assert result.ok
    assert result.restart_required == ("ApplicationName", "Collector.Host")
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("ApplicationName, Collector.Host" in w for w in warnings)
    assert agent._span_config[0] == 2  # applied: the snapshot moved


@pytest.mark.parametrize("status, name", [
    (1, "no_agent"),
    (2, "config_file_active"),
    (3, "invalid_yaml"),
    (4, "invalid_config"),
    (5, "error"),
    (99, "error"),  # an enumerator this wrapper does not know
])
def test_refused_reload_keeps_the_snapshot_and_names_the_status(
        caplog, make_agent, status, name):
    native, agent = make_agent((status, [], "because"))
    before = native.native_agent.snapshot_calls

    with caplog.at_level(logging.WARNING, logger="pinpoint"):
        result = agent.reload_config("x: 1")

    assert result == ConfigReloadResult(name, (), "because")
    assert not result
    assert native.native_agent.snapshot_calls == before  # nothing refetched
    assert agent._span_config[0] == 1
    assert any("nothing changed" in r.getMessage() and "because" in r.getMessage()
               for r in caplog.records)


def test_native_exception_becomes_an_error_result(make_agent):
    native, agent = make_agent(RuntimeError("marshal"))
    result = agent.reload_config("x: 1")
    assert result.status == "error"
    assert not result
    assert "RuntimeError" in result.message and "marshal" in result.message
    assert agent._span_config[0] == 1


def test_yaml_is_passed_as_str(make_agent):
    native, agent = make_agent((0, [], ""))

    class _Yaml:
        def __str__(self):
            return "Sampling: {PercentRate: 1}"

    assert agent.reload_config(_Yaml()).ok
    assert native.reload_calls == ["Sampling: {PercentRate: 1}"]


def test_shut_down_agent_answers_no_agent_without_a_native_call(make_agent):
    native, agent = make_agent((0, [], ""))
    agent.shutdown()
    result = agent.reload_config("x: 1")
    assert result.status == "no_agent"
    assert native.reload_calls == []


def test_binding_without_the_call_is_an_error_not_a_crash(monkeypatch):
    monkeypatch.setattr(agent_mod, "_native", object())  # no reload_agent_config
    agent = Agent(_NativeAgent(), Config(application_name="app"))
    result = agent.reload_config("x: 1")
    assert result.status == "error"
    assert "reload_agent_config" in result.message


def test_null_agent_answers_no_agent():
    for null in (_NullAgent(Config(application_name="app")),
                 _NullAgent(Config(application_name="app"), pending=True)):
        result = null.reload_config("x: 1")
        assert result.status == "no_agent"
        assert not result


# ---------------------------------------------------------------------------
# pinpoint.reload_config (module level)
# ---------------------------------------------------------------------------

def test_module_function_without_init_answers_no_agent(monkeypatch):
    monkeypatch.setattr(agent_mod, "_instance", None)
    result = pinpoint.reload_config("x: 1")
    assert result.status == "no_agent"
    assert "init()" in result.message


def test_module_function_delegates_to_the_instance(monkeypatch, make_agent):
    native, agent = make_agent((0, ["Collector.AgentPort"], ""))
    monkeypatch.setattr(agent_mod, "_instance", agent)
    result = pinpoint.reload_config("Collector: {AgentPort: 1}")
    assert result.ok
    assert result.restart_required == ("Collector.AgentPort",)
    assert native.reload_calls == ["Collector: {AgentPort: 1}"]


def test_public_exports():
    assert "reload_config" in pinpoint.__all__
    assert "ConfigReloadResult" in pinpoint.__all__
    assert pinpoint.ConfigReloadResult is ConfigReloadResult
