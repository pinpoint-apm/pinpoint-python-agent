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

"""Core-module integration tests: real agent ↔ in-process mock collector.

Unlike the instrumentation modules (which swap the native span sink for
``_recorder``), nothing is stubbed here: ``pinpoint.init()`` builds the real
C++ agent, which registers, pings, and drains spans / metadata / stats over
the real gRPC wire protocol into ``_collector.MockCollector``. Covered
surface: agent lifecycle (init → handshake → enabled → shutdown → re-init,
prefork pending-master fork, warm-master fork degrade), Config→YAML→native
plumbing and ``PINPOINT_PY_*`` env overrides, root spans and span events with
annotations, API/string/SQL
metadata registration, error recording, trace-context injection /
continuation, sampling drops, async span chunks, and agent stats.

Needs no Docker daemon (``no_docker``) — the collector is an in-process
``grpc.server`` on an ephemeral localhost port.
"""

from __future__ import annotations

import os
import logging
import subprocess
import sys
import textwrap
import threading
import time

import pytest

pytestmark = pytest.mark.no_docker

pytest.importorskip("grpc")
pytest.importorskip("grpc_tools")

import pinpoint
from pinpoint import agent as agent_mod
from pinpoint import propagator
from pinpoint.annotation import ANNOTATION_HTTP_REQUEST_HEADER, ANNOTATION_SQL_ID
from pinpoint.http_helper import (trace_http_client_request,
                                  trace_http_server_request)
from pinpoint.service_type import (
    APP_TYPE_PYTHON,
    SERVICE_TYPE_PYTHON_METHOD,
)

from tests.integration._collector import MockCollector

_APP_NAME = "core-it-app"
_SERVER_INFO = "Core IT Server"


def _wait(predicate, timeout: float = 20.0, what: str = "condition"):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(0.02)


@pytest.fixture(scope="module")
def collector():
    c = MockCollector().start()
    yield c
    c.stop()


@pytest.fixture
def core_agent(collector, monkeypatch):
    """Factory: init the real process-wide agent against the mock collector.

    The dev shell exports ``PINPOINT_PY_COLLECTOR_HOST`` (and possibly more)
    pointing at a live collector, and the native agent reads ``PINPOINT_PY_*``
    itself with env-over-YAML precedence — so every inherited variable is
    stripped first and tests opt back in via ``env=``.

    Returns ``(agent, agent_id)``; the factory blocks until the AgentInfo
    handshake flips ``agent.enabled`` unless ``wait_enabled=False``. Teardown
    shuts the process-wide agent down, so each test gets a fresh lifecycle.
    """
    for key in list(os.environ):
        if key.startswith("PINPOINT_PY_"):
            monkeypatch.delenv(key)

    def _make(*, wait_enabled=True, env=None, **overrides):
        assert agent_mod.get_agent() is None, (
            "process-wide agent already initialized; shutdown() first")
        for key, value in (env or {}).items():
            monkeypatch.setenv(key, value)
        with collector._cond:
            registrations_before = len(collector.agent_infos)
        cfg = dict(
            application_name=_APP_NAME,
            agent_name="core-it-agent",
            collector_host="127.0.0.1",
            collector_agent_port=collector.port,
            collector_span_port=collector.port,
            collector_stat_port=collector.port,
            # Keep the pipeline snappy and the background chatter minimal.
            span_batch_flush_interval_ms=100,
            span_batch_collect_deadline_ms=50,
            agent_info_send_retry_interval_ms=200,
            stat_enabled=False,
        )
        cfg.update(overrides)
        agent = pinpoint.init(server_info=_SERVER_INFO, **cfg)
        if wait_enabled:
            _wait(lambda: agent.enabled, what="agent handshake")
            metadata, _info = collector.wait_for(
                lambda: (
                    collector.agent_infos[registrations_before]
                    if len(collector.agent_infos) > registrations_before
                    else None
                ),
                what="new agent registration",
            )
            agent_id = metadata["agentid"]
        else:
            agent_id = None
        return agent, agent_id

    yield _make
    pinpoint.shutdown()


def _tx_tuple(tx) -> tuple:
    return (tx.agentId, tx.agentStartTime, tx.sequence)


def _parse_trace_id(trace_id: str) -> tuple:
    agent_id, start_time, seq = trace_id.rsplit("^", 2)
    return (agent_id, int(start_time), int(seq))


def _annotations(pb) -> dict:
    return {a.key: a.value for a in pb.annotation}


def _record_callstack_exception(span, operation: str) -> None:
    try:
        raise RuntimeError(f"{operation} boom")
    except RuntimeError as exc:
        with span.new_span_event(operation) as event:
            event.set_error(exc)


def _exception_meta_for(collector, span_pb, start: int = 0):
    return next((meta for meta in collector.exception_metas[start:]
                 if meta.spanId == span_pb.spanId
                 and _tx_tuple(meta.transactionId) ==
                 _tx_tuple(span_pb.transactionId)), None)


def _wait_exception_meta(collector, span_pb, start: int = 0):
    return collector.wait_for(
        lambda: _exception_meta_for(collector, span_pb, start),
        what=f"exception metadata for spanId={span_pb.spanId}",
    )


def _write_callstack_config(path, collector, *, enabled: bool,
                            throughput: int = 1000) -> None:
    path.write_text(textwrap.dedent(f"""\
        ApplicationName: "{_APP_NAME}"
        AgentName: "callstack-file-agent"
        EnableConfigFileWatcher: true
        EnableCallstackTrace: {str(enabled).lower()}
        CallstackTraceNewThroughput: {throughput}
        Collector:
          Host: "127.0.0.1"
          AgentPort: {collector.port}
          SpanPort: {collector.port}
          StatPort: {collector.port}
          AgentInfo:
            SendRetryIntervalMs: 200
        Sampling:
          Type: "COUNTER"
          CounterRate: 1
        Span:
          Batch:
            FlushIntervalMs: 100
            CollectDeadlineMs: 50
        Sql:
          TraceBindValue: false
        Stat:
          Enable: false
    """))


def _uri_stat_counts(collector, uri: str) -> tuple[int, int]:
    total = failed = 0
    for message in collector.stat_messages:
        if not message.HasField("agentUriStat"):
            continue
        for stat in message.agentUriStat.eachUriStat:
            if stat.uri != uri:
                continue
            total += sum(stat.totalHistogram.histogram)
            failed += sum(stat.failedHistogram.histogram)
    return total, failed


# ---------------------------------------------------------------------------
# Lifecycle & registration
# ---------------------------------------------------------------------------

def test_handshake_registers_agent_info(core_agent, collector):
    agent, agent_id = core_agent()

    metadata, info = collector.wait_agent_info(agent_id)

    # gRPC call metadata carries the agent identity on every RPC.
    assert len(agent_id) == 22
    assert agent_id != "core-it-agent"
    assert metadata["applicationname"] == _APP_NAME
    assert metadata["agentname"] == "core-it-agent"
    assert metadata["starttime"].isdigit()

    # PAgentInfo body: process identity + server metadata from init().
    assert info.pid == os.getpid()
    assert info.hostname
    assert info.agentVersion
    assert info.serviceType == APP_TYPE_PYTHON
    assert info.container is False
    assert info.serverMetaData.serverInfo == _SERVER_INFO
    # StartAgent() passes sys.argv and the loaded (non-stdlib) packages.
    assert list(info.serverMetaData.vmArg)
    libraries = [
        lib
        for service in info.serverMetaData.serviceInfo
        for lib in service.serviceLib
    ]
    assert any(lib.startswith("pytest") for lib in libraries)

    # The ping stream comes up alongside registration.
    collector.wait_for(lambda: collector.ping_count >= 1, what="first ping")


def test_config_file_option_and_watcher_reconfigure_sampling(
    core_agent,
    collector,
    tmp_path,
):
    """The Python binding must pass AgentOptions.config_file_path through to
    C++ StartAgent, including its opt-in watcher lifecycle."""
    config_path = tmp_path / "pinpoint.yaml"

    def _write_config(counter_rate: int) -> None:
        max_depth = 3 if counter_rate == 0 else 7
        max_sequence = 5 if counter_rate == 0 else 11
        request_header = "HEADERS-ALL" if counter_rate == 0 else "X-Allowed"
        config_path.write_text(textwrap.dedent(f"""\
            ApplicationName: "{_APP_NAME}"
            AgentName: "file-config-agent"
            EnableConfigFileWatcher: true
            Collector:
              Host: "127.0.0.1"
              AgentPort: {collector.port}
              SpanPort: {collector.port}
              StatPort: {collector.port}
              AgentInfo:
                SendRetryIntervalMs: 200
            Sampling:
              Type: "COUNTER"
              CounterRate: {counter_rate}
            Span:
              MaxEventDepth: {max_depth}
              MaxEventSequence: {max_sequence}
            Http:
              Server:
                RecordRequestHeader: ["{request_header}"]
            Stat:
              Enable: false
        """))

    _write_config(0)
    agent, agent_id = core_agent(
        application_name="",
        config_file_path=str(config_path),
        # The file replaces these stale inline values wholesale.
        http_server_record_request_header=["HEADERS-ALL"],
        span_max_event_depth=2,
        span_max_event_sequence=4,
    )
    metadata, _ = collector.wait_agent_info(agent_id)
    assert metadata["agentname"] == "file-config-agent"

    disabled = agent.new_span("watcher-before", "/watcher/before")
    assert disabled.sampled is False
    disabled.end()

    _write_config(1)

    def _sample_after_reload():
        span = agent.new_span("watcher-after", "/watcher/after")
        if not span.sampled:
            span.end()
            return False
        try:
            assert span._max_event_depth == 7
            assert span._max_event_sequence == 11
            with span.new_span_event("outbound"):
                headers = dict(span.inject_context_items())
            assert headers[propagator.HEADER_PARENT_APP_NAME] == _APP_NAME

            trace_http_server_request(
                span,
                "127.0.0.1",
                "example.test",
                {"X-Allowed": "yes", "Authorization": "secret"},
            )
            recorded_headers = [
                item for item in (span._annotations or ())
                if item[0] == 2 and item[1] == ANNOTATION_HTTP_REQUEST_HEADER
            ]
            assert recorded_headers == [
                (2, ANNOTATION_HTTP_REQUEST_HEADER, "X-Allowed", "yes"),
            ]
            return True
        finally:
            span.end()

    _wait(_sample_after_reload, timeout=8.0, what="config watcher reload")


def test_error_verdict_uses_the_span_captured_config_generation(
    core_agent,
    collector,
    tmp_path,
):
    config_path = tmp_path / "error-policy.yaml"

    def _write_config(*, ignore: bool, max_depth: int) -> None:
        ignore_value = ('[{"name": "OldGenerationIgnored"}]'
                        if ignore else "[]")
        config_path.write_text(textwrap.dedent(f"""\
            ApplicationName: "{_APP_NAME}"
            AgentName: "error-policy-file-agent"
            EnableConfigFileWatcher: true
            Collector:
              Host: "127.0.0.1"
              AgentPort: {collector.port}
              SpanPort: {collector.port}
              StatPort: {collector.port}
              AgentInfo:
                SendRetryIntervalMs: 200
            Sampling:
              Type: "COUNTER"
              CounterRate: 1
            Span:
              MaxEventDepth: {max_depth}
              MaxEventSequence: 4
              IgnoreErrors: {ignore_value}
            Stat:
              Enable: false
        """))

    _write_config(ignore=True, max_depth=2)
    agent, _ = core_agent(
        application_name="",
        config_file_path=str(config_path),
    )
    old_span = agent.new_span("old-generation", "/core/policy-generation/old")
    assert old_span._max_event_depth == 2

    _write_config(ignore=False, max_depth=7)
    def _new_generation_loaded():
        candidate = agent.new_span(
            "new-generation-probe", "/core/policy-generation/probe")
        loaded = candidate._max_event_depth == 7
        candidate.end()
        return loaded

    _wait(_new_generation_loaded, timeout=8.0,
          what="error policy config watcher reload")
    new_span = agent.new_span(
        "new-generation", "/core/policy-generation/new")

    for span in (old_span, new_span):
        for i in range(4):
            span.new_span_event(f"kept-{i}").end()
        event = span.new_span_event("discarded")
        event.set_error("OldGenerationIgnored", "boom")
        event.end()
        span.end()

    assert collector.wait_span("/core/policy-generation/old").err == 0
    assert collector.wait_span("/core/policy-generation/new").err == 2


@pytest.mark.parametrize(("old_enabled", "new_enabled"), [
    (False, True),
    (True, False),
])
def test_callstack_reload_keeps_each_span_generation(
    core_agent, collector, tmp_path, old_enabled, new_enabled,
):
    path = tmp_path / "callstack-reload.yaml"
    _write_callstack_config(path, collector, enabled=old_enabled,
                            throughput=0)
    with collector._cond:
        metadata_start = len(collector.exception_metas)
    agent, _ = core_agent(application_name="", config_file_path=str(path))

    old_rpc = f"/core/callstack/reload/{int(old_enabled)}-old"
    new_rpc = f"/core/callstack/reload/{int(new_enabled)}-new"
    old_span = agent.new_span("callstack-old-generation", old_rpc)
    old_revision = old_span._config_snapshot[11]
    assert old_span._enable_callstack_trace is old_enabled

    _write_callstack_config(path, collector, enabled=new_enabled,
                            throughput=0)
    _wait(
        lambda: (agent._native.get_config_snapshot()[11] > old_revision
                 and agent._native.get_config_snapshot()[14] is new_enabled),
        timeout=8.0,
        what="callstack config watcher reload",
    )
    new_span = agent.new_span("callstack-new-generation", new_rpc)
    assert new_span._enable_callstack_trace is new_enabled
    assert old_span._enable_callstack_trace is old_enabled

    _record_callstack_exception(old_span, "old-generation-error")
    _record_callstack_exception(new_span, "new-generation-error")
    old_span.end()
    new_span.end()

    old_pb = collector.wait_span(old_rpc)
    new_pb = collector.wait_span(new_rpc)
    assert old_pb.spanEvent[0].HasField("exceptionInfo")
    assert new_pb.spanEvent[0].HasField("exceptionInfo")
    enabled_pb = old_pb if old_enabled else new_pb
    disabled_pb = new_pb if old_enabled else old_pb
    meta = _wait_exception_meta(collector, enabled_pb, metadata_start)
    assert meta.exceptions[0].stackTraceElement

    # Includes the true->false case: the old span's metadata must survive the
    # current agent generation becoming disabled, while the new span has none.
    pinpoint.shutdown()
    assert _exception_meta_for(collector, disabled_pb, metadata_start) is None


def test_reloaded_callstack_throughput_limits_native_metadata_not_python_walk(
    core_agent, collector, tmp_path,
):
    path = tmp_path / "callstack-throughput.yaml"
    _write_callstack_config(path, collector, enabled=False, throughput=0)
    with collector._cond:
        metadata_start = len(collector.exception_metas)
    agent, _ = core_agent(application_name="", config_file_path=str(path))
    old_revision = agent._native.get_config_snapshot()[11]

    _write_callstack_config(path, collector, enabled=True, throughput=1)
    _wait(
        lambda: (agent._native.get_config_snapshot()[11] > old_revision
                 and agent._native.get_config_snapshot()[14] is True),
        timeout=8.0,
        what="callstack throughput watcher reload",
    )
    spans = [agent.new_span(f"throughput-{i}",
                            f"/core/callstack/throughput/{i}")
             for i in range(2)]
    for i, span in enumerate(spans):
        _record_callstack_exception(span, f"throughput-error-{i}")
        # Both Python frame lists already exist before native admission.
        assert len(span._active_events) == 0
        assert len(span._finished_events[0][-1][0]) == 4

    # Replay both chains back-to-back inside one token interval. Python has
    # already walked both tracebacks; native admission accepts only the first.
    for span in spans:
        span.end()
    pbs = [collector.wait_span(f"/core/callstack/throughput/{i}")
           for i in range(2)]
    pinpoint.shutdown()
    metas = [_exception_meta_for(collector, pb, metadata_start) for pb in pbs]
    assert sum(meta is not None for meta in metas) == 1


# ---------------------------------------------------------------------------
# Span pipeline
# ---------------------------------------------------------------------------

def test_root_span_reaches_collector(core_agent, collector):
    agent, agent_id = core_agent()

    span = agent.new_span("core-root-op", "/core/root")
    trace_id = span.trace_id
    span_id = span.span_id
    span.set_service_type(APP_TYPE_PYTHON)
    span.set_remote_address("10.9.8.7")
    span.set_end_point("svc.example:8080")
    span.set_status_code(200)
    span.annotate_string(999, "core-hello")
    span.end()

    pb = collector.wait_span("/core/root")

    assert _tx_tuple(pb.transactionId) == _parse_trace_id(trace_id)
    assert pb.transactionId.agentId == agent_id
    assert pb.spanId == span_id
    assert pb.serviceType == APP_TYPE_PYTHON
    assert pb.applicationServiceType == APP_TYPE_PYTHON
    assert pb.acceptEvent.rpc == "/core/root"
    assert pb.acceptEvent.endPoint == "svc.example:8080"
    assert pb.acceptEvent.remoteAddr == "10.9.8.7"
    assert pb.err == 0
    now_ms = time.time() * 1000
    assert abs(pb.startTime - now_ms) < 60_000
    assert pb.elapsed >= 0

    anns = _annotations(pb)
    assert anns[999].stringValue == "core-hello"

    # The operation name is registered once as API metadata and referenced
    # from the span by id.
    meta = collector.wait_api_meta("core-root-op")
    assert pb.apiId == meta.apiId


def test_span_event_annotations_and_api_metadata(core_agent, collector):
    agent, _ = core_agent()

    with agent.new_span("core-ev-root", "/core/events") as span:
        span.set_service_type(APP_TYPE_PYTHON)
        with span.new_span_event("core-child-op") as ev:
            ev.annotate_int(101, 7)
            ev.annotate_long(102, 1 << 40)
            ev.annotate_string(103, "abc")
            ev.annotate_string_string(104, "first", "second")
            ev.set_destination("dest-1")
            ev.set_end_point("ep-1:1234")

    pb = collector.wait_span("/core/events")
    assert len(pb.spanEvent) == 1
    ev_pb = pb.spanEvent[0]

    assert ev_pb.sequence == 0
    assert ev_pb.depth == 1
    assert ev_pb.serviceType == SERVICE_TYPE_PYTHON_METHOD

    anns = _annotations(ev_pb)
    assert anns[101].intValue == 7
    assert anns[102].longValue == 1 << 40
    assert anns[103].stringValue == "abc"
    assert anns[104].stringStringValue.stringValue1.value == "first"
    assert anns[104].stringStringValue.stringValue2.value == "second"

    # Only destinationId/nextSpanId are serialized into the message event;
    # the event endpoint has no PSpanEvent slot in the current builder.
    assert ev_pb.nextEvent.messageEvent.destinationId == "dest-1"

    meta = collector.wait_api_meta("core-child-op")
    assert ev_pb.apiId == meta.apiId


def test_error_recording(core_agent, collector):
    agent, _ = core_agent()

    with agent.new_span("core-err-root", "/core/error") as span:
        with span.new_span_event("core-err-child") as ev:
            ev.set_error(ValueError("event boom"))
        span.set_error("RuntimeError", "span kaput")

    pb = collector.wait_span("/core/error")
    assert pb.err == 2  # ErrorCategory::kException wire bit

    ev_pb = pb.spanEvent[0]
    assert ev_pb.exceptionInfo.intValue != 0
    assert ev_pb.exceptionInfo.stringValue.value == "event boom"
    # The error class name travels as string metadata keyed by that id.
    name_meta = collector.wait_for(
        lambda: collector.string_meta_for("ValueError"),
        what="string metadata for ValueError")
    assert ev_pb.exceptionInfo.intValue == name_meta.stringId


@pytest.mark.parametrize("source", [
    "file", "profile", "env-profile", "env-flag",
])
def test_callstack_capture_uses_native_resolved_config(
    core_agent, collector, tmp_path, source,
):
    """Every native config source must drive actual Python frame capture."""
    kwargs = {}
    env = None
    if source == "file":
        path = tmp_path / "callstack.yaml"
        _write_callstack_config(path, collector, enabled=True)
        kwargs.update(application_name="", config_file_path=str(path))
    elif source == "profile":
        kwargs.update(
            enable_callstack_trace=False,
            active_profile="enabled",
            profiles={"enabled": {"EnableCallstackTrace": True}},
        )
    elif source == "env-profile":
        kwargs.update(
            enable_callstack_trace=False,
            active_profile="disabled",
            profiles={
                "disabled": {"EnableCallstackTrace": False},
                "enabled": {"EnableCallstackTrace": True},
            },
        )
        env = {"PINPOINT_PY_ACTIVE_PROFILE": "enabled"}
    else:
        kwargs.update(
            enable_callstack_trace=False,
            active_profile="disabled",
            profiles={"disabled": {"EnableCallstackTrace": False}},
        )
        env = {"PINPOINT_PY_ENABLE_CALLSTACK_TRACE": "true"}

    with collector._cond:
        metadata_start = len(collector.exception_metas)
    agent, _ = core_agent(env=env, **kwargs)
    rpc = f"/core/callstack/{source}"
    with agent.new_span(f"callstack-{source}", rpc) as span:
        assert span._config_snapshot[11] > 0
        assert span._config_snapshot[12] is False
        assert span._config_snapshot[14] is True
        assert span._enable_callstack_trace is True
        _record_callstack_exception(span, f"source-{source}")

    pb = collector.wait_span(rpc)
    assert pb.spanEvent[0].HasField("exceptionInfo")
    meta = _wait_exception_meta(collector, pb, metadata_start)
    exception = meta.exceptions[0]
    assert exception.exceptionClassName == "RuntimeError"
    assert exception.stackTraceElement
    assert any(frame.methodName.endswith("_record_callstack_exception")
               for frame in exception.stackTraceElement)


def test_callstack_exception_chain_shares_one_exception_id(core_agent, collector):
    with collector._cond:
        metadata_start = len(collector.exception_metas)
    agent, _ = core_agent(enable_callstack_trace=True)
    rpc = "/core/callstack/chain"
    with agent.new_span("callstack-chain", rpc) as span:
        try:
            try:
                raise KeyError("root")
            except KeyError as root:
                raise ValueError("middle") from root
        except ValueError as exc:
            with span.new_span_event("chain") as event:
                event.set_error(exc)

    pb = collector.wait_span(rpc)
    ev = pb.spanEvent[0]
    assert ev.exceptionInfo.stringValue.value == "middle"
    chain_ids = [a.value.longValue for a in ev.annotation if a.key == -52]
    meta = _wait_exception_meta(collector, pb, metadata_start)
    assert [e.exceptionClassName for e in meta.exceptions] == ["ValueError", "KeyError"]
    assert [e.exceptionDepth for e in meta.exceptions] == [0, 1]
    assert len({e.exceptionId for e in meta.exceptions}) == 1
    assert chain_ids == [meta.exceptions[0].exceptionId]
    assert all(e.stackTraceElement for e in meta.exceptions)


def test_ignore_error_subclass_rule_records_without_failing(core_agent, collector):
    agent, _ = core_agent(span_ignore_errors=[
        {"name": "ConnectionError", "match_subclasses": True}])
    rpc = "/core/ignore/subclass"
    with agent.new_span("ignore-subclass", rpc) as span:
        with span.new_span_event("step") as event:
            event.set_error(ConnectionResetError("peer closed"))
        span.set_error(ConnectionResetError("root too"))

    pb = collector.wait_span(rpc)
    assert pb.err == 0
    assert pb.exceptionInfo.stringValue.value == "root too"
    assert pb.spanEvent[0].exceptionInfo.stringValue.value == "peer closed"
    # The concrete class, not the rule's name, is what exceptionInfo names.
    name_meta = collector.wait_for(
        lambda: collector.string_meta_for("ConnectionResetError"), what="error name meta")
    assert pb.spanEvent[0].exceptionInfo.intValue == name_meta.stringId
    assert collector.string_meta_for("ConnectionError") is None


def test_callstack_env_false_overrides_profile_without_dropping_error_info(
    core_agent, collector,
):
    with collector._cond:
        metadata_start = len(collector.exception_metas)
    agent, _ = core_agent(
        enable_callstack_trace=True,
        active_profile="enabled",
        profiles={"enabled": {"EnableCallstackTrace": True}},
        env={"PINPOINT_PY_ENABLE_CALLSTACK_TRACE": "false"},
    )
    rpc = "/core/callstack/env-disabled"
    with agent.new_span("callstack-env-disabled", rpc) as span:
        assert span._enable_callstack_trace is False
        _record_callstack_exception(span, "env-disabled")

    pb = collector.wait_span(rpc)
    assert pb.spanEvent[0].HasField("exceptionInfo")
    pinpoint.shutdown()  # drain both span and metadata workers before absence check
    assert _exception_meta_for(collector, pb, metadata_start) is None


@pytest.mark.parametrize("overflow_kind", ["depth", "sequence"])
def test_overflow_error_marks_root_without_recording_error_detail(
    core_agent,
    collector,
    overflow_kind,
):
    rpc = f"/core/overflow/{overflow_kind}"
    error_name = f"Python{overflow_kind.title()}OverflowError"
    kwargs = ({"span_max_event_depth": 2}
              if overflow_kind == "depth"
              else {"span_max_event_sequence": 4})
    agent, _ = core_agent(**kwargs)

    with agent.new_span(f"core-{overflow_kind}-overflow-root", rpc) as span:
        if overflow_kind == "depth":
            kept = [span.new_span_event(f"kept-depth-{i}") for i in range(3)]
            overflow = span.new_span_event("discarded-depth")
            overflow.set_error(error_name, "boom")
            overflow.end()
            for event in reversed(kept):
                event.end()
        else:
            for i in range(4):
                span.new_span_event(f"kept-sequence-{i}").end()
            overflow = span.new_span_event("discarded-sequence")
            overflow.set_error(error_name, "boom")
            overflow.end()

    pb = collector.wait_span(rpc)
    assert pb.err == 2
    assert len(pb.spanEvent) == (3 if overflow_kind == "depth" else 4)
    assert not pb.HasField("exceptionInfo")
    assert all(not event.HasField("exceptionInfo") for event in pb.spanEvent)
    assert collector.api_meta_for(f"discarded-{overflow_kind}") is None
    assert collector.string_meta_for(error_name) is None


@pytest.mark.parametrize(
    ("source", "error_name", "expected_err"),
    [
        ("inline-ignore", "InlineIgnored", 0),
        ("profile-ignore", "ProfileIgnored", 0),
        ("env-ignore", "EnvIgnored", 0),
        ("category-exclude", "ExcludedCategory", 0),
        ("nonmatching-ignore", "RealFailure", 2),
    ],
)
def test_overflow_error_policy_uses_native_resolved_config(
    core_agent,
    collector,
    source,
    error_name,
    expected_err,
):
    overrides = {"span_max_event_sequence": 4}
    env = None
    if source == "inline-ignore":
        overrides["span_ignore_errors"] = [{"name": error_name}]
    elif source == "profile-ignore":
        overrides.update(
            active_profile="errors",
            profiles={
                "errors": {"Span": {"IgnoreErrors": [{"name": error_name}]}}
            },
        )
    elif source == "env-ignore":
        env = {"PINPOINT_PY_SPAN_IGNORE_ERRORS": error_name}
    elif source == "category-exclude":
        overrides["span_error_mark_exclude"] = ["exception"]
    else:
        overrides["span_ignore_errors"] = [{"name": "SomeOtherError"}]

    agent, _ = core_agent(env=env, **overrides)
    rpc = f"/core/overflow-policy/{source}"
    with agent.new_span(f"core-overflow-policy-{source}", rpc) as span:
        for i in range(4):
            span.new_span_event(f"kept-{i}").end()
        event = span.new_span_event("discarded")
        event.set_error(error_name, "policy message")
        event.end()

    pb = collector.wait_span(rpc)
    assert pb.err == expected_err
    assert len(pb.spanEvent) == 4
    assert all(not event.HasField("exceptionInfo") for event in pb.spanEvent)
    assert not pb.HasField("exceptionInfo")
    assert collector.string_meta_for(error_name) is None


class _UnsampledResetError(ConnectionError):
    """Unique name: the module-scoped collector keeps earlier string metas."""


@pytest.mark.parametrize(
    ("policy", "expected_failed"),
    [
        ("none", 1),
        ("ignore", 0),
        ("nonmatching-ignore", 1),
        ("category-exclude", 0),
        ("subclass-ignore", 0),
    ],
)
def test_unsampled_errors_fail_only_uri_stat_under_native_policy(
    core_agent,
    collector,
    policy,
    expected_failed,
):
    error_name = f"Unsampled{policy.title().replace('-', '')}Error"
    overrides = dict(
        sampling_type="PERCENT",
        sampling_percent_rate=0,
        http_collect_url_stat=True,
        stat_enabled=True,
        stat_batch_count=1,
        stat_batch_interval=1000,
    )
    if policy == "subclass-ignore":
        # Python-side matching: the rule names the parent class.
        error_name = _UnsampledResetError.__name__
        overrides["span_ignore_errors"] = [
            {"name": "ConnectionError", "match_subclasses": True}]
    elif policy == "ignore":
        overrides["span_ignore_errors"] = [{"name": error_name}]
    elif policy == "nonmatching-ignore":
        overrides["span_ignore_errors"] = [{"name": "OtherError"}]
    elif policy == "category-exclude":
        overrides["span_error_mark_exclude"] = ["exception"]

    with collector._cond:
        stat_messages_before = len(collector.stat_messages)
    agent, _ = core_agent(**overrides)
    collector.wait_for(
        lambda: next((message for message in
                      collector.stat_messages[stat_messages_before:]
                      if message.HasField("agentStatBatch")), None),
        what="warm agent stat stream",
    )
    rpc = f"/core/unsampled-policy/{policy}"
    span = agent.new_span(f"core-unsampled-{policy}", rpc)
    assert span.sampled is False
    span.set_url_stat(rpc, "GET", 200)
    event = span.new_span_event("unsampled-event")

    def error(message):
        # Only an exception instance can be matched by subclass.
        return (_UnsampledResetError(message) if policy == "subclass-ignore"
                else error_name)

    event.set_error(error("event failure"), "event failure")
    # Repeated event error plus span error exercise both wrappers; the native
    # verdict is an idempotent category bit.
    event.set_error(error("event failure again"), "event failure again")
    span.set_error(error("span failure"), "span failure")
    event.end()
    span.end()

    # The request path enqueues URL stats into a shard which the native add
    # worker drains every 10 ms. Shutdown deliberately stops that producer
    # before flushing the aggregated snapshot, so let this freshly enqueued
    # entry reach the snapshot first. The shutdown flush then makes the open
    # 30-second tick observable without slowing this test by a full tick.
    time.sleep(0.05)
    # Unsampled spans/events and their error metadata never enter the span or
    # metadata streams.
    pinpoint.shutdown()
    total, failed = collector.wait_for(
        lambda: (_uri_stat_counts(collector, rpc)
                 if _uri_stat_counts(collector, rpc)[0] >= 1 else None),
        timeout=5.0,
        what=f"URI stat for {rpc}",
    )
    assert (total, failed) == (1, expected_failed)
    assert collector.spans_with_rpc(rpc) == []
    assert collector.string_meta_for(error_name) is None


def test_sql_metadata_registration(core_agent, collector):
    agent, _ = core_agent()

    with agent.new_span("core-sql-root", "/core/sql") as span:
        with span.new_span_event("core-sql-child") as ev:
            ev.set_sql_query("SELECT * FROM users WHERE id = 1", "1")

    pb = collector.wait_span("/core/sql")
    ev_pb = pb.spanEvent[0]
    anns = _annotations(ev_pb)
    assert ANNOTATION_SQL_ID in anns
    sql_id = anns[ANNOTATION_SQL_ID].intStringStringValue.intValue

    meta = collector.wait_for(
        lambda: next((m for m in collector.sql_metas if m.sqlId == sql_id),
                     None),
        what=f"sql metadata id={sql_id}")
    assert "users" in meta.sql.lower()


# ---------------------------------------------------------------------------
# Trace-context propagation
# ---------------------------------------------------------------------------

def test_context_propagation_continues_trace(core_agent, collector):
    agent, agent_id = core_agent()

    parent = agent.new_span("core-prop-parent", "/core/prop/parent")
    parent_trace_id = parent.trace_id
    parent_span_id = parent.span_id
    with parent:
        # Injection rides the active span event, mirroring the HTTP client
        # wrappers (open outbound event → inject → send). The destination is
        # what keys the serialized messageEvent carrying nextSpanId.
        with parent.new_span_event("core-prop-client") as ev:
            ev.set_destination("api.internal")
            pairs = dict(propagator.inject_items(parent))

    assert pairs[propagator.HEADER_TRACE_ID] == parent_trace_id
    assert pairs[propagator.HEADER_PARENT_SPAN_ID] == str(parent_span_id)
    assert pairs[propagator.HEADER_PARENT_APP_NAME] == _APP_NAME
    assert pairs[propagator.HEADER_PARENT_APP_TYPE] == str(APP_TYPE_PYTHON)
    # Sampled transactions omit Pinpoint-Sampled; "s0" is written only for
    # drops (see test_unsampled_span_is_dropped_and_propagates_s0).
    assert propagator.HEADER_SAMPLED not in pairs
    next_span_id = int(pairs[propagator.HEADER_SPAN_ID])

    # Downstream service: same wire headers arrive on an inbound request.
    child = agent.new_span(
        "core-prop-child", "/core/prop/child",
        headers={**pairs, propagator.HEADER_HOST: "api.internal"})
    assert child.sampled is True
    assert child.trace_id == parent_trace_id
    child.end()

    child_pb = collector.wait_span("/core/prop/child")
    parent_pb = collector.wait_span("/core/prop/parent")

    # Same transaction, correct parent/child linkage.
    assert _tx_tuple(child_pb.transactionId) == _tx_tuple(parent_pb.transactionId)
    assert child_pb.spanId == next_span_id
    assert child_pb.parentSpanId == parent_span_id
    info = child_pb.acceptEvent.parentInfo
    assert info.parentApplicationName == _APP_NAME
    # Extracted from Pinpoint-Host and preserved across the bare end() —
    # finalize must not wipe values the wrapper never set.
    assert info.acceptorHost == "api.internal"
    assert child_pb.acceptEvent.endPoint == "api.internal"
    assert child_pb.acceptEvent.remoteAddr == "api.internal"
    # The client-side event pre-recorded the downstream span id.
    client_ev = parent_pb.spanEvent[0]
    assert client_ev.nextEvent.messageEvent.nextSpanId == next_span_id


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

def test_unsampled_span_is_dropped_and_propagates_s0(core_agent, collector):
    agent, _ = core_agent(sampling_counter_rate=2)

    # COUNTER 1-out-of-2: exactly one of two back-to-back new transactions is
    # sampled; partition instead of assuming the counter's phase.
    first = agent.new_span("core-samp-a", "/core/sampling/a")
    second = agent.new_span("core-samp-b", "/core/sampling/b")
    sampled, dropped = ((first, second) if first.sampled
                        else (second, first))
    assert sampled.sampled is True
    assert dropped.sampled is False
    assert dropped.trace_id == ""
    # An unsampled span still carries the real native span id (its
    # active-span registration lives until end()), unlike the pure no-op
    # _NullSpan whose span_id is 0.
    assert dropped.span_id != 0

    # An unsampled span propagates only the drop marker downstream.
    with dropped.new_span_event("core-samp-client"):
        drop_pairs = dict(propagator.inject_items(dropped))
    assert drop_pairs == {propagator.HEADER_SAMPLED: "s0"}

    # End the dropped span *before* the sampled one: batches preserve order,
    # so once the sampled span arrives, a wrongly-exported dropped span would
    # already be visible.
    dropped_rpc = ("/core/sampling/b" if dropped is second
                   else "/core/sampling/a")
    sampled_rpc = ("/core/sampling/a" if dropped is second
                   else "/core/sampling/b")
    dropped.end()
    sampled.end()
    collector.wait_span(sampled_rpc)
    assert collector.spans_with_rpc(dropped_rpc) == []

    # Continuation of an s0 upstream decision is dropped as well.
    cont = agent.new_span(
        "core-samp-cont", "/core/sampling/cont",
        headers={
            propagator.HEADER_TRACE_ID: "other-agent^1700000000000^7",
            propagator.HEADER_SPAN_ID: "12345",
            propagator.HEADER_SAMPLED: "s0",
        })
    assert cont.sampled is False
    cont.end()


def test_env_var_overrides_yaml_config(core_agent, collector):
    # YAML says sample everything; PINPOINT_PY_* must win inside the native
    # agent (same precedence operators rely on in production).
    agent, _ = core_agent(
        sampling_counter_rate=1,
        env={"PINPOINT_PY_SAMPLING_COUNTER_RATE": "0"})

    assert agent.enabled is True
    span = agent.new_span("core-env-op", "/core/env")
    assert span.sampled is False
    span.end()


# ---------------------------------------------------------------------------
# Async spans
# ---------------------------------------------------------------------------

def test_async_span_exported_as_chunk(core_agent, collector):
    agent, _ = core_agent()

    root = agent.new_span("core-async-root", "/core/async")
    with root:
        root.set_service_type(APP_TYPE_PYTHON)
        with root.new_span_event("core-async-launcher"):
            async_span = root.new_async_span("core-async-work")

        def _worker():
            with async_span:
                with async_span.new_span_event("core-async-inner"):
                    pass

        t = threading.Thread(target=_worker)
        t.start()
        t.join()

    root_pb = collector.wait_span("/core/async")
    chunk = collector.wait_chunk(root_pb.spanId)

    assert _tx_tuple(chunk.transactionId) == _tx_tuple(root_pb.transactionId)
    assert chunk.HasField("localAsyncId")
    # The launcher event carries the async link the chunk resolves against.
    launcher = next(e for e in root_pb.spanEvent
                    if e.apiId == collector.wait_api_meta("core-async-launcher").apiId)
    assert launcher.asyncEvent == chunk.localAsyncId.asyncId
    # The chunk's own events reference the async operation's API metadata.
    work_meta = collector.wait_api_meta("core-async-work")
    assert any(e.apiId == work_meta.apiId for e in chunk.spanEvent) or any(
        e.apiId == collector.wait_api_meta("core-async-inner").apiId
        for e in chunk.spanEvent)


def test_async_span_callstack_inherits_root_snapshot_and_keeps_trace_link(
    core_agent, collector,
):
    with collector._cond:
        metadata_start = len(collector.exception_metas)
    agent, _ = core_agent(enable_callstack_trace=True,
                          callstack_trace_new_throughput=0)
    rpc = "/core/callstack/async"
    root = agent.new_span("callstack-async-root", rpc)
    with root.new_span_event("callstack-async-launch"):
        child = root.new_async_span("callstack-async-work")

    assert child._config_snapshot == root._config_snapshot
    assert child._enable_callstack_trace is True
    _record_callstack_exception(child, "callstack-async-error")
    child.end()
    root.end()

    root_pb = collector.wait_span(rpc)
    chunk = collector.wait_chunk(root_pb.spanId)
    assert _tx_tuple(chunk.transactionId) == _tx_tuple(root_pb.transactionId)
    error_event = next(event for event in chunk.spanEvent
                       if event.HasField("exceptionInfo"))
    assert error_event.exceptionInfo.stringValue.value == (
        "callstack-async-error boom")
    meta = _wait_exception_meta(collector, root_pb, metadata_start)
    assert meta.spanId == root_pb.spanId
    assert meta.exceptions[0].stackTraceElement
    assert any(frame.methodName.endswith("_record_callstack_exception")
               for frame in meta.exceptions[0].stackTraceElement)


def test_async_child_overflow_error_marks_root_before_child_end(
    core_agent,
    collector,
):
    agent, _ = core_agent(span_max_event_sequence=4)
    root = agent.new_span("core-async-overflow-root", "/core/async-overflow")
    with root.new_span_event("core-async-overflow-launcher"):
        child = root.new_async_span("core-async-overflow-work")

    # The async native span already owns sequence 0. Fill its remaining three
    # slots, then overflow at max=4. Mark it and serialize the root before
    # ending the child: batching this verdict at child.end() would lose it.
    for i in range(3):
        child.new_span_event(f"kept-async-{i}").end()
    overflow = child.new_span_event("discarded-async-event")
    overflow.set_error("AsyncOverflowFailure", "boom")
    root.end()

    root_pb = collector.wait_span("/core/async-overflow")
    assert root_pb.err == 2
    assert not root_pb.HasField("exceptionInfo")
    assert collector.string_meta_for("AsyncOverflowFailure") is None

    overflow.end()
    child.end()
    chunk = collector.wait_chunk(root_pb.spanId)
    assert all(not event.HasField("exceptionInfo") for event in chunk.spanEvent)
    assert collector.api_meta_for("discarded-async-event") is None


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def test_agent_stats_reported(core_agent, collector):
    # 1000ms is the native floor for the collect interval; one-stat batches
    # make the first PStatMessage arrive right after the first tick.
    core_agent(stat_enabled=True, stat_batch_count=1, stat_batch_interval=1000)

    msg = collector.wait_for(
        lambda: next((m for m in collector.stat_messages
                      if m.HasField("agentStatBatch")), None),
        what="agent stat batch")
    batch = msg.agentStatBatch
    assert len(batch.agentStat) >= 1
    stat = batch.agentStat[0]
    assert stat.timestamp > 0
    # Native reports elapsed wall time, including scheduler delay.
    assert stat.collectInterval >= 1000


# ---------------------------------------------------------------------------
# Shutdown / re-init lifecycle
# ---------------------------------------------------------------------------

def test_shutdown_drains_and_reinit_registers_fresh_agent(core_agent, collector):
    _, first_id = core_agent()
    agent = agent_mod.get_agent()
    # Warm the span channel first: the drain-at-shutdown guarantee sends
    # still-queued spans over an already-connected channel (it must not start
    # waiting for a connection while exiting).
    agent.new_span("core-warm-op", "/core/warm").end()
    collector.wait_span("/core/warm")

    span = agent.new_span("core-drain-op", "/core/drain")
    span.end()
    # No flush wait: shutdown() must drain the queued span before returning.
    pinpoint.shutdown()
    pb = collector.wait_span("/core/drain", timeout=5.0)
    assert pb.transactionId.agentId == first_id
    assert agent_mod.get_agent() is None

    # Same process, fresh lifecycle: new registration, spans under the new id.
    agent2, second_id = core_agent()
    assert second_id != first_id
    collector.wait_agent_info(second_id)
    span2 = agent2.new_span("core-reinit-op", "/core/reinit")
    span2.end()
    pb2 = collector.wait_span("/core/reinit")
    assert pb2.transactionId.agentId == second_id


def test_native_logs_reach_python_and_replace_file_sink(
    core_agent, collector, tmp_path,
):
    logger = logging.getLogger("pinpoint.native")
    saved = logger.level, list(logger.handlers), logger.propagate
    records = []

    class Capture(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger.handlers[:] = [Capture()]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    log_path = tmp_path / "must-not-be-written.log"
    try:
        agent, _ = core_agent(
            native_log_to_python=True,
            log_output=str(log_path),
            log_level="INFO",
        )
        _wait(
            lambda: any("resolved agent identity" in r.getMessage()
                        for r in records),
            what="native startup log in Python",
        )
        agent.new_span("native-log-span", "/core/native-log").end()
        collector.wait_span("/core/native-log")
        pinpoint.shutdown()

        assert all(record.name == "pinpoint.native" for record in records)
        assert any(record.levelno == logging.INFO for record in records)
        assert any("[pinpoint][" in record.getMessage() for record in records)
        assert any("agent shutdown" in record.getMessage()
                   for record in records)
        # Python-side agent lines land in the file; the native sink must not.
        assert not log_path.exists() or "[pinpoint][" not in log_path.read_text()
        assert agent.native_log_dropped == 0
    finally:
        logger.level, logger.handlers[:], logger.propagate = saved


def test_native_sink_disabled_preserves_native_file_output(core_agent, tmp_path):
    log_path = tmp_path / "native-built-in.log"
    core_agent(log_output=str(log_path), log_level="INFO")
    pinpoint.shutdown()
    assert "[pinpoint]" in log_path.read_text()


def test_slow_native_log_handler_does_not_stall_span_pipeline(
    core_agent, collector,
):
    logger = logging.getLogger("pinpoint.native")
    saved = logger.level, list(logger.handlers), logger.propagate
    entered = threading.Event()
    release = threading.Event()

    class Slow(logging.Handler):
        def emit(self, record):
            entered.set()
            release.wait(3)

    logger.handlers[:] = [Slow()]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    try:
        agent, _ = core_agent(
            native_log_to_python=True, log_level="INFO")
        assert entered.wait(2)
        agent.new_span("slow-log-handler", "/core/slow-log-handler").end()
        collector.wait_span("/core/slow-log-handler", timeout=5)
    finally:
        release.set()
        pinpoint.shutdown()
        logger.level, logger.handlers[:], logger.propagate = saved


def test_startup_config_failure_is_delivered_to_python_logger(monkeypatch):
    for key in list(os.environ):
        if key.startswith("PINPOINT_PY_"):
            monkeypatch.delenv(key)
    logger = logging.getLogger("pinpoint.native")
    saved = logger.level, list(logger.handlers), logger.propagate
    messages = []

    class Capture(logging.Handler):
        def emit(self, record):
            messages.append(record.getMessage())

    logger.handlers[:] = [Capture()]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    try:
        agent = pinpoint.init(
            application_name="invalid-yaml",
            native_log_to_python=True,
            extra_yaml="broken: [yaml",
            log_level="INFO",
        )
        assert isinstance(agent, agent_mod._NullAgent)
        assert any("yaml parsing exception" in message for message in messages)
        assert any("address of collector is required" in message
                   for message in messages)
    finally:
        pinpoint.shutdown()
        logger.level, logger.handlers[:], logger.propagate = saved


# ---------------------------------------------------------------------------
# Fork (prefork-server model)
# ---------------------------------------------------------------------------
#
# The native agent supports one prefork model: the master makes no native
# agent call (init(prefork=True) stores Python configuration only), and each
# worker's after-fork hook performs that process's first StartAgent/grpc_init.
# A *warm* master (default init(), agent started before fork) hands children
# an unrecoverable gRPC runtime, so the hook degrades them to no-op agents.
# Both contracts are asserted here with real fork() + real gRPC.

# The tracing process must stay alive until the collector has seen its data:
# the shutdown() drain only sends over an already-connected span channel, so
# a trace-then-exit-immediately process may legitimately drop its last batch
# (see test_shutdown_drains_and_reinit_registers_fresh_agent, which warms the
# channel first). The scenarios therefore park on a "done file" that the test
# creates once the expected spans arrived, and only then shut down and exit.
_FORK_SCRIPT_COMMON = textwrap.dedent("""\
    import os, sys, time

    sys.path.insert(0, {repo_root!r})
    import pinpoint

    def wait_enabled(agent, exit_code, what):
        deadline = time.monotonic() + 30
        while not agent.enabled:
            if time.monotonic() >= deadline:
                print("timed out waiting for", what, file=sys.stderr)
                os._exit(exit_code)
            time.sleep(0.02)

    def wait_done_file(exit_code):
        deadline = time.monotonic() + 60
        while not os.path.exists({done_file!r}):
            if time.monotonic() >= deadline:
                print("timed out waiting for done file", file=sys.stderr)
                os._exit(exit_code)
            time.sleep(0.05)

    def run_child(child):
        pid = os.fork()
        if pid == 0:
            child()
            os._exit(0)
        return pid

    pinpoint.init(
        application_name={app_name!r},
        agent_name={agent_name!r},
        collector_host="127.0.0.1",
        collector_agent_port={port},
        collector_span_port={port},
        collector_stat_port={port},
        span_batch_flush_interval_ms=100,
        span_batch_collect_deadline_ms=50,
        agent_info_send_retry_interval_ms=200,
        stat_enabled=False,
        native_log_to_python=True,
        prefork={prefork},
    )
""")

# Exit codes shared by both scenarios: 3 = master precondition failed;
# 4 = the after-fork hook broke its contract in the child; 5 = child
# handshake never completed (hard failure — the cold model must connect);
# 6 = the test never confirmed span receipt; 0 = success.
_PREFORK_SCRIPT = _FORK_SCRIPT_COMMON + textwrap.dedent("""\

    # Pending master: no native agent was created, so it must not trace.
    master = pinpoint.get_agent()
    if master.enabled:
        print("cold master unexpectedly enabled", file=sys.stderr)
        os._exit(3)

    def child():
        # The hook must have started this process's first native agent, and
        # its channels must actually connect.
        agent = pinpoint.get_agent()
        if agent is None or type(agent).__name__ in ("_PendingAgent", "_NullAgent"):
            print("child agent not started:", agent, file=sys.stderr)
            os._exit(4)
        wait_enabled(agent, 5, "child handshake")
        agent.new_span("fork-child-op", "/fork/child").end()
        # Stay alive until the test has seen the span, then drain and exit.
        wait_done_file(6)
        pinpoint.shutdown()

    pid = run_child(child)
    _, status = os.waitpid(pid, 0)

    # The master stays cold for its whole life: still disabled, and a span
    # created here is a no-op that must never reach the collector.
    master = pinpoint.get_agent()
    if master.enabled:
        print("master became enabled after fork", file=sys.stderr)
        os._exit(3)
    span = master.new_span("fork-master-op", "/fork/master-noop")
    if span.sampled:
        print("pending master produced a sampled span", file=sys.stderr)
        os._exit(3)
    span.end()

    pinpoint.shutdown()
    sys.exit(os.waitstatus_to_exitcode(status))
""")

_WARM_SCRIPT = _FORK_SCRIPT_COMMON + textwrap.dedent("""\

    wait_enabled(pinpoint.get_agent(), 3, "parent handshake")

    def child():
        # Warm-master fork: the hook must degrade this child to a no-op
        # agent (a working rebuild is impossible on the inherited gRPC
        # runtime) — not leave a wedged half-alive agent behind.
        agent = pinpoint.get_agent()
        if type(agent).__name__ != "_NullAgent":
            print("child agent not degraded:", agent, file=sys.stderr)
            os._exit(4)
        if agent.enabled:
            print("degraded child agent claims enabled", file=sys.stderr)
            os._exit(4)
        span = agent.new_span("fork-child-op", "/fork/warm-child")
        if span.sampled:
            print("degraded child produced a sampled span", file=sys.stderr)
            os._exit(4)
        span.end()
        pinpoint.shutdown()

    pid = run_child(child)
    _, status = os.waitpid(pid, 0)

    # The parent's own tracing must be completely unaffected by the fork.
    agent = pinpoint.get_agent()
    agent.new_span("fork-parent-op", "/fork/parent").end()
    # Stay alive until the test has seen the span, then drain and exit.
    wait_done_file(6)
    pinpoint.shutdown()
    sys.exit(os.waitstatus_to_exitcode(status))
""")


def _run_fork_scenario(tmp_path, script_template, *, agent_name, port, prefork,
                       wait_while_running):
    """Launch a fork scenario subprocess, run ``wait_while_running()`` while
    it is parked on the done file, then release it and assert a clean exit."""
    repo_root = os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))
    done_file = tmp_path / "fork_scenario_done"
    script = tmp_path / "fork_scenario.py"
    script.write_text(script_template.format(
        repo_root=repo_root, app_name=_APP_NAME, agent_name=agent_name,
        port=port, prefork=prefork, done_file=str(done_file)))

    env = {k: v for k, v in os.environ.items()
           if not k.startswith("PINPOINT_PY_")}
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        wait_while_running()
    finally:
        # Always release the parked scenario — on a wait failure this turns
        # a 60s subprocess hang into an immediate, diagnosable exit.
        done_file.touch()
        try:
            out, err = proc.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
            raise
    assert proc.returncode == 0, (
        f"fork scenario failed (rc={proc.returncode})\n"
        f"stdout:\n{out}\nstderr:\n{err}")


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_fork_prefork_pending_master_child_registers_and_traces(
    collector,
    tmp_path,
):
    """gunicorn --preload model, done right: init(prefork=True) keeps the
    master native-free; each worker hook performs that process's first
    StartAgent() and gets fully working gRPC channels."""
    with collector._cond:
        registrations_before = len(collector.agent_infos)
    _run_fork_scenario(
        tmp_path, _PREFORK_SCRIPT,
        agent_name="core-it-prefork", port=collector.port, prefork=True,
        wait_while_running=lambda: collector.wait_span("/fork/child"))

    child_pb = collector.wait_span("/fork/child")
    child_agent_id = child_pb.transactionId.agentId
    assert len(child_agent_id) == 22
    assert child_agent_id != "core-it-prefork"
    collector.wait_agent_info(child_agent_id)

    # The pending master never registers; only the child did.
    with collector._cond:
        new_registrations = collector.agent_infos[registrations_before:]
    assert len(new_registrations) == 1
    assert collector.spans_with_rpc("/fork/master-noop") == []


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_fork_warm_master_child_degrades_to_null_agent(collector, tmp_path):
    """Default init() then fork: the child cannot be given a working agent
    (the inherited gRPC runtime is dead and its leaked channels pin the
    C-core), so the hook must disable tracing there cleanly while the
    parent keeps tracing."""
    with collector._cond:
        registrations_before = len(collector.agent_infos)
    _run_fork_scenario(
        tmp_path, _WARM_SCRIPT,
        agent_name="core-it-warm", port=collector.port, prefork=False,
        wait_while_running=lambda: collector.wait_span("/fork/parent"))

    parent_pb = collector.wait_span("/fork/parent")
    parent_agent_id = parent_pb.transactionId.agentId
    assert len(parent_agent_id) == 22
    collector.wait_agent_info(parent_agent_id)
    # The degraded child neither registers nor exports spans.
    assert collector.spans_with_rpc("/fork/warm-child") == []
    with collector._cond:
        new_registrations = collector.agent_infos[registrations_before:]
    assert [md.get("agentid") for md, _ in new_registrations] == [
        parent_agent_id,
    ]


@pytest.mark.parametrize("options,expected", [
    ({}, 2 | 4 | 8),
    ({"span_error_mark_exclude": ["http-status", "sql"]}, 2),
    ({"span_error_mark": ["sql"]}, 8),
    ({"span_ignore_errors": [{"name": "ValueError", "message_contains": "ignored"}]}, 4 | 8),
])
def test_main_error_policy_options_reach_native(core_agent, collector, options, expected):
    agent, _ = core_agent(sql_error_count=2, **options)
    rpc = "/main/error-policy/" + str(expected)
    with agent.new_span("policy", rpc) as span:
        with span.new_span_event("query") as ev:
            ev.set_sql_query("SELECT 1")
        with span.new_span_event("second-query") as ev:
            ev.set_sql_query("SELECT 2")
            # Native stops counting SQL once another cause already failed
            # the trace, so reach the SQL threshold before recording this.
            ev.set_error(ValueError("ignored error"))
        span.set_status_code(503)
    assert collector.wait_span(rpc).err == expected


def test_main_depth_limit_reaches_collector(core_agent, collector):
    agent, _ = core_agent(span_max_event_depth=2)
    with agent.new_span("depth", "/main/depth") as span:
        for i in range(4):
            span.new_span_event(f"nested-{i}")
    pb = collector.wait_span("/main/depth")
    assert [(ev.sequence, ev.depth) for ev in pb.spanEvent] == [(0, 1), (1, 2), (2, 3)]


def test_main_large_sql_is_normalized_before_metadata_abbreviation(core_agent, collector):
    agent, _ = core_agent(sql_remove_comments=True)
    metadata_before = len(collector.sql_metas)
    sql = "SELECT /*" + "x" * 70000 + "*/ value FROM main_review_table WHERE id = 123"
    with agent.new_span("sql", "/main/large-sql") as span:
        with span.new_span_event("query") as ev:
            ev.set_sql_query(sql)
            ev.set_sql_query("x" * (1024 * 1024 + 1))
    pb = collector.wait_span("/main/large-sql")
    sql_annotations = [a for a in pb.spanEvent[0].annotation if a.key == ANNOTATION_SQL_ID]
    assert len(sql_annotations) == 1
    sql_id = sql_annotations[0].value.intStringStringValue.intValue
    meta = collector.wait_for(
        lambda: next((m for m in collector.sql_metas[metadata_before:] if m.sqlId == sql_id), None),
        what="large normalized SQL metadata")
    assert "main_review_table" in meta.sql
    assert len(meta.sql) < 200


def test_main_profile_selection_and_env_precedence(core_agent, collector):
    agent, agent_id = core_agent(
        active_profile="discard",
        profiles={"discard": {"Sampling": {"CounterRate": 0}},
                  "keep": {"Sampling": {"CounterRate": 1},
                           "Span": {"MaxEventDepth": 2}}},
        env={"PINPOINT_PY_ACTIVE_PROFILE": "keep", "PINPOINT_PY_SPAN_MAX_EVENT_DEPTH": "3"},
        log_max_backups=2,
        sql_cache_length_limit=1024,
        sql_cache_expire_hours=24,
        callstack_trace_new_throughput=10,
    )
    with agent.new_span("profile", "/main/profile") as span:
        assert span.sampled
        assert span._max_event_depth == 3
        span.new_span_event("recorded").end()
    assert collector.wait_span("/main/profile").spanEvent


@pytest.mark.parametrize("trace_headers,flags", [
    ({}, "0"),
    ({"Pinpoint-TraceID": "upstream^100^7"}, "0"),
    ({"Pinpoint-TraceID": "bad^oops^7", "Pinpoint-SpanID": "42", "Pinpoint-pSpanID": "41"}, "0"),
    ({"Pinpoint-TraceID": "upstream^+100^007^ignored", "Pinpoint-SpanID": "42", "Pinpoint-pSpanID": "41"}, "7"),
])
def test_main_continuation_flags_and_acceptor(core_agent, collector, trace_headers, flags):
    agent, _ = core_agent()
    rpc = "/main/continue/" + str(len(trace_headers)) + "/" + flags
    headers = {**trace_headers, "Pinpoint-Flags": "7", "Pinpoint-Host": "peer.test",
               "Pinpoint-pAppName": "upstream-app", "Pinpoint-pAppType": "1000"}
    with agent.new_span("incoming", rpc, headers=headers) as span:
        trace_http_server_request(span, "127.0.0.1", "endpoint.test", {})
        with span.new_span_event("outgoing"):
            outgoing = dict(span.inject_context_items())
            assert outgoing["Pinpoint-Flags"] == flags
            assert "Pinpoint-pAppNamespace" not in outgoing
            assert "Pinpoint-Host" not in outgoing
        assert span._acceptor_host == ("peer.test" if flags == "7" else "endpoint.test")
    pb = collector.wait_span(rpc)
    assert pb.flag == int(flags)


def test_main_existing_extra_yaml_profile_selection(core_agent, collector):
    agent, _ = core_agent(
        sampling_counter_rate=0,
        profiles={"keep": {"Sampling": {"CounterRate": 1}}},
        extra_yaml="ActiveProfile: keep",
    )
    with agent.new_span("extra-profile", "/main/extra-profile") as span:
        assert span.sampled
        span.new_span_event("recorded").end()
    assert collector.wait_span("/main/extra-profile").spanEvent


def _proxy_wire_values(pb):
    from pinpoint.annotation import ANNOTATION_HTTP_PROXY_HEADER

    values = [a.value.longIntIntByteByteStringValue for a in pb.annotation
              if a.key == ANNOTATION_HTTP_PROXY_HEADER]
    return [(v.longValue, v.intValue1, v.intValue2,
             v.byteValue1, v.byteValue2, v.stringValue.value) for v in values]


def _write_user_proxy_config(path, collector, names, **overrides):
    from pinpoint.config import Config

    cfg = dict(
        application_name=_APP_NAME, collector_host="127.0.0.1",
        collector_agent_port=collector.port, collector_span_port=collector.port,
        collector_stat_port=collector.port, stat_enabled=False,
        span_batch_flush_interval_ms=100, enable_config_file_watcher=True,
        http_server_proxy_user_header_names=names,
    )
    cfg.update(overrides)
    # Atomic replacement prevents the watcher from reading a partial document.
    pending = path.with_suffix(".tmp")
    pending.write_text(Config(**cfg).to_yaml())
    pending.replace(path)


@pytest.mark.parametrize("source", ["kwargs", "file", "profile", "env"])
def test_user_proxy_resolved_config_and_wire(core_agent, collector, tmp_path, source):
    names = ["X-Millis", "X-Micros", "X-Seconds", "X-Bad", "X-" + "가" * 11]
    profile = {"edge": {"Http": {"Server": {"ProxyUserHeaderNames": names}}}}
    kwargs = dict(http_server_proxy_user_header_names=["X-Stale"])
    if source == "kwargs":
        kwargs["http_server_proxy_user_header_names"] = names
    elif source == "profile":
        kwargs.update(active_profile="edge", profiles=profile)
    else:
        path = tmp_path / "proxy.yaml"
        _write_user_proxy_config(path, collector, names if source == "file" else ["X-File"],
                                 profiles={"edge": {"Http": {"Server": {
                                     "ProxyUserHeaderNames": ["X-Profile"],
                                 }}}})
        kwargs["config_file_path"] = str(path)
        if source == "env":
            kwargs["env"] = {
                "PINPOINT_PY_ACTIVE_PROFILE": "edge",
                "PINPOINT_PY_HTTP_SERVER_PROXY_USER_HEADER_NAMES": ",".join(names),
            }
    agent, _ = core_agent(**kwargs)
    span = agent.new_span("proxy", f"/core/proxy/{source}")
    assert span._config_snapshot[11] > 0
    assert span._config_snapshot[12] is False
    assert span._config_snapshot[13] == names
    trace_http_server_request(span, "127.0.0.1", "example.test", {
        "x-millis": "t=1504230492763 D=42",
        "x-micros": "t=1504230492763123 D=2147483648",
        "x-seconds": "t=1504230492.763 D=0.123",
        "x-bad": "t=invalid D=10",
        "X-Stale": "t=1504230492763", "X-File": "t=1504230492763",
        names[-1]: "t=1504230492763",
        "Pinpoint-ProxyApp": "t=1504230492763 app=builtin",
    })
    span.end()
    assert _proxy_wire_values(collector.wait_span(f"/core/proxy/{source}")) == [
        (1504230492763, 1, -1, -1, -1, "builtin"),
        (1504230492763, 4, 42, -1, -1, "X-Millis"),
        (1504230492763, 4, -1, -1, -1, "X-Micros"),
        (1504230492763, 4, 123000, -1, -1, "X-Seconds"),
        (1504230492763, 4, -1, -1, -1, "X-" + "가" * 10),
    ]


@pytest.mark.parametrize("source", ["kwargs", "env"])
def test_proxy_header_enable_disables_all_wire_annotations(
    core_agent, collector, source,
):
    kwargs = {"http_server_proxy_user_header_names": ["X-Proxy"]}
    if source == "kwargs":
        kwargs["http_server_proxy_header_enable"] = False
    else:
        kwargs["env"] = {"PINPOINT_PY_HTTP_SERVER_PROXY_HEADER_ENABLE": "false"}
    agent, _ = core_agent(**kwargs)
    span = agent.new_span("proxy-disabled", f"/core/proxy-disabled/{source}")
    trace_http_server_request(span, "127.0.0.1", "example.test", {
        "Pinpoint-ProxyApp": "t=1504230492763 app=builtin",
        "X-Proxy": "t=1504230492763",
    })
    span.end()
    assert _proxy_wire_values(collector.wait_span(
        f"/core/proxy-disabled/{source}")) == []


@pytest.mark.parametrize("env_override", [False, True])
def test_user_proxy_reload_keeps_each_span_revision(
    core_agent, collector, tmp_path, env_override,
):
    path = tmp_path / "proxy-reload.yaml"
    _write_user_proxy_config(path, collector, ["X-Old"])
    env = ({"PINPOINT_PY_HTTP_SERVER_PROXY_USER_HEADER_NAMES": "X-Env"}
           if env_override else {})
    agent, _ = core_agent(config_file_path=str(path), env=env,
                         http_server_proxy_user_header_names=["X-Stale"])
    spans = [agent.new_span("old", f"/core/proxy/reload/{env_override}/old")]
    old_revision = spans[0]._config_snapshot[11]
    expected_old = ["X-Env"] if env_override else ["X-Old"]
    assert spans[0]._config_snapshot[13] == expected_old
    try:
        # A profile-selected list replaces the file's base list on reload.
        _write_user_proxy_config(path, collector, ["X-Base"], active_profile="next",
                                 profiles={"next": {"Http": {"Server": {
                                     "ProxyUserHeaderNames": ["X-New"],
                                 }}}})
        _wait(lambda: agent._native.get_config_snapshot()[11] > old_revision,
              timeout=8, what="proxy profile reload")
        spans.append(agent.new_span("new", f"/core/proxy/reload/{env_override}/new"))
        assert spans[1]._config_snapshot[13] == (["X-Env"] if env_override else ["X-New"])
        assert spans[0]._native.get_config_snapshot()[13] == expected_old

        # Async descendants created after reload inherit their parent's revision.
        with spans[0].new_span_event("async-parent"):
            child = spans[0].new_async_span("async-child")
            try:
                assert child._config_snapshot == spans[0]._config_snapshot
                assert child._native.get_config_snapshot()[13] == expected_old
            finally:
                child.end()

        revision = spans[1]._config_snapshot[11]
        _write_user_proxy_config(path, collector, [])
        _wait(lambda: agent._native.get_config_snapshot()[11] > revision,
              timeout=8, what="proxy recording disabled by reload")
        spans.append(agent.new_span("disabled", f"/core/proxy/reload/{env_override}/disabled"))
        assert spans[2]._config_snapshot[13] == (["X-Env"] if env_override else [])
        # Process every request after both reloads, including the still-open old span.
        headers = {name: "t=1504230492763 D=0.010"
                   for name in ("X-Old", "X-New", "X-Base", "X-Stale", "X-Env")}
        for span in spans:
            trace_http_server_request(span, "127.0.0.1", "example.test", headers)
    finally:
        for span in spans:
            span.end()
    for label, names in zip(("old", "new", "disabled"),
                            (["X-Env"],) * 3 if env_override
                            else (["X-Old"], ["X-New"], [])):
        assert _proxy_wire_values(collector.wait_span(
            f"/core/proxy/reload/{env_override}/{label}")) == [
                (1504230492763, 4, 10000, -1, -1, name) for name in names
            ]


def _write_http_recording_config(path, collector, **overrides):
    from pinpoint.config import Config

    cfg = dict(
        application_name=_APP_NAME, collector_host="127.0.0.1",
        collector_agent_port=collector.port, collector_span_port=collector.port,
        collector_stat_port=collector.port, stat_enabled=False,
        span_batch_flush_interval_ms=100, enable_config_file_watcher=True,
    )
    cfg.update(overrides)
    pending = path.with_suffix(".tmp")
    pending.write_text(Config(**cfg).to_yaml())
    pending.replace(path)


@pytest.mark.parametrize("source", ["kwargs", "file", "profile", "env"])
def test_python_recorded_http_settings_follow_the_resolved_config(
    core_agent, collector, tmp_path, source,
):
    """Query recording and real-IP resolution run interpreter-side, but they
    read the span's resolved snapshot -- so a config file, a profile and an
    env var reach them exactly like they reach a native-side setting."""
    from pinpoint.annotation import ANNOTATION_HTTP_PARAM, ANNOTATION_HTTP_URL

    on = dict(http_server_record_request_param=True,
              http_client_record_url_query=True,
              http_server_real_ip_header=["CF-Connecting-IP"],
              http_server_real_ip_empty_value="unknown")
    if source == "kwargs":
        kwargs = dict(on)
    else:
        path = tmp_path / "http-recording.yaml"
        profile = {"edge": {"Http": {
            "Server": {"RecordRequestParam": True,
                       "RealIpHeader": ["CF-Connecting-IP"],
                       "RealIpEmptyValue": "unknown"},
            "Client": {"RecordUrlQuery": True},
        }}}
        # The file never carries the Python defaults these tests flip: a
        # config file replaces the rendered YAML wholesale, so only the
        # file/profile/env value below can turn recording on.
        if source == "file":
            _write_http_recording_config(path, collector, **on)
        else:
            _write_http_recording_config(
                path, collector, profiles=profile,
                active_profile="edge" if source == "profile" else "")
        kwargs = {"config_file_path": str(path)}
        if source == "env":
            kwargs["env"] = {
                "PINPOINT_PY_HTTP_SERVER_RECORD_REQUEST_PARAM": "true",
                "PINPOINT_PY_HTTP_CLIENT_RECORD_URL_QUERY": "true",
                "PINPOINT_PY_HTTP_SERVER_REAL_IP_HEADER": "CF-Connecting-IP",
                "PINPOINT_PY_HTTP_SERVER_REAL_IP_EMPTY_VALUE": "unknown",
            }
    agent, _ = core_agent(**kwargs)
    rpc = f"/core/http-recording/{source}"
    span = agent.new_span("http-recording", rpc)
    trace_http_server_request(
        span, "9.9.9.9:1234", "example.test",
        {"X-Forwarded-For": "1.1.1.1", "CF-Connecting-IP": "Unknown, 2.2.2.2"},
        query_string="a=1&b=2")
    with span.new_span_event("client-call") as event:
        trace_http_client_request(event, "up.example", "https://up.example/p?t=s", None)
    span.end()

    pb = collector.wait_span(rpc)
    # "Unknown" is the configured placeholder, so its header is skipped and
    # the socket address wins -- proving the placeholder resolved too.
    assert pb.acceptEvent.remoteAddr == "9.9.9.9"
    # Exactly one annotation each: the native helpers read the same resolved
    # keys, but the Python layer is the only recorder on this path.
    params = [a for a in pb.annotation if a.key == ANNOTATION_HTTP_PARAM]
    urls = [a for a in pb.spanEvent[0].annotation if a.key == ANNOTATION_HTTP_URL]
    assert [a.value.stringValue for a in params] == ["a=1&b=2"]
    assert [a.value.stringValue for a in urls] == ["https://up.example/p?t=s"]


def test_http_recording_reload_keeps_each_span_revision(
    core_agent, collector, tmp_path,
):
    """A hot reload flips the Python-side recorders, and a span already open
    keeps the generation it was admitted under."""
    from pinpoint.annotation import ANNOTATION_HTTP_PARAM

    path = tmp_path / "http-reload.yaml"
    _write_http_recording_config(path, collector,
                                 http_server_record_request_param=True,
                                 http_server_real_ip_header=["X-Old-Ip"])
    agent, _ = core_agent(config_file_path=str(path),
                          http_server_record_request_param=False)
    old = agent.new_span("old", "/core/http-reload/old")
    old_revision = old._config_snapshot[11]
    try:
        _write_http_recording_config(path, collector,
                                     http_server_record_request_param=False,
                                     http_server_real_ip_header=["X-New-Ip"])
        _wait(lambda: agent._native.get_config_snapshot()[11] > old_revision,
              timeout=8, what="http recording reload")
        new = agent.new_span("new", "/core/http-reload/new")
        headers = {"X-Old-Ip": "1.1.1.1", "X-New-Ip": "2.2.2.2"}
        for span in (old, new):
            trace_http_server_request(span, "9.9.9.9:1", "example.test",
                                      headers, query_string="a=1")
        new.end()
    finally:
        old.end()

    before = collector.wait_span("/core/http-reload/old")
    after = collector.wait_span("/core/http-reload/new")
    assert before.acceptEvent.remoteAddr == "1.1.1.1"
    assert _annotations(before)[ANNOTATION_HTTP_PARAM].stringValue == "a=1"
    assert after.acceptEvent.remoteAddr == "2.2.2.2"
    assert ANNOTATION_HTTP_PARAM not in _annotations(after)


def test_long_span_streams_event_chunks_before_end(core_agent, collector):
    """Finished events reach the collector as a (non-async) SpanChunk while
    the root span is still open, then the root arrives with the remainder."""
    agent, _ = core_agent(span_event_chunk_size=3)
    rpc = "/core/stream-chunks"

    root = agent.new_span("core-stream-root", rpc)
    with root:
        root.set_service_type(APP_TYPE_PYTHON)
        for i in range(4):
            with root.new_span_event(f"core-stream-{i}"):
                pass
        # Still inside the span: the first three events must already be out.
        chunk = collector.wait_chunk(root.span_id)
        assert not chunk.HasField("localAsyncId")
        assert len(chunk.spanEvent) == 3
        assert [e.sequence for e in chunk.spanEvent] == [0, 1, 2]

    root_pb = collector.wait_span(rpc)
    assert [e.sequence for e in root_pb.spanEvent] == [3]
    assert _tx_tuple(chunk.transactionId) == _tx_tuple(root_pb.transactionId)
