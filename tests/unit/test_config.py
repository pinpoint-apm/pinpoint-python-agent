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

"""Config → YAML round trip is the only structural contract we have with the
native agent. Keep it under test."""

import pytest

from pinpoint.config import Config, apply_native_env_overrides
from pinpoint.service_type import APP_TYPE_PYTHON


def test_defaults():
    cfg = Config()
    assert not hasattr(cfg, "agent_id")
    assert cfg.application_type == APP_TYPE_PYTHON
    assert cfg.collector_agent_port == 9991
    assert cfg.http_url_stat_limit == 1000
    assert cfg.http_url_stat_enable_trim_path is False
    assert cfg.span_batch_size == 20
    assert cfg.grpc_keepalive_time_ms == 30000
    assert cfg.agent_info_refresh_interval_ms == 24 * 60 * 60 * 1000
    assert cfg.http_url_stat_queue_size == 1024
    assert cfg.asyncio_task_span_timeout_ms == 5 * 60 * 1000
    assert cfg.sql_enable_raw_sql_cache is True
    assert cfg.sql_trace_bind_values is False


def test_to_yaml_contains_required_keys():
    cfg = Config(application_name="demo", agent_name="demo-api")
    y = cfg.to_yaml()
    assert 'ApplicationName: "demo"' in y
    assert 'AgentName: "demo-api"' in y
    assert "AgentId:" not in y
    # ApplicationType is not a YAML key; it reaches native through
    # AgentOptions.app_type.
    assert "ApplicationType" not in y
    assert "AgentPort: 9991" in y


def test_from_kwargs_sets_fields_and_ignores_unknown():
    cfg = Config.from_kwargs(
        application_name="code",
        collector_agent_port=12345,
        not_a_real_option=True,  # ignored, so a typo cannot break startup
    )
    assert cfg.application_name == "code"
    assert cfg.collector_agent_port == 12345
    assert not hasattr(cfg, "not_a_real_option")


def test_from_kwargs_ignores_method_names():
    """A kwarg colliding with a method name (``to_yaml``) must be ignored like
    any other unknown key — not shadow the method. Otherwise ``to_yaml()`` would
    crash inside ``init()`` and silently degrade the agent to a null agent."""
    cfg = Config.from_kwargs(to_yaml="oops", application_name="app")
    assert cfg.application_name == "app"
    assert callable(cfg.to_yaml)
    # The method still works (not overwritten by the string).
    assert "ApplicationName:" in cfg.to_yaml()


def test_yaml_escapes_newlines_and_control_chars():
    # A newline in a value round-trips as an escape, not a folded/broken scalar.
    y = Config(application_name="a\nb\ttab").to_yaml()
    app_lines = [ln for ln in y.splitlines() if ln.startswith("ApplicationName:")]
    assert len(app_lines) == 1
    assert "\\n" in app_lines[0] and "\\t" in app_lines[0]


def test_native_env_mirror_applies_python_side_gates(monkeypatch):
    monkeypatch.setenv("PINPOINT_PY_APPLICATION_NAME", "envapp")
    monkeypatch.setenv("PINPOINT_PY_CONFIG_FILE", "/etc/pinpoint.yaml")
    monkeypatch.setenv("PINPOINT_PY_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("PINPOINT_PY_ENABLE", "false")
    monkeypatch.setenv("PINPOINT_PY_ENABLE_CALLSTACK_TRACE", "true")
    monkeypatch.setenv("PINPOINT_PY_HTTP_COLLECT_URL_STAT", "true")
    monkeypatch.setenv("PINPOINT_PY_ASYNCIO_TASK_SPAN_TIMEOUT_MS", "1234")
    monkeypatch.setenv("PINPOINT_PY_EXCEPTION_CHAIN_MAX_DEPTH", "7")
    monkeypatch.setenv("PINPOINT_PY_PREFORK", "true")
    monkeypatch.setenv("PINPOINT_PY_SQL_TRACE_BIND_VALUE", "true")
    monkeypatch.setenv("PINPOINT_PY_COLLECTOR_HOST", "collector.example.com")
    monkeypatch.setenv("PINPOINT_PY_COLLECTOR_AGENT_PORT", "19991")
    monkeypatch.setenv("PINPOINT_PY_COLLECTOR_SPAN_PORT", "19993")
    monkeypatch.setenv("PINPOINT_PY_COLLECTOR_STAT_PORT", "19992")
    cfg = apply_native_env_overrides(Config())
    assert cfg.application_name == "envapp"
    # agent.init() logs these, and to_yaml() hands them to the native agent —
    # both must name the collector the env actually points at.
    assert cfg.collector_host == "collector.example.com"
    assert cfg.collector_agent_port == 19991
    assert cfg.collector_span_port == 19993
    assert cfg.collector_stat_port == 19992
    assert cfg.config_file_path == "/etc/pinpoint.yaml"
    assert cfg.log_level == "DEBUG"
    assert cfg.enabled is False
    assert cfg.enable_callstack_trace is True
    assert cfg.http_collect_url_stat is True
    assert cfg.asyncio_task_span_timeout_ms == 1234
    assert cfg.exception_chain_max_depth == 7
    assert "ExceptionChainMaxDepth" not in cfg.to_yaml()
    assert cfg.prefork is True
    assert cfg.sql_trace_bind_values is True


def test_sql_config_defaults_and_native_yaml_mapping():
    """Bound values (SQL binds, Mongo payloads) hold PII/secrets, so capture is
    opt-in in Python, and the native gate must receive that same decision."""
    y = Config(application_name="demo").to_yaml()
    assert "  EnableRawSqlCache: true" in y
    assert "  TraceBindValue: false" in y

    y = Config(
        application_name="demo",
        sql_enable_raw_sql_cache=False,
        sql_trace_bind_values=True,
    ).to_yaml()
    assert "  EnableRawSqlCache: false" in y
    assert "  TraceBindValue: true" in y


def test_native_sql_trace_bind_value_env_uses_canonical_singular_name(monkeypatch):
    # Only the canonical singular suffix is read; the plural is ignored.
    monkeypatch.setenv("PINPOINT_PY_SQL_TRACE_BIND_VALUES", "true")
    cfg = apply_native_env_overrides(Config())
    assert cfg.sql_trace_bind_values is False
    assert "  TraceBindValue: false" in cfg.to_yaml()


def test_header_recording_env_vars_are_not_mirrored_python_side(monkeypatch):
    # Production reads come from the per-span native snapshot, and the native
    # parser folds these env vars over the rendered YAML itself — the Python
    # Config deliberately ignores them (see _ENV_MIRROR).
    monkeypatch.setenv(
        "PINPOINT_PY_HTTP_SERVER_RECORD_REQUEST_HEADER",
        "Authorization",
    )
    cfg = apply_native_env_overrides(Config())
    assert cfg.http_server_record_request_header == []


def test_python_recorded_http_settings_reach_the_native_yaml():
    """The four settings the Python layer records itself still have native
    counterparts, so they must be rendered like any other key: that is what
    lets a config file, a profile and a hot reload reach the Python gates
    through the per-span config snapshot."""
    y = Config(
        application_name="demo",
        http_client_record_url_query=True,
        http_server_record_request_param=True,
        http_server_real_ip_header=["CF-Connecting-IP"],
        http_server_real_ip_empty_value="unknown",
    ).to_yaml()
    assert "    RecordUrlQuery: true" in y
    assert "    RecordRequestParam: true" in y
    assert '    RealIpHeader: ["CF-Connecting-IP"]' in y
    assert '    RealIpEmptyValue: "unknown"' in y
    # Defaults are emitted explicitly too, so the inline path never inherits a
    # native default that differs from the documented Python one.
    default = Config(application_name="demo").to_yaml()
    assert "    RecordUrlQuery: false" in default
    assert "    RecordRequestParam: false" in default
    assert '    RealIpHeader: ["X-Forwarded-For", "X-Real-Ip"]' in default
    assert '    RealIpEmptyValue: ""' in default
    # An explicit empty list must survive: it means "trust no header".
    assert "    RealIpHeader: []" in Config(
        application_name="demo", http_server_real_ip_header=[]).to_yaml()


def test_prefork_defaults_off_and_stays_out_of_native_yaml():
    """prefork keeps all native API calls out of the master; it is never a
    native YAML key."""
    assert Config().prefork is False
    y = Config(application_name="demo", prefork=True).to_yaml()
    assert "prefork" not in y.lower()


def test_native_log_bridge_defaults_off_and_stays_out_of_native_yaml():
    cfg = Config(application_name="demo")
    assert cfg.native_log_to_python is False
    assert cfg.native_log_queue_size == 1024
    y = cfg.to_yaml()
    assert "native_log" not in y.lower()
    assert "NativeLog" not in y


@pytest.mark.parametrize("value, expected", [
    ("true", True),
    ("false", False),
])
def test_native_log_bridge_environment_opt_in(monkeypatch, value, expected):
    monkeypatch.setenv("PINPOINT_PY_NATIVE_LOG_TO_PYTHON", value)
    monkeypatch.setenv("PINPOINT_PY_NATIVE_LOG_QUEUE_SIZE", "17")
    cfg = apply_native_env_overrides(Config())
    assert cfg.native_log_to_python is expected
    assert cfg.native_log_queue_size == 17


@pytest.mark.parametrize("value", [1, 0, -1, 4097, "bad", None])
def test_invalid_native_log_queue_size_falls_back_without_failing(value):
    cfg = apply_native_env_overrides(Config(native_log_queue_size=value))
    assert cfg.native_log_queue_size == 1024


def test_config_file_path_is_agent_option_not_yaml():
    y = Config(
        application_name="demo",
        config_file_path="/etc/pinpoint.yaml",
    ).to_yaml()
    assert "config_file_path" not in y.lower()
    assert "/etc/pinpoint.yaml" not in y


def test_native_env_mirror_beats_kwargs(monkeypatch):
    """Same precedence as the native agent: env overrides the YAML/kwargs."""
    monkeypatch.setenv("PINPOINT_PY_APPLICATION_NAME", "envapp")
    cfg = apply_native_env_overrides(Config.from_kwargs(application_name="code"))
    assert cfg.application_name == "envapp"


def test_non_mirrored_env_vars_are_ignored_by_python(monkeypatch):
    """Only the Python-side gates are mirrored; everything else is parsed by
    the native agent, so Python's Config must not react to it."""
    monkeypatch.setenv("PINPOINT_PY_SAMPLING_COUNTER_RATE", "7")
    monkeypatch.setenv("PINPOINT_PY_SPAN_BATCH_SIZE", "64")
    cfg = apply_native_env_overrides(Config())
    assert cfg.sampling_counter_rate == 1
    assert cfg.span_batch_size == 20


def test_yaml_quotes_structural_chars():
    cfg = Config(application_name="a:b")
    assert '"a:b"' in cfg.to_yaml()


def test_to_yaml_emits_identity_and_nested_block_values():
    cfg = Config(
        application_name="demo",
        agent_name="demo-api",
        uid_version="v4",
        service_name="checkout",
        api_key="secret",
        grpc_ssl_trust_cert_file_path="/certs/trust.pem",
        grpc_ssl_root_cert_file_path="/certs/root.pem",
        grpc_ssl_enable=True,
        grpc_keepalive_time_ms=31000,
        grpc_keepalive_timeout_ms=62000,
        grpc_keepalive_permit_without_calls=True,
        grpc_max_send_message_size=5242880,
        grpc_max_receive_message_size=6291456,
        grpc_sender_queue_size=1100,
        grpc_channel_max_age_ms=600000,
        grpc_stream_max_age_ms=300000,
        span_batch_size=64,
        span_batch_flush_interval_ms=250,
        span_batch_collect_deadline_ms=125,
        span_batch_max_concurrent_requests=4,
        agent_info_refresh_interval_ms=60000,
        agent_info_send_retry_interval_ms=25,
        agent_info_max_try_per_attempt=2,
        http_url_stat_queue_size=4096,
    )
    y = cfg.to_yaml()
    assert 'UidVersion: "v4"' in y
    assert 'ServiceName: "checkout"' in y
    assert 'ApiKey: "secret"' in y
    assert "  Grpc:\n    SslEnable: true" in y
    assert '    TrustCertFilePath: "/certs/trust.pem"' in y
    assert '    RootCertFilePath: "/certs/root.pem"' in y
    assert "    KeepAliveTimeMs: 31000" in y
    assert "    KeepAliveTimeoutMs: 62000" in y
    assert "    KeepAlivePermitWithoutCalls: true" in y
    assert "    MaxSendMessageSize: 5242880" in y
    assert "    MaxReceiveMessageSize: 6291456" in y
    assert "    SenderQueueSize: 1100" in y
    assert "    ChannelMaxAgeMs: 600000" in y
    assert "    StreamMaxAgeMs: 300000" in y
    assert "  SpanBatch:\n    Size: 64" in y
    assert "    FlushIntervalMs: 250" in y
    assert "    CollectDeadlineMs: 125" in y
    assert "    MaxConcurrentRequests: 4" in y
    assert "  AgentInfo:\n    RefreshIntervalMs: 60000" in y
    assert "    SendRetryIntervalMs: 25" in y
    assert "    MaxTryPerAttempt: 2" in y
    assert "  UrlStatQueueSize: 4096" in y


def test_to_yaml_emits_every_native_section():
    """Schema lock: every block the native config parser reads must appear
    with at least one key. Catches the silent-drop bug class where a Python
    field exists but to_yaml forgets to render it."""
    y = Config(application_name="x", agent_name="x-api").to_yaml()
    expected_substrings = [
        "AgentName:",
        "UidVersion:",
        "ServiceName:",
        "ApiKey:",
        "IsContainer:",
        "EnableCallstackTrace:",
        "Enable:",
        "Log:",
        "  MaxFileSize:",
        "Collector:",
        "  Host:",
        "  AgentPort:",
        "  SpanPort:",
        "  StatPort:",
        "  Grpc:",
        "    SslEnable:",
        "    TrustCertFilePath:",
        "    RootCertFilePath:",
        "    KeepAliveTimeMs:",
        "    KeepAliveTimeoutMs:",
        "    KeepAlivePermitWithoutCalls:",
        "    MaxSendMessageSize:",
        "    MaxReceiveMessageSize:",
        "    SenderQueueSize:",
        "    ChannelMaxAgeMs:",
        "    StreamMaxAgeMs:",
        "    IdleTimeoutMs:",
        "  AgentInfo:",
        "    RefreshIntervalMs:",
        "    SendRetryIntervalMs:",
        "    MaxTryPerAttempt:",
        "  SpanBatch:",
        "    Size:",
        "    FlushIntervalMs:",
        "    CollectDeadlineMs:",
        "    MaxConcurrentRequests:",
        "Sampling:",
        "  NewThroughput:",
        "  ContinueThroughput:",
        "Stat:",
        "  BatchCount:",
        "  BatchInterval:",
        "Span:",
        "  QueueSize:",
        "  MaxEventDepth:",
        "  MaxEventSequence:",
        "  EventChunkSize:",
        "Http:",
        "  CollectUrlStat:",
        "  UrlStatLimit:",
        "  UrlStatQueueSize:",
        "  UrlStatEnableTrimPath:",
        "  UrlStatTrimPathDepth:",
        "  UrlStatMethodPrefix:",
        "  Server:",
        "    StatusCodeErrors:",
        "    ExcludeUrl:",
        "    ExcludeMethod:",
        "    RecordRequestHeader:",
        "    RecordRequestCookie:",
        "    RecordResponseHeader:",
        "    ProxyHeaderEnable:",
        "    RecordRequestParam:",
        "    RealIpHeader:",
        "    RealIpEmptyValue:",
        "  Client:",
        "    RecordUrlQuery:",
        "Sql:",
        "  MaxBindArgsSize:",
        "  EnableSqlStats:",
        "  EnableRawSqlCache:",
        "  TraceBindValue:",
        "  CacheSize:",
    ]
    for needle in expected_substrings:
        assert needle in y, f"missing {needle!r} in YAML:\n{y}"


def test_yaml_lists_render_flow_style():
    cfg = Config(
        http_server_status_code_errors=["5xx", "404"],
        http_server_exclude_url=["/health"],
    )
    y = cfg.to_yaml()
    assert '    StatusCodeErrors: ["5xx", "404"]' in y
    assert '    ExcludeUrl: ["/health"]' in y
    # Empty lists must still render, or native keeps stale values from a
    # previous reload.
    assert "    RecordResponseHeader: []" in y


def test_yaml_roundtrips_through_native_yaml_cpp():
    """The emitted YAML (JSON-escaped scalars in a block mapping) must parse
    on the real native side: start an agent against the mock collector and
    require registration under the exact hostile ApplicationName."""
    from pinpoint import _native
    from tests.integration._collector import MockCollector

    app_name = "unit-config-roundtrip"
    collector = MockCollector().start()
    yaml = Config(
        application_name=app_name,
        agent_name="roundtrip-agent",
        collector_host="127.0.0.1",
        collector_agent_port=collector.port,
        collector_span_port=collector.port,
        collector_stat_port=collector.port,
        agent_info_send_retry_interval_ms=100,
        # Hostile scalars ride in a free-string list: if json.dumps escaping
        # were not valid YAML for yaml-cpp, the whole document would fail to
        # parse and registration below would never happen.
        http_server_exclude_url=['/health', 'we"ird: [{path}]\n#not-a-comment'],
        stat_enabled=False,
        # A novel key the parser ignores: proves the appended fragment still
        # yields a document yaml-cpp accepts (duplicate keys would be ambiguous).
        extra_yaml="ExtraEscapeHatch:\n  Probe: 1",
    ).to_yaml()
    agent = _native.start_agent("", yaml, "", 1700, "unit-test", [], [])
    try:
        collector.wait_agent_info_for_application(app_name)
    finally:
        agent.shutdown()
        collector.stop()


def test_env_mirror_bool_accepts_common_truthy(monkeypatch):
    for truthy in ("1", "true", "TRUE", "yes", "on"):
        monkeypatch.setenv("PINPOINT_PY_HTTP_COLLECT_URL_STAT", truthy)
        assert apply_native_env_overrides(Config()).http_collect_url_stat is True
    for falsy in ("0", "false", "no", "off", ""):
        monkeypatch.setenv("PINPOINT_PY_HTTP_COLLECT_URL_STAT", falsy)
        assert apply_native_env_overrides(Config()).http_collect_url_stat is False

    monkeypatch.setenv("PINPOINT_PY_HTTP_SERVER_PROXY_HEADER_ENABLE", "false")
    assert apply_native_env_overrides(Config()).http_server_proxy_header_enable is False


def test_user_proxy_header_names_yaml():
    import json

    assert "ProxyUserHeaderNames: []" in Config().to_yaml()
    names = ['X-Proxy', 'X-"quoted', 'X-가']
    cfg = Config.from_kwargs(http_server_proxy_user_header_names=names)
    line = next(line for line in cfg.to_yaml().splitlines()
                if "ProxyUserHeaderNames:" in line)
    assert json.loads(line.split(":", 1)[1]) == names
    assert "ProxyHeaderEnable: true" in Config().to_yaml()


def test_unknown_kwarg_is_reported_with_the_closest_option(monkeypatch):
    """A typo'd option is still ignored (startup must not crash) but it is
    named, with a suggestion — a silently dropped kwarg is indistinguishable
    from a setting that had no effect."""
    from pinpoint import config as config_mod

    messages = []
    monkeypatch.setattr(config_mod._log, "warning",
                        lambda msg, *args: messages.append(msg % args))

    cfg = Config.from_kwargs(application_name="app", collector_hostt="h",
                             totally_unrelated=1)

    assert cfg.application_name == "app"      # valid keys still applied
    assert cfg.collector_host == "localhost"  # the typo changed nothing
    assert len(messages) == 2
    typo = next(m for m in messages if "collector_hostt" in m)
    assert "did you mean 'collector_host'?" in typo
    unrelated = next(m for m in messages if "totally_unrelated" in m)
    assert "did you mean" not in unrelated


def test_known_kwargs_warn_about_nothing(monkeypatch):
    from pinpoint import config as config_mod

    monkeypatch.setattr(config_mod._log, "warning",
                        lambda *a, **k: pytest.fail("valid kwargs must be silent"))
    Config.from_kwargs(application_name="app", sampling_percent_rate=10.0)
