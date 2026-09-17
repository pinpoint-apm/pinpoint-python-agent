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

"""Configuration for the agent.

Users typically call `pinpoint.init(...)`, which builds a `Config`, renders it
to YAML and hands that to the native agent. Almost every field maps 1:1 to one
native YAML key; :meth:`Config.to_yaml` is the exact mapping.

Every native option is reachable from Python in three ways:
1. Constructor / `init()` kwargs (e.g. `init(span_max_event_depth=128)`).
2. `PINPOINT_PY_*` environment variables, read by the *native* agent itself:
   `agent.init()` passes `ENV_VAR_PREFIX` in `AgentOptions`, so every config
   env var resolves as ``PINPOINT_PY_<SUFFIX>`` (e.g.
   ``PINPOINT_PY_COLLECTOR_HOST``) and overrides the rendered YAML. Python
   does not duplicate that parsing — see :func:`apply_native_env_overrides`
   for the few values mirrored because they gate Python-side behavior.
3. `extra_yaml=` raw fragment as an escape hatch for keys not surfaced here.

`application_type` and `config_file_path` are the fields without YAML keys —
they are passed through `AgentOptions` instead.
"""

from __future__ import annotations

import difflib
import json
import os
from dataclasses import dataclass, field, fields
from typing import Any, Dict, List

from ._log import get_logger
from .service_type import APP_TYPE_PYTHON

_log = get_logger("config")

# Prefix for every config environment variable, native and Python alike:
# handed to _native.start_agent() during agent.init().
ENV_VAR_PREFIX = "PINPOINT_PY"

NATIVE_LOG_DEFAULT_QUEUE_SIZE = 1024
NATIVE_LOG_MIN_QUEUE_SIZE = 2
NATIVE_LOG_MAX_QUEUE_SIZE = 4096


@dataclass
class Config:
    # ---- identity -----------------------------------------------------------
    application_name: str = ""              # PINPOINT_PY_APPLICATION_NAME
    agent_name: str = ""                    # PINPOINT_PY_AGENT_NAME (display name)
    uid_version: str = ""                   # PINPOINT_PY_UID_VERSION (empty -> v3)
    service_name: str = ""                  # PINPOINT_PY_SERVICE_NAME (required for v4)
    api_key: str = ""                       # PINPOINT_PY_API_KEY (required for v4)
    # Not a YAML key (and no env var): passed through native AgentOptions.
    # Default 1700.
    application_type: int = APP_TYPE_PYTHON
    is_container: bool = False              # PINPOINT_PY_IS_CONTAINER

    # Native AgentOptions input, not a YAML key. When set, the file replaces the
    # inline YAML rendered here; env vars still take precedence.
    config_file_path: str = ""

    # ---- collector ---------------------------------------------------------
    collector_host: str = "localhost"       # PINPOINT_PY_COLLECTOR_HOST
    collector_agent_port: int = 9991        # PINPOINT_PY_COLLECTOR_AGENT_PORT
    collector_span_port: int = 9993         # PINPOINT_PY_COLLECTOR_SPAN_PORT
    collector_stat_port: int = 9992         # PINPOINT_PY_COLLECTOR_STAT_PORT

    # ---- sampling ----------------------------------------------------------
    sampling_type: str = "COUNTER"          # COUNTER | PERCENT
    sampling_counter_rate: int = 1          # 1-out-of-N
    sampling_percent_rate: float = 100.0
    sampling_new_throughput: int = 0        # PINPOINT_PY_SAMPLING_NEW_THROUGHPUT
    sampling_continue_throughput: int = 0   # PINPOINT_PY_SAMPLING_CONTINUE_THROUGHPUT

    # ---- log ---------------------------------------------------------------
    log_level: str = "INFO"                 # PINPOINT_PY_LOG_LEVEL
    log_output: str = "stderr"              # "stderr" -> empty FilePath, else treated as path
    log_max_file_size: int = 10             # MB; rotation is native-side
    log_max_backups: int = 1               # rotated files retained
    # Python-only: when enabled, a bounded native queue replaces the native
    # stdout/Log.FilePath sink and a daemon consumer emits to pinpoint.native.
    native_log_to_python: bool = False
    native_log_queue_size: int = NATIVE_LOG_DEFAULT_QUEUE_SIZE

    # ---- gRPC transport ----------------------------------------------------
    grpc_ssl_trust_cert_file_path: str = ""  # PINPOINT_PY_GRPC_SSL_TRUST_CERT_FILE_PATH
    grpc_ssl_root_cert_file_path: str = ""   # PINPOINT_PY_GRPC_SSL_ROOT_CERT_FILE_PATH
    grpc_ssl_enable: bool = False            # PINPOINT_PY_GRPC_SSL_ENABLE
    grpc_keepalive_time_ms: int = 30000      # PINPOINT_PY_GRPC_KEEPALIVE_TIME_MS
    grpc_keepalive_timeout_ms: int = 60000   # PINPOINT_PY_GRPC_KEEPALIVE_TIMEOUT_MS
    grpc_keepalive_permit_without_calls: bool = False  # PINPOINT_PY_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS
    grpc_max_send_message_size: int = 4194304     # PINPOINT_PY_GRPC_MAX_SEND_MESSAGE_SIZE
    grpc_max_receive_message_size: int = 4194304  # PINPOINT_PY_GRPC_MAX_RECEIVE_MESSAGE_SIZE
    grpc_sender_queue_size: int = 1000       # PINPOINT_PY_GRPC_SENDER_QUEUE_SIZE
    # Periodic collector connection renewal, for collectors behind an L4 LB or
    # scaled out in Kubernetes. 0 disables both.
    grpc_channel_max_age_ms: int = 0         # PINPOINT_PY_GRPC_CHANNEL_MAX_AGE_MS
    grpc_stream_max_age_ms: int = 0          # PINPOINT_PY_GRPC_STREAM_MAX_AGE_MS
    # gRPC client idle timeout. 0 disables, so a quiet channel keeps its
    # connection; a positive value below gRPC's 1s minimum is raised to 1000.
    grpc_idle_timeout_ms: int = 0            # PINPOINT_PY_GRPC_IDLE_TIMEOUT_MS

    # ---- stat --------------------------------------------------------------
    stat_enabled: bool = True               # PINPOINT_PY_STAT_ENABLE
    stat_batch_count: int = 6               # PINPOINT_PY_STAT_BATCH_COUNT
    stat_batch_interval: int = 5000         # PINPOINT_PY_STAT_BATCH_INTERVAL (ms)

    # ---- span queueing -----------------------------------------------------
    span_queue_size: int = 1024
    span_max_event_depth: int = 64
    span_max_event_sequence: int = 5000
    span_event_chunk_size: int = 20
    # Names match type(error).__name__; message_contains is a substring.
    # Python-only per-rule bools: match_subclasses also matches subclasses of
    # `name`; match_cause also matches the __cause__/__context__ chain. Both
    # default False and are stripped before the native YAML is rendered (see
    # to_yaml / tracer.set_ignore_rules).
    span_ignore_errors: List[Dict[str, Any]] = field(default_factory=list)
    span_error_mark: List[str] = field(default_factory=list)
    span_error_mark_exclude: List[str] = field(default_factory=list)
    span_batch_size: int = 20
    span_batch_flush_interval_ms: int = 1000
    span_batch_collect_deadline_ms: int = 500
    span_batch_max_concurrent_requests: int = 10
    # Python-only safety bound for implicit asyncio-task forks; zero disables the
    # timer for apps that intentionally trace longer-running tasks. NOTE: with
    # the timer off, the task's done-callback is the ONLY reaper — a
    # fire-and-forget task that never completes then pins its async span (and
    # native handle) for the process's life.
    asyncio_task_span_timeout_ms: int = 5 * 60 * 1000

    # HTTP recording gates, off by default: query strings carry tokens, ids and
    # free text. The Python instrumentations do this recording themselves, but
    # read the resolved Http.{Client.RecordUrlQuery, Server.RecordRequestParam}
    # out of the per-span snapshot, so a config file and a hot reload reach them
    # like any native key (see http_helper).
    http_client_record_url_query: bool = False    # PINPOINT_PY_HTTP_CLIENT_RECORD_URL_QUERY
    http_server_record_request_param: bool = False  # PINPOINT_PY_HTTP_SERVER_RECORD_REQUEST_PARAM
    # Real-IP resolution: headers are tried in order and the first non-empty
    # value that is not the placeholder wins; [] trusts no header (socket
    # address). Resolved per span from the snapshot, same as the gates above.
    http_server_real_ip_header: List[str] = field(
        default_factory=lambda: ["X-Forwarded-For", "X-Real-Ip"])
    http_server_real_ip_empty_value: str = ""

    # ---- agent info --------------------------------------------------------
    agent_info_refresh_interval_ms: int = 24 * 60 * 60 * 1000
    agent_info_send_retry_interval_ms: int = 3000
    agent_info_max_try_per_attempt: int = 3

    # ---- HTTP url stat -----------------------------------------------------
    http_collect_url_stat: bool = False
    http_url_stat_limit: int = 1000
    http_url_stat_queue_size: int = 1024
    http_url_stat_enable_trim_path: bool = False
    http_url_stat_trim_path_depth: int = 3
    http_url_stat_method_prefix: bool = False

    # ---- HTTP server filters -----------------------------------------------
    # Status codes to mark as errored. Values are like "5xx", "404", etc.
    http_server_status_code_errors: List[str] = field(default_factory=lambda: ["5xx"])
    http_server_exclude_url: List[str] = field(default_factory=list)
    http_server_exclude_method: List[str] = field(default_factory=list)
    http_server_record_request_header: List[str] = field(default_factory=list)
    http_server_record_request_cookie: List[str] = field(default_factory=list)
    http_server_record_response_header: List[str] = field(default_factory=list)
    http_server_proxy_user_header_names: List[str] = field(default_factory=list)
    http_server_proxy_header_enable: bool = True

    # ---- HTTP client recording --------------------------------------------
    http_client_record_request_header: List[str] = field(default_factory=list)
    http_client_record_request_cookie: List[str] = field(default_factory=list)
    http_client_record_response_header: List[str] = field(default_factory=list)

    # ---- SQL ---------------------------------------------------------------
    sql_max_bind_args_size: int = 1024
    sql_enable_sql_stats: bool = False
    # Cache normalized SQL by raw query text (native Sql.EnableRawSqlCache).
    sql_enable_raw_sql_cache: bool = True
    sql_remove_comments: bool = True
    sql_cache_size: int = 1024              # entries per SQL cache (id/uid/raw)
    sql_cache_length_limit: int = 2048      # -1 = cache all, 0 = bypass all
    sql_cache_expire_hours: int = 168       # 0 = no expiry
    sql_error_count: int = 100             # 0 = disable transaction error mark
    # Capture the bound parameter VALUES sent to a datastore (SQL bind args, Mongo
    # command document values). OFF by default: those routinely carry PII/secrets.
    #
    # This field is only the inline-YAML input and the no-snapshot fallback; the
    # live gate is _util.sql_bind_values_enabled, reading the resolved
    # Sql.TraceBindValue out of the per-span snapshot. Note a config file replaces
    # the rendered YAML wholesale and the native default is *true* — see
    # agent._warn_on_implicit_bind_value_capture and docs/config.md#sql-configuration.
    sql_trace_bind_values: bool = False

    # ---- callstack capture for SpanEvent.set_error ------------------------
    enable_callstack_trace: bool = False
    callstack_trace_new_throughput: int = 1000  # 0 = unlimited
    # Python-only: entries (thrown exception + __cause__/__context__ chain)
    # recorded per SpanEvent.set_error; 0 = unlimited. Never emitted to the
    # native YAML.
    exception_chain_max_depth: int = 5

    # Native configuration source controls. Profiles use the native YAML keys.
    enable_config_file_watcher: bool = False
    active_profile: str = ""
    profiles: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Escape hatch for a native key not declared here.
    extra_yaml: str = ""

    # Soft-disable switch for tests / local dev. Emits Enable: false to the
    # native side AND short-circuits the Python instrumentations.
    enabled: bool = True

    # Prefork-master mode (PINPOINT_PY_PREFORK). Python-only, never emitted to the
    # native YAML: init() only stores the pending config, and each worker's
    # after-fork hook makes the first StartAgent() call. The master never traces.
    prefork: bool = False

    @classmethod
    def from_kwargs(cls, **overrides) -> "Config":
        """Build a Config from ``init()`` kwargs.

        Unknown keys are ignored, so an ``init()`` typo degrades to defaults
        instead of crashing app startup — but each one is reported at WARNING
        with the closest real option, because a silently dropped kwarg
        otherwise looks exactly like a setting that had no effect.

        Environment variables are NOT read here — the native agent reads
        ``PINPOINT_PY_*`` itself; see :func:`apply_native_env_overrides`.
        """
        cfg = cls()
        # Declared fields only: ``hasattr`` would also match methods, so a typo
        # like ``init(to_yaml="x")`` would shadow one and crash init() into a null
        # agent instead of being ignored like any other unknown key.
        field_names = {f.name for f in fields(cls)}
        for key, value in overrides.items():
            if key in field_names:
                setattr(cfg, key, value)
            else:
                close = difflib.get_close_matches(key, field_names, n=1)
                _log.warning(
                    "pinpoint.init(): unknown option %r was ignored%s", key,
                    f" -- did you mean {close[0]!r}?" if close else "")
        return cfg

    def to_yaml(self) -> str:
        # Map operator-friendly names to what the native logger accepts.
        level = (self.log_level or "").strip().lower()
        if level in ("warn", "warning"):
            level = "warning"

        # log_output == "stderr" means "no file logging"; native then writes to
        # stdout (its default). Any other value is treated as a path.
        file_path = "" if self.log_output.strip().lower() == "stderr" else self.log_output

        # ApplicationType goes through AgentOptions.app_type, not the YAML. The
        # coercions below raise a clear Python error rather than shipping a mistyped
        # scalar to the native parser.
        strs = lambda items: [str(item) for item in items]  # noqa: E731
        data: Dict[str, Any] = {
            "ApplicationName": str(self.application_name),
            "AgentName": str(self.agent_name),
            "UidVersion": str(self.uid_version),
            "ServiceName": str(self.service_name),
            "ApiKey": str(self.api_key),
            "Enable": bool(self.enabled),
            "IsContainer": bool(self.is_container),
            "EnableCallstackTrace": bool(self.enable_callstack_trace),
            "CallstackTraceNewThroughput": int(self.callstack_trace_new_throughput),
            "EnableConfigFileWatcher": bool(self.enable_config_file_watcher),
            "ActiveProfile": str(self.active_profile),
            "Log": {
                "Level": str(level),
                "FilePath": str(file_path),
                "MaxFileSize": int(self.log_max_file_size),
                "MaxBackups": int(self.log_max_backups),
            },
            "Collector": {
                "Host": str(self.collector_host),
                "AgentPort": int(self.collector_agent_port),
                "SpanPort": int(self.collector_span_port),
                "StatPort": int(self.collector_stat_port),
                "Grpc": {
                    "SslEnable": bool(self.grpc_ssl_enable),
                    "TrustCertFilePath": str(self.grpc_ssl_trust_cert_file_path),
                    "RootCertFilePath": str(self.grpc_ssl_root_cert_file_path),
                    "KeepAliveTimeMs": int(self.grpc_keepalive_time_ms),
                    "KeepAliveTimeoutMs": int(self.grpc_keepalive_timeout_ms),
                    "KeepAlivePermitWithoutCalls":
                        bool(self.grpc_keepalive_permit_without_calls),
                    "MaxSendMessageSize": int(self.grpc_max_send_message_size),
                    "MaxReceiveMessageSize": int(self.grpc_max_receive_message_size),
                    "SenderQueueSize": int(self.grpc_sender_queue_size),
                    "ChannelMaxAgeMs": int(self.grpc_channel_max_age_ms),
                    "StreamMaxAgeMs": int(self.grpc_stream_max_age_ms),
                    "IdleTimeoutMs": int(self.grpc_idle_timeout_ms),
                },
                "AgentInfo": {
                    "RefreshIntervalMs": int(self.agent_info_refresh_interval_ms),
                    "SendRetryIntervalMs": int(self.agent_info_send_retry_interval_ms),
                    "MaxTryPerAttempt": int(self.agent_info_max_try_per_attempt),
                },
                "SpanBatch": {
                    "Size": int(self.span_batch_size),
                    "FlushIntervalMs": int(self.span_batch_flush_interval_ms),
                    "CollectDeadlineMs": int(self.span_batch_collect_deadline_ms),
                    "MaxConcurrentRequests":
                        int(self.span_batch_max_concurrent_requests),
                },
            },
            "Sampling": {
                "Type": str(self.sampling_type),
                "CounterRate": int(self.sampling_counter_rate),
                "PercentRate": float(self.sampling_percent_rate),
                "NewThroughput": int(self.sampling_new_throughput),
                "ContinueThroughput": int(self.sampling_continue_throughput),
            },
            "Stat": {
                "Enable": bool(self.stat_enabled),
                "BatchCount": int(self.stat_batch_count),
                "BatchInterval": int(self.stat_batch_interval),
            },
            "Span": {
                "QueueSize": int(self.span_queue_size),
                "MaxEventDepth": int(self.span_max_event_depth),
                "MaxEventSequence": int(self.span_max_event_sequence),
                "EventChunkSize": int(self.span_event_chunk_size),
                "IgnoreErrors": [{k: v for k, v in rule.items()
                                  if k in ("name", "message_contains")}
                                 for rule in self.span_ignore_errors],
                "ErrorMark": strs(self.span_error_mark),
                "ErrorMarkExclude": strs(self.span_error_mark_exclude),
            },
            "Http": {
                "CollectUrlStat": bool(self.http_collect_url_stat),
                "UrlStatLimit": int(self.http_url_stat_limit),
                "UrlStatQueueSize": int(self.http_url_stat_queue_size),
                "UrlStatEnableTrimPath": bool(self.http_url_stat_enable_trim_path),
                "UrlStatTrimPathDepth": int(self.http_url_stat_trim_path_depth),
                "UrlStatMethodPrefix": bool(self.http_url_stat_method_prefix),
                "Server": {
                    "StatusCodeErrors": strs(self.http_server_status_code_errors),
                    "ExcludeUrl": strs(self.http_server_exclude_url),
                    "ExcludeMethod": strs(self.http_server_exclude_method),
                    "RecordRequestHeader": strs(self.http_server_record_request_header),
                    "RecordRequestCookie": strs(self.http_server_record_request_cookie),
                    "RecordResponseHeader": strs(self.http_server_record_response_header),
                    "ProxyUserHeaderNames": strs(self.http_server_proxy_user_header_names),
                    "ProxyHeaderEnable": bool(self.http_server_proxy_header_enable),
                    "RecordRequestParam": bool(self.http_server_record_request_param),
                    "RealIpHeader": strs(self.http_server_real_ip_header),
                    "RealIpEmptyValue": str(self.http_server_real_ip_empty_value),
                },
                "Client": {
                    "RecordRequestHeader": strs(self.http_client_record_request_header),
                    "RecordRequestCookie": strs(self.http_client_record_request_cookie),
                    "RecordResponseHeader": strs(self.http_client_record_response_header),
                    "RecordUrlQuery": bool(self.http_client_record_url_query),
                },
            },
            "Sql": {
                "MaxBindArgsSize": int(self.sql_max_bind_args_size),
                "EnableSqlStats": bool(self.sql_enable_sql_stats),
                "EnableRawSqlCache": bool(self.sql_enable_raw_sql_cache),
                "TraceBindValue": bool(self.sql_trace_bind_values),
                "RemoveComments": bool(self.sql_remove_comments),
                "CacheSize": int(self.sql_cache_size),
                "CacheLengthLimit": int(self.sql_cache_length_limit),
                "CacheExpireHours": int(self.sql_cache_expire_hours),
                "ErrorCount": int(self.sql_error_count),
            },
        }
        # Do not emit these top-level defaults ahead of the extra_yaml
        # fragment: yaml-cpp resolves the first duplicate key, so a default
        # emitted here would hide an override in that fragment.
        for key, value, default in (
            ("ActiveProfile", self.active_profile, ""),
            ("EnableConfigFileWatcher", self.enable_config_file_watcher, False),
            ("CallstackTraceNewThroughput", self.callstack_trace_new_throughput, 1000),
        ):
            if value == default:
                data.pop(key)
        if self.profiles:
            # Flow-style JSON is valid YAML and quotes arbitrary profile names
            # and nested keys safely (the normal mapping keys are constants).
            profile_yaml = "Profile: " + json.dumps(self.profiles)
        else:
            profile_yaml = ""

        body = "\n".join(_yaml_lines(data))
        if profile_yaml:
            body += "\n" + profile_yaml
        if self.extra_yaml.strip():
            body += "\n" + self.extra_yaml.strip()
        return body + "\n"


# Parsers take (raw env value, current field value) and return the new value.
def _parse_nonempty(v, cur):
    return v or cur


def _parse_bool(v, _cur):
    return v.strip().lower() in ("1", "true", "yes", "on")


def _parse_list(v, _cur):
    return [item.strip() for item in v.split(",") if item.strip()]


def _parse_nonnegative_int(v, cur):
    try:
        return max(0, int(v))
    except ValueError:
        return cur


def _normalize_native_log_queue_size(value) -> int:
    """Clamp hostile Python-only queue settings without failing tracing."""
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return NATIVE_LOG_DEFAULT_QUEUE_SIZE
    if parsed < NATIVE_LOG_MIN_QUEUE_SIZE or parsed > NATIVE_LOG_MAX_QUEUE_SIZE:
        return NATIVE_LOG_DEFAULT_QUEUE_SIZE
    return parsed


# env var suffix -> (Config field, parser). Only values the Python layer
# consumes locally are mirrored; see apply_native_env_overrides.
_ENV_MIRROR = {
    "APPLICATION_NAME": ("application_name", _parse_nonempty),
    "CONFIG_FILE": ("config_file_path", lambda v, _cur: v),
    # Mirrored so the rendered YAML and agent.init()'s startup log report the
    # collector the native agent actually dials, not the pre-override default.
    "COLLECTOR_HOST": ("collector_host", _parse_nonempty),
    "COLLECTOR_AGENT_PORT": ("collector_agent_port", _parse_nonnegative_int),
    "COLLECTOR_SPAN_PORT": ("collector_span_port", _parse_nonnegative_int),
    "COLLECTOR_STAT_PORT": ("collector_stat_port", _parse_nonnegative_int),
    "LOG_LEVEL": ("log_level", _parse_nonempty),
    "NATIVE_LOG_TO_PYTHON": ("native_log_to_python", _parse_bool),
    "NATIVE_LOG_QUEUE_SIZE": (
        "native_log_queue_size", lambda v, _cur: _normalize_native_log_queue_size(v)),
    "ENABLE": ("enabled", _parse_bool),
    "ENABLE_CALLSTACK_TRACE": ("enable_callstack_trace", _parse_bool),
    "EXCEPTION_CHAIN_MAX_DEPTH": ("exception_chain_max_depth", _parse_nonnegative_int),
    "HTTP_COLLECT_URL_STAT": ("http_collect_url_stat", _parse_bool),
    "HTTP_SERVER_PROXY_HEADER_ENABLE": (
        "http_server_proxy_header_enable", _parse_bool),
    "ASYNCIO_TASK_SPAN_TIMEOUT_MS": (
        "asyncio_task_span_timeout_ms", _parse_nonnegative_int),
    # The HTTP_{SERVER,CLIENT}_RECORD_* env vars are deliberately NOT mirrored:
    # every production read of those lists comes from the per-span native
    # config snapshot, and the native parser folds the same env vars over the
    # rendered YAML itself. http_helper's Python-side Config fallback only
    # fires for targets without a native snapshot (test doubles, unsampled
    # spans), where nothing is recorded anyway.
    "PREFORK": ("prefork", _parse_bool),
    # These four ARE mirrored, unlike the header lists above: their Config
    # value is also the no-snapshot fallback the Python recorders fall back to
    # (see http_helper._snapshot_gate / _real_ip_config), and a mirrored env
    # keeps that fallback agreeing with what the native agent resolved.
    "HTTP_CLIENT_RECORD_URL_QUERY": ("http_client_record_url_query", _parse_bool),
    "HTTP_SERVER_RECORD_REQUEST_PARAM": (
        "http_server_record_request_param", _parse_bool),
    "HTTP_SERVER_REAL_IP_HEADER": ("http_server_real_ip_header", _parse_list),
    "HTTP_SERVER_REAL_IP_EMPTY_VALUE": (
        "http_server_real_ip_empty_value", lambda v, _cur: v),
    "SQL_TRACE_BIND_VALUE": ("sql_trace_bind_values", _parse_bool),
    # SPAN_MAX_EVENT_{DEPTH,SEQUENCE} are deliberately NOT mirrored: the
    # native parser folds those env vars over the rendered YAML itself, and
    # Agent._load_span_config reads the limits back from the native config
    # snapshot — the Python Config fields never gate anything at runtime.
}


def apply_native_env_overrides(cfg: Config) -> Config:
    """Mirror the few ``PINPOINT_PY_*`` values that gate Python-side behavior.

    All config env vars are parsed by the native agent itself (its prefix is
    set to :data:`ENV_VAR_PREFIX` during ``agent.init()``) and override the
    rendered YAML; the values in :data:`_ENV_MIRROR` get the same
    env-over-config precedence here. Everything else is deliberately not read
    from the environment on the Python side.
    """
    for suffix, (attr, parse) in _ENV_MIRROR.items():
        v = os.environ.get(f"{ENV_VAR_PREFIX}_{suffix}")
        if v is not None:
            setattr(cfg, attr, parse(v, getattr(cfg, attr)))
    cfg.native_log_queue_size = _normalize_native_log_queue_size(
        cfg.native_log_queue_size)
    return cfg


def _yaml_lines(mapping: Dict[str, Any], indent: int = 0) -> List[str]:
    """Render a nested dict as block-mapping YAML lines.

    Scalars and lists are serialized with ``json.dumps``: JSON scalars and
    flow collections are valid YAML 1.2, so quoting and escaping (colons,
    quotes, newlines, unicode) come for free, and empty lists render as the
    explicit ``[]`` native needs to clear stale values on reload.
    """
    pad = "  " * indent
    lines: List[str] = []
    for key, value in mapping.items():
        if isinstance(value, dict):
            lines.append(f"{pad}{key}:")
            lines.extend(_yaml_lines(value, indent + 1))
        else:
            lines.append(f"{pad}{key}: {json.dumps(value)}")
    return lines
