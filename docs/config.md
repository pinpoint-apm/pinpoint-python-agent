# Pinpoint Python Agent - Configuration Guide

Consolidated reference for every configuration option of
`pinpoint-python-agent`. Each option is listed once, in the table for its
section, with its `init()` kwarg, environment variable, type and default.

Each kwarg corresponds to one key in the native YAML the agent runs on
(`span_max_event_depth` ↔ `Span.MaxEventDepth`): `init()` renders the kwargs
into that YAML and hands it to the embedded native core. Config files
therefore use the YAML keys, and
[`Config.to_yaml()`](../pinpoint/config.py) is the exact kwarg → key mapping.

---

## Configuration Methods & Precedence

The agent merges configuration from four sources. **Later sources override
earlier ones:**

1. **Default values** (lowest priority) — the [`Config`](../pinpoint/config.py)
   dataclass defaults.
2. **`init()` kwargs** — rendered into an inline YAML document.
3. **Configuration file** — when `config_file_path=` (or
   `PINPOINT_PY_CONFIG_FILE`) is set, the file **replaces the inline YAML
   wholesale**; it is not merged with the kwargs.
4. **Environment variables** (highest priority) — `PINPOINT_PY_*`, parsed by
   the native agent and applied last.

Out-of-range values are normalised (clamped) by the native agent after the
merge. Two sharp edges worth knowing:

- **Unknown `init()` kwargs are ignored, but reported** — a typo degrades to
  the default instead of crashing startup, and each one is logged at `WARNING`
  with the closest real option (`unknown option 'collector_hostt' was ignored
  -- did you mean 'collector_host'?`). If a setting seems to have no effect,
  check that warning first.
- **Environment variables are read only at startup.** A [hot reload](#configuration-hot-reload)
  re-reads the file, never the environment.

### Method 1: `init()` kwargs

```python
import pinpoint

pinpoint.init(
    application_name="MyApplication",
    collector_host="collector.internal",
    sampling_type="PERCENT",
    sampling_percent_rate=1.0,          # 1% of transactions
)
```

`server_info=` is also an `init()` parameter (default
`"Python Application"`) — it labels the AgentInfo server metadata and is not
part of the YAML config.

### Method 2: Environment variables

Every option is read as `PINPOINT_PY_<SUFFIX>`; the tables below give the
exact variable for each one:

```bash
export PINPOINT_PY_APPLICATION_NAME="MyApplication"
export PINPOINT_PY_COLLECTOR_HOST="collector.internal"
export PINPOINT_PY_LOG_LEVEL="info"
```

List-typed options accept **comma-separated values**:

```bash
export PINPOINT_PY_HTTP_SERVER_RECORD_REQUEST_HEADER="Content-Type,User-Agent,X-Request-Id"
```

`PINPOINT_PY_SPAN_IGNORE_ERRORS` uses `Name@message substring` entries, e.g.
`ValueError@expected,@timeout`. `@timeout` matches any error name. Use a YAML
list of maps or `span_ignore_errors` for rules containing commas.

### Method 3: Configuration file

```python
pinpoint.init(config_file_path="/path/to/pinpoint-config.yaml")
```

Or `export PINPOINT_PY_CONFIG_FILE=/path/to/pinpoint-config.yaml`. The file
uses the native YAML keys (`ApplicationName`, `Collector.Host`, …) and
replaces the kwargs-rendered YAML entirely.

### Escape hatch: `extra_yaml`

A native key not surfaced as a kwarg yet can be appended verbatim:

```python
pinpoint.init(application_name="MyApp", extra_yaml="FutureNativeOption: true")
```

---

## Agent Configuration

| `init()` kwarg | Environment Variable | Type | Default | Notes |
|---|---|---|---|---|
| `application_name` | `PINPOINT_PY_APPLICATION_NAME` | str | `""` | **Required.** When neither this nor `config_file_path` is set, `init()` logs a warning and disables the agent. Max 24 chars for `uid_version` v1, otherwise 254. |
| `agent_name` | `PINPOINT_PY_AGENT_NAME` | str | `""` | Optional human-readable label. The agent id is not configurable — always an auto-generated per-process UUIDv7. |
| `uid_version` | `PINPOINT_PY_UID_VERSION` | str | `""` (→ `v3`) | Agent identity version. `v3` (the default) needs only `application_name`; `v1` is the legacy format and caps `application_name` at 24 characters; `v4` additionally requires `service_name` and `api_key`. |
| `service_name` | `PINPOINT_PY_SERVICE_NAME` | str | `""` | **Required for v4**, together with `api_key`. |
| `api_key` | `PINPOINT_PY_API_KEY` | str | `""` | **Required for v4.** Unused otherwise. |
| `enabled` | `PINPOINT_PY_ENABLE` | bool | `True` | `False` disables tracing without code changes: the native side gets `Enable: false` **and** the Python instrumentations short-circuit. |
| `is_container` | `PINPOINT_PY_IS_CONTAINER` | bool | `False` | The rendered YAML always writes this key, so set it explicitly in containers. (Auto-detection applies only when a config file omits the key.) |
| `application_type` | — | int | `1700` | Pinpoint service type (`APP_TYPE_PYTHON`). Passed via native `AgentOptions`, not YAML; no env var. |
| `config_file_path` | `PINPOINT_PY_CONFIG_FILE` | str | `""` | See [Method 3](#method-3-configuration-file). |
| `server_info=` (init param) | `PINPOINT_PY_SERVER_INFO` | str | `"Python Application"` | AgentInfo server metadata label. The env var is read by the `pinpoint-run` bootstrap (and its `--server-info` flag), not by a manual `init()`. |

---

## Logging Configuration

| `init()` kwarg | Environment Variable | Type | Default | Notes |
|---|---|---|---|---|
| `log_level` | `PINPOINT_PY_LOG_LEVEL` | str | `"INFO"` | `debug`, `info`, `warn`/`warning`, `error` (case-insensitive). Configures the native agent **and** the Python-side `pinpoint.*` loggers. |
| `log_output` | `PINPOINT_PY_LOG_FILE_PATH` | str | `"stderr"` | `"stderr"` = no file logging (the native agent writes to stdout). Any other value is a file path with rotation; supports the per-worker `%pid%` placeholder. |
| `log_max_file_size` | `PINPOINT_PY_LOG_MAX_FILE_SIZE` | int | `10` | Max log file size in MB before rotation. |
| `log_max_backups` | `PINPOINT_PY_LOG_MAX_BACKUPS` | int | `1` | Rotated files retained; native enforces at least 1. |
| `native_log_to_python` | `PINPOINT_PY_NATIVE_LOG_TO_PYTHON` | bool | `False` | **Python-only opt-in.** Route native-agent diagnostics to `logging.getLogger("pinpoint.native")` through a bounded asynchronous bridge. Also available as `pinpoint-run --native-log-to-python`. |
| `native_log_queue_size` | `PINPOINT_PY_NATIVE_LOG_QUEUE_SIZE` | int | `1024` | Python-only bridge record capacity, 2–4096. Invalid/out-of-range values fall back to 1024. The queue also has a 4 MiB byte budget and truncates each message at a valid UTF-8 boundary no later than 4 KiB. |

The bridge is off by default, preserving the native logger's existing stdout
or `Log.FilePath` behavior. When enabled, the native sink contract is replacement,
not teeing: **neither stdout nor `Log.FilePath` receives native lines**, even if
`log_output` is configured. Python does not duplicate those lines. Configure
handlers, filters and propagation on `pinpoint.native` (or its `pinpoint`
parent); Python logging makes the final output decision. Native levels map as
`debug` → `DEBUG`, `info` → `INFO`, `warning` → `WARNING`, `error` → `ERROR`,
with unknown values falling back to `WARNING`. The native message is passed as
already formatted (`[pinpoint][file:line] text`) with no added timestamp or
newline beyond whatever the selected Python formatter adds.

```python
pinpoint.init(
    application_name="MyApplication",
    native_log_to_python=True,
    native_log_queue_size=1024,
)
```

The native callback never runs Python handlers. It copies into a nonblocking
bounded queue and returns; when full, new records are dropped and
`agent.native_log_dropped` reports the cumulative count. A daemon consumer
drains the queue through normal Python logging. Shutdown waits up to one second
for it; a handler that never returns cannot indefinitely block tracing teardown.
Fixed slots occupy about 4 MiB at the default capacity and about 16 MiB at the
4096-record maximum; none of that memory is allocated while the opt-in is off.
If the optional queue or consumer thread cannot be created, tracing still
starts and that lifecycle falls back to the native stdout/file sink.

> **Multi-process hosts:** size rotation is not safe when several workers
> share one log file — use `%pid%` to give each worker its own
> (e.g. `log_output="/var/log/pinpoint/agent-%pid%.log"`). See the
> [Pre-fork Integration Guide](prefork.md).

---

## Collector Configuration

| `init()` kwarg | Environment Variable | Type | Default | Notes |
|---|---|---|---|---|
| `collector_host` | `PINPOINT_PY_COLLECTOR_HOST` | str | `"localhost"` | Pinpoint Collector hostname or IP. |
| `collector_agent_port` | `PINPOINT_PY_COLLECTOR_AGENT_PORT` | int | `9991` | gRPC port for agent metadata. |
| `collector_span_port` | `PINPOINT_PY_COLLECTOR_SPAN_PORT` | int | `9993` | gRPC port for span data. |
| `collector_stat_port` | `PINPOINT_PY_COLLECTOR_STAT_PORT` | int | `9992` | gRPC port for statistics. |
| `span_batch_size` | `PINPOINT_PY_SPAN_BATCH_SIZE` | int | `20` | Max spans per send batch. |
| `span_batch_flush_interval_ms` | `PINPOINT_PY_SPAN_BATCH_FLUSH_INTERVAL_MS` | int | `1000` | Span batch flush interval. |
| `span_batch_collect_deadline_ms` | `PINPOINT_PY_SPAN_BATCH_COLLECT_DEADLINE_MS` | int | `500` | Deadline for collecting a batch before send. |
| `span_batch_max_concurrent_requests` | `PINPOINT_PY_SPAN_BATCH_MAX_CONCURRENT_REQUESTS` | int | `10` | Max concurrent span-send requests. |
| `agent_info_refresh_interval_ms` | `PINPOINT_PY_AGENT_INFO_REFRESH_INTERVAL_MS` | int | `86400000` | AgentInfo refresh interval. |
| `agent_info_send_retry_interval_ms` | `PINPOINT_PY_AGENT_INFO_SEND_RETRY_INTERVAL_MS` | int | `3000` | Retry interval for sending AgentInfo. |
| `agent_info_max_try_per_attempt` | `PINPOINT_PY_AGENT_INFO_MAX_TRY_PER_ATTEMPT` | int | `3` | Max send attempts per AgentInfo refresh. |

---

## gRPC Transport Configuration

| `init()` kwarg | Environment Variable | Type | Default | Notes |
|---|---|---|---|---|
| `grpc_ssl_enable` | `PINPOINT_PY_GRPC_SSL_ENABLE` | bool | `False` | Enables TLS for all gRPC channels. |
| `grpc_ssl_trust_cert_file_path` | `PINPOINT_PY_GRPC_SSL_TRUST_CERT_FILE_PATH` | str | `""` | PEM trust certificate path. |
| `grpc_ssl_root_cert_file_path` | `PINPOINT_PY_GRPC_SSL_ROOT_CERT_FILE_PATH` | str | `""` | Alias; `trust_cert` wins when both are set. |
| `grpc_keepalive_time_ms` | `PINPOINT_PY_GRPC_KEEPALIVE_TIME_MS` | int | `30000` | `GRPC_ARG_KEEPALIVE_TIME_MS`. |
| `grpc_keepalive_timeout_ms` | `PINPOINT_PY_GRPC_KEEPALIVE_TIMEOUT_MS` | int | `60000` | `GRPC_ARG_KEEPALIVE_TIMEOUT_MS`. |
| `grpc_keepalive_permit_without_calls` | `PINPOINT_PY_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS` | bool | `False` | `GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS`: keep pinging a channel that has no active RPC. Off by default — a collector or proxy may close a channel that pings while idle. |
| `grpc_max_send_message_size` | `PINPOINT_PY_GRPC_MAX_SEND_MESSAGE_SIZE` | int | `4194304` | `-1` = unlimited. |
| `grpc_max_receive_message_size` | `PINPOINT_PY_GRPC_MAX_RECEIVE_MESSAGE_SIZE` | int | `4194304` | `-1` = unlimited. |
| `grpc_sender_queue_size` | `PINPOINT_PY_GRPC_SENDER_QUEUE_SIZE` | int | `1000` | Metadata sender queue; spans use `span_queue_size`. |
| `grpc_channel_max_age_ms` | `PINPOINT_PY_GRPC_CHANNEL_MAX_AGE_MS` | int | `0` | Replaces a channel older than this (±10% jitter) with a freshly connected one. `0` disables. |
| `grpc_stream_max_age_ms` | `PINPOINT_PY_GRPC_STREAM_MAX_AGE_MS` | int | `0` | Max lifetime (±10% jitter) of the ping, stat and command streams. `0` disables. |
| `grpc_idle_timeout_ms` | `PINPOINT_PY_GRPC_IDLE_TIMEOUT_MS` | int | `0` | Time without an RPC after which gRPC drops a channel to IDLE. `0` disables; a positive value below gRPC's 1s minimum is raised to `1000`. |

---

## Stat Configuration

| `init()` kwarg | Environment Variable | Type | Default | Notes |
|---|---|---|---|---|
| `stat_enabled` | `PINPOINT_PY_STAT_ENABLE` | bool | `True` | System statistics collection. |
| `stat_batch_count` | `PINPOINT_PY_STAT_BATCH_COUNT` | int | `6` | Batches collected before sending. |
| `stat_batch_interval` | `PINPOINT_PY_STAT_BATCH_INTERVAL` | int | `5000` | Collection interval in ms; valid range `1000`–`10000`, otherwise the default is used. |

---

## Sampling Configuration

| `init()` kwarg | Environment Variable | Type | Default | Notes |
|---|---|---|---|---|
| `sampling_type` | `PINPOINT_PY_SAMPLING_TYPE` | str | `"COUNTER"` | `"COUNTER"` or `"PERCENT"`. |
| `sampling_counter_rate` | `PINPOINT_PY_SAMPLING_COUNTER_RATE` | int | `1` | Sample 1/N transactions. `0` = disable. |
| `sampling_percent_rate` | `PINPOINT_PY_SAMPLING_PERCENT_RATE` | float | `100.0` | `0` or negative = never sample; values above `100` become `100`. The rate is stored as hundredths of a percent and **truncates** (`0.29` samples 0.28%), so a positive rate below `0.01` truncates to `0` and samples nothing either — the agent logs a `WARNING` for it. `0.01` is the smallest rate that still samples. |
| `sampling_new_throughput` | `PINPOINT_PY_SAMPLING_NEW_THROUGHPUT` | int | `0` | TPS cap for new transactions. `0` = unlimited. |
| `sampling_continue_throughput` | `PINPOINT_PY_SAMPLING_CONTINUE_THROUGHPUT` | int | `0` | TPS cap for continuing transactions. `0` = unlimited. |

Throughput limiting activates automatically when either throughput value is
greater than `0`; it is not a separate sampling type. Unsampled transactions
still propagate `Pinpoint-Sampled: s0` downstream — see
[API Contracts §6](api_contracts.md#6-unsampled-and-no-op-spans-are-deliberately-silent).

---

## Span Configuration

| `init()` kwarg | Environment Variable | Type | Default | Notes |
|---|---|---|---|---|
| `span_queue_size` | `PINPOINT_PY_SPAN_QUEUE_SIZE` | int | `1024` | Buffered spans awaiting shipment. |
| `span_max_event_depth` | `PINPOINT_PY_SPAN_MAX_EVENT_DEPTH` | int | `64` | Allows depth 1 through `max + 1` (65 by default); `-1` = native maximum. |
| `span_max_event_sequence` | `PINPOINT_PY_SPAN_MAX_EVENT_SEQUENCE` | int | `5000` | `-1` = unlimited. |
| `span_event_chunk_size` | `PINPOINT_PY_SPAN_EVENT_CHUNK_SIZE` | int | `20` | Events per transmission chunk. Also the number of finished events the Python wrapper buffers before replaying them to native mid-span; `0` keeps every event until `span.end()`. The kwarg drives the Python threshold, so set it via `init()` rather than only the env var when you change it. |
| `span_ignore_errors` | `PINPOINT_PY_SPAN_IGNORE_ERRORS` | list[dict] | `[]` | Rules with `name` (Python exception class name) and/or `message_contains` (substring), plus optional `match_subclasses` / `match_cause` bools (kwargs only). Records the exception but suppresses its failure mark. See "Matching rules" under Advanced Configuration. |
| `span_error_mark` | `PINPOINT_PY_SPAN_ERROR_MARK` | list[str] | `[]` | Enabled error categories: `exception`, `http-status`, `sql`; empty means all. |
| `span_error_mark_exclude` | `PINPOINT_PY_SPAN_ERROR_MARK_EXCLUDE` | list[str] | `[]` | Remove categories from `span_error_mark`. |

---

## HTTP Configuration

### URL Statistics

| `init()` kwarg | Environment Variable | Type | Default | Notes |
|---|---|---|---|---|
| `http_collect_url_stat` | `PINPOINT_PY_HTTP_COLLECT_URL_STAT` | bool | `False` | Aggregate response-time histograms per `(url, method, status)`, bucketed into 30-second ticks and reported on the stat stream. The bundled HTTP integrations pass the route template (Flask's `rule`, and the equivalent for the others), so keys stay bounded; a span whose URL is empty is aggregated under `/NULL`. |
| `http_url_stat_limit` | `PINPOINT_PY_HTTP_URL_STAT_LIMIT` | int | `1000` | Max distinct URL keys tracked per 30-second tick. `0` records none; negative values fall back to the default. |
| `http_url_stat_queue_size` | `PINPOINT_PY_HTTP_URL_STAT_QUEUE_SIZE` | int | `1024` | Records buffered per queue shard (16 shards) while waiting for aggregation; anything beyond is dropped. Valid range `1`–`65536`, otherwise the default. |
| `http_url_stat_enable_trim_path` | `PINPOINT_PY_HTTP_URL_STAT_ENABLE_TRIM_PATH` | bool | `False` | Trim the recorded URL to `http_url_stat_trim_path_depth` leading segments plus `*`. See the note below. |
| `http_url_stat_trim_path_depth` | `PINPOINT_PY_HTTP_URL_STAT_TRIM_PATH_DEPTH` | int | `3` | Leading path segments kept when trimming is on. See the note below. |
| `http_url_stat_method_prefix` | `PINPOINT_PY_HTTP_URL_STAT_METHOD_PREFIX` | bool | `False` | Prefix each key with the HTTP method and a space (`GET /api/users`), so the same path is counted separately per method. |

Collection is decided by the **resolved** native config, but the Python wrapper
only buffers the `set_url_stat()` tuple when it has a reason to: this kwarg,
`enable_callstack_trace` (which reuses the URL template for exception
metadata), or any config source the Python layer cannot read ahead of time
(`config_file_path`, `profiles`, `extra_yaml`). Turning statistics on from a
config file alone therefore works, while `http_collect_url_stat=False` with no
file stays on the cheapest path.

`http_url_stat_enable_trim_path` is off by default, so a recorded URI template
is aggregated verbatim — turn it on only when the caller can pass nothing but a
raw request URL. With it on, the path is cut to `http_url_stat_trim_path_depth`
leading segments plus a `*` suffix (depth `2`: `/api/v1/users` → `/api/v1/*`). A
path with no more segments than the depth is kept as is, so depth `3` keeps
`/api/users/123` and trims only from the fourth segment. Values below `1` are
treated as `1`.

### Server-side Tracing

| `init()` kwarg | Environment Variable | Type | Default | Notes |
|---|---|---|---|---|
| `http_server_status_code_errors` | `PINPOINT_PY_HTTP_SERVER_STATUS_CODE_ERRORS` | list[str] | `["5xx"]` | Status codes that mark the transaction failed (error category `http-status`). Each entry is a class token `1xx`–`5xx` (case-insensitive) or an exact code such as `404`; anything else — a malformed token, or a number outside `0`–`599` — is ignored with a warning. An empty list means no status code fails a transaction. |
| `http_server_exclude_url` | `PINPOINT_PY_HTTP_SERVER_EXCLUDE_URL` | list[str] | `[]` | Request paths never traced (health checks, static assets). Ant-style wildcards, see below. An excluded request still gets a span object back, so integration code paths are unchanged, but it is a pure no-op: `span.sampled` is `False`, its span id is `0`, and — unlike a genuinely [unsampled](#sampling-configuration) request — it propagates no `Pinpoint-Sampled: s0` and records no URL stat. |
| `http_server_exclude_method` | `PINPOINT_PY_HTTP_SERVER_EXCLUDE_METHOD` | list[str] | `[]` | HTTP methods never traced, e.g. `["OPTIONS", "HEAD"]`; matched case-insensitively, with the same no-op span as `http_server_exclude_url`. Applies only where the integration passes the request method (the bundled WSGI, ASGI, aiohttp and Tornado ones do); non-HTTP entry points such as messaging consumers and gRPC skip the filter. |
| `http_server_record_request_header` | `PINPOINT_PY_HTTP_SERVER_RECORD_REQUEST_HEADER` | list[str] | `[]` | Inbound request headers recorded as annotations. Empty (the default) records none; names are matched case-insensitively and the annotation carries the configured spelling. |
| `http_server_record_request_cookie` | `PINPOINT_PY_HTTP_SERVER_RECORD_REQUEST_COOKIE` | list[str] | `[]` | Cookies of the inbound request, recorded the same way. Off by default; cookies routinely carry session ids. |
| `http_server_record_response_header` | `PINPOINT_PY_HTTP_SERVER_RECORD_RESPONSE_HEADER` | list[str] | `[]` | Response headers recorded at span end. |
| `http_server_proxy_user_header_names` | `PINPOINT_PY_HTTP_SERVER_PROXY_USER_HEADER_NAMES` | list[str] | `[]` | Request headers carrying a user-defined proxy header. See [User Proxy Headers](#user-proxy-headers). |
| `http_server_proxy_header_enable` | `PINPOINT_PY_HTTP_SERVER_PROXY_HEADER_ENABLE` | bool | `True` | `False` stops all proxy-header parsing, builtin names included. `init()`/environment only. See [User Proxy Headers](#user-proxy-headers). |
| `http_server_record_request_param` | `PINPOINT_PY_HTTP_SERVER_RECORD_REQUEST_PARAM` | bool | `False` | Record the inbound query string as `http.param`. See below. |
| `http_server_real_ip_header` | `PINPOINT_PY_HTTP_SERVER_REAL_IP_HEADER` | list[str] | `["X-Forwarded-For", "X-Real-Ip"]` | Headers the client address is resolved from, in order. See below. |
| `http_server_real_ip_empty_value` | `PINPOINT_PY_HTTP_SERVER_REAL_IP_EMPTY_VALUE` | str | `""` | Placeholder value that makes a real-IP header be skipped. See below. |

A single entry `HEADERS-ALL` (case-insensitive) in any of the three recording
lists records **every** header instead of an allow-list — a debugging setting,
not a production one. Never list `Authorization` or `Cookie` in production.

`http_server_record_request_param` records the inbound query string as
`http.param` (`k=v&k=v`, each key and value cut at 64 chars and the whole
string at 512 with a trailing `...`). It is off by default because query
strings routinely carry tokens and ids.

`http_server_real_ip_header` lists the headers consulted, in order, for the
client address recorded as `remoteAddr`: the first hop of the first non-empty
value wins, and `Forwarded` (RFC 7239) is parsed for its `for=` pair. `[]`
trusts no header and records the socket address.
`http_server_real_ip_empty_value` names the placeholder a proxy sends when it
has no client address (e.g. `unknown`); a header whose first hop equals it
(case-insensitively) is skipped.

These three and `http_client_record_url_query` are recorded by the Python
integrations rather than natively, but they resolve out of each span's native
configuration snapshot (`Http.Server.RecordRequestParam`,
`Http.Server.RealIpHeader`, `Http.Server.RealIpEmptyValue`,
`Http.Client.RecordUrlQuery`) — so a config file, a profile and a watcher
reload reach them like any other key, and open spans keep the generation they
started with. `http_server_proxy_header_enable` is the exception that still
needs `init()` or the environment variable; see
[User Proxy Headers](#user-proxy-headers).

`http_server_exclude_url` patterns use Ant-style wildcards: `?` matches one
character, `*` matches any run of characters, and `**` matches across path
segments; neither `?` nor `*` crosses a `/`. A pattern must match the **whole**
path including its leading `/`, so `"*.css"` never matches `/static/main.css` —
write `"/**/*.css"`.

### User Proxy Headers

Set `http_server_proxy_header_enable=False` to skip all built-in and user proxy
headers. `http_server_proxy_user_header_names=["X-Edge-Timing", "X-Gateway-Timing"]`
sets native `Http.Server.ProxyUserHeaderNames`. The environment variable accepts
comma-separated names. Each configured name is looked up case-insensitively;
`HEADERS-ALL` is a literal name here. All valid builtin and user headers produce
separate proxy annotations, with user proxy code `4`. The displayed name is the
configured header name, capped at 32 UTF-8 bytes without splitting a character.

The enable switch reaches this parser through `init()` or
`PINPOINT_PY_HTTP_SERVER_PROXY_HEADER_ENABLE` only. A value supplied solely by a
config file or a profile reload does not turn proxy-header parsing on or off.

Values contain space-separated `key=value` tokens, for example
`t=1504230492.763 D=0.123`. The received timestamp is stored in milliseconds;
user timestamps use the native parser's inference order:

- Fewer than 13 bytes: rejected.
- At least 16 bytes: microseconds; remove the final three bytes before parsing.
- Otherwise, a decimal point indicates `sec.mmm`, with at least 10 digits before
  the point and exactly three after it; integers are milliseconds.

The resulting timestamp must be positive and fit int64. `D=123` is already
microseconds; `D=0.123` is converted to `123000` microseconds. Missing, zero,
malformed, or int32-overflowing durations use `-1` (not reported). Apache `i`
and `b` percentages accept only unsigned values from 0 through 100; other or
missing values also use `-1`. An invalid timestamp discards only that header.
User headers ignore `i`, `b`, and `app`; duplicate fields use the last value.

Names come from each span's resolved native configuration: file configuration
replaces inline kwargs, the selected profile overlays the file, and environment
variables override both (also on reload). With the file watcher enabled, new
spans use the reloaded list while open spans and their async descendants keep
their captured revision. An empty list disables user proxy recording.

### Client-side Tracing

| `init()` kwarg | Environment Variable | Type | Default | Notes |
|---|---|---|---|---|
| `http_client_record_request_header` | `PINPOINT_PY_HTTP_CLIENT_RECORD_REQUEST_HEADER` | list[str] | `[]` | Headers of the outbound request, recorded on the client span event. |
| `http_client_record_request_cookie` | `PINPOINT_PY_HTTP_CLIENT_RECORD_REQUEST_COOKIE` | list[str] | `[]` | Cookies sent with the outbound request. |
| `http_client_record_response_header` | `PINPOINT_PY_HTTP_CLIENT_RECORD_RESPONSE_HEADER` | list[str] | `[]` | Headers of the response the call returned. |
| `http_client_record_url_query` | `PINPOINT_PY_HTTP_CLIENT_RECORD_URL_QUERY` | bool | `False` | Keep the `?query` part of the recorded URL. See below. |

The lists behave exactly like their server-side counterparts, `HEADERS-ALL`
included (debugging only).

`http_client_record_url_query` keeps the `?query` part of outbound URLs in the
`http.url` annotation; it is off by default because query strings carry tokens
and ids, and it resolves per span from native `Http.Client.RecordUrlQuery` (see
[Server-side Tracing](#server-side-tracing)).

The header allow-lists are applied by the native recorder through
[`pinpoint.http_helper`](../pinpoint/http_helper.py), which every bundled HTTP
integration uses.

---

## SQL Configuration

Raw SQL is preserved up to 1 MiB of UTF-8. Larger statements are dropped whole;
metadata is abbreviated by native **after** normalization. This lets a long SQL
literal or comment normalize to the same ID as the equivalent short query.

| `init()` kwarg | Environment Variable | Type | Default | Notes |
|---|---|---|---|---|
| `sql_max_bind_args_size` | `PINPOINT_PY_SQL_MAX_BIND_ARGS_SIZE` | int | `1024` | Max bytes of recorded bind arguments. |
| `sql_enable_sql_stats` | `PINPOINT_PY_SQL_ENABLE_SQL_STATS` | bool | `False` | Record SQL metadata keyed by UID (`SQL-UID` annotation) instead of ID (`SQL-ID`), for collectors that aggregate SQL statistics by uid. Applies to sampled spans only. |
| `sql_enable_raw_sql_cache` | `PINPOINT_PY_SQL_ENABLE_RAW_SQL_CACHE` | bool | `True` | Cache normalized SQL by raw text. |
| `sql_remove_comments` | `PINPOINT_PY_SQL_REMOVE_COMMENTS` | bool | `True` | Strip comments before normalization; fixed at startup. |
| `sql_cache_size` | `PINPOINT_PY_SQL_CACHE_SIZE` | int | `1024` | Entries each SQL cache (id, uid, raw) holds; eviction re-registers the statement. Fixed at startup. |
| `sql_cache_length_limit` | `PINPOINT_PY_SQL_CACHE_LENGTH_LIMIT` | int | `2048` | SQL at or above this byte length bypasses caches. `-1` caches all; `0` bypasses all. Fixed at startup. |
| `sql_cache_expire_hours` | `PINPOINT_PY_SQL_CACHE_EXPIRE_HOURS` | int | `168` | Re-publish expired SQL UID metadata; `0` never expires. Fixed at startup. |
| `sql_error_count` | `PINPOINT_PY_SQL_ERROR_COUNT` | int | `100` | Mark the transaction failed when the SQL count reaches this limit, including async children; counting stops after another cause fails the trace. `0` or negative disables. |
| `sql_trace_bind_values` | `PINPOINT_PY_SQL_TRACE_BIND_VALUE` | bool | `False` | Record bound parameter **values**. Off by default: bind values routinely carry PII/secrets. The query template and endpoint are recorded either way. Settable from a config file too — mind the default below. |

> **A config file that omits `Sql.TraceBindValue` turns capture ON.** The Python
> layer reads this setting from the resolved native config snapshot, so all
> three methods reach it — kwargs, the environment variable, and a config file
> (including a hot reload). But the kwarg default of `False` only applies to the
> YAML `init()` renders: a **config file replaces that YAML wholesale**, and the
> native agent's own default for `Sql.TraceBindValue` is `true`. So a config
> file that never mentions the key records bind values.
>
> The agent logs a warning at startup when this happens — when the resolved
> config enables capture and the process never asked for it. To keep it off from
> a config file, say so explicitly:
>
> ```yaml
> Sql:
>   TraceBindValue: false
> ```

---

## Advanced Configuration

The native error field is a bitmask: exception `2`, HTTP status `4`, SQL count
`8`; combinations accumulate. Consumers should test `err != 0` for failure.
Ignore-error names use `type(exception).__name__`, e.g. `ValueError`.

**Matching rules.** A rule matches by exact class name by default. Two
optional bools widen it, and both default to **False** so a rule without them
keeps exact-name semantics: `match_subclasses` also matches subclasses (a `ConnectionError`
rule catches `ConnectionResetError`), and `match_cause` also walks
`__cause__` (else `__context__` unless `raise ... from None`), up to 8 links.
`message_contains` is tested against the message of the link whose class
matched. A widened match still records the exception (exceptionInfo, chain);
only the transaction failure mark is suppressed. These bools are evaluated in
Python from the `pinpoint.init()` kwargs only: rules from a config file or
profile, and `PINPOINT_PY_SPAN_IGNORE_ERRORS`, stay exact-name.

```python
pinpoint.init(
    application_name="MyApp",
    span_ignore_errors=[{"name": "ValueError", "message_contains": "expected"}],
    span_error_mark_exclude=["http-status"],
    active_profile="prod",
    profiles={"prod": {"Sampling": {"Type": "PERCENT", "PercentRate": 1.0}}},
)
```

Profiles and file reload are resolved natively. Python frame capture uses the
resolved native `EnableCallstackTrace` value captured when each sampled span is
created. Config files, active profiles, environment overrides, and watcher
reloads therefore apply without a duplicate Python startup setting. Reloads
affect new root spans; existing spans and their async children retain the
generation they started with.

| `init()` kwarg | Environment Variable | Type | Default | Notes |
|---|---|---|---|---|
| `enable_callstack_trace` | `PINPOINT_PY_ENABLE_CALLSTACK_TRACE` | bool | `False` | Attach a Python frame trace (up to 64 frames) to `set_error` on sampled span events. The resolved native value is fixed in each span snapshot; reload applies to new spans. |
| `callstack_trace_new_throughput` | `PINPOINT_PY_CALLSTACK_TRACE_NEW_THROUGHPUT` | int | `1000` | Native admission limit for new exception chains/second, not Python traceback capture cost. `0` = unlimited. |
| `enable_config_file_watcher` | `PINPOINT_PY_ENABLE_CONFIG_FILE_WATCHER` | bool | `False` | Install the native file watcher at startup. With a config file, put this key in that file or use the environment variable. |
| `active_profile` | `PINPOINT_PY_ACTIVE_PROFILE` | str | `""` | Select `Profile.<name>` before native environment overrides. |
| `profiles` | — | dict[str, dict] | `{}` | Emits native `Profile` subtrees; nested keys use the native YAML names. A config file replaces this input too. |

---

## Python-only Options

Options resolved by the Python layer itself. None of them is emitted into the
native YAML, so the values below come from `init()` kwargs and the
`PINPOINT_PY_*` variables only.

> Not listed here: the query-recording and real-IP options
> (`http_server_record_request_param`, `http_client_record_url_query`,
> `http_server_real_ip_header`, `http_server_real_ip_empty_value`). The Python
> integrations record those themselves, but the keys are rendered into the
> native config and read back per span, so a config file, a profile and a
> reload reach them like any other key — see
> [Server-side](#server-side-tracing) and
> [Client-side Tracing](#client-side-tracing).
> `http_server_proxy_header_enable` is the one setting of that shape that is
> still `init()`/environment only.

| `init()` kwarg | Environment Variable | Type | Default | Notes |
|---|---|---|---|---|
| `prefork` | `PINPOINT_PY_PREFORK` | bool | `False` | Prefork-master mode: `init()` stores configuration only, and each forked worker's after-fork hook makes the first native `StartAgent()` call. See the [Pre-fork Integration Guide](prefork.md). |
| `asyncio_task_span_timeout_ms` | `PINPOINT_PY_ASYNCIO_TASK_SPAN_TIMEOUT_MS` | int | `300000` | Lifetime cap for *implicit* asyncio-task spans (fire-and-forget tasks that inherit the current span). `0` disables. See [API Contracts §7](api_contracts.md#7-async-spans). |
| `exception_chain_max_depth` | `PINPOINT_PY_EXCEPTION_CHAIN_MAX_DEPTH` | int | `5` | Entries recorded per exception chain on a recording `SpanEvent.set_error`: the raised exception plus its `__cause__`/`__context__` chain, outermost first. `0` = unlimited. Needs `enable_callstack_trace`. See [API Contracts §9](api_contracts.md#9-error-recording-and-call-stacks). |
| `extra_yaml` | — | str | `""` | Raw YAML fragment appended to the rendered config. |
| — | `PINPOINT_PY_AUTOLOAD` | bool | `0` | Read by the `pinpoint-run` bootstrap's `sitecustomize.py`: truthy = call `init()` + `autoload()` at interpreter start. `pinpoint-run` sets it automatically. |
| — | `PINPOINT_PY_DISABLED_INSTRUMENTATIONS` | list | `""` | Comma-separated integrations to skip, e.g. `flask,redis`. Accepts a top-level package alias (disables all of its hooks) or a full module name (disables one). See [`autoload.py`](../pinpoint/autoload.py). |
| — | `PINPOINT_PY_SERVER_INFO` | str | — | Server metadata label for the `pinpoint-run` bootstrap path. |

---

## Configuration Hot Reload

Hot reload is a native-agent feature and requires a **config file** — inline
kwargs are not watched. Enable it with `EnableConfigFileWatcher: true` in the
file or `PINPOINT_PY_ENABLE_CONFIG_FILE_WATCHER=true`; the native agent then
polls the file's timestamp once per second and re-applies changes without a
process restart.

Identity, collector-transport, stat, URL-stat, span-queue, and SQL
normalization/cache options are not reloadable; logging, sampling, per-span
limits, callstack capture, HTTP filters, header recording, and the rest of SQL
tracing are. A reload that flips `Sql.TraceBindValue`,
`Http.Server.RecordRequestParam`, `Http.Client.RecordUrlQuery` or the
`Http.Server.RealIp*` pair reaches the Python-side recorders too, via the
config snapshot each new span is admitted under. A reload is applied atomically: a
span already in flight keeps the generation it started with.
Environment variables are re-applied to reloadable fields on every reload and
continue to override file/profile values. `Enable`, identity, transport, and
the four SQL normalization/cache settings (`sql_remove_comments`,
`sql_cache_size`, `sql_cache_length_limit`, `sql_cache_expire_hours`) remain
fixed. Native callstack
recording is reloadable; the Python frame-capture gate has the limitation above.

---

## Configuration Examples

### Development

Full sampling, debug logging, local collector:

```python
pinpoint.init(
    application_name="MyApp-Dev",
    collector_host="localhost",
    log_level="debug",
    sampling_type="PERCENT",
    sampling_percent_rate=100.0,       # sample all
    http_collect_url_stat=True,
    http_server_record_request_header=["HEADERS-ALL"],
    sql_trace_bind_values=True,
    enable_callstack_trace=True,
)
```

### Production

Throughput-capped percentage sampling, file logging, selective recording:

```python
pinpoint.init(
    application_name="MyApp-Prod",
    agent_name="prod-server-01",
    collector_host="pinpoint-collector.prod.example.com",
    log_level="warn",
    log_output="/var/log/pinpoint/agent-%pid%.log",
    log_max_file_size=50,
    sampling_type="PERCENT",
    sampling_percent_rate=1.0,          # 1% of transactions
    sampling_new_throughput=500,
    sampling_continue_throughput=1000,
    span_queue_size=2048,
    http_collect_url_stat=True,
    http_server_exclude_url=["/health", "/metrics"],
    http_server_exclude_method=["OPTIONS", "HEAD"],
    http_server_record_request_header=["Content-Type", "User-Agent"],
    sql_max_bind_args_size=512,
)
```

### Security

- Never record sensitive headers or cookies unless necessary; audit the
  allow-lists regularly.
- Keep `sql_trace_bind_values=False` unless literal values are required for
  diagnostics, and bound `sql_max_bind_args_size` when it is on.
- Annotations are visible to anyone with Pinpoint UI access — no passwords,
  secrets, or free-text PII.

---

## Symptom → Key Index

Diagnosis lives in the [Troubleshooting Guide](troubleshooting.md); this is
the reverse index once you know the symptom:

| Symptom | Keys to change |
|---|---|
| Agent never connects | `collector_host`, `collector_*_port` ([Collector](#collector-configuration)) |
| Nothing is traced at all | `application_name` unset disables the agent; `enabled=False` / `PINPOINT_PY_ENABLE=false` disables it deliberately ([Agent](#agent-configuration)) |
| Transactions missing | `sampling_type="PERCENT"` with `sampling_percent_rate=100.0` to sample all; clear `http_server_exclude_url` / `exclude_method` ([Sampling](#sampling-configuration), [HTTP](#http-configuration)) |
| One library not traced | `PINPOINT_PY_DISABLED_INSTRUMENTATIONS` ([Python-only](#python-only-options)) |
| Memory too high | Lower `span_queue_size`, `span_max_event_sequence`, `http_url_stat_limit` |
| CPU / latency overhead | Lower `sampling_percent_rate` or set the throughput caps; turn off `http_collect_url_stat`, `stat_enabled`. Measured per-call costs: [benchmark/api_overhead/RESULTS.md](../benchmark/api_overhead/RESULTS.md) |
| Traces truncated | Raise `span_max_event_depth` / `span_max_event_sequence` (`-1` = unlimited) |
| A setting seems ignored | `log_level="info"` makes the native agent log the **resolved** config (the `config:` dump). Remember: an env var silently wins over everything, a config file replaces the kwargs wholesale, and a typo'd kwarg is ignored with a `WARNING` naming it ([Precedence](#configuration-methods--precedence)) |
