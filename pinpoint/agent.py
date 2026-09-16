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

"""Process-wide Agent singleton and lifecycle."""

from __future__ import annotations

import atexit
import importlib.metadata as importlib_metadata
import os
import signal
import sys
import _thread
import threading
from typing import Mapping, Optional

from . import _native  # type: ignore[import-not-found]
from . import callstack as _callstack
from ._log import configure as _configure_log, get_logger
from ._native_log import NativeLogConsumer
from .config import ENV_VAR_PREFIX, Config, apply_native_env_overrides
from .context import _current_span
from .propagator import (
    HEADER_FLAG,
    HEADER_HOST,
    HEADER_PARENT_SPAN_ID,
    HEADER_SPAN_ID,
    HEADER_TRACE_ID,
    HEADER_PARENT_APP_NAME,
    HEADER_PARENT_APP_TYPE,
    HEADER_PARENT_SERVICE_NAME,
    HEADER_SAMPLED,
    extract_pinpoint_headers,
)
from .tracer import (
    _python_ignored,
    set_ignore_rules as _tracer_set_ignore_rules,
    _DEFAULT_MAX_EVENT_DEPTH,
    _DEFAULT_MAX_EVENT_SEQUENCE,
    _MAX_ERROR_VERDICTS,
    Span,
    _error_name_message,
    _reseed_span_ids,
)

_log = get_logger("agent")

# Reentrant so the SIGTERM handler can call shutdown() even when the signal lands
# while the main thread is already inside a lock-holding init()/shutdown().
_lock: "threading.RLock" = threading.RLock()
_instance: Optional["Agent"] = None
# One-shot per process: re-registering per init/shutdown cycle would queue
# redundant shutdown() calls (harmless — it is idempotent — but wasteful).
_atexit_registered = False
# Also one-shot, installed on the first init() of an *enabled* agent: a user can
# install their own handler post-init() without a later init() clobbering it.
_sigterm_handler_installed = False
# The callable SIGTERM handler present when we installed ours; our handler drains
# the agent and then chains to it. ``None`` means the prior disposition was
# SIG_DFL and there is nothing to chain to.
_previous_sigterm_handler = None
# Also one-shot: the after-in-child hook is process-wide and fires on every fork.
_fork_hook_registered = False
# One-shot latch for the nested-root-span warning below. Two instrumentations
# opening a transaction on one thread is a wiring mistake that repeats on every
# request, so it is reported once; after that the check is a single bool load.
_nested_root_span_warned = False
_DEFAULT_SERVER_INFO = "Python Application"
_SELF_TOP_LEVEL_PACKAGE = __package__.partition(".")[0] or "pinpoint"
# Range native accepts for Pinpoint-Flags; see new_span.
_INT32_BOUND = 2 ** 31


def _continued_context(headers, trace_id: str) -> bool:
    """Carry peer metadata only if native retained the inbound trace.

    Compare against the resolved identity, allowing native's numeric
    canonicalization and ignored fourth trace-id field.
    """
    if (not trace_id or HEADER_SPAN_ID not in headers
            or HEADER_PARENT_SPAN_ID not in headers):
        return False
    raw = headers.get(HEADER_TRACE_ID, "")
    if raw == trace_id:
        return True
    try:
        agent_id, start, sequence = raw.split("^", 3)[:3]
        for part in (start, sequence):
            digits = part[1:] if part[:1] in ("+", "-") else part
            if (len(part) > 20 or not digits or not digits.isascii()
                    or not digits.isdecimal() or not -(2 ** 63) <= int(part) < 2 ** 63):
                return False
        return f"{agent_id}^{int(start)}^{int(sequence)}" == trace_id
    except (ValueError, TypeError):
        return False


def _warn_on_nested_root_span(operation: str, rpc_point: str) -> None:
    """Report a second root span opened while one is already active.

    Raising into a user request to complain about instrumentation would be the
    wrong trade, so the second transaction is created anyway (the trace is
    split, not lost) and the wiring problem is named once.

    The binding is read straight off the ContextVar rather than through
    ``current_span()``: this runs before every root span until it fires, and
    the two cases that helper resolves are both *not* nesting — an ended span
    left bound, and a context copied into a worker thread, where a fresh root
    span is exactly right.
    """
    global _nested_root_span_warned
    binding = _current_span.get()
    if binding is None:
        return
    span = binding.span
    if getattr(span, "_ended", False) or binding.thread_id != _thread.get_ident():
        return
    _nested_root_span_warned = True
    _log.warning(
        "pinpoint: new_span(%r, %r) opened a second transaction while one is "
        "already active on this thread (trace_id=%s). Two instrumentations are "
        "tracing the same entry point, or a @pinpoint.span function is being "
        "called inside a traced request; the work will appear as two separate "
        "transactions instead of one tree. Use pinpoint.trace()/@spanevent for "
        "work inside an existing transaction. Reported once per process.",
        operation, rpc_point, getattr(span, "trace_id", "") or "<unsampled>",
    )


def _context_int(raw, bound, default=0):
    """Bounded signed ASCII integer, like native absl::SimpleAtoi."""
    if raw is None:
        return default
    text = raw.strip(" \t\r\n\v\f")
    digits = text[1:] if text[:1] in ("+", "-") else text
    if not digits or not digits.isascii() or not digits.isdecimal():
        return default
    try:
        value = int(text)
    except ValueError:
        return default
    return value if -bound <= value < bound else default


# Per-span config inputs used when the native config snapshot is unavailable
# (test doubles without get_config_snapshot, startup edge). The None revision
# never matches a native one, so every sampled span retries the fetch. Layout
# mirrors Agent._load_span_config's return value.
_FALLBACK_SPAN_CONFIG = (None, (), (),
                         _DEFAULT_MAX_EVENT_DEPTH, _DEFAULT_MAX_EVENT_SEQUENCE)


class Agent:
    """Thin wrapper around `_native.Agent`.

    Not constructed directly — call `init()` / retrieve via `get_agent()`.
    """

    __slots__ = ("_native", "_config", "_shutdown", "_shutdown_lock", "_enabled",
                 "_server_info", "_collect_url_stat", "_async_task_span_timeout",
                 "_span_config", "_native_log_consumer", "_event_flush_size")

    def __init__(self, native_agent: "_native.Agent", config: Config,
                 server_info: Optional[str] = None,
                 native_log_consumer: Optional[NativeLogConsumer] = None):
        self._native = native_agent
        # The consumer owns the pybind bridge; its shared C++ queue is also
        # captured by AgentOptions.log_sink. Keep it alive until native
        # shutdown has disabled the sink and waited out in-flight callbacks.
        self._native_log_consumer = native_log_consumer
        self._config = config
        self._shutdown = False
        self._shutdown_lock = threading.RLock()
        self._enabled = False
        # So the fork hook can re-create the child's agent with the same label.
        self._server_info = server_info
        # SetUrlStat also supplies the URL template for exception metadata.
        # File/profile/extra YAML can enable either feature without updating
        # the Python Config. Buffer the request tuple for those sources and
        # let native's resolved config decide whether to collect statistics.
        self._collect_url_stat = bool(
            config.http_collect_url_stat or config.enable_callstack_trace
            or config.config_file_path or config.extra_yaml or config.profiles)
        # init() kwargs reach Config unvalidated, and this field is Python-only
        # so native startup never vets it either. Fall back to the default the
        # same way the env-var path does (config._parse_nonnegative_int) rather
        # than losing tracing over one bad optional value.
        try:
            timeout_ms = max(0, int(config.asyncio_task_span_timeout_ms))
        except (TypeError, ValueError):
            timeout_ms = Config.asyncio_task_span_timeout_ms
            _log.warning(
                "asyncio_task_span_timeout_ms=%r is not a number; using %d",
                config.asyncio_task_span_timeout_ms, timeout_ms)
        self._async_task_span_timeout = timeout_ms / 1000.0
        # Mid-span replay threshold; mirrors the native chunk size so one
        # Python flush yields one SpanChunk. Bad values fall back to 20.
        try:
            self._event_flush_size = max(0, int(config.span_event_chunk_size))
        except (TypeError, ValueError):
            self._event_flush_size = Config.span_event_chunk_size
        # Native-resolved config snapshot plus its revision, fetched once here
        # and re-fetched by new_span whenever a sampled span reports a
        # different revision — i.e. the native config-file watcher hot-reloaded.
        self._span_config = self._load_span_config()

    @property
    def enabled(self) -> bool:
        # The gRPC handshake runs in a background thread, so `enable()` flips True
        # only once registration completes — query until it does, then cache.
        if self._shutdown or not self._config.enabled:
            return False
        if self._enabled:
            return True
        try:
            self._enabled = bool(self._native.enable())
            return self._enabled
        except Exception:  # noqa: BLE001
            return False

    @property
    def config(self) -> Config:
        return self._config

    def _load_span_config(self, native_span=None):
        """Fetch the native config snapshot and derive the per-span inputs.

        Returns ``(revision, snapshot, inject_base, max_event_depth,
        max_event_sequence)``, one immutable tuple so racing span creations
        always read a consistent generation. ``None`` when the native agent
        cannot serve a resolved snapshot (test doubles, startup failure):
        span creation then falls back to defaults and retries per span.
        """
        try:
            source = (native_span if native_span is not None and
                      hasattr(native_span, "get_config_snapshot") else self._native)
            snapshot = tuple(source.get_config_snapshot())
            # Revision 0 marks an unresolved (default) snapshot — treat it
            # like a fetch failure rather than caching empty identity values.
            if not snapshot[11]:
                return None
            service_name = str(snapshot[2])
            inject_base = (
                (HEADER_PARENT_APP_NAME, str(snapshot[0])),
                (HEADER_PARENT_APP_TYPE, str(int(snapshot[1]))),
                *(((HEADER_PARENT_SERVICE_NAME, service_name),)
                  if service_name else ()),
            )
            _warn_on_implicit_bind_value_capture(snapshot, self._config)
            return (int(snapshot[11]), snapshot, inject_base,
                    int(snapshot[3]), int(snapshot[4]))
        except Exception:  # noqa: BLE001
            _log.debug("native config snapshot unavailable", exc_info=True)
            return None

    # ---- span creation -----------------------------------------------------
    def new_span(self, operation: str, rpc_point: str,
                 headers=None, method: str = "") -> Span:
        """Create a new root span (new transaction).

        ``headers`` may be either a ``Mapping[str, str]`` (any key case) or
        one of :mod:`pinpoint.http_helper`'s lazy readers. Either way the
        ``Pinpoint-*`` propagation headers are extracted interpreter-side
        (:func:`pinpoint.propagator.extract_pinpoint_headers`) into the plain
        dict the native ``NewSpan`` overload takes, so native never calls
        back into a Python reader.

        With either form Pinpoint-* upstream headers are honoured and the
        resulting span links into the caller's trace.

        ``method`` is the inbound HTTP method, passed through so the native
        ``Http.Server.ExcludeMethod`` filter can reject the request before a
        span is recorded. It is empty for non-HTTP entry points (messaging
        consumers, gRPC), where the native side skips the filter.

        Sampling is decided by the native agent at creation time. When the
        decision is "not sampled" we wrap the native span in an
        :class:`UnSampledSpan` so subsequent annotation calls don't cross the
        pybind11 boundary. When HTTP URL-stat collection is enabled,
        ``set_url_stat()`` is kept on the wrapper and flushed with ``end()``;
        ``end()`` always reaches native so lifecycle state is cleaned up.
        """
        # operation/rpc_point arrive as module constants or already-built strings,
        # so trust callers rather than re-coercing on every root span.
        flags = 0
        if not _nested_root_span_warned:
            _warn_on_nested_root_span(operation, rpc_point)
        pinpoint_headers = {} if headers is None else extract_pinpoint_headers(headers)
        native_span, sampled, trace_id, span_id, revision = self._native.new_span(
            operation, rpc_point, pinpoint_headers, method)
        continued = _continued_context(pinpoint_headers, trace_id)
        if continued:
            flags = _context_int(pinpoint_headers.get(HEADER_FLAG), _INT32_BOUND)
        if not sampled:
            if not span_id:
                # Span id 0 is the native *noop* span: an excluded url/method,
                # a disabled agent, or a failed admission. It owns no native
                # lifetime (the immortal singleton registers no active span
                # and its EndSpan is empty), records nothing and propagates
                # nothing, so hand out the pure Python no-op rather than a
                # wrapper that keeps the handle and pays an end() call per
                # request. The native null sentinel is -1, so a real unsampled
                # span drawing exactly 0 is a 1-in-2^64 event; the inject probe
                # in UnSampledSpan already reads that as noop, and this dispatch
                # follows the same rule.
                return _NullSpan()
            return UnSampledSpan(
                native_span,
                collect_url_stat=self._collect_url_stat,
                span_id=span_id,
            )
        # The span reports the revision of the native config generation it was
        # admitted under; a mismatch with the cached snapshot means the config
        # file was hot-reloaded (or the snapshot was never fetched). Refresh
        # from this span, whose captured generation survives concurrent reloads.
        span_config = self._span_config
        if span_config is None or span_config[0] != revision:
            span_config = self._span_config = self._load_span_config(native_span)
            if span_config is None:
                span_config = _FALLBACK_SPAN_CONFIG
        _, config_snapshot, inject_base, max_event_depth, max_event_sequence = (
            span_config)
        return Span(
            native_span,
            collect_url_stat=self._collect_url_stat,
            async_task_span_timeout=self._async_task_span_timeout,
            flags=flags,
            inject_base=inject_base,
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=(_context_int(pinpoint_headers.get(HEADER_PARENT_SPAN_ID),
                                         2 ** 63, -1) if continued else -1),
            acceptor_host=(pinpoint_headers.get(HEADER_HOST, "") if continued else ""),
            max_event_depth=max_event_depth,
            max_event_sequence=max_event_sequence,
            config_snapshot=config_snapshot,
            enable_callstack_trace=_snapshot_callstack_enabled(config_snapshot),
            event_flush_size=self._event_flush_size,
        )

    def shutdown(self) -> None:
        # Reachable outside the module ``shutdown()``'s lock (direct callers,
        # ``__del__``, the SIGTERM handler racing a user thread), so claim the
        # flag atomically: the native Shutdown is idempotent, but running it
        # twice concurrently is still a needless race. RLock: a logging
        # handler inside consumer.stop()'s drain may call back into shutdown.
        with self._shutdown_lock:
            if self._shutdown:
                return
            self._shutdown = True
        self._enabled = False
        try:
            self._native.shutdown()
        except Exception:  # noqa: BLE001
            _log.warning("agent shutdown failed", exc_info=True)
        finally:
            # Native shutdown clears the callback as its lifecycle requires.
            # Only then deactivate and bounded-drain the bridge before
            # releasing its Python owner.
            consumer = self._native_log_consumer
            if consumer is not None:
                consumer.stop()

    @property
    def native_log_dropped(self) -> int:
        """Number of native records this agent's bounded bridge dropped."""
        consumer = self._native_log_consumer
        return 0 if consumer is None else consumer.dropped

    def _abandon_native_log_after_fork(self) -> None:
        consumer = self._native_log_consumer
        if consumer is not None:
            consumer.abandon_after_fork()

    def __del__(self):
        # Direct users of the internal wrapper can drop it without calling the
        # module singleton's shutdown/atexit path. All failures are unraisable.
        try:
            self.shutdown()
        except Exception:  # noqa: BLE001
            pass


def init(server_info: Optional[str] = None, **overrides) -> Agent:
    """Initialize the process-wide Pinpoint agent.

    Idempotent: subsequent calls return the already-initialized instance and
    ignore new kwargs. Use `shutdown()` between tests if you need a fresh
    agent. ``server_info`` labels AgentInfo server metadata and defaults to
    ``"Python Application"``.

    >>> pinpoint.init(
    ...     application_name="checkout",
    ...     agent_name="checkout-api",
    ...     server_info="Python Application",
    ... )

    Prefork masters (gunicorn ``--preload``, uWSGI without lazy-apps): pass
    ``prefork=True`` (or set ``PINPOINT_PY_PREFORK=1``). The master stores the
    pending configuration but makes no native agent API calls; the after-fork
    hook calls native ``StartAgent()`` for the first time in each worker.
    Threads and gRPC channels therefore never cross the fork boundary. The
    master itself records no spans — ``enabled`` stays False there.
    """
    global _instance

    with _lock:
        if _instance is not None:
            _log.debug("agent.init() called twice; returning existing instance")
            return _instance

        # Kwargs build the YAML config; PINPOINT_PY_* env vars are parsed natively
        # and override it. The mirror call folds in only the Python-side gates.
        cfg = apply_native_env_overrides(Config.from_kwargs(**overrides))
        resolved_server_info = _server_info_or_default(server_info)
        # Best-effort: a bad config value here must never crash the user's app on
        # startup, so degrade to defaults and keep going.
        try:
            _configure_log(cfg.log_level)
        except Exception:  # noqa: BLE001
            _log.debug("log configuration failed; continuing", exc_info=True)
        try:
            _callstack.set_chain_max_depth(cfg.exception_chain_max_depth)
            _tracer_set_ignore_rules(cfg.span_ignore_errors)
        except Exception:  # noqa: BLE001
            _log.debug("error-capture configuration failed; continuing",
                       exc_info=True)

        # A native config file may supply ApplicationName and replaces the inline
        # YAML wholesale, so defer validation to StartAgent() when one is set.
        if not cfg.application_name and not cfg.config_file_path:
            _log.warning(
                "pinpoint init without application_name — agent disabled. "
                "Set application_name=, config_file_path=, or "
                "%s_APPLICATION_NAME/%s_CONFIG_FILE.",
                ENV_VAR_PREFIX,
                ENV_VAR_PREFIX,
            )
            cfg.enabled = False

        if not cfg.enabled:
            _instance = _NullAgent(cfg)
            return _instance

        if cfg.prefork:
            # Every native API call must happen in the worker, so the master
            # keeps Python data only and _after_fork_reinit makes the first
            # native call in each child.
            _instance = _NullAgent(cfg, resolved_server_info, pending=True)
            # Warm the distribution scan now: the pending master never calls
            # _start_native_agent, so without this every worker would run the
            # scan cold inside its at-fork handler (and again on each gunicorn
            # max_requests recycle) instead of inheriting the master's.
            _top_level_distributions()
        else:
            try:
                _instance = _start_agent(cfg, resolved_server_info)
            except Exception:  # noqa: BLE001
                _log.exception(
                    "pinpoint native agent startup failed — disabling")
                cfg.enabled = False
                _instance = _NullAgent(cfg)
                return _instance

        _register_atexit_shutdown()
        # A prefork master has nothing to flush; each worker installs its own.
        if not cfg.prefork:
            _install_sigterm_exit_handler()
        _register_fork_hook()
        if cfg.prefork:
            _log.info(
                "pinpoint agent configuration pending for a prefork master "
                "(app=%s name=%s collector=%s:%d) — this process makes no "
                "native agent calls; forked workers start their own agents",
                cfg.application_name, cfg.agent_name,
                cfg.collector_host, cfg.collector_agent_port,
            )
        else:
            _log.info(
                "pinpoint agent initialized (app=%s name=%s collector=%s:%d)",
                cfg.application_name, cfg.agent_name,
                cfg.collector_host, cfg.collector_agent_port,
            )
        return _instance


# Index of the resolved Sql.TraceBindValue in the native snapshot tuple built
# by to_python_config in src/_native.cpp (appended past the revision at 11 so
# the header indices http_helper reads never shift). http_helper reads it per
# span; both must stay in sync with the C++ side.
_SNAPSHOT_SQL_TRACE_BIND_VALUE = 12

# Append-only native snapshot index. Older bindings/test doubles may return a
# shorter tuple; Span treats absence as false because this gates frame capture.
_SNAPSHOT_ENABLE_CALLSTACK_TRACE = 14


def _snapshot_callstack_enabled(snapshot) -> bool:
    """Read the append-only capture flag; a shorter tuple reads as off."""
    try:
        return bool(snapshot[_SNAPSHOT_ENABLE_CALLSTACK_TRACE])
    except (IndexError, TypeError):
        return False


def _warn_on_implicit_bind_value_capture(snapshot, cfg: Config) -> None:
    """Say so out loud when the resolved config turned bind-value capture on and
    this process never asked for it.

    ``Config.to_yaml`` always writes ``Sql.TraceBindValue`` explicitly, so the
    inline path can only enable capture on request. A **config file** replaces
    that YAML wholesale, and a file that omits the key inherits the native
    default of *true*, which reaches the Python instrumentations through the
    snapshot. Capturing bind values nobody asked for is a privacy problem, so
    it must not happen quietly.

    Runs once per config generation (``_load_span_config`` is called at init and
    again only when a reload moves the revision), so a hot reload that flips the
    setting is reported too.
    """
    if not snapshot[_SNAPSHOT_SQL_TRACE_BIND_VALUE]:
        return
    if cfg.sql_trace_bind_values:
        return  # asked for via kwargs or the environment
    _log.warning(
        "pinpoint: SQL/Mongo bound parameter VALUES are being recorded because "
        "the resolved config has Sql.TraceBindValue enabled, but this process "
        "did not request it (sql_trace_bind_values / "
        "%s_SQL_TRACE_BIND_VALUE are unset). A config file that omits the key "
        "inherits the native default of true. Bind values routinely carry "
        "PII/secrets -- set 'Sql: {TraceBindValue: false}' in the config file "
        "to turn it off.",
        ENV_VAR_PREFIX,
    )


def _start_native_agent(cfg: Config, server_info: str):
    """Build the AgentOptions payload and start one process-local agent."""
    consumer = None
    bridge = None
    if cfg.native_log_to_python:
        try:
            bridge = _native.NativeLogBridge(int(cfg.native_log_queue_size))
            consumer = NativeLogConsumer(bridge)
            # Start before native config parsing so even synchronous
            # startup/config errors can be drained through Python logging.
            consumer.start()
        except Exception:  # noqa: BLE001
            # An optional host-log integration must never disable tracing. If
            # its Python thread cannot start, omit the native sink and retain
            # the built-in stdout/file behavior for this lifecycle.
            _log.warning(
                "native Python log consumer could not start; retaining native "
                "stdout/file logging",
                exc_info=True,
            )
            consumer = None
            bridge = None
    try:
        native_agent = _native.start_agent(
            str(cfg.config_file_path or ""),
            cfg.to_yaml(),
            ENV_VAR_PREFIX,
            int(cfg.application_type),
            server_info,
            _command_line_args(),
            _loaded_dynamic_library_packages(),
            *(() if bridge is None else (bridge,)),
        )
    except Exception:
        if consumer is not None:
            # Deactivate and drain without replacing the original startup
            # exception, even if a Python logging handler itself is broken.
            consumer.stop()
        raise
    return native_agent, consumer


def _start_agent(cfg: Config, server_info: str) -> Agent:
    """Start the native agent and wrap it, leaving nothing running if the
    wrapper cannot be built.

    ``Agent.__init__`` reads config values, so a hostile one can still raise
    here. An orphaned native agent is worse than no agent: nothing owns it,
    so ``_register_atexit_shutdown`` never drains it and its destructor
    throws at finalization — see that function for the SIGABRT it prevents.
    """
    native_agent, native_log_consumer = _start_native_agent(cfg, server_info)
    try:
        return Agent(native_agent, cfg, server_info, native_log_consumer)
    except Exception:
        try:
            native_agent.shutdown()
        except Exception:  # noqa: BLE001
            _log.debug("native agent cleanup after failed init failed",
                       exc_info=True)
        if native_log_consumer is not None:
            native_log_consumer.stop()
        raise


def _server_info_or_default(server_info: Optional[str]) -> str:
    return str(server_info or "") or _DEFAULT_SERVER_INFO


def _command_line_args() -> list[str]:
    return [str(arg) for arg in getattr(sys, "argv", [])]


def _loaded_dynamic_library_packages() -> list[str]:
    # Intersecting loaded top-level names with packages_distributions() yields
    # exactly the pip-installed ones: stdlib and app code are never keys there.
    distributions = _top_level_distributions()
    loaded = {name.partition(".")[0] for name in tuple(sys.modules)}
    loaded.discard(_SELF_TOP_LEVEL_PACKAGE)
    return [
        _package_with_version(name, distributions)
        for name in sorted(loaded & distributions.keys())
    ]


def _package_with_version(
    top_level: str,
    top_level_distributions: Mapping[str, list[str]],
) -> str:
    candidates = top_level_distributions.get(top_level, ())
    for distribution in sorted(candidates, key=str.lower):
        try:
            version = importlib_metadata.version(distribution)
        except Exception:  # noqa: BLE001
            # A metadata-lookup failure (missing dist, corrupted ``.dist-info``)
            # degrades only the version label, never the whole agent.
            continue
        if version:
            return f"{top_level}=={version}"
    return top_level


# packages_distributions() reads the metadata of every installed distribution
# (tens to hundreds of ms in fat venvs) and the result is fixed for the
# process image — cache it so prefork workers (which re-run the native start
# in _after_fork_reinit, including on gunicorn max_requests recycling) inherit
# the master's scan through the forked globals instead of re-paying it.
_top_level_distributions_cache: Optional[Mapping[str, list[str]]] = None


def _top_level_distributions() -> Mapping[str, list[str]]:
    global _top_level_distributions_cache
    if _top_level_distributions_cache is None:
        try:
            _top_level_distributions_cache = (
                importlib_metadata.packages_distributions())
        except Exception:  # noqa: BLE001
            return {}
    return _top_level_distributions_cache


def _register_atexit_shutdown() -> None:
    """Ensure ``shutdown()`` runs during normal interpreter teardown.

    Without this hook the native ``shared_ptr<AgentImpl>`` is destroyed
    by ``__cxa_finalize_ranges`` *after* Python has gone away — by that
    point the agent's gRPC worker threads are still parked, and
    ``AgentImpl::~AgentImpl()`` throws while trying to tear them down.
    A destructor that leaks an exception calls ``std::terminate()`` →
    ``abort()`` → SIGABRT. Running ``shutdown()`` at atexit lets the
    native side drain queues and join threads *before* its statics are
    finalized, so the dtor finds the agent in a quiescent state.

    Note: this covers normal Python exits (return from ``main``,
    ``sys.exit``, uncaught exception). For SIGTERM — which by default
    kills the interpreter without running atexit — see
    :func:`_install_sigterm_exit_handler`.
    """
    global _atexit_registered
    if _atexit_registered:
        return
    try:
        atexit.register(shutdown)
        _atexit_registered = True
    except Exception:  # noqa: BLE001
        _log.debug("atexit.register(shutdown) failed", exc_info=True)


def _install_sigterm_exit_handler() -> None:
    """Install a SIGTERM handler that drains the native agent, then lets the
    signal run its normal course without changing the host's exit semantics.

    SIGTERM's default disposition skips atexit, so without this the native
    gRPC threads are torn down abruptly (see :func:`_register_atexit_shutdown`).
    Constraints: chains to a callable Python handler present at install time,
    takes ownership only from SIG_DFL, and backs off from anything else —
    SIG_IGN is a deliberate user choice, and ``None`` means a C-installed
    handler (uWSGI/mod_wsgi reload paths) that ``signal.signal()`` would
    clobber irrecoverably. One-shot; main-thread only; failures are swallowed.
    """
    global _sigterm_handler_installed, _previous_sigterm_handler
    if _sigterm_handler_installed:
        return
    try:
        current = signal.getsignal(signal.SIGTERM)
        if current is signal.SIG_DFL or callable(current):
            # SIG_DFL is not callable, so there's nothing to chain in that case.
            _previous_sigterm_handler = current if callable(current) else None
            signal.signal(signal.SIGTERM, _sigterm_exit_handler)
        else:
            _log.debug("SIGTERM disposition %r cannot be chained safely; "
                       "skipping pinpoint's shutdown hook", current)
        _sigterm_handler_installed = True  # don't re-check next init
    except (ValueError, OSError, RuntimeError):
        # ValueError: non-main thread. OSError/RuntimeError: platform restriction.
        _log.debug("SIGTERM handler install failed", exc_info=True)


def _sigterm_exit_handler(signum, frame):
    """Drain the native agent on SIGTERM, chain any prior handler, then let
    the signal terminate the process with its normal semantics.

    Module-level (not a closure) so identity comparisons in tests are stable.
    Order: drain via ``shutdown()`` first; chain to the captured prior handler
    if any; otherwise restore SIG_DFL and re-raise so the process dies with the
    normal ``128 + signum`` status, which reports failure to supervisors and
    can't be swallowed by a broad ``except`` in the app's main loop.
    """
    try:
        shutdown()
    except Exception:  # noqa: BLE001
        _log.debug("shutdown() during SIGTERM handling failed", exc_info=True)

    previous = _previous_sigterm_handler
    if callable(previous):
        # The user's handler owns the disposition from here.
        previous(signum, frame)
        return

    # No prior handler: reproduce SIG_DFL's exit semantics, plus a clean drain.
    try:
        signal.signal(signum, signal.SIG_DFL)
    except (ValueError, OSError, RuntimeError):
        # Off the main thread / platform restriction: raise_signal below still
        # terminates correctly if SIG_DFL is active.
        _log.debug("could not restore SIG_DFL for re-raise", exc_info=True)
    signal.raise_signal(signum)


def _register_fork_hook() -> None:
    """Install an ``os.register_at_fork`` after-in-child hook exactly once.

    Prefork servers (gunicorn ``--preload``, uWSGI, ``multiprocessing`` with
    the ``fork`` start method) may call :func:`init` in a master and then
    ``fork()`` worker processes. In ``prefork=True`` mode init stores only
    Python configuration, so the hook makes the worker's first native call by
    invoking ``StartAgent()``. If a live agent was inherited from a warm
    master, the hook detaches it without touching inherited native mutexes and
    degrades the child to a no-op; gRPC cannot be initialized again there.

    One-shot, mirroring the atexit/SIGTERM registration: the hook is
    process-wide and fires on *every* fork, so registering once is enough.

    ``os.register_at_fork`` only exists on fork-capable platforms and only
    affects ``fork()``-based process creation — the ``spawn``/``forkserver``
    start methods launch a fresh interpreter that re-runs ``init()`` on its
    own, so no rebuild is needed (or possible) there.
    """
    global _fork_hook_registered
    if _fork_hook_registered:
        return
    register_at_fork = getattr(os, "register_at_fork", None)
    if register_at_fork is None:
        # Non-fork platform (e.g. Windows); nothing to guard against.
        _fork_hook_registered = True
        return
    try:
        register_at_fork(after_in_child=_after_fork_reinit)
        _fork_hook_registered = True
    except Exception:  # noqa: BLE001
        _log.debug("os.register_at_fork(after_in_child) failed", exc_info=True)


def _detach_inherited_span(binding) -> None:
    """Drop the native handle of the span that was current at fork time.

    Clearing the contextvar stops *lookups* from reaching that span, but the
    instrumentation frame that opened it is still on the forking thread's
    stack, and that stack is exactly what the child continues on: its
    ``finally`` calls ``end()`` on the object directly. That crosses into the
    inherited ``SpanImpl::EndSpan``, which unlinks from an active-span shard
    guarded by a ``std::mutex`` — and a mutex another thread held at the fork
    instant is inherited locked, with no thread left to unlock it, so the
    child blocks forever. ``Span.end()`` already treats a ``None`` handle as
    "flush nothing", so detaching here makes that call a clean no-op.

    Only the current span is reachable and only it matters: a span held by
    another thread has no thread left in the child to end it.

    Dropping the handle here may destroy the native span in the child; that is
    safe because the native active-span release is pid guarded and abandons the
    node instead of taking the inherited shard mutex.

    Covers the span the forking thread owns, not every live span — that would
    need a process-wide live-span registry, i.e. a write on the per-span hot
    path to defend an already-unsupported pattern (a raw ``os.fork()`` inside
    a traced request in a multithreaded process).
    """
    if binding is None:
        return
    try:
        binding.span._native = None
    except Exception:  # noqa: BLE001
        _log.debug("could not detach the inherited span", exc_info=True)


def _after_fork_reinit() -> None:
    """Start a pending worker agent, or disable a warm inherited agent.

    The module lock is replaced before any acquisition because it may have
    been held by a parent thread that no longer exists. Every failure is
    contained: tracing becomes a no-op instead of breaking the child process.
    """
    global _instance, _lock

    _lock = threading.RLock()
    # Before the early return below: the child inherited the parent's span-id
    # PRNG state, and that holds whether or not there is an agent to rebuild.
    _reseed_span_ids()
    # The forking thread's context still holds the parent's live span, and the
    # thread ident survives fork, so current_span() would take the same-thread
    # branch and hand the child a span whose native handle belongs to the
    # parent's agent (shut down or replaced below).
    _detach_inherited_span(_current_span.get())
    _current_span.set(None)
    old = _instance
    if old is None:
        # init() never ran in the parent; nothing to rebuild.
        return

    cfg = old.config
    server_info = _server_info_or_default(
        getattr(old, "_server_info", None))
    _instance = None

    if getattr(old, "_pending", False):
        try:
            _instance = _start_agent(cfg, server_info)
            _install_sigterm_exit_handler()
            _log.info(
                "pinpoint agent started after fork "
                "(pid=%s app=%s name=%s)",
                os.getpid(), cfg.application_name, cfg.agent_name,
            )
            return
        except Exception:  # noqa: BLE001
            _log.exception(
                "pinpoint agent startup after fork failed — disabling")
            _instance = _NullAgent(cfg)
            return

    old_native = getattr(old, "_native", None)
    if old_native is not None:
        # A warm-fork child has no copy of the parent's consumer thread. Make
        # the inherited C++ callback state inert with an atomic store before
        # detaching the native handle; never acquire an inherited Event/queue
        # synchronization primitive in the child.
        old._abandon_native_log_after_fork()
        # Do not call native shutdown in the child. Teardown is pid guarded,
        # but Shutdown() first takes the inherited global-agent mutex, which a
        # vanished parent thread may have held at the fork instant. The native
        # global holder deliberately stays leaked and inert in this unsupported
        # child, while all Python paths switch to _NullAgent and never touch
        # the handle again.
        old._enabled = False
        old._shutdown = True
        old._native = None
        _log.warning(
            "pinpoint: process %d inherited an already-started agent; "
            "tracing is disabled because gRPC cannot survive fork(). "
            "Initialize in each worker, or use prefork=True / "
            "PINPOINT_PY_PREFORK=1 in the master.",
            os.getpid(),
        )

    _instance = _NullAgent(cfg)


def get_agent() -> Optional[Agent]:
    """Return the current agent instance, or None if `init()` wasn't called."""
    return _instance


def shutdown() -> None:
    """Shut down the process-wide agent. Idempotent."""
    global _instance
    with _lock:
        if _instance is None:
            return
        try:
            _instance.shutdown()
        finally:
            _instance = None


class _NullAgent(Agent):  # type: ignore[misc]
    """Fallback when native init fails. Everything is a no-op.

    With ``pending=True`` it is the Python-only prefork-master placeholder: the
    master must make no native agent API call before forking, so the fork hook
    checks the flag and performs that worker's first ``StartAgent()`` call.
    """

    def __init__(self, config: Config, server_info: Optional[str] = None,
                 pending: bool = False):  # noqa: D401
        # Bypass Agent.__init__ which requires a native agent.
        object.__setattr__(self, "_native", None)
        object.__setattr__(self, "_native_log_consumer", None)
        object.__setattr__(self, "_config", config)
        object.__setattr__(self, "_shutdown", True)
        object.__setattr__(self, "_shutdown_lock", threading.RLock())
        object.__setattr__(self, "_enabled", False)
        object.__setattr__(self, "_server_info", server_info)
        object.__setattr__(self, "_pending", pending)

    @property
    def enabled(self) -> bool:  # type: ignore[override]
        return False

    def new_span(self, operation: str, rpc_point: str,
                 headers: Optional[Mapping[str, str]] = None,
                 method: str = "") -> Span:
        return _NullSpan()

    def shutdown(self) -> None:
        return None


class _NullSpan(Span):  # type: ignore[misc]
    """Do-nothing span.

    Handed out wherever a span-shaped object must exist but nothing may be
    recorded: the :class:`_NullAgent` fallback when native init failed, the
    detached context views given to foreign threads, the async children of
    ended or unsampled spans, and every request the native side answered with
    a noop span — a url/method excluded by the HTTP filters, a disabled agent,
    a failed admission (``Agent.new_span`` recognizes those by their span id of
    0). It owns no native handle, so every method — including ``end()`` — is a
    pure no-op that never crosses the pybind11 boundary.

    Transactions the native agent *decided* not to sample are not this class:
    an unsampled span registers an active span, keeps a URL-stat entry and
    propagates ``s0``, so it owns a native lifetime and is wrapped in
    :class:`UnSampledSpan` instead.
    """

    def __init__(self) -> None:
        # Constructed on no-op fast paths, so init only the fields the inherited
        # __enter__/__exit__ and outside readers (propagator, context.py, the
        # URL-stat gates) actually touch. _tokens must exist for __enter__.
        self._native = None
        self._ended = False
        self._tokens = []
        self._collect_url_stat = False
        self._url_stat = None
        # Read by _fork_for_async_task's timeout callback, which cannot know it
        # was handed a null span (new_async_span returns one for an overflowed
        # or already-ended parent); without it the timer raises into the event
        # loop instead of reaping.
        self._active_events: list = []

    @property
    def trace_id(self) -> str:  # type: ignore[override]
        return ""

    @property
    def span_id(self) -> int:  # type: ignore[override]
        return 0

    @property
    def span_id_str(self) -> str:  # type: ignore[override]
        return "0"

    @property
    def sampled(self) -> bool:  # type: ignore[override]
        return False

    def _noop(self, *_a, **_kw):
        """Every annotator: accept anything, record nothing, stay chainable.

        Must stay a no-op rather than inheriting ``Span``'s buffering
        versions — ``UnSampledSpan`` subclasses this and carries the
        high-volume unsampled path, which owns no ``_annotations`` buffer to
        append to and nothing to flush it.
        """
        return self

    # Every Span annotator, annotate_long_iibbs included — Span's version
    # would touch the absent _annotations slot.
    set_service_type = set_remote_address = set_end_point = \
        set_acceptor_host = set_status_code = set_error = set_url_stat = \
        set_logging = annotate_int = annotate_long = annotate_string = \
        annotate_string_string = annotate_long_iibbs = _noop

    def new_span_event(self, operation, service_type=None):  # type: ignore[override]
        return _NULL_SPAN_EVENT

    def new_async_span(self, operation):  # type: ignore[override]
        return _NullSpan()

    def inject_context_items(self):  # type: ignore[override]
        # Nothing is propagated — downstream keeps making its own sampling
        # decision.
        return ()

    def _fork_for_async_task(self, task):  # type: ignore[override]
        # A null span has no native event stack to protect, so a child task can
        # share it. Skips the linked async span, lifetime timer, and done callback
        # that sampled Span.fork allocates per task — all no-ops here anyway.
        return self

    def _detached_context_span(self):  # type: ignore[override]
        # Records nothing either, but give each binding its own instance so
        # foreign threads never share _tokens.
        return _NullSpan()

    def end(self) -> None:  # type: ignore[override]
        return None


class UnSampledSpan(_NullSpan):  # type: ignore[misc]
    """Span for a transaction the native agent decided not to sample. A
    request the native side refused outright (filtered url/method, disabled
    agent) gets a noop span instead, which :meth:`Agent.new_span` wraps in
    :class:`_NullSpan`, not here.

    Profiling-data methods inherit the :class:`_NullSpan` no-ops so unsampled
    annotation calls never cross the pybind11 boundary. ``set_error()`` on the
    span is the sole exception: it retains only ``(name, message)`` for native
    policy evaluation at end. Span events are the shared
    :data:`_NULL_SPAN_EVENT`, so an error recorded on an *event* is dropped.
    Unlike a pure null span, an unsampled transaction owns a native lifetime:

    * ``end()`` always reaches native, releasing the active-span
      registration and recording the response-time stat taken at creation;
    * ``set_url_stat()`` is cached (only while HTTP URL-stat collection is
      enabled) and flushed through the native URL-stat end helper —
      unsampled requests are the majority when sampling is on, so dropping
      them would skew URL stats;
    * ``span_id`` is the real native span id; ``trace_id`` stays ``""``;
    * :func:`pinpoint.propagator.inject_items` propagates
      ``Pinpoint-Sampled: s0`` downstream, built here without a native call.
    """

    def __init__(self, native_span: "_native.Span",
                 collect_url_stat: bool = True,
                 span_id: Optional[int] = None) -> None:
        # Runs on every unsampled request — see _NullSpan.__init__ on the minimal
        # field set. Adds the native handle, the lock serializing racing
        # end/inject, and the span identity captured at creation.
        self._native = native_span
        # _thread.RLock directly, as the sampled Span does — this runs per
        # unsampled request, the majority path under sampling.
        self._native_lock = _thread.RLock()
        self._ended = False
        self._tokens = []
        self._collect_url_stat = bool(collect_url_stat)
        self._span_id = span_id
        self._span_id_str = None
        self._url_stat = None
        # Lazy: a normal unsampled request still makes only its end call
        # and allocates no error list.
        self._error_verdicts = None

    # An unsampled span has a real span id, so reuse the sampled Span
    # properties (captured at creation) instead of _NullSpan's constant 0.
    span_id = Span.span_id
    span_id_str = Span.span_id_str

    def _record_error_verdict(self, name: str, message: str) -> None:
        with self._native_lock:
            native = self._native
            if self._ended or native is None or not self._span_id:
                return
            errors = self._error_verdicts
            if errors is not None and len(errors) >= _MAX_ERROR_VERDICTS:
                # Keep memory bounded without letting a run of ignored errors
                # hide a later non-ignored one. This pathological overflow is
                # the only unsampled error path that crosses before span end.
                try:
                    native.mark_error(name, message)
                except Exception:  # noqa: BLE001
                    _log.debug("native unsampled error verdict failed",
                               exc_info=True)
                return
            if errors is None:
                self._error_verdicts = [(name, message)]
            else:
                errors.append((name, message))

    def set_error(self, error_or_name, message=None,
                  mark_error=True):  # type: ignore[override]
        # An unsampled span records no profiling data, so the verdict is the
        # only thing set_error does here: mark_error=False leaves nothing.
        if self._ended or not mark_error or _python_ignored(error_or_name):
            return self
        name, msg = _error_name_message(error_or_name, message)
        self._record_error_verdict(name, msg)
        return self

    def set_url_stat(self, url_pattern, method, status_code):  # type: ignore[override]
        if self._native is not None and self._collect_url_stat:
            self._url_stat = (str(url_pattern) or "/NULL", str(method), int(status_code))
        return self

    def inject_context_items(self):  # type: ignore[override]
        # Only the drop marker, so a downstream agent skips the transaction
        # too. The span-id probe guards a wrapper built around a native *noop*
        # span: Agent.new_span hands those out as _NullSpan, but a test double
        # or a direct construction can still land here, and a noop span must
        # propagate nothing at all. Span id 0 is the probe — a genuinely
        # unsampled span carries a real one.
        with self._native_lock:
            if self._ended or self._native is None:
                return ()
        if not self.span_id:
            return ()
        return ((HEADER_SAMPLED, "s0"),)

    def end(self) -> None:  # type: ignore[override]
        with self._native_lock:
            if self._ended:
                return
            self._ended = True
            native = self._native
            self._native = None
            url_stat = self._url_stat
            self._url_stat = None
            error_verdicts = self._error_verdicts or ()
            self._error_verdicts = None
            if native is None:
                return
            url_pattern, method, status_code = url_stat or ("", "", 0)
            try:
                native.end_span(
                    url_pattern, method, int(status_code), error_verdicts)
            except Exception:  # noqa: BLE001
                _log.debug("end_span failed", exc_info=True)


class _NullSpanEvent:
    """No-op SpanEvent used for pure-noop transactions.

    Stateless and safe to share as a module-level singleton — every annotator
    is an explicit ``return self``, covering the whole
    :class:`pinpoint.tracer.SpanEvent` surface."""

    __slots__ = ()

    def _noop(self, *_a, **_kw):
        """Every annotator: accept anything, record nothing, stay chainable."""
        return self

    set_service_type = set_operation_name = set_destination = \
        set_end_point = set_error = set_sql_query = annotate_int = \
        annotate_long = annotate_string = annotate_string_string = _noop

    # ---- lifecycle / context-manager protocol ------------------------------
    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return None

    def end(self) -> None:
        return None


# Shared by _NullSpan.new_span_event() so the unsampled path allocates
# nothing. Stateless, with a no-op __exit__.
_NULL_SPAN_EVENT = _NullSpanEvent()
