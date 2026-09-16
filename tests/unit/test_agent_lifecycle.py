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

"""Agent process lifecycle — init / shutdown / atexit registration.

``atexit.register(shutdown)`` is what keeps teardown from aborting the
process: without it the native ``shared_ptr<AgentImpl>`` is destroyed by the
C runtime ``__cxa_finalize`` step *after* Python has gone away, and the agent
destructor throws while joining gRPC worker threads → ``std::terminate()`` →
``abort()``.
"""

from __future__ import annotations

import gc
import os
import signal
import subprocess
import sys
import textwrap

import pytest

from pinpoint import agent as agent_mod
from pinpoint.config import Config
from pinpoint.service_type import APP_TYPE_PYTHON


@pytest.fixture(autouse=True)
def _reset_agent_module(monkeypatch):
    """Each test starts with no agent and no prior atexit/signal handler
    registration. Restore the SIGTERM handler too so we don't poison
    later tests that rely on the default disposition."""
    saved_sigterm = signal.getsignal(signal.SIGTERM)
    monkeypatch.setattr(agent_mod, "_instance", None)
    monkeypatch.setattr(agent_mod, "_atexit_registered", False)
    monkeypatch.setattr(agent_mod, "_sigterm_handler_installed", False)
    monkeypatch.setattr(agent_mod, "_previous_sigterm_handler", None)
    monkeypatch.setattr(agent_mod, "_fork_hook_registered", False)
    _FakeLogConsumer.events = []
    yield
    try:
        signal.signal(signal.SIGTERM, saved_sigterm)
    except (ValueError, OSError, RuntimeError):
        pass


class _RecordingNativeAgent:
    """Fake native agent recording shutdown into a shared log."""

    def __init__(self, calls, tag, on_shutdown=None):
        self._calls = calls
        self._tag = tag
        self._on_shutdown = on_shutdown

    def shutdown(self):
        self._calls.append((self._tag, "shutdown"))
        if self._on_shutdown is not None:
            self._on_shutdown()


class _FakeNative:
    """Stand-in ``_native`` module: records every StartAgent payload in
    ``starts`` and hands back an agent that only knows how to shut down
    (logged into ``calls`` as ``("child", "shutdown")``)."""

    def __init__(self, on_shutdown=None):
        self.starts = []
        self.calls = []
        self.created = []
        self.bridges = []
        self._on_shutdown = on_shutdown

    def NativeLogBridge(self, capacity):
        bridge = type("FakeBridge", (), {"capacity": capacity})()
        self.bridges.append(bridge)
        return bridge

    def start_agent(self, *args):
        self.starts.append(args)
        native = _RecordingNativeAgent(self.calls, "child", self._on_shutdown)
        self.created.append(native)
        return native


def _stub_native(monkeypatch, on_shutdown=None, args=(), libs=()):
    """Install a :class:`_FakeNative` and pin the process-metadata helpers, so
    StartAgent payload assertions never depend on this pytest process."""
    fake = _FakeNative(on_shutdown)
    monkeypatch.setattr(agent_mod, "_native", fake)
    monkeypatch.setattr(agent_mod, "_command_line_args", lambda: list(args))
    monkeypatch.setattr(agent_mod, "_loaded_dynamic_library_packages",
                        lambda: list(libs))
    return fake


class _FakeLogConsumer:
    events = []

    def __init__(self, bridge):
        self.bridge = bridge
        self.dropped = 0

    def start(self):
        self.events.append(("consumer", "start", self.bridge.capacity))

    def stop(self):
        self.events.append(("consumer", "stop", self.bridge.capacity))

    def abandon_after_fork(self):
        self.events.append(("consumer", "abandon", self.bridge.capacity))


def test_register_atexit_shutdown_hooks_module_shutdown(monkeypatch):
    """``_register_atexit_shutdown`` must hand ``atexit`` the
    module-level ``shutdown`` callable so the native agent gets a chance
    to quiesce before its static dtor runs at ``__cxa_finalize``."""
    registered = []
    monkeypatch.setattr(agent_mod.atexit, "register",
                        lambda fn, *_a, **_k: registered.append(fn))

    agent_mod._register_atexit_shutdown()
    assert registered == [agent_mod.shutdown]
    assert agent_mod._atexit_registered is True


def test_register_atexit_shutdown_is_idempotent(monkeypatch):
    """Multiple calls must register exactly once. The flag prevents
    queuing up duplicate shutdown calls across init/shutdown cycles."""
    registered = []
    monkeypatch.setattr(agent_mod.atexit, "register",
                        lambda fn, *_a, **_k: registered.append(fn))

    agent_mod._register_atexit_shutdown()
    agent_mod._register_atexit_shutdown()
    agent_mod._register_atexit_shutdown()
    assert registered == [agent_mod.shutdown]


def test_atexit_handler_drives_native_shutdown(monkeypatch):
    """The function atexit holds is module-level ``shutdown``, which
    must drive the native ``shutdown()`` on whatever singleton is live
    at fire time — that's the C++ object whose worker threads we need
    to quiesce before its statics are finalized."""

    class _FakeNative:
        def __init__(self):
            self.shut = False

        def shutdown(self):
            self.shut = True

    fake_native = _FakeNative()
    cfg = Config(application_name="atexit-fake", agent_name="x")
    monkeypatch.setattr(agent_mod, "_instance",
                        agent_mod.Agent(fake_native, cfg))

    # Simulate the atexit firing.
    agent_mod.shutdown()
    assert fake_native.shut is True
    assert agent_mod._instance is None  # cleared by shutdown()


def test_agent_enabled_caches_after_native_flips_true():
    class _FakeNative:
        def __init__(self):
            self.calls = 0
            self.results = [False, False, True]

        def enable(self):
            self.calls += 1
            return self.results.pop(0) if self.results else True

    native = _FakeNative()
    agent = agent_mod.Agent(
        native,
        Config(application_name="enabled-cache", agent_name="x"),
    )

    assert agent.enabled is False
    assert agent.enabled is False
    assert native.calls == 2

    assert agent.enabled is True
    assert agent.enabled is True
    assert native.calls == 3


def test_agent_enabled_config_disabled_skips_native_call():
    class _FakeNative:
        calls = 0

        def enable(self):
            self.calls += 1
            return True

    native = _FakeNative()
    agent = agent_mod.Agent(
        native,
        Config(application_name="disabled", agent_name="x", enabled=False),
    )

    assert agent.enabled is False
    assert native.calls == 0


def test_register_swallows_atexit_failure(monkeypatch):
    """If ``atexit.register`` itself raises (eg. interpreter is mid-
    shutdown), the helper must NOT crash — agent creation is the
    priority over teardown safety."""
    def _bad_register(*_a, **_k):
        raise RuntimeError("interpreter shutting down")

    monkeypatch.setattr(agent_mod.atexit, "register", _bad_register)

    # Must not raise.
    agent_mod._register_atexit_shutdown()
    # Flag stays False because the registration didn't succeed, so a
    # later retry (eg. next init in tests) is possible.
    assert agent_mod._atexit_registered is False


def test_init_calls_register_atexit_shutdown_on_success(monkeypatch):
    """End-to-end: when ``init()`` takes the success path (real native
    agent created), it must invoke the atexit registrar so subsequent
    teardown is safe. We monkey-patch ``_native`` to force the success
    branch without depending on the C++ build."""
    registered = []

    _stub_native(monkeypatch)
    monkeypatch.setattr(agent_mod.atexit, "register",
                        lambda fn, *_a, **_k: registered.append(fn))

    agent_mod.init(application_name="init-success", agent_name="x")
    assert registered == [agent_mod.shutdown]


def test_init_survives_invalid_log_level(monkeypatch):
    """A bad log level (config typo like ``log_level="garbage"``) must not
    crash ``init()`` — ``_configure_log`` sits outside the native try block,
    and ``setLevel`` would otherwise raise ValueError. The agent still comes
    up on the normal (non-null) path."""
    _stub_native(monkeypatch)

    # Must not raise.
    agent = agent_mod.init(
        application_name="bad-log-level", agent_name="x", log_level="garbage",
    )
    assert not isinstance(agent, agent_mod._NullAgent)


def test_init_continues_when_configure_log_raises(monkeypatch):
    """Even if ``_configure_log`` itself blows up for some unexpected
    reason, init must swallow it and continue to native agent creation."""
    def _boom(_level):
        raise RuntimeError("logging subsystem exploded")

    monkeypatch.setattr(agent_mod, "_configure_log", _boom)

    _stub_native(monkeypatch)

    agent = agent_mod.init(application_name="log-boom", agent_name="x")
    assert not isinstance(agent, agent_mod._NullAgent)


def test_init_passes_agent_options_to_native(monkeypatch):
    """``init`` should send config sources and server metadata in the single
    StartAgent options payload."""
    fake = _stub_native(monkeypatch, args=["app.py", "--port=8080"],
                        libs=["grpc", "numpy"])

    agent_mod.init(
        application_name="metadata-app",
        agent_name="x",
        config_file_path="/etc/pinpoint.yaml",
        server_info="gunicorn",
    )

    (payload,) = fake.starts
    config_file, yaml, prefix, app_type, server_info, args, libs = payload
    assert config_file == "/etc/pinpoint.yaml"
    assert 'ApplicationName: "metadata-app"' in yaml
    assert prefix == agent_mod.ENV_VAR_PREFIX
    assert app_type == APP_TYPE_PYTHON
    assert server_info == "gunicorn"
    assert args == ["app.py", "--port=8080"]
    assert libs == ["grpc", "numpy"]


def test_init_defaults_server_info_to_python_application(monkeypatch):
    fake = _stub_native(monkeypatch)

    agent_mod.init(application_name="metadata-app", agent_name="x")

    assert [s[3:] for s in fake.starts] == [
        (APP_TYPE_PYTHON, "Python Application", [], [])]


def test_native_log_bridge_is_opt_in_and_shutdown_follows_native(monkeypatch):
    order = []
    fake = _stub_native(monkeypatch, on_shutdown=lambda: order.append("native"))

    class OrderedConsumer(_FakeLogConsumer):
        def stop(self):
            order.append("consumer")
            super().stop()

    monkeypatch.setattr(agent_mod, "NativeLogConsumer", OrderedConsumer)
    agent = agent_mod.init(
        application_name="logs", native_log_to_python=True,
        native_log_queue_size=17)

    assert len(fake.bridges) == 1
    assert len(fake.starts[0]) == 8
    assert fake.starts[0][-1] is fake.bridges[0]
    assert _FakeLogConsumer.events == [("consumer", "start", 17)]

    agent_mod.shutdown()
    assert order == ["native", "consumer"]


def test_native_log_bridge_default_path_allocates_nothing(monkeypatch):
    fake = _stub_native(monkeypatch)
    agent_mod.init(application_name="no-bridge")
    assert fake.bridges == []
    assert len(fake.starts[0]) == 7


def test_native_log_bridge_setup_failure_keeps_tracing_on_builtin_sink(monkeypatch):
    fake = _stub_native(monkeypatch)

    def fail_bridge(_capacity):
        raise MemoryError("queue allocation failed")

    fake.NativeLogBridge = fail_bridge
    agent = agent_mod.init(
        application_name="bridge-fallback", native_log_to_python=True)

    assert not isinstance(agent, agent_mod._NullAgent)
    assert len(fake.starts) == 1
    assert len(fake.starts[0]) == 7


def test_reinit_creates_a_fresh_native_log_bridge(monkeypatch):
    fake = _stub_native(monkeypatch)
    monkeypatch.setattr(agent_mod, "NativeLogConsumer", _FakeLogConsumer)
    first = agent_mod.init(application_name="logs", native_log_to_python=True)
    first_bridge = fake.bridges[-1]
    agent_mod.shutdown()
    second = agent_mod.init(application_name="logs", native_log_to_python=True)
    assert second is not first
    assert fake.bridges[-1] is not first_bridge
    agent_mod.shutdown()


def test_agent_gc_shuts_native_before_log_consumer():
    events = []
    native = _RecordingNativeAgent(events, "native")

    class Consumer:
        dropped = 0

        def stop(self):
            events.append(("consumer", "stop"))

    agent = agent_mod.Agent(
        native, Config(application_name="gc"), native_log_consumer=Consumer())
    del agent
    gc.collect()

    assert events == [("native", "shutdown"), ("consumer", "stop")]


def test_startup_failure_stops_bridge_consumer_and_keeps_null_agent(monkeypatch):
    fake = _stub_native(monkeypatch)
    monkeypatch.setattr(agent_mod, "NativeLogConsumer", _FakeLogConsumer)

    def fail_start(*args):
        fake.starts.append(args)
        raise RuntimeError("original startup failure")

    fake.start_agent = fail_start
    result = agent_mod.init(
        application_name="bad", native_log_to_python=True,
        native_log_queue_size=23)

    assert isinstance(result, agent_mod._NullAgent)
    assert _FakeLogConsumer.events == [
        ("consumer", "start", 23),
        ("consumer", "stop", 23),
    ]


def test_init_passes_native_env_var_prefix_to_start_agent(monkeypatch):
    """The native agent reads environment variables while StartAgent builds
    its initial configuration, so the options payload must carry PINPOINT_PY."""
    fake = _stub_native(monkeypatch)

    agent_mod.init(application_name="prefix-app", agent_name="x")

    assert [s[2] for s in fake.starts] == [agent_mod.ENV_VAR_PREFIX]


def test_command_line_args_returns_sys_argv_copy(monkeypatch):
    argv = ["python", "-m", "demo", "--debug"]
    monkeypatch.setattr(agent_mod.sys, "argv", argv)

    result = agent_mod._command_line_args()

    assert result == argv
    assert result is not argv


def test_loaded_dynamic_library_packages_returns_top_level_names_with_versions(monkeypatch):
    fake_modules = {
        "alpha._native": type("M", (), {"__file__": "/tmp/alpha/_native.so"})(),
        "alpha.extra": type("M", (), {"__file__": "/tmp/alpha/extra.py"})(),
        "beta": type("M", (), {"__file__": "/tmp/beta.py"})(),
        "os": type("M", (), {"__file__": "/stdlib/os.py"})(),
        "pinpoint._native": type("M", (), {"__file__": "/tmp/pinpoint/_native.so"})(),
        "_private": type("M", (), {"__file__": "/tmp/_private.py"})(),
        "builtins": type("M", (), {
            "__spec__": type("S", (), {"origin": "built-in"})(),
        })(),
        "missing": type("M", (), {})(),
    }
    monkeypatch.setattr(agent_mod.sys, "modules", fake_modules)
    # The distributions scan is cached process-wide (prefork workers reuse the
    # master's); reset it so this test's fake is actually consulted.
    monkeypatch.setattr(agent_mod, "_top_level_distributions_cache", None)
    metadata_scans = 0

    def _packages_distributions():
        nonlocal metadata_scans
        metadata_scans += 1
        return {
            "alpha": ["alpha-dist"],
            "beta": ["missing-dist"],
        }

    monkeypatch.setattr(
        agent_mod.importlib_metadata,
        "packages_distributions",
        _packages_distributions,
    )

    def _version(distribution):
        if distribution == "alpha-dist":
            return "1.2.3"
        raise agent_mod.importlib_metadata.PackageNotFoundError

    monkeypatch.setattr(agent_mod.importlib_metadata, "version", _version)

    assert agent_mod._loaded_dynamic_library_packages() == ["alpha==1.2.3", "beta"]
    assert metadata_scans == 1


def test_package_with_version_degrades_on_corrupt_metadata(monkeypatch):
    """A corrupted ``.dist-info`` can make ``version()`` raise something other
    than PackageNotFoundError (e.g. ValueError). It must degrade to the bare
    top-level label, not propagate and disable the whole agent."""
    def _version(distribution):
        raise ValueError("corrupt METADATA")

    monkeypatch.setattr(agent_mod.importlib_metadata, "version", _version)

    assert agent_mod._package_with_version(
        "alpha", {"alpha": ["alpha-dist"]},
    ) == "alpha"


# ---------------------------------------------------------------------------
# SIGTERM handler
# ---------------------------------------------------------------------------

def test_install_sigterm_handler_replaces_sig_dfl(monkeypatch):
    """When SIGTERM is at its default disposition, the agent installs its
    drain-then-re-raise handler and records that there is no prior handler
    to chain to."""
    # Force SIG_DFL as the starting handler.
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    agent_mod._install_sigterm_exit_handler()
    installed = signal.getsignal(signal.SIGTERM)
    assert installed is agent_mod._sigterm_exit_handler
    assert agent_mod._sigterm_handler_installed is True
    # SIG_DFL is not callable, so there's nothing to chain.
    assert agent_mod._previous_sigterm_handler is None


def test_install_sigterm_handler_chains_user_handler(monkeypatch):
    """A pre-existing user handler must be captured and installed-over (not
    left in place), so our drain runs first and the user handler is chained
    from ours rather than skipped."""
    def _user_handler(_signum, _frame):
        pass

    signal.signal(signal.SIGTERM, _user_handler)

    agent_mod._install_sigterm_exit_handler()
    # Our handler is now in place, wrapping the user's.
    assert signal.getsignal(signal.SIGTERM) is agent_mod._sigterm_exit_handler
    assert agent_mod._previous_sigterm_handler is _user_handler
    assert agent_mod._sigterm_handler_installed is True


def test_install_sigterm_handler_leaves_sig_ign_alone(monkeypatch):
    """SIG_IGN is an explicit user choice (ignore SIGTERM entirely).
    Replacing it would silently re-enable termination — never do that."""
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    agent_mod._install_sigterm_exit_handler()
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_IGN
    # Flag flips so we don't re-check on later inits, and nothing is chained.
    assert agent_mod._sigterm_handler_installed is True
    assert agent_mod._previous_sigterm_handler is None


def test_install_sigterm_handler_leaves_c_level_handler_alone(monkeypatch):
    """``signal.getsignal()`` returns ``None`` when SIGTERM was installed from
    C rather than Python — exactly what embedding servers like uWSGI and
    mod_wsgi do for graceful shutdown/reload. We can neither see nor chain such
    a handler, and ``signal.signal()`` would overwrite it irrecoverably, so we
    must back off and leave the host's disposition intact."""
    calls = []
    monkeypatch.setattr(agent_mod.signal, "getsignal", lambda _num: None)
    monkeypatch.setattr(agent_mod.signal, "signal",
                        lambda num, h: calls.append((num, h)))

    agent_mod._install_sigterm_exit_handler()

    # The C-level handler is left untouched: signal.signal() is never called.
    assert calls == []
    # One-shot flag still flips so we don't re-check (and clobber) on a later
    # init, and there is nothing callable to chain to.
    assert agent_mod._sigterm_handler_installed is True
    assert agent_mod._previous_sigterm_handler is None


def test_install_sigterm_handler_is_idempotent(monkeypatch):
    """Multiple calls should result in exactly one ``signal.signal``
    write so a user can later swap our handler out without us fighting
    them on a subsequent ``init()``."""
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    calls = []
    real_signal = signal.signal

    def _spy_signal(num, handler):
        calls.append((num, handler))
        return real_signal(num, handler)

    monkeypatch.setattr(agent_mod.signal, "signal", _spy_signal)

    agent_mod._install_sigterm_exit_handler()
    agent_mod._install_sigterm_exit_handler()
    agent_mod._install_sigterm_exit_handler()
    # Only the first call actually wrote a handler.
    assert len(calls) == 1
    assert calls[0] == (signal.SIGTERM, agent_mod._sigterm_exit_handler)


def test_sigterm_exit_handler_drains_then_reraises_default(monkeypatch):
    """With no prior handler, the handler drains the agent, restores SIG_DFL
    and re-raises the signal — no ``SystemExit`` that a broad ``except`` could
    swallow, and the process dies with the signal's own semantics."""
    events = []
    monkeypatch.setattr(agent_mod, "shutdown",
                        lambda: events.append("shutdown"))
    monkeypatch.setattr(agent_mod, "_previous_sigterm_handler", None)
    monkeypatch.setattr(agent_mod.signal, "signal",
                        lambda num, h: events.append(("restore", num, h)))
    monkeypatch.setattr(agent_mod.signal, "raise_signal",
                        lambda num: events.append(("raise", num)),
                        raising=False)

    agent_mod._sigterm_exit_handler(signal.SIGTERM, None)

    assert events == [
        "shutdown",
        ("restore", signal.SIGTERM, signal.SIG_DFL),
        ("raise", signal.SIGTERM),
    ]


def test_sigterm_exit_handler_drains_then_chains_prior_handler(monkeypatch):
    """With a prior handler, the agent is drained first and then the prior
    handler is invoked — which owns the disposition, so we do NOT re-raise."""
    events = []
    monkeypatch.setattr(agent_mod, "shutdown",
                        lambda: events.append("shutdown"))

    def _prev(signum, _frame):
        events.append(("prev", signum))

    monkeypatch.setattr(agent_mod, "_previous_sigterm_handler", _prev)
    monkeypatch.setattr(agent_mod.signal, "signal",
                        lambda *_a: events.append("restore"))
    monkeypatch.setattr(agent_mod.signal, "raise_signal",
                        lambda num: events.append(("raise", num)),
                        raising=False)

    agent_mod._sigterm_exit_handler(signal.SIGTERM, None)

    # Drained, chained, and NOT re-raised (no "restore"/"raise").
    assert events == ["shutdown", ("prev", signal.SIGTERM)]


def test_install_sigterm_handler_swallows_value_error(monkeypatch):
    """Off the main thread ``signal.signal`` raises ValueError. The
    helper must swallow it — losing SIGTERM safety is acceptable, but
    crashing agent init is not."""
    def _raise(*_a, **_k):
        raise ValueError("signal only works in main thread")

    monkeypatch.setattr(agent_mod.signal, "signal", _raise)
    monkeypatch.setattr(agent_mod.signal, "getsignal",
                        lambda _: signal.SIG_DFL)

    # Must not raise.
    agent_mod._install_sigterm_exit_handler()
    # Flag stays False because installation actually failed.
    assert agent_mod._sigterm_handler_installed is False


def test_init_installs_sigterm_handler_on_success(monkeypatch):
    """End-to-end: success path of ``init()`` installs both the atexit
    hook and the SIGTERM handler for an enabled agent."""
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    _stub_native(monkeypatch)

    agent_mod.init(application_name="init-sigterm", agent_name="x")
    assert signal.getsignal(signal.SIGTERM) is agent_mod._sigterm_exit_handler


def test_init_skips_sigterm_handler_when_disabled(monkeypatch):
    """A degraded agent (missing application_name → disabled) must not touch
    the host's SIGTERM disposition, even though native creation succeeds."""
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    _stub_native(monkeypatch)

    agent = agent_mod.init(agent_name="x")  # no application_name → disabled
    assert agent.config.enabled is False
    # SIGTERM left exactly as we found it.
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL
    assert agent_mod._sigterm_handler_installed is False


# --- Real signal delivery in a spawned subprocess -------------------------
#
# These prove the observable process behavior the unit tests above only
# approximate: correct exit status, non-swallowable termination, and that a
# pre-existing user handler still runs. They import pinpoint (hence _native),
# so they run under the dev.sh environment inherited by the subprocess.

def _run_sigterm_script(body: str) -> subprocess.CompletedProcess:
    script = textwrap.dedent(body)
    return subprocess.run([sys.executable, "-c", script],
                          capture_output=True, text=True, timeout=60)


def test_sigterm_default_disposition_exits_with_signal_semantics():
    """(a) With the default SIGTERM disposition, our handler drains and then
    the process terminates with the signal's normal status (returncode is the
    negated signal number → shells see 128+SIGTERM)."""
    proc = _run_sigterm_script(
        """
        import os, signal, sys
        from pinpoint import agent as agent_mod

        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        agent_mod._install_sigterm_exit_handler()
        os.kill(os.getpid(), signal.SIGTERM)
        # If we get here the signal was swallowed — fail loudly.
        sys.stdout.write("NOT_TERMINATED\\n"); sys.stdout.flush()
        os._exit(99)
        """
    )
    assert "NOT_TERMINATED" not in proc.stdout, proc.stdout
    assert proc.returncode == -signal.SIGTERM, (
        f"expected -{int(signal.SIGTERM)}, got {proc.returncode}: {proc.stderr}"
    )


def test_sigterm_not_swallowed_by_broad_except():
    """(b) A main loop with ``except BaseException`` must NOT prevent
    termination — re-raising the signal is OS-level, not a catchable
    ``SystemExit``."""
    proc = _run_sigterm_script(
        """
        import os, signal, sys
        from pinpoint import agent as agent_mod

        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        agent_mod._install_sigterm_exit_handler()
        try:
            os.kill(os.getpid(), signal.SIGTERM)
            while True:
                pass
        except BaseException:  # noqa: BLE001
            sys.stdout.write("SWALLOWED\\n"); sys.stdout.flush()
            os._exit(0)
        """
    )
    assert "SWALLOWED" not in proc.stdout, proc.stdout
    assert proc.returncode == -signal.SIGTERM, (
        f"expected -{int(signal.SIGTERM)}, got {proc.returncode}: {proc.stderr}"
    )


def test_sigterm_chains_preexisting_user_handler():
    """(c) A user's SIGTERM handler installed before init still runs — our
    handler drains first, then chains to theirs."""
    proc = _run_sigterm_script(
        """
        import os, signal, sys
        from pinpoint import agent as agent_mod

        def user_handler(signum, frame):
            sys.stdout.write("USER_HANDLER\\n"); sys.stdout.flush()
            os._exit(7)

        signal.signal(signal.SIGTERM, user_handler)
        agent_mod._install_sigterm_exit_handler()
        os.kill(os.getpid(), signal.SIGTERM)
        while True:
            pass
        """
    )
    assert "USER_HANDLER" in proc.stdout, f"{proc.stdout}\n{proc.stderr}"
    # The user handler owns the disposition — it exited with 7.
    assert proc.returncode == 7, f"{proc.returncode}: {proc.stderr}"


# ---------------------------------------------------------------------------
# Fork safety (os.register_at_fork after-in-child rebuild)
# ---------------------------------------------------------------------------

def test_after_fork_reinit_reseeds_span_ids(monkeypatch):
    """fork() does not reseed a PRNG, so sibling workers would otherwise hand
    out identical outbound child span ids.

    Runs before the "nothing to rebuild" early return: the child inherited the
    state either way.
    """
    from pinpoint import tracer as tracer_mod

    reseeded = []
    monkeypatch.setattr(tracer_mod._span_id_random, "seed",
                        lambda *a: reseeded.append(True))
    monkeypatch.setattr(agent_mod, "_instance", None)

    agent_mod._after_fork_reinit()
    assert reseeded == [True]


def test_after_fork_reinit_clears_inherited_current_span(monkeypatch):
    """The forking thread's ident survives fork, so the inherited binding would
    pass the same-thread check and hand the child the parent's live span."""
    from pinpoint import context as context_mod

    class _FakeSpan:
        _ended = False

    token = context_mod._current_span.set(
        context_mod._SpanBinding(_FakeSpan(), None))
    try:
        monkeypatch.setattr(agent_mod, "_instance", None)
        agent_mod._after_fork_reinit()
        assert context_mod.current_span() is None
    finally:
        context_mod._current_span.reset(token)


def test_span_ids_do_not_use_the_shared_random_module(monkeypatch):
    """The generator must be private: reseeding ``random`` itself would silently
    break an application that seeded it for reproducible behaviour."""
    import random

    from pinpoint import tracer as tracer_mod

    assert tracer_mod._span_id_random is not random
    # A caller-seeded global PRNG keeps its own sequence across span-id draws.
    random.seed(1234)
    expected = [random.getrandbits(64) for _ in range(3)]
    random.seed(1234)
    tracer_mod._generate_span_id()
    tracer_mod._generate_span_id()
    assert [random.getrandbits(64) for _ in range(3)] == expected


def test_after_fork_reinit_starts_agent_from_pending_master(monkeypatch):
    """A prefork master owns no native object; the child hook makes that
    process's first native call and installs the returned agent."""
    fake = _stub_native(monkeypatch, args=["app.py"])
    installed_sigterm = []
    monkeypatch.setattr(
        agent_mod, "_install_sigterm_exit_handler",
        lambda: installed_sigterm.append(True),
    )

    cfg = Config(application_name="forked", agent_name="workers",
                 collector_host="c",
                 prefork=True)
    parent = agent_mod._NullAgent(cfg, "gunicorn", pending=True)
    monkeypatch.setattr(agent_mod, "_instance", parent)
    old_lock = agent_mod._lock

    agent_mod._after_fork_reinit()

    (payload,) = fake.starts
    assert payload[2] == agent_mod.ENV_VAR_PREFIX
    assert payload[4] == "gunicorn"
    assert payload[5] == ["app.py"]
    assert fake.calls == []  # nothing was shut down
    assert installed_sigterm == [True]
    new_instance = agent_mod.get_agent()
    assert isinstance(new_instance, agent_mod.Agent)
    assert not isinstance(new_instance, agent_mod._NullAgent)
    assert new_instance is not parent
    assert new_instance._native is fake.created[-1]
    # The inherited lock may have belonged to a vanished parent thread.
    assert agent_mod._lock is not old_lock


def test_after_fork_reinit_degrades_child_of_warm_master(monkeypatch):
    """Warm master (default: init() started the agent before the fork): the
    child cannot be given working gRPC channels. Python detaches the inherited
    handle without touching native mutexes, then installs a no-op agent."""
    fake = _stub_native(monkeypatch)

    parent_native = _RecordingNativeAgent(fake.calls, "parent")
    cfg = Config(application_name="forked", agent_name="workers",
                 collector_host="c")
    parent = agent_mod.Agent(parent_native, cfg, "gunicorn")
    monkeypatch.setattr(agent_mod, "_instance", parent)

    agent_mod._after_fork_reinit()

    assert fake.calls == []
    assert fake.created == []
    assert isinstance(agent_mod.get_agent(), agent_mod._NullAgent)
    assert agent_mod.get_agent().enabled is False


def test_warm_fork_abandons_inherited_log_bridge_before_native_shutdown(monkeypatch):
    events = []
    native = _RecordingNativeAgent(events, "native")

    class ForkConsumer(_FakeLogConsumer):
        def abandon_after_fork(self):
            events.append(("bridge", "abandon"))
            super().abandon_after_fork()

        def stop(self):
            # Agent.shutdown calls this, but an abandoned consumer must itself
            # be a no-op in the real implementation.
            events.append(("bridge", "stop"))

    consumer = ForkConsumer(type("Bridge", (), {"capacity": 8})())
    parent = agent_mod.Agent(
        native, Config(application_name="forked", native_log_to_python=True),
        "gunicorn", consumer)
    monkeypatch.setattr(agent_mod, "_instance", parent)

    agent_mod._after_fork_reinit()

    assert events == [("bridge", "abandon")]


def test_after_fork_reinit_noop_when_never_initialized(monkeypatch):
    """If init() never ran in the parent, the child hook does nothing."""
    fake = _stub_native(monkeypatch)
    monkeypatch.setattr(agent_mod, "_instance", None)

    agent_mod._after_fork_reinit()

    assert agent_mod.get_agent() is None
    assert fake.calls == []


def test_after_fork_reinit_disabled_parent_degrades_to_null(monkeypatch):
    """A disabled parent has no native handle and stays disabled."""
    fake = _stub_native(monkeypatch)

    cfg = Config(application_name="off", agent_name="x", enabled=False)
    monkeypatch.setattr(agent_mod, "_instance", agent_mod._NullAgent(cfg))

    agent_mod._after_fork_reinit()

    assert isinstance(agent_mod.get_agent(), agent_mod._NullAgent)
    assert fake.calls == []


def test_after_fork_reinit_degrades_to_null_on_failure(monkeypatch):
    """A native failure during rebuild must not crash the child — the agent
    degrades to a no-op instead."""
    class _BoomNative(_FakeNative):
        def start_agent(self, *_a, **_k):
            raise RuntimeError("collector unreachable at fork")

    _stub_native(monkeypatch)
    monkeypatch.setattr(agent_mod, "_native", _BoomNative())

    cfg = Config(application_name="forked", agent_name="x",
                 collector_host="c", prefork=True)
    monkeypatch.setattr(
        agent_mod, "_instance", agent_mod._NullAgent(cfg, "srv", pending=True))

    agent_mod._after_fork_reinit()  # must not raise

    assert isinstance(agent_mod.get_agent(), agent_mod._NullAgent)


def test_register_fork_hook_registers_once(monkeypatch):
    registered = []
    monkeypatch.setattr(agent_mod.os, "register_at_fork",
                        lambda **kw: registered.append(kw))

    agent_mod._register_fork_hook()
    agent_mod._register_fork_hook()

    assert len(registered) == 1
    assert registered[0]["after_in_child"] is agent_mod._after_fork_reinit
    assert agent_mod._fork_hook_registered is True


def test_register_fork_hook_tolerates_missing_api(monkeypatch):
    """On platforms without os.register_at_fork (e.g. Windows) the helper is a
    no-op that still marks itself done so init() doesn't retry each call."""
    monkeypatch.delattr(agent_mod.os, "register_at_fork", raising=False)

    agent_mod._register_fork_hook()  # must not raise

    assert agent_mod._fork_hook_registered is True


def test_init_registers_fork_hook_on_success(monkeypatch):
    registered = []

    _stub_native(monkeypatch)
    monkeypatch.setattr(agent_mod.os, "register_at_fork",
                        lambda **kw: registered.append(kw))

    agent_mod.init(application_name="fork-hook", agent_name="x")

    assert len(registered) == 1
    assert registered[0]["after_in_child"] is agent_mod._after_fork_reinit


def test_init_prefork_makes_no_native_agent_call(monkeypatch):
    """prefork=True stores Python configuration only in the master."""
    fake = _stub_native(monkeypatch)

    agent = agent_mod.init(
        application_name="cold", agent_name="x", prefork=True)

    assert isinstance(agent, agent_mod._NullAgent)
    assert agent._pending is True
    assert fake.starts == []


def test_init_prefork_with_native_logging_creates_no_master_consumer(monkeypatch):
    fake = _stub_native(monkeypatch)
    monkeypatch.setattr(agent_mod, "NativeLogConsumer", _FakeLogConsumer)

    agent = agent_mod.init(
        application_name="cold", prefork=True, native_log_to_python=True)

    assert isinstance(agent, agent_mod._NullAgent)
    assert fake.starts == []
    assert fake.bridges == []
    assert _FakeLogConsumer.events == []


def test_init_default_calls_start_agent_once(monkeypatch):
    fake = _stub_native(monkeypatch)

    agent_mod.init(application_name="warm", agent_name="x")

    assert len(fake.starts) == 1


def test_init_disabled_makes_no_native_agent_call(monkeypatch):
    fake = _stub_native(monkeypatch)

    agent_mod.init(application_name="off", agent_name="x", enabled=False)

    assert fake.starts == []


def test_init_survives_a_non_numeric_async_task_timeout(monkeypatch):
    """A bad optional kwarg must cost only that value, not the whole agent:
    init() cannot raise into the host app and tracing stays on."""
    fake = _stub_native(monkeypatch)

    agent = agent_mod.init(application_name="bad-timeout", agent_name="x",
                           asyncio_task_span_timeout_ms="5m")

    assert not isinstance(agent, agent_mod._NullAgent)
    assert agent._async_task_span_timeout == (
        Config.asyncio_task_span_timeout_ms / 1000.0)


def test_failed_agent_construction_shuts_the_native_agent_down(monkeypatch):
    """An orphaned native agent has no owner to drain it and aborts at
    finalization, so a construction failure must not leave one running."""
    shutdowns = []

    _stub_native(monkeypatch, on_shutdown=lambda: shutdowns.append(True))
    monkeypatch.setattr(agent_mod, "Agent", _raising_agent)

    agent = agent_mod.init(application_name="boom", agent_name="x")

    assert isinstance(agent, agent_mod._NullAgent)
    assert shutdowns == [True]


def _raising_agent(*_args, **_kwargs):
    raise RuntimeError("agent construction failed")


# ---------------------------------------------------------------------------
# End-to-end fork behavior against the real native agent.
#
# Runs in a spawned subprocess (not the test runner's own process) so a real
# os.fork() can't destabilise pytest on darwin. Skipped when the native
# extension lacks the post-fork hook (e.g. a stale build) or when fork is
# unavailable. The collector is intentionally unreachable in both scenarios;
# handshake-level assertions live in tests/integration/test_core.py.
# ---------------------------------------------------------------------------

def _native_has_fork_support() -> bool:
    try:
        from pinpoint import _native
    except Exception:  # noqa: BLE001
        return False
    return hasattr(_native, "start_agent")


_FORK_E2E_SCRIPT = """
import os, sys
import pinpoint
from pinpoint import agent as agent_mod

# Collector is intentionally unreachable; we only assert the child ends up
# with the contract-mandated agent object and does not crash.
pinpoint.init(application_name="fork-e2e", collector_host="127.0.0.1",
              log_level="ERROR", prefork={prefork})
parent_native = agent_mod.get_agent()._native

r, w = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(r)
    out = os.fdopen(w, "w")
    try:
        child = agent_mod.get_agent()
        same = child is not None and child._native is parent_native
        is_null = isinstance(child, agent_mod._NullAgent)
        # Exercise the span path to prove the child agent is usable.
        span = child.new_span("op", "/rpc")
        span.end()
        out.write("SAME=%s NULL=%s SPAN_OK=1\\n" % (same, is_null))
    except BaseException as e:  # noqa: BLE001
        out.write("CHILD_EXC=%r\\n" % (e,))
    finally:
        out.flush(); out.close()
    os._exit(0)
else:
    os.close(w)
    data = os.fdopen(r).read()
    _, status = os.waitpid(pid, 0)
    sys.stdout.write(data)
    sys.stdout.write("EXIT_OK=%s\\n" % (os.WIFEXITED(status)
                                        and os.WEXITSTATUS(status) == 0))
"""


def _run_fork_e2e(prefork: bool) -> str:
    proc = subprocess.run(
        [sys.executable, "-c", _FORK_E2E_SCRIPT.format(prefork=prefork)],
        capture_output=True, text=True, timeout=60)
    out = proc.stdout
    assert "CHILD_EXC" not in out, f"child raised: {out}\n{proc.stderr}"
    assert "EXIT_OK=True" in out, (
        f"child did not exit cleanly: {out}\n{proc.stderr}")
    return out


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork() unavailable")
@pytest.mark.skipif(not _native_has_fork_support(),
                    reason="native extension lacks post-fork hook")
def test_fork_child_of_pending_master_gets_working_agent_end_to_end():
    """prefork=True: after a real fork, the child starts its first native
    agent and exits cleanly; the master had no native agent to inherit."""
    out = _run_fork_e2e(prefork=True)
    assert "SAME=False" in out, f"child reused inherited native agent: {out}"
    assert "NULL=False" in out, f"child degraded to null agent: {out}"
    assert "SPAN_OK=1" in out, f"child span path failed: {out}"


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork() unavailable")
@pytest.mark.skipif(not _native_has_fork_support(),
                    reason="native extension lacks post-fork hook")
def test_fork_child_of_warm_master_degrades_end_to_end():
    """Default init() (warm master): after a real fork the child is cleanly
    degraded to a no-op agent whose span path still works, and exits without
    crashing or wedging."""
    out = _run_fork_e2e(prefork=False)
    assert "SAME=False" in out, f"child kept routing through dead agent: {out}"
    assert "NULL=True" in out, f"warm-master child was not degraded: {out}"
    assert "SPAN_OK=1" in out, f"degraded child span path failed: {out}"


def test_fork_detaches_the_inherited_current_span():
    """Clearing the contextvar stops lookups, but the instrumentation frame
    that opened the span is still on the forking thread's stack — the stack the
    child continues on — so its finally calls end() on the object directly.
    That would cross into the inherited native span, whose active-span shard
    mutex a parent thread may have held at the fork instant (inherited locked,
    no thread left to unlock it). Detaching makes the child's end() a no-op."""
    import _fakes

    from pinpoint import context as ppctx
    from pinpoint.tracer import Span

    rec = _fakes.Recorder()
    native = _fakes.FakeNativeSpan("root", recorder=rec)
    span = Span(native)
    token = ppctx.set_current_span(span)
    try:
        agent_mod._detach_inherited_span(agent_mod._current_span.get())
    finally:
        ppctx.reset_current_span(token)

    assert span._native is None
    rec.events.clear()

    span.end()  # what the child's `finally` runs

    assert rec.events == []  # nothing crossed into the inherited native span
    assert span._ended is True


def test_detaching_an_inherited_span_tolerates_a_missing_binding():
    assert agent_mod._detach_inherited_span(None) is None
    assert agent_mod._detach_inherited_span(object()) is None


# ---------------------------------------------------------------------------
# Nested root spans: the second one is created anyway, and we warn once.
# ---------------------------------------------------------------------------


class _NestedNativeAgent:
    """Minimal native double: new_span always yields an unsampled span, so the
    test exercises the warning, not the Span wrapper."""

    def get_config_snapshot(self):
        return ("app", 1000, "", 64, 5000, (), (), (), (), (), (), 1, False)

    def new_span(self, *_args):
        return object(), False, "", 7, 0


@pytest.fixture
def nested_warnings(monkeypatch):
    """Reset the one-shot latch and capture the warning text."""
    from pinpoint import context as ctx_mod

    warnings = []
    monkeypatch.setattr(agent_mod, "_nested_root_span_warned", False)
    monkeypatch.setattr(agent_mod._log, "warning",
                        lambda msg, *args: warnings.append(msg % args))
    token = ctx_mod._current_span.set(None)
    yield warnings
    ctx_mod._current_span.reset(token)


def _nested_agent():
    return agent_mod.Agent(_NestedNativeAgent(), Config(application_name="app"))


def test_root_span_without_an_active_span_is_silent(nested_warnings):
    _nested_agent().new_span("GET /", "/")
    assert nested_warnings == []


def test_nested_root_span_warns_once(nested_warnings):
    from pinpoint import context as ctx_mod

    agent = _nested_agent()
    with agent.new_span("GET /outer", "/outer") as outer:
        ctx_mod.set_current_span(outer)
        agent.new_span("GET /inner", "/inner")
        agent.new_span("GET /inner2", "/inner2")

    assert len(nested_warnings) == 1
    assert "/inner" in nested_warnings[0]
    assert agent_mod._nested_root_span_warned is True


def test_ended_span_left_bound_is_not_nesting(nested_warnings):
    from pinpoint import context as ctx_mod

    agent = _nested_agent()
    span = agent.new_span("GET /done", "/done")
    ctx_mod.set_current_span(span)
    span.end()
    agent.new_span("GET /next", "/next")
    assert nested_warnings == []


def test_worker_thread_with_a_copied_context_is_not_nesting(nested_warnings):
    """A framework threadpool copies the request context; a root span opened
    there is a new transaction, not a nested one."""
    import contextvars
    import threading

    from pinpoint import context as ctx_mod

    agent = _nested_agent()
    with agent.new_span("GET /outer", "/outer") as outer:
        ctx_mod.set_current_span(outer)
        ctx = contextvars.copy_context()
        worker = threading.Thread(
            target=lambda: ctx.run(agent.new_span, "job", "/job"))
        worker.start()
        worker.join()

    assert nested_warnings == []
