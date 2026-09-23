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

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from pinpoint.bootstrap import cli
from pinpoint.bootstrap.cli import (
    _parse_agent_flags,
    _prepend_pythonpath,
    main,
)


def test_prepend_pythonpath_sets_empty_value():
    env = {}

    _prepend_pythonpath(env, "/bootstrap")

    assert env["PYTHONPATH"] == "/bootstrap"


def test_prepend_pythonpath_preserves_existing_entries_without_duplicates():
    env = {"PYTHONPATH": os.pathsep.join(("/existing", "/bootstrap"))}

    _prepend_pythonpath(env, "/bootstrap")

    assert env["PYTHONPATH"] == os.pathsep.join(("/existing", "/bootstrap"))


def test_prepend_pythonpath_adds_bootstrap_first():
    env = {"PYTHONPATH": "/existing"}

    _prepend_pythonpath(env, "/bootstrap")

    assert env["PYTHONPATH"] == os.pathsep.join(("/bootstrap", "/existing"))


def test_parse_agent_flags_includes_server_info():
    env, remaining = _parse_agent_flags([
        "--app-name", "checkout",
        "--agent-name", "checkout-api",
        "--collector", "collector.internal",
        "--server-info", "FastAPI",
        "--",
        "python", "app.py",
    ])

    assert env == {
        "PINPOINT_PY_APPLICATION_NAME": "checkout",
        "PINPOINT_PY_AGENT_NAME": "checkout-api",
        "PINPOINT_PY_COLLECTOR_HOST": "collector.internal",
        "PINPOINT_PY_SERVER_INFO": "FastAPI",
    }
    assert remaining == ["python", "app.py"]


def test_parse_agent_flags_config_file_and_active_profile(tmp_path, monkeypatch):
    """``--config-file`` lands in PINPOINT_PY_CONFIG_FILE as an absolute path
    (so a chdir in the wrapped program cannot lose it) and
    ``--active-profile`` in PINPOINT_PY_ACTIVE_PROFILE."""
    config = tmp_path / "pinpoint-config.yaml"
    config.write_text("ApplicationName: checkout\n")
    monkeypatch.chdir(tmp_path)

    env, remaining = _parse_agent_flags([
        "--config-file", "pinpoint-config.yaml",
        "--active-profile", "production",
        "--", "python", "app.py",
    ])

    assert env == {
        "PINPOINT_PY_CONFIG_FILE": str(config),
        "PINPOINT_PY_ACTIVE_PROFILE": "production",
    }
    assert os.path.isabs(env["PINPOINT_PY_CONFIG_FILE"])
    assert remaining == ["python", "app.py"]


def test_parse_agent_flags_rejects_missing_config_file(tmp_path, capsys):
    """Fail in the launcher, not in the child: a typo'd path must not start
    the program with an agent that silently fell back to defaults."""
    missing = tmp_path / "nope.yaml"

    with pytest.raises(SystemExit) as excinfo:
        _parse_agent_flags(["--config-file", str(missing), "python", "app.py"])

    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert "--config-file" in err
    assert "no such file" in err


def test_main_execs_command_with_config_file_and_profile(tmp_path, monkeypatch):
    config = tmp_path / "pinpoint-config.yaml"
    config.write_text("ApplicationName: checkout\n")
    captured = {}

    def _execvpe(command, argv, env):
        captured.update(command=command, argv=argv, env=env)
        raise OSError("stop after capture")

    monkeypatch.setattr(cli.os, "execvpe", _execvpe)

    assert main([
        "--config-file", str(config),
        "--active-profile", "staging",
        "--", "python", "app.py",
    ]) == 127
    assert captured["env"]["PINPOINT_PY_CONFIG_FILE"] == str(config)
    assert captured["env"]["PINPOINT_PY_ACTIVE_PROFILE"] == "staging"
    assert captured["env"]["PINPOINT_PY_AUTOLOAD"] == "1"


def test_parse_agent_flags_enables_native_python_logging_bridge():
    env, remaining = _parse_agent_flags([
        "--native-log-to-python",
        "--native-log-queue-size", "64",
        "python", "app.py",
    ])
    assert env == {
        "PINPOINT_PY_NATIVE_LOG_TO_PYTHON": "true",
        "PINPOINT_PY_NATIVE_LOG_QUEUE_SIZE": "64",
    }
    assert remaining == ["python", "app.py"]


def test_parse_agent_flags_stops_at_first_command_argument():
    """Agent flags after the command belong to the command. ``pinpoint-run
    myserver --collector foo`` must pass ``--collector foo`` through verbatim
    (myserver's own option), not steal it into the agent environment and
    launch the target with missing arguments."""
    env, remaining = _parse_agent_flags([
        "--app-name", "checkout",
        "myserver", "--collector", "foo", "--agent-name", "bar",
    ])

    assert env == {"PINPOINT_PY_APPLICATION_NAME": "checkout"}
    assert remaining == ["myserver", "--collector", "foo", "--agent-name", "bar"]


def test_parse_agent_flags_without_separator_or_flags():
    """A bare command with flag-looking arguments passes through untouched."""
    env, remaining = _parse_agent_flags(["python", "-m", "http.server"])

    assert env == {}
    assert remaining == ["python", "-m", "http.server"]


def test_main_without_command_prints_usage(capsys):
    assert main([]) == 2
    assert "usage: pinpoint-run" in capsys.readouterr().err


def test_main_execs_command_with_agent_environment(monkeypatch):
    captured = {}

    def _execvpe(command, argv, env):
        captured.update(command=command, argv=argv, env=env)
        raise OSError("stop after capture")

    monkeypatch.setattr(cli.os, "execvpe", _execvpe)

    assert main([
        "--app-name", "checkout",
        "--collector", "collector.internal",
        "--", "python", "app.py",
    ]) == 127
    assert captured["command"] == "python"
    assert captured["argv"] == ["python", "app.py"]
    assert captured["env"]["PINPOINT_PY_APPLICATION_NAME"] == "checkout"
    assert captured["env"]["PINPOINT_PY_COLLECTOR_HOST"] == "collector.internal"
    assert captured["env"]["PINPOINT_PY_AUTOLOAD"] == "1"
    assert captured["env"]["PYTHONPATH"].split(os.pathsep)[0] == cli._bootstrap_dir()


def test_main_reports_missing_executable(monkeypatch, capsys):
    monkeypatch.setattr(
        cli.os, "execvpe",
        lambda *_args: (_ for _ in ()).throw(FileNotFoundError("missing")),
    )

    assert main(["missing-command"]) == 127
    assert "pinpoint-run: missing-command: missing" in capsys.readouterr().err


def test_sitecustomize_activates_stub_agent_in_fresh_interpreter(tmp_path):
    """Exercise the real interpreter-start activation without starting gRPC."""
    marker = tmp_path / "activation.txt"
    package = tmp_path / "pinpoint"
    package.mkdir()
    (package / "__init__.py").write_text(
        "import os\n"
        "def init(server_info=None):\n"
        "    open(os.environ['MARKER'], 'a').write('init:' + server_info + '\\n')\n"
    )
    (package / "autoload.py").write_text(
        "import os\n"
        "def autoload():\n"
        "    open(os.environ['MARKER'], 'a').write('autoload\\n')\n"
    )
    env = dict(os.environ)
    env.update({
        "MARKER": str(marker),
        "PINPOINT_PY_AUTOLOAD": "1",
        "PINPOINT_PY_SERVER_INFO": "Bootstrap Test",
        "PYTHONPATH": os.pathsep.join((cli._bootstrap_dir(), str(tmp_path))),
    })
    env.pop("PINPOINT_PY_PREV_SITECUSTOMIZE", None)

    subprocess.run([sys.executable, "-c", "pass"], env=env, check=True)

    assert marker.read_text().splitlines() == ["init:Bootstrap Test", "autoload"]


def test_sitecustomize_activation_failure_does_not_block_user_code(tmp_path):
    package = tmp_path / "pinpoint"
    package.mkdir()
    (package / "__init__.py").write_text(
        "def init(server_info=None):\n    raise RuntimeError('broken init')\n"
    )
    (package / "autoload.py").write_text("def autoload():\n    raise AssertionError\n")
    env = dict(os.environ)
    env.update({
        "PINPOINT_PY_AUTOLOAD": "1",
        "PYTHONPATH": os.pathsep.join((cli._bootstrap_dir(), str(tmp_path))),
    })
    env.pop("PINPOINT_PY_PREV_SITECUSTOMIZE", None)

    proc = subprocess.run(
        [sys.executable, "-c", "print('user-code-ran')"],
        env=env, capture_output=True, text=True, check=True,
    )

    assert proc.stdout.strip() == "user-code-ran"
    assert "bootstrap failed: broken init" in proc.stderr


def test_sitecustomize_executes_explicit_previous_file(tmp_path):
    marker = tmp_path / "previous.txt"
    previous = tmp_path / "previous_sitecustomize.py"
    previous.write_text(f"open({str(marker)!r}, 'w').write('ran')\n")
    env = dict(os.environ)
    env.update({
        "PINPOINT_PY_AUTOLOAD": "0",
        "PINPOINT_PY_PREV_SITECUSTOMIZE": str(previous),
        "PYTHONPATH": cli._bootstrap_dir(),
    })

    subprocess.run([sys.executable, "-c", "pass"], env=env, check=True)

    assert marker.read_text() == "ran"


def test_sitecustomize_chains_to_shadowed_sitecustomize(tmp_path):
    """pinpoint's bootstrap sitecustomize shadows any pre-existing
    sitecustomize (virtualenv, company-wide, PYTHONPATH). It must chain to
    the shadowed one after activating, not silently swallow it."""
    import os
    import subprocess
    import sys

    from pinpoint.bootstrap import cli

    marker = tmp_path / "marker.txt"
    (tmp_path / "sitecustomize.py").write_text(
        f"open({str(marker)!r}, 'w').write('ran')\n"
    )

    env = dict(os.environ)
    env["PYTHONPATH"] = cli._bootstrap_dir() + os.pathsep + str(tmp_path)
    env.pop("PINPOINT_PY_AUTOLOAD", None)
    env.pop("PINPOINT_PY_PREV_SITECUSTOMIZE", None)

    subprocess.run(
        [sys.executable, "-c", "pass"], env=env, check=True, timeout=60,
    )
    assert marker.exists() and marker.read_text() == "ran"


def test_sitecustomize_quiet_when_nothing_to_chain_to(tmp_path):
    """The common container/venv case: no *other* sitecustomize exists to
    chain to, so our chain re-import raises ImportError. Our block must leave
    ``sys.modules["sitecustomize"]`` populated, or the import machinery's
    post-exec bookkeeping (``sys.modules.pop("sitecustomize")`` in
    importlib._bootstrap._load_unlocked) raises ``KeyError`` and
    ``site.execsitecustomize()`` prints "Error in sitecustomize ..." to stderr
    on *every* interpreter start.

    This drives the real stdlib entry point (``site.execsitecustomize``) and
    the real import machinery against our real module. We pin ``sys.path`` to
    just our bootstrap dir (and run with ``-S``) so nothing else resolves as
    ``sitecustomize`` — reproducing the "nothing to chain to" condition even on
    boxes whose interpreter ships a stdlib sitecustomize (e.g. Homebrew)."""
    import os
    import pathlib
    import subprocess
    import sys
    import textwrap

    # Derive the bootstrap dir from *this* test tree (not cli._bootstrap_dir(),
    # which follows `import pinpoint` and can resolve to another checkout on
    # sys.path) so the child always loads the sitecustomize under test.
    bootstrap_dir = str(
        pathlib.Path(__file__).resolve().parents[2]
        / "pinpoint" / "bootstrap" / "_pythonpath"
    )

    driver = tmp_path / "driver.py"
    driver.write_text(textwrap.dedent(f"""
        import sys, site
        # Our bootstrap dir is the only place `sitecustomize` can be resolved,
        # so the chain re-import finds nothing (ImportError).
        sys.path[:] = [{bootstrap_dir!r}]
        assert "sitecustomize" not in sys.modules
        site.execsitecustomize()  # the exact function CPython runs at startup
        sys.stdout.write("OK")
    """))

    env = dict(os.environ)
    env.pop("PINPOINT_PY_AUTOLOAD", None)
    env.pop("PINPOINT_PY_PREV_SITECUSTOMIZE", None)
    env.pop("PYTHONPATH", None)

    proc = subprocess.run(
        [sys.executable, "-S", str(driver)],
        env=env, capture_output=True, text=True, check=False, timeout=60,
    )

    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == "OK", f"driver did not finish: {proc.stdout!r}"
    assert proc.stderr == "", f"startup emitted stderr noise: {proc.stderr!r}"


def test_sitecustomize_preserves_chained_syspath_additions(tmp_path):
    """A shadowed sitecustomize frequently exists precisely to extend
    ``sys.path`` (``site.addsitedir``, vendoring). When we chain to it those
    additions must survive: the previous code snapshotted ``sys.path`` before
    chaining and restored it wholesale in a ``finally``, silently discarding
    whatever the chained module appended."""
    import os
    import pathlib
    import subprocess
    import sys

    # See the sibling test: pin the bootstrap dir to this tree, not
    # cli._bootstrap_dir(), so the child loads the sitecustomize under test.
    bootstrap_dir = str(
        pathlib.Path(__file__).resolve().parents[2]
        / "pinpoint" / "bootstrap" / "_pythonpath"
    )

    vendor_dir = tmp_path / "vendor"
    vendor_dir.mkdir()
    compete_dir = tmp_path / "compete"
    compete_dir.mkdir()
    (compete_dir / "sitecustomize.py").write_text(
        f"import sys\nsys.path.append({str(vendor_dir)!r})\n"
    )

    env = dict(os.environ)
    env["PYTHONPATH"] = bootstrap_dir + os.pathsep + str(compete_dir)
    env.pop("PINPOINT_PY_AUTOLOAD", None)
    env.pop("PINPOINT_PY_PREV_SITECUSTOMIZE", None)

    check = (
        "import sys\n"
        f"sys.stdout.write('YES' if {str(vendor_dir)!r} in sys.path else 'NO')\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", check],
        env=env, capture_output=True, text=True, check=False, timeout=60,
    )

    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == "YES", (
        "chained sitecustomize's sys.path addition was dropped; "
        f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )


def test_pythonpath_entry_exposes_nothing_but_sitecustomize():
    """``pinpoint-run`` puts this directory FIRST on ``PYTHONPATH``, so every
    module in it becomes importable under its bare top-level name — for the
    launched process and, since ``PYTHONPATH`` is inherited, for every Python
    child it spawns. Shadowing ``sitecustomize`` is the point; shadowing
    anything else silently replaces a module in the user's application.

    ``sitecustomize`` drops the directory back off ``sys.path`` once it runs,
    but that is not a safety net: it does not happen under
    ``PINPOINT_PY_PREV_SITECUSTOMIZE``, nor in a ``python -S`` child where
    ``site`` never runs the module at all.

    ``__init__.py`` is exempt — package discovery needs it to ship the
    directory, and ``import __init__`` shadows nothing real.
    """
    import pathlib

    entry = pathlib.Path(cli._bootstrap_dir())
    exposed = sorted(
        child.name for child in entry.iterdir()
        if not child.name.startswith("__")
        and (child.suffix == ".py" or child.is_dir())
    )

    assert exposed == ["sitecustomize.py"], (
        f"{entry} exposes extra top-level module names: {exposed}")


def test_sitecustomize_chains_even_when_the_error_report_fails(tmp_path):
    """_activate guards its body; what is left unguarded is its own error
    handler — str(exc) and the stderr write. An exception with a raising
    __str__ (or a broken stderr) would abort the module before the chaining
    block and silently drop the user's real sitecustomize in every process."""
    package = tmp_path / "pinpoint"
    package.mkdir()
    (package / "__init__.py").write_text(
        "class _Unprintable(RuntimeError):\n"
        "    def __str__(self):\n"
        "        raise ValueError('cannot render')\n"
        "\n"
        "def init(server_info=None):\n"
        "    raise _Unprintable()\n"
    )
    (package / "autoload.py").write_text("def autoload():\n    raise AssertionError\n")

    marker = tmp_path / "chained.txt"
    (tmp_path / "sitecustomize.py").write_text(
        f"open({str(marker)!r}, 'w').write('ran')\n"
    )

    env = dict(os.environ)
    env.update({
        "PINPOINT_PY_AUTOLOAD": "1",
        "PYTHONPATH": os.pathsep.join((cli._bootstrap_dir(), str(tmp_path))),
    })
    env.pop("PINPOINT_PY_PREV_SITECUSTOMIZE", None)

    proc = subprocess.run(
        [sys.executable, "-c", "print('user-code-ran')"],
        env=env, capture_output=True, text=True, check=True, timeout=60,
    )

    assert proc.stdout.strip() == "user-code-ran"
    assert marker.exists() and marker.read_text() == "ran"
