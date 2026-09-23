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

"""`pinpoint-run` CLI.

Usage:
    pinpoint-run [agent-options] <command> [command args...]

Agent options map one-to-one onto ``PINPOINT_PY_*`` env vars — ``--app-name``,
``--agent-name``, ``--collector``, ``--server-info``, ``--config-file``,
``--active-profile`` and the native-log flags — so a flag and the matching
env var are interchangeable; the flag wins when both are given.

Prepends `pinpoint/bootstrap/_pythonpath` to PYTHONPATH so the
sitecustomize.py there fires at interpreter start, then execs the target
command. That directory holds nothing but sitecustomize.py on purpose — see
its `__init__` docstring; this module deliberately sits outside it.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List, Sequence


def _bootstrap_dir() -> str:
    """The directory to put on PYTHONPATH — the one holding only
    ``sitecustomize.py``, never this package's own directory."""
    return str(Path(__file__).resolve().parent / "_pythonpath")


def _prepend_pythonpath(env: dict, path: str) -> None:
    existing = env.get("PYTHONPATH", "")
    if not existing:
        env["PYTHONPATH"] = path
        return
    parts = existing.split(os.pathsep)
    if path not in parts:
        env["PYTHONPATH"] = path + os.pathsep + existing


def _existing_file(value: str) -> str:
    """argparse type for ``--config-file``: fail fast in the launcher rather
    than letting the agent in the child process discover a missing file.
    Returned absolute so the value survives a ``chdir`` in the wrapped
    program (gunicorn ``--chdir``, daemonizers) and a native config reload."""
    path = os.path.abspath(value)
    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"no such file: {value}")
    return path


# Agent flag -> (the env var it sets, argparse type, help). Everything else
# is tuned through the PINPOINT_PY_* env vars the native agent reads itself.
_FLAGS = {
    "app_name": (
        "PINPOINT_PY_APPLICATION_NAME", str,
        "application name shown in the Pinpoint Web UI"),
    "agent_name": (
        "PINPOINT_PY_AGENT_NAME", str,
        "agent display label (defaults to a generated id)"),
    "collector": (
        "PINPOINT_PY_COLLECTOR_HOST", str,
        "collector host to report to"),
    "server_info": (
        "PINPOINT_PY_SERVER_INFO", str,
        "server metadata label sent in AgentInfo"),
    "config_file": (
        "PINPOINT_PY_CONFIG_FILE", _existing_file,
        ("YAML config file; replaces the inline configuration wholesale, "
         "PINPOINT_PY_* env vars and the other flags still override it")),
    "active_profile": (
        "PINPOINT_PY_ACTIVE_PROFILE", str,
        ("name of the Profile.<name> section to apply on top of the base "
         "configuration")),
}
_FLAG_ENV = {flag: var for flag, (var, _type, _help) in _FLAGS.items()}

_parser = argparse.ArgumentParser(
    prog="pinpoint-run",
    description="Run a Python program with the Pinpoint agent bootstrapped "
                "at interpreter start.",
)
for _flag, (_var, _type, _help) in _FLAGS.items():
    _parser.add_argument("--" + _flag.replace("_", "-"), type=_type,
                         help=f"{_help} (sets {_var})")
_parser.add_argument(
    "--native-log-to-python",
    action="store_true",
    default=None,
    help="route native agent diagnostics through the pinpoint.native logger",
)
_parser.add_argument(
    "--native-log-queue-size",
    type=int,
    help="bounded native-to-Python log queue record capacity",
)
# Agent flags must precede the command: REMAINDER hands everything from the
# first non-flag argument on to the command verbatim, so the wrapped program
# may define options with the same names as ours.
_parser.add_argument("command", nargs=argparse.REMAINDER,
                     help="the program to run, with its arguments")


def _parse_agent_flags(argv: Sequence[str]) -> tuple[dict, List[str]]:
    """Split ``argv`` into the env vars our flags set and the command."""
    ns = _parser.parse_args(list(argv))
    env = {var: getattr(ns, flag) for flag, var in _FLAG_ENV.items()
           if getattr(ns, flag) is not None}
    if ns.native_log_to_python is not None:
        env["PINPOINT_PY_NATIVE_LOG_TO_PYTHON"] = "true"
    if ns.native_log_queue_size is not None:
        env["PINPOINT_PY_NATIVE_LOG_QUEUE_SIZE"] = str(
            ns.native_log_queue_size)
    # A leading ``--`` stays in the REMAINDER on some Python versions.
    command = ns.command[1:] if ns.command[:1] == ["--"] else ns.command
    return env, command


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    env_updates, remaining = _parse_agent_flags(argv)
    if not remaining:
        _parser.print_usage(sys.stderr)
        return 2

    new_env = dict(os.environ)
    new_env.update(env_updates)
    new_env["PINPOINT_PY_AUTOLOAD"] = "1"
    _prepend_pythonpath(new_env, _bootstrap_dir())

    # execvpe replaces the current process — no extra wrapper is inherited.
    try:
        os.execvpe(remaining[0], remaining, new_env)
    except OSError as exc:
        print(f"pinpoint-run: {remaining[0]}: {exc}", file=sys.stderr)
        return 127


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
