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

"""The native agent's stdout log through a pipe, as docker and kubernetes give it.

With no ``log_output`` file the native agent writes its log to ``std::cout``,
which goes through C stdio's ``stdout``. On a pipe that is fully buffered, so
without a flush per line the native lines waited for 4 KiB of others, came out
in 4096-byte writes cut mid-line, and lost their tail when a worker left
through ``os._exit``. Sibling workers sharing the pipe, and the Python
logger's lines on the same fd, then landed in the middle of those cuts.

These tests start the real agent in child processes whose stdout is a pipe
and check that a native line is readable as soon as it is written, and that
lines from several processes and both loggers never mix. Needs no Docker
daemon (``no_docker``): the collector is ``_collector.MockCollector``.
"""

from __future__ import annotations

import os
import re
import select
import subprocess
import sys
import textwrap
import time

import pytest

pytestmark = pytest.mark.no_docker

pytest.importorskip("grpc")
pytest.importorskip("grpc_tools")

from tests.integration._collector import MockCollector

_READ_TIMEOUT_SECONDS = 20.0
_CHILDREN = 4
_LINES_PER_CHILD = 200

# What the native logger prefixes to each message, and what pinpoint's Python
# handler prefixes to each record (pinpoint/_log.py).
_NATIVE_PREFIX = r"\[\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3}\]\[(?:debug|info|warning|error)\]\[pinpoint\]"
_PYTHON_PREFIX = r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3} \[pinpoint"
# Either prefix anywhere but at the start of a line: another write landed in
# the middle of this one.
_MIXED = re.compile(rf"(?<=.)(?:{_NATIVE_PREFIX}|{_PYTHON_PREFIX})")

_CHILD_TEMPLATE = textwrap.dedent('''\
    import os
    import sys
    import time

    import pinpoint
    from pinpoint._log import get_logger

    agent = pinpoint.init(
        application_name="stdout-log-it",
        agent_name="stdout-log-it",
        service_name="stdout-log-it-service",
        collector_host="127.0.0.1",
        collector_agent_port={collector_port},
        collector_span_port={collector_port},
        collector_stat_port={collector_port},
        agent_info_send_retry_interval_ms=200,
        stat_enabled=False,
    )
    deadline = time.monotonic() + 20
    while not agent.enabled and time.monotonic() < deadline:
        time.sleep(0.05)
    log = get_logger("it")
    tag = sys.argv[1]
    for i in range(int(sys.argv[2])):
        # The native agent warns about the unknown key, which carries the
        # tag, then refuses the reload (the YAML has no collector).
        result = pinpoint.reload_config(
            "native-line-" + tag + "-" + str(i) + ": 1")
        assert not result
        log.warning("python-line %s %d", tag, i)
    log.warning("child-ready %s", tag)
    while not os.path.exists(sys.argv[3]):
        time.sleep(0.05)
    # gunicorn and multiprocessing workers leave this way: no exit-time
    # flush of C stdio, so a line still buffered here would be lost.
    os._exit(0)
''')


@pytest.fixture(scope="module")
def collector():
    c = MockCollector().start()
    yield c
    c.stop()


_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))


def _child_env() -> dict[str, str]:
    # The dev shell may point PINPOINT_PY_* at a live collector or a log
    # file, and native reads them over the YAML.
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("PINPOINT_PY_")}
    # The child script sits in tmp_path, so the checkout's pinpoint/ must be
    # put on its path.
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (_REPO_ROOT, env.get("PYTHONPATH")) if p)
    return env


def _read_until(fd: int, buf: bytearray, needles: list[bytes],
                procs: list[subprocess.Popen]) -> None:
    deadline = time.monotonic() + _READ_TIMEOUT_SECONDS
    while not all(n in buf for n in needles):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            missing = [n for n in needles if n not in buf]
            raise AssertionError(
                f"not read in time: {missing}\n--- read so far ---\n"
                f"{buf.decode(errors='replace')}")
        ready, _, _ = select.select([fd], [], [], remaining)
        if ready:
            chunk = os.read(fd, 65536)
            if not chunk:
                codes = [p.poll() for p in procs]
                raise AssertionError(
                    f"children exited early ({codes})\n"
                    f"{buf.decode(errors='replace')}")
            buf += chunk


def _spawn(tmp_path, collector, tag: str, lines: int, stdout_fd: int):
    script = tmp_path / "child.py"
    if not script.exists():
        script.write_text(_CHILD_TEMPLATE.format(collector_port=collector.port))
    return subprocess.Popen(
        [sys.executable, str(script), tag, str(lines),
         str(tmp_path / "done")],
        env=_child_env(), stdout=stdout_fd, stderr=subprocess.STDOUT,
    )


def _finish(procs, read_fd: int, buf: bytearray, done_file) -> None:
    done_file.touch()
    for proc in procs:
        proc.wait(timeout=30)
    while True:
        ready, _, _ = select.select([read_fd], [], [], 0)
        if not ready:
            break
        chunk = os.read(read_fd, 65536)
        if not chunk:
            break
        buf += chunk


def test_native_line_is_readable_before_the_process_exits(collector, tmp_path):
    read_fd, write_fd = os.pipe()
    buf = bytearray()
    procs = []
    try:
        procs.append(_spawn(tmp_path, collector, "solo", 1, write_fd))
        os.close(write_fd)
        write_fd = -1
        # The native line is logged before the Python marker, so with a
        # flush per line it is in the pipe by the time the marker is.
        _read_until(read_fd, buf, [b"child-ready solo"], procs)
        text = buf.decode(errors="replace")
        assert "native-line-solo-0" in text, text
        assert "AgentInfo sent" in text, text
        # Nothing is held back mid-line either.
        assert text.endswith("\n"), text[-300:]
    finally:
        _finish(procs, read_fd, buf, tmp_path / "done")
        if write_fd >= 0:
            os.close(write_fd)
        os.close(read_fd)


def test_lines_of_several_processes_and_both_loggers_do_not_mix(
    collector, tmp_path,
):
    read_fd, write_fd = os.pipe()
    buf = bytearray()
    procs = []
    tags = [f"w{i}" for i in range(_CHILDREN)]
    try:
        for tag in tags:
            procs.append(_spawn(tmp_path, collector, tag, _LINES_PER_CHILD,
                                write_fd))
        os.close(write_fd)
        write_fd = -1
        _read_until(read_fd, buf,
                    [f"child-ready {tag}".encode() for tag in tags], procs)
    finally:
        _finish(procs, read_fd, buf, tmp_path / "done")
        if write_fd >= 0:
            os.close(write_fd)
        os.close(read_fd)

    lines = buf.decode(errors="replace").splitlines()
    mixed = [line for line in lines if _MIXED.search(line)]
    assert not mixed, "\n".join(mixed[:10])
    for tag in tags:
        for kind, pattern in (
            ("native", rf"^{_NATIVE_PREFIX}.*native-line-{tag}-(\d+)\b"),
            ("python", rf"^{_PYTHON_PREFIX}.*python-line {tag} (\d+)$"),
        ):
            seen = {int(m.group(1)) for line in lines
                    if (m := re.search(pattern, line))}
            assert seen == set(range(_LINES_PER_CHILD)), (
                f"{tag}: {kind} lines missing: "
                f"{sorted(set(range(_LINES_PER_CHILD)) - seen)[:10]}")
