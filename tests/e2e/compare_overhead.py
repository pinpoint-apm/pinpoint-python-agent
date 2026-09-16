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

"""Side-by-side CPU/RSS overhead comparison for it_test_server.

Boots ``test_server.py`` twice — once with ``PINPOINT_DISABLE=true`` (no
agent init, no autoload, no instrumentation wraps), once with the agent
enabled — drives the same workload with ``max_throughput_test.py`` against
each, samples the server's CPU% and RSS via psutil every
``--sample-interval`` seconds, and prints a comparison table.

Run via the wrapper script ``tests/e2e/run-compare.sh`` so dev.sh
is sourced and psutil is installed. The wrapper forwards all CLI args here.

What it measures
----------------
- **Server-process CPU%** as reported by psutil (per-process, multi-core
  aware — can exceed 100% on multi-core boxes; that's expected).
- **Server-process RSS** in megabytes.
- **Throughput**: total HTTP requests served (from /stats) and average RPS.

What it does NOT measure
------------------------
- The cost of *importing* ``pinpoint`` (the .so still loads in disabled
  mode, just no init/autoload). For a true zero-import baseline you'd
  need a separate process that never imports the module.
- Tail latency. Pure load-throughput numbers; for p99 latency use a real
  load generator like wrk/hey/k6.
- Collector-side overhead. The load driver measures client→server only;
  the agent's collector traffic affects RSS but not request RPS.

Usage
-----
    tests/e2e/run-compare.sh                          # defaults
    tests/e2e/run-compare.sh -m mixed -d 30 -c 8      # explicit
    tests/e2e/run-compare.sh -m grpc-all -d 60 -c 4   # gRPC workload
    tests/e2e/run-compare.sh --require-agent-ready     # collector required
    tests/e2e/run-compare.sh --profile                # +cProfile each variant

cProfile profiling (``--profile``)
----------------------------------
Each variant runs under ``python -m cProfile -o <profile_dir>/<variant>.prof``;
the orchestrator sends SIGINT (not SIGTERM) so uvicorn shuts down gracefully
and cProfile's atexit-registered dump fires. After the comparison table the
script prints ``snakeviz`` launch commands for both files. RPS numbers when
``--profile`` is on are NOT representative — cProfile itself adds 1.5–2×
overhead — but CPU breakdowns are still meaningful for comparing where the
agent spends time.

Multi-threading caveat: stdlib cProfile only profiles the thread that called
``enable()``. FastAPI's sync handlers run in a threadpool, so their bodies
appear partially in the main-loop profile (as time spent in
``run_in_executor``). For full call-graph visibility into the handler bodies,
use ``yappi`` or ``py-spy`` — neither is wired in here.

By default the script does not require a Pinpoint collector. With no collector,
the ``on`` variant spends some CPU on failed handshake retries, so the result is
"agent active without collector," not steady-state tracing overhead. When a
collector is expected, pass ``--require-agent-ready``: measurement then starts
only after registration succeeds and fails after ``--agent-ready-timeout``
instead of silently measuring the untraced startup path.
"""

from __future__ import annotations

import argparse
import os
import signal
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

import psutil

from fixed_rps_test import ServerAddress, get_json, percentile


REPO_ROOT = Path(__file__).resolve().parents[2]
VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"
HTTP_SERVER = REPO_ROOT / "tests" / "e2e" / "test_server.py"
GRPC_SERVER = REPO_ROOT / "tests" / "e2e" / "grpc_server.py"
DRIVER = REPO_ROOT / "tests" / "e2e" / "max_throughput_test.py"

# Modes whose endpoints actually call out to the gRPC backend. mixed/stress
# only include HTTP-side scenarios — no need to start grpc_server for them.
GRPC_MODES = {"grpc-unary", "grpc-stream", "grpc-bidi", "grpc-all", "full"}


# ---------------------------------------------------------------------------
# Process helpers
# ---------------------------------------------------------------------------


def _wait_for(url: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as resp:
                resp.read()
            return
        except (urllib.error.URLError, ConnectionError, OSError):
            time.sleep(0.3)
    raise RuntimeError(f"server at {url} never became ready within {timeout}s")


def _wait_for_agent(port: int, timeout: float = 30.0) -> None:
    """Block until /stats reports the agent registered and enabled."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if _fetch_stats(port).get("agent_enabled"):
                return
        except (urllib.error.URLError, ConnectionError, OSError, ValueError):
            pass
        time.sleep(0.2)
    raise RuntimeError(
        f"agent on :{port} never became enabled within {timeout}s — the "
        "measurement would understate its overhead")


def _fetch_stats(port: int) -> Dict[str, float]:
    return get_json(ServerAddress("http", "127.0.0.1", port, "/"), "/stats", 2.0)


def _start(script: Path, env: Dict[str, str],
           profile_out: Optional[Path] = None) -> subprocess.Popen:
    """Launch the server. When ``profile_out`` is set, run under
    ``python -m cProfile -o <path>`` so the .prof file is written on clean
    interpreter shutdown (which we trigger via SIGINT in ``_stop``)."""
    cmd: List[str] = [str(VENV_PYTHON), "-u"]
    if profile_out is not None:
        cmd += ["-m", "cProfile", "-o", str(profile_out)]
    cmd.append(str(script))
    return subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _stop(p: Optional[subprocess.Popen], timeout: float = 10.0) -> None:
    """Send SIGINT (Ctrl+C) so uvicorn's signal handler can run the ASGI
    lifecycle shutdown and Python's atexit fires — required for cProfile's
    on-exit dump. Fall back to SIGTERM then SIGKILL on timeout.

    Timeout is generous because under cProfile the interpreter shutdown
    (dumping stats, finalizing the C profiler) can take a couple of seconds
    for large traces."""
    if p is None or p.poll() is not None:
        return
    p.send_signal(signal.SIGINT)
    try:
        p.wait(timeout=timeout)
        return
    except subprocess.TimeoutExpired:
        pass
    p.terminate()
    try:
        p.wait(timeout=3.0)
        return
    except subprocess.TimeoutExpired:
        pass
    p.kill()
    p.wait()


# ---------------------------------------------------------------------------
# Sampler
# ---------------------------------------------------------------------------


class _Sampler(threading.Thread):
    """Background thread that records (cpu%, rss_mb) tuples for ``pid``.

    ``psutil.Process.cpu_percent(None)`` returns CPU% since the previous
    call on this Process instance — the first call returns 0.0 by design,
    so we discard it as a warm-up.
    """

    def __init__(self, pid: int, interval: float) -> None:
        super().__init__(daemon=True)
        self.pid = pid
        self.interval = interval
        self._stop = threading.Event()
        self.cpu: List[float] = []
        self.rss_mb: List[float] = []

    def run(self) -> None:
        try:
            proc = psutil.Process(self.pid)
            proc.cpu_percent(None)  # warm-up (discarded)
        except psutil.NoSuchProcess:
            return
        while not self._stop.is_set():
            try:
                self.cpu.append(proc.cpu_percent(None))
                self.rss_mb.append(proc.memory_info().rss / (1024 * 1024))
            except psutil.NoSuchProcess:
                return
            self._stop.wait(self.interval)

    def stop(self) -> None:
        self._stop.set()


# ---------------------------------------------------------------------------
# Variant runner
# ---------------------------------------------------------------------------


def run_variant(label: str, args: argparse.Namespace, *,
                with_pinpoint: bool,
                profile_out: Optional[Path] = None) -> Dict[str, object]:
    env = os.environ.copy()
    env["PORT"] = str(args.port)
    env["HOST"] = "127.0.0.1"
    env["GRPC_TARGET"] = f"localhost:{args.grpc_port}"
    env["PINPOINT_PY_LOG_LEVEL"] = "warning"
    if with_pinpoint:
        env.pop("PINPOINT_DISABLE", None)
    else:
        env["PINPOINT_DISABLE"] = "true"

    profile_suffix = f" [cProfile → {profile_out.name}]" if profile_out else ""
    print(f"\n[{label}] starting it_test_server on :{args.port}{profile_suffix} ...",
          flush=True)
    server = _start(HTTP_SERVER, env, profile_out=profile_out)
    try:
        _wait_for(f"http://127.0.0.1:{args.port}/stats")
        if with_pinpoint and args.require_agent_ready:
            # The agent registers on a background thread and span creation is
            # gated on it: sampling before it completes measures the untraced
            # path and reports the overhead as lower than it is. Make that
            # strict behavior opt-in so the documented collector-less mode
            # remains usable.
            _wait_for_agent(args.port, timeout=args.agent_ready_timeout)
        # Let the server quiesce after startup before we start sampling.
        time.sleep(args.warmup)

        initial = _fetch_stats(args.port)

        sampler = _Sampler(server.pid, args.sample_interval)
        sampler.start()

        print(
            f"[{label}] driving load: mode={args.mode} duration={args.duration}s "
            f"concurrency={args.concurrency}",
            flush=True,
        )
        driver_env = os.environ.copy()
        driver_env["HOST"] = "127.0.0.1"
        driver_env["PORT"] = str(args.port)
        driver = subprocess.run(
            [
                str(VENV_PYTHON), str(DRIVER),
                "-m", args.mode,
                "-d", str(args.duration),
                "-c", str(args.concurrency),
                "--warmup", "0",  # this script does its own warm-up sleep
            ],
            env=driver_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

        sampler.stop()
        sampler.join(timeout=3)

        final = _fetch_stats(args.port)
    finally:
        _stop(server)

    requests = int(final.get("total_requests", 0) - initial.get("total_requests", 0))
    return {
        "label": label,
        "requests": requests,
        "rps": requests / max(args.duration, 1),
        "cpu_avg": statistics.fmean(sampler.cpu) if sampler.cpu else 0.0,
        "cpu_p95": percentile(sampler.cpu, 95),
        "cpu_max": max(sampler.cpu) if sampler.cpu else 0.0,
        "rss_avg": statistics.fmean(sampler.rss_mb) if sampler.rss_mb else 0.0,
        "rss_max": max(sampler.rss_mb) if sampler.rss_mb else 0.0,
        "rss_start": sampler.rss_mb[0] if sampler.rss_mb else 0.0,
        "rss_end": sampler.rss_mb[-1] if sampler.rss_mb else 0.0,
        "rss_delta": (sampler.rss_mb[-1] - sampler.rss_mb[0])
                     if len(sampler.rss_mb) >= 2 else 0.0,
        "samples": len(sampler.cpu),
        "driver_exit": driver.returncode,
        "profile_path": str(profile_out) if profile_out else "",
    }


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _delta_pct(base: float, new: float) -> str:
    if base <= 0:
        return "  n/a "
    d = (new - base) / base * 100.0
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:6.1f}%"


def print_report(off: Dict[str, object], on: Dict[str, object], args: argparse.Namespace) -> None:
    bar = "=" * 70
    print()
    print(bar)
    print("  Pinpoint Overhead Comparison — it_test_server")
    print(bar)
    print(f"  Mode:        {args.mode}")
    print(f"  Duration:    {args.duration}s")
    print(f"  Concurrency: {args.concurrency}")
    print(f"  Samples:     off={off['samples']}  on={on['samples']}")
    print(f"  Sample int:  {args.sample_interval}s")
    print(bar)

    def row(label: str, off_v: float, on_v: float, fmt: str = "{:>14.1f}") -> None:
        print(f"  {label:<14}{fmt.format(off_v)}{fmt.format(on_v)}    {_delta_pct(off_v, on_v)}")

    head_fmt = "{:>14}"
    print(f"  {'Metric':<14}{head_fmt.format('off')}{head_fmt.format('on')}    {'Δ':<8}")
    print(f"  {'-'*14}{'-'*14}{'-'*14}    {'-'*8}")
    row("requests",   off['requests'], on['requests'], "{:>14.0f}")
    row("RPS",        off['rps'],      on['rps'])
    print()
    row("CPU%  avg",  off['cpu_avg'],  on['cpu_avg'])
    row("CPU%  p95",  off['cpu_p95'],  on['cpu_p95'])
    row("CPU%  max",  off['cpu_max'],  on['cpu_max'])
    print()
    row("RSS MB avg", off['rss_avg'],  on['rss_avg'])
    row("RSS MB max", off['rss_max'],  on['rss_max'])
    row("RSS MB Δ",   off['rss_delta'],on['rss_delta'])
    print(bar)
    if on['rps'] > 0 and off['rps'] > 0:
        slowdown = (off['rps'] - on['rps']) / off['rps'] * 100.0
        print(f"  Throughput loss: {slowdown:+.1f}%   ({off['rps']:.0f} → {on['rps']:.0f} RPS)")
    if off['rss_avg'] > 0:
        rss_overhead = on['rss_avg'] - off['rss_avg']
        print(f"  RSS overhead:    {rss_overhead:+.1f} MB ({_delta_pct(off['rss_avg'], on['rss_avg']).strip()})")
    print(bar)

    # cProfile output — print snakeviz launch commands if files exist.
    if off.get("profile_path") or on.get("profile_path"):
        print()
        print("  cProfile output:")
        for label, path in (("off", off.get("profile_path", "")),
                            ("on ", on.get("profile_path", ""))):
            if not path:
                continue
            try:
                size = os.path.getsize(path)
                size_str = f"{size / 1024:.0f} KB"
            except OSError:
                size_str = "missing!"
            print(f"    {label}: {path}   ({size_str})")
        print()
        print("  View in snakeviz:")
        for label, path in (("off", off.get("profile_path", "")),
                            ("on ", on.get("profile_path", ""))):
            if path:
                print(f"    snakeviz {path}")
        print(bar)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Compare it_test_server CPU/RSS with vs without Pinpoint.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="See module docstring for caveats about collector availability.",
    )
    ap.add_argument("-d", "--duration",    type=int,   default=30,
                    help="seconds of load per variant (default: 30)")
    ap.add_argument("-c", "--concurrency", type=int,   default=8,
                    help="load worker threads (default: 8)")
    ap.add_argument("-m", "--mode",        type=str,   default="mixed",
                    help="workload mode: simple/deep/wide/annotated/mixed/"
                         "stress/db-*/grpc-*/full (default: mixed)")
    ap.add_argument("--port",              type=int,   default=18090,
                    help="HTTP server port (default: 18090)")
    ap.add_argument("--grpc-port",         type=int,   default=18051,
                    help="gRPC server port (default: 18051; only used for gRPC modes)")
    ap.add_argument("--sample-interval",   type=float, default=0.5,
                    help="psutil sampling interval, seconds (default: 0.5)")
    ap.add_argument("--warmup",            type=int,   default=3,
                    help="seconds after server-ready before sampling starts (default: 3)")
    ap.add_argument("--require-agent-ready", action="store_true",
                    help="require the on variant to register with a collector "
                         "before measurement (default: disabled)")
    ap.add_argument("--agent-ready-timeout", type=float, default=30.0,
                    help="seconds to wait when --require-agent-ready is set "
                         "(default: 30)")
    ap.add_argument("--profile",           action="store_true",
                    help="run each server under cProfile and write .prof files "
                         "(adds 1.5-2x overhead; RPS numbers no longer meaningful)")
    ap.add_argument("--profile-dir",       type=str,   default="/tmp",
                    help="directory for .prof files (default: /tmp)")
    return ap.parse_args(argv)


def _start_grpc_backend(args: argparse.Namespace, *,
                        with_pinpoint: bool) -> Optional[subprocess.Popen]:
    """Start the gRPC half with the same Pinpoint state as its HTTP variant."""
    if args.mode not in GRPC_MODES:
        return None

    state = "on" if with_pinpoint else "off"
    print(f"starting grpc_server on :{args.grpc_port} (pinpoint {state}) ...",
          flush=True)
    env = os.environ.copy()
    env["GRPC_BIND"] = f"[::]:{args.grpc_port}"
    env["PINPOINT_PY_LOG_LEVEL"] = "warning"
    if with_pinpoint:
        env.pop("PINPOINT_DISABLE", None)
    else:
        env["PINPOINT_DISABLE"] = "true"
    server = _start(GRPC_SERVER, env)
    # gRPC has no /stats endpoint — retain the existing fixed grace period.
    time.sleep(5)
    return server


def _run_variant_with_backend(label: str, args: argparse.Namespace, *,
                              with_pinpoint: bool,
                              profile_out: Optional[Path]) -> Dict[str, object]:
    grpc_server = _start_grpc_backend(args, with_pinpoint=with_pinpoint)
    try:
        return run_variant(
            label, args, with_pinpoint=with_pinpoint, profile_out=profile_out,
        )
    finally:
        _stop(grpc_server)


def main() -> int:
    args = parse_args()

    profile_off: Optional[Path] = None
    profile_on: Optional[Path] = None
    if args.profile:
        pdir = Path(args.profile_dir)
        pdir.mkdir(parents=True, exist_ok=True)
        profile_off = pdir / f"it_test_server_off_{args.mode}.prof"
        profile_on = pdir / f"it_test_server_on_{args.mode}.prof"

    # gRPC modes run two instrumented processes. Restart the backend for each
    # variant so the off baseline disables Pinpoint on both halves and the on
    # measurement enables it on both halves.
    off = _run_variant_with_backend(
        "off (no pinpoint)", args, with_pinpoint=False,
        profile_out=profile_off,
    )
    on = _run_variant_with_backend(
        "on  (pinpoint active)", args, with_pinpoint=True,
        profile_out=profile_on,
    )

    print_report(off, on, args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
