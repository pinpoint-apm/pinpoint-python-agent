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

"""What instrumenting an operation costs, measured against not instrumenting it.

Each scenario is one traced operation expressed through this agent's public
API — a root span plus the child events, annotations, SQL or context
propagation that instrumentation adds. Every scenario is measured twice:

- **on** — agent enabled against a collector, sampling 1/1. Spans are built,
  queued, serialized and delivered.
- **off** — agent initialized with ``enabled=False``. ``init()`` hands back a
  ``_NullAgent``, so the very same calls resolve to the Python no-op span and
  never cross the pybind11 boundary.

``on − off`` per operation is the number to quote: that is what tracing an
operation of this shape costs. The absolute ``off`` column is the residual
cost of leaving instrumentation calls in the source with the agent switched
off — the honest "not instrumented" state for code that keeps the calls.

Agent state is process-wide and cannot be flipped in place, so the driver runs
each variant as a subprocess, interleaved per repetition (thermal drift over
minutes otherwise reads as a variant difference), and reports medians with the
min–max spread. A difference smaller than the spread is noise, not a result.

    scripts/dev.sh release   # asserted at startup
    benchmark/api_overhead/run.sh --repeats 5

Rules that keep the numbers meaningful:

- **Check span delivery before reading any timing.** The agent drops spans
  once the queue saturates, and the drop path is cheaper than the send path —
  a saturated run measures as faster while delivering less. Every ``on``
  repetition reports the spans it created, the driver's collector reports what
  arrived, and a repetition that delivered less is called out as invalid.
- Drain between scenarios so none inherits the previous one's backlog.
- Buy precision with repetitions, not with more ops per repetition.
- Idle machine, mains power.

Two properties of the Python side to read with:

- The measured loop pays one Python function call per op (the scenario body).
  It is a fixed ~100 ns in both variants, so it cancels in ``on − off``.
- ``s6_threads_N`` measures the GIL, not cross-core contention. This is
  standard CPython with the GIL on, so the per-thread ns/op growth is real
  cost a threaded Python application pays — it just is not a scaling curve.
"""

from __future__ import annotations

import argparse
import math
import os
import resource
import statistics
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Sequence

LATENCY_SAMPLE_EVERY = 16  # two clock reads per op would otherwise dominate

REPO_ROOT = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------------------
# Refuse to measure a Debug build
# ---------------------------------------------------------------------------
# The package wires _native through symlinks; a debug-preset artifact loads
# and runs identically but its numbers are meaningless. Resolve the real
# build tree and check its CMakeCache rather than trusting wiring intentions.


def assert_release_native(native_module) -> None:
    real = os.path.realpath(native_module.__file__)
    build_dir = os.path.dirname(real)
    cache = os.path.join(build_dir, "CMakeCache.txt")
    if not os.path.exists(cache):
        print(f"[bench] warning: no CMakeCache.txt next to {real}; "
              "cannot verify this is a Release build", file=sys.stderr)
        return
    with open(cache, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if line.startswith("CMAKE_BUILD_TYPE:"):
                build_type = line.split("=", 1)[1].strip()
                if build_type not in ("Release", "RelWithDebInfo"):
                    raise SystemExit(
                        f"[bench] refusing to run: _native resolves to a "
                        f"'{build_type}' build ({real}). Build the release "
                        f"preset and re-point the package symlinks "
                        f"(scripts/dev.sh release).")
                return
    print(f"[bench] warning: CMAKE_BUILD_TYPE not found in {cache}",
          file=sys.stderr)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def inbound_headers() -> dict[str, str]:
    return {
        "Host": "bench.local:8080",
        "User-Agent": "pinpoint-api-overhead/1.0",
        "Accept": "*/*",
        "Content-Type": "application/json",
        "X-Forwarded-For": "10.0.0.7",
    }


def unsampled_headers() -> dict[str, str]:
    from pinpoint import propagator
    headers = inbound_headers()
    # Carries an explicit unsampled decision so the agent takes the
    # continue-unsampled path without changing process-wide sampling config.
    headers[propagator.HEADER_SAMPLED] = "s0"
    headers[propagator.HEADER_TRACE_ID] = "bench-agent^1700000000000^42"
    headers[propagator.HEADER_SPAN_ID] = "7777"
    headers[propagator.HEADER_PARENT_SPAN_ID] = "8888"
    headers[propagator.HEADER_FLAG] = "0"
    return headers


def sql_statement_pool(count: int) -> list[str]:
    return [
        f"SELECT o.id, o.total, c.name FROM orders_{i}"
        " o JOIN customers c ON c.id = o.customer_id "
        "WHERE o.status = 'SHIPPED' AND o.created_at > ? AND o.total > ? "
        "ORDER BY o.created_at DESC LIMIT 100"
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# Measurement harness
# ---------------------------------------------------------------------------


def percentile(sorted_samples: Sequence[int], fraction: float) -> float:
    """Nearest-rank percentile."""
    if not sorted_samples:
        return 0.0
    return float(sorted_samples[math.ceil(fraction * len(sorted_samples)) - 1])


class Harness:
    def __init__(self, label: str, warmup_ops: int, drain_ms: int,
                 only_scenario: str) -> None:
        self.label = label
        self.warmup_ops = warmup_ops
        self.drain_ms = drain_ms
        self.only_scenario = only_scenario
        # Every scenario creates exactly one span per operation; comparing
        # this count against what the collector received is how a run proves
        # it did not silently measure the drop path.
        self.recording_spans = 0

    def run_scenario(self, name: str, threads: int, ops_per_thread: int,
                     body: Callable[[int, int], None],
                     records: bool = True) -> dict | None:
        if self.only_scenario and name != self.only_scenario:
            return None

        ready = threading.Barrier(threads + 1)
        go = threading.Event()
        per_thread: list[list[int]] = [[] for _ in range(threads)]

        def worker(t: int) -> None:
            samples = per_thread[t]
            # Warm the thread and any lazily created state (metadata id
            # caches, code objects) so the measured loop is steady-state.
            for i in range(self.warmup_ops):
                body(t, i)
            ready.wait()
            go.wait()

            perf = time.perf_counter_ns
            for i in range(ops_per_thread):
                if i % LATENCY_SAMPLE_EVERY == 0:
                    started = perf()
                    body(t, i)
                    samples.append(perf() - started)
                else:
                    body(t, i)

        workers = [threading.Thread(target=worker, args=(t,), daemon=True)
                   for t in range(threads)]
        for w in workers:
            w.start()
        ready.wait()          # all threads warmed up and parked at the gate
        started = time.perf_counter_ns()
        go.set()
        for w in workers:
            w.join()
        elapsed_ns = time.perf_counter_ns() - started

        samples = sorted(s for thread_samples in per_thread
                         for s in thread_samples)

        if records:
            self.recording_spans += (ops_per_thread + self.warmup_ops) * threads

        # Let the sender catch up before the next scenario is timed.
        time.sleep(self.drain_ms / 1000.0)

        return {
            "name": name,
            "threads": threads,
            "total_ops": ops_per_thread * threads,
            # Wall time divided by per-thread ops. Growth across thread counts
            # is mostly GIL serialization — real cost, see the module docstring.
            "ns_per_op": elapsed_ns / float(ops_per_thread),
            "p50": percentile(samples, 0.50),
            "p99": percentile(samples, 0.99),
        }

    def emit(self, result: dict | None) -> None:
        if not result:
            return
        print("RESULT\t{}\t{}\t{}\t{:.1f}\t{:.1f}\t{:.1f}\t{}".format(
            self.label, result["name"], result["threads"],
            result["ns_per_op"], result["p50"], result["p99"],
            result["total_ops"]), flush=True)


def peak_rss_kib() -> int:
    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux reports kibibytes.
    return maxrss // 1024 if sys.platform == "darwin" else maxrss


# ---------------------------------------------------------------------------
# Worker: one variant, one repetition
# ---------------------------------------------------------------------------


def run_worker(args: argparse.Namespace) -> int:
    import pinpoint
    from pinpoint import _native, propagator
    from pinpoint.annotation import (
        ANNOTATION_API,
        ANNOTATION_HTTP_REQUEST_HEADER,
        ANNOTATION_HTTP_STATUS_CODE,
        ANNOTATION_HTTP_URL,
    )
    from pinpoint.service_type import (
        APP_TYPE_PYTHON,
        SERVICE_TYPE_MYSQL,
        SERVICE_TYPE_PYTHON_HTTP_CLIENT,
        SERVICE_TYPE_PYTHON_METHOD,
    )

    assert_release_native(_native)

    label = args.variant
    instrumented = label == "on"

    # Queue sized to absorb a whole scenario without dropping (65536 is
    # MAX_SPAN_QUEUE_SIZE in the core; larger values silently reset to the
    # 1024 default). At the shipped default the sender saturates and takes the
    # cheap drop path, measuring as faster while delivering less. The cost:
    # peak RSS reflects this setting, not a shipped default.
    agent = pinpoint.init(
        application_name="py-api-overhead",
        collector_host=args.host,
        collector_agent_port=args.collector_port,
        collector_span_port=args.collector_port,
        collector_stat_port=args.collector_port,
        sampling_type="COUNTER",
        sampling_counter_rate=1,
        span_queue_size=65536,
        stat_enabled=False,
        log_level="ERROR",
        enable_callstack_trace=False,
        http_collect_url_stat=False,
        enabled=instrumented,
    )

    if instrumented:
        print(f"[{label}] agent against {args.host}:{args.collector_port}",
              file=sys.stderr)
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline and not agent.enabled:
            time.sleep(0.02)
        if not agent.enabled:
            print(f"[{label}] agent never reported enabled; a collector must "
                  "be reachable, otherwise only noop spans are measured",
                  file=sys.stderr)
            return 1
        print(f"[{label}] agent enabled", file=sys.stderr)
    else:
        # enabled=False resolves init() to _NullAgent: no native agent, no
        # collector, nothing to drain between scenarios.
        if agent.enabled:
            print(f"[{label}] agent reports enabled with enabled=False; "
                  "aborting rather than measuring the wrong variant",
                  file=sys.stderr)
            return 1
        print(f"[{label}] agent disabled (no-op spans)", file=sys.stderr)

    harness = Harness(label, args.warmup,
                      args.drain_ms if instrumented else 0, args.scenario)

    statements = sql_statement_pool(4096)
    binds = ("1700000000000", "99.50", "SHIPPED", "100")
    no_binds = ()

    # One dict each, built here and shared by every thread: the agent only
    # reads them (extract_pinpoint_headers copies what it needs), so
    # construction stays out of the timed region and out of the op cost.
    sampled_hdrs = inbound_headers()
    unsampled_hdrs = unsampled_headers()

    # --- scenario bodies ---------------------------------------------------

    def span_lifecycle(t: int, i: int) -> None:
        # S1: one sampled span with three child events — metadata id caches,
        # per-span wrapper cost, pybind boundary, queue enqueue at end().
        span = agent.new_span("BenchController.handle()", "/bench/order",
                              sampled_hdrs)
        span.set_service_type(APP_TYPE_PYTHON)
        span.set_remote_address("10.0.0.7")
        span.set_end_point("bench.local:8080")
        for _ in range(3):
            event = span.new_span_event("OrderService.step()",
                                        SERVICE_TYPE_PYTHON_METHOD)
            event.set_end_point("bench.local:8080")
            event.end()
        span.set_status_code(200)
        span.end()

    def unsampled(t: int, i: int) -> None:
        # S2: the continue-unsampled path, entered via Pinpoint-Sampled: s0.
        span = agent.new_span("BenchController.handle()", "/bench/order",
                              unsampled_hdrs)
        event = span.new_span_event("OrderService.step()",
                                    SERVICE_TYPE_PYTHON_METHOD)
        event.annotate_string(ANNOTATION_HTTP_URL, "/bench/order")
        event.end()
        span.end()

    def annotation_heavy(t: int, i: int) -> None:
        # S3: annotation recording and its buffering/ownership.
        span = agent.new_span("BenchController.annotated()",
                              "/bench/annotated", sampled_hdrs)
        span.set_service_type(APP_TYPE_PYTHON)
        span.annotate_string(ANNOTATION_HTTP_URL, "/bench/annotated?page=3")
        span.annotate_int(ANNOTATION_HTTP_STATUS_CODE, 200)

        event = span.new_span_event("OrderService.annotate()",
                                    SERVICE_TYPE_PYTHON_METHOD)
        event.annotate_string(ANNOTATION_API,
                              "OrderService.annotate(String, int)")
        event.annotate_int(ANNOTATION_HTTP_STATUS_CODE, 200)
        event.annotate_long(ANNOTATION_API, 1700000000000)
        event.annotate_string(ANNOTATION_HTTP_URL,
                              "https://downstream.local/api/v2/orders")
        event.annotate_string_string(ANNOTATION_HTTP_REQUEST_HEADER,
                                     "page", "3")
        event.annotate_string_string(ANNOTATION_HTTP_REQUEST_HEADER,
                                     "sort", "created_at:desc")
        event.end()
        span.end()

    def sql_span(statement: str, sql_binds) -> None:
        span = agent.new_span("BenchController.query()", "/bench/query",
                              sampled_hdrs)
        span.set_service_type(APP_TYPE_PYTHON)
        event = span.new_span_event("OrderRepository.find()",
                                    SERVICE_TYPE_MYSQL)
        event.set_destination("orders-db")
        event.set_end_point("mysql.local:3306")
        event.set_sql_query(statement, sql_binds)
        event.end()
        span.end()

    def nested_events(depth: int, width: int) -> None:
        # S5: event-shape stress; depth nests before unwinding, width runs
        # events sequentially.
        span = agent.new_span("BenchController.nested()", "/bench/nested",
                              sampled_hdrs)
        span.set_service_type(APP_TYPE_PYTHON)
        stack = [span.new_span_event("Nested.level()",
                                     SERVICE_TYPE_PYTHON_METHOD)
                 for _ in range(depth)]
        for event in reversed(stack):
            event.end()
        for _ in range(width):
            event = span.new_span_event("Sequential.step()",
                                        SERVICE_TYPE_PYTHON_METHOD)
            event.end()
        span.end()

    def propagation(t: int, i: int) -> None:
        # S7: outbound context injection into a caller-owned header dict —
        # the shape every Python HTTP-client instrumentation uses.
        span = agent.new_span("BenchController.call()", "/bench/call",
                              sampled_hdrs)
        span.set_service_type(APP_TYPE_PYTHON)
        event = span.new_span_event("HttpClient.get()",
                                    SERVICE_TYPE_PYTHON_HTTP_CLIENT)
        event.set_destination("downstream.local")
        event.set_end_point("downstream.local:8081")
        outgoing: dict[str, str] = {}
        for key, value in propagator.inject_items(span):
            outgoing[str(key)] = str(value)
        event.end()
        span.end()

    # --- sampled/unsampled probe --------------------------------------------
    # With the agent on, verify the split actually happened; a silent fallback
    # to noop spans would make the whole "on" column meaningless.
    if instrumented:
        probe = agent.new_span("probe", "/probe", sampled_hdrs)
        probe_sampled = probe.sampled
        probe.end()
        uprobe = agent.new_span("probe", "/probe", unsampled_hdrs)
        probe_unsampled_ok = not uprobe.sampled
        uprobe.end()
        print(f"[{label}] probe: sampled={probe_sampled} "
              f"unsampled_path={probe_unsampled_ok}", file=sys.stderr)
        if not probe_sampled:
            print(f"[{label}] sampled probe span was not sampled; aborting",
                  file=sys.stderr)
            return 1
        if not probe_unsampled_ok:
            print(f"[{label}] WARNING: Pinpoint-Sampled:s0 did not yield an "
                  "unsampled span; s2_unsampled measures the sampled path",
                  file=sys.stderr)

    # --- schedule -----------------------------------------------------------

    ops = args.ops
    results = [
        harness.run_scenario("s1_span_lifecycle", 1, ops, span_lifecycle),
        # Unsampled spans are never queued, so they must not count toward what
        # the collector is expected to receive.
        harness.run_scenario("s2_unsampled", 1, ops, unsampled, records=False),
        harness.run_scenario("s3_annotation_heavy", 1, ops, annotation_heavy),
        harness.run_scenario("s4a_sql_hit", 1, ops,
                             lambda t, i: sql_span(statements[0], no_binds)),
        harness.run_scenario("s4b_sql_hit_binds", 1, ops,
                             lambda t, i: sql_span(statements[0], binds)),
        harness.run_scenario(
            "s4c_sql_miss", 1, ops,
            lambda t, i: sql_span(statements[i % len(statements)], no_binds)),
        harness.run_scenario("s5a_deep_events", 1, ops // 4,
                             lambda t, i: nested_events(30, 0)),
        harness.run_scenario("s5b_wide_events", 1, ops // 4,
                             lambda t, i: nested_events(0, 100)),
        harness.run_scenario("s7_propagation", 1, ops, propagation),
    ]
    for threads in (1, 2, 4, 8):
        results.append(harness.run_scenario(
            f"s6_threads_{threads}", threads, ops, span_lifecycle))

    for result in results:
        harness.emit(result)
    print(f"PEAKRSS\t{label}\t{peak_rss_kib()}", flush=True)
    # The sampled probe span above is recorded too. Nothing is recorded with
    # the agent off, so the delivery gate has nothing to check there.
    created = harness.recording_spans + 1 if instrumented else 0
    print(f"SPANS\t{label}\t{created}", flush=True)

    agent.shutdown()
    return 0


# ---------------------------------------------------------------------------
# Driver: interleave the variants, aggregate, compare
# ---------------------------------------------------------------------------


def spawn_worker(args: argparse.Namespace, variant: str,
                 port: int) -> dict[str, object]:
    """Run one variant in a child process and parse its TSV back."""
    cmd = [sys.executable, "-u", os.path.abspath(__file__),
           "--worker", "--variant", variant,
           "--collector-port", str(port),
           "--ops", str(args.ops),
           "--warmup", str(args.warmup),
           "--drain-ms", str(args.drain_ms)]
    if args.scenario:
        cmd += ["--scenario", args.scenario]
    # stderr is inherited so the worker's own progress lines stay live.
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, text=True, check=False)
    if proc.returncode != 0:
        raise SystemExit(f"[driver] {variant} worker failed "
                         f"(rc={proc.returncode})")

    rows: list[dict] = []
    raw: list[str] = []
    peak_rss = 0
    created = 0
    for line in proc.stdout.splitlines():
        parts = line.split("\t")
        if parts[0] == "RESULT":
            rows.append({"name": parts[2], "threads": int(parts[3]),
                         "ns_per_op": float(parts[4]), "p50": float(parts[5]),
                         "p99": float(parts[6])})
            raw.append("\t".join(parts[1:]))
        elif parts[0] == "PEAKRSS":
            peak_rss = int(parts[2])
        elif parts[0] == "SPANS":
            created = int(parts[2])
    if not rows:
        raise SystemExit(f"[driver] {variant} worker produced no results")
    return {"rows": rows, "raw": raw, "peak_rss": peak_rss,
            "created": created}


def run_driver(args: argparse.Namespace) -> int:
    # The mock collector the integration suite already uses: a real grpc.server
    # speaking the collector services on one ephemeral port. It runs here in
    # the driver, never in a worker — Python gRPC handler threads would
    # contend for the GIL with the measured loop.
    sys.path.insert(0, os.path.join(REPO_ROOT, "tests", "integration"))
    # Spawning a worker while this process holds gRPC threads makes gRPC log
    # its fork-handler notices at INFO on every repetition; nothing here is
    # affected by them.
    os.environ.setdefault("GRPC_VERBOSITY", "error")
    from _collector import MockCollector

    collector = MockCollector().start()
    print(f"[driver] mock collector on 127.0.0.1:{collector.port}",
          file=sys.stderr)

    # (name, threads) -> variant -> [ns_per_op per rep]; p50/p99 for "on" only.
    order: list[tuple[str, int]] = []
    samples: dict[tuple[str, int], dict[str, list[float]]] = {}
    latency: dict[tuple[str, int], dict[str, list[float]]] = {}
    peak_rss: dict[str, list[int]] = {"on": [], "off": []}
    delivery: list[tuple[int, int, int]] = []
    # Every repetition's own rows, so an outlier in the min-max column can be
    # traced to one repetition (machine-level) or one scenario (noise). The
    # summary alone cannot tell those apart.
    raw: list[str] = []

    try:
        for rep in range(1, args.repeats + 1):
            for variant in ("on", "off"):
                print(f"[driver] rep {rep}/{args.repeats} variant {variant}",
                      file=sys.stderr)
                outcome = spawn_worker(args, variant, collector.port)
                for row in outcome["rows"]:
                    key = (row["name"], row["threads"])
                    if key not in samples:
                        order.append(key)
                        samples[key] = {"on": [], "off": []}
                        latency[key] = {"p50": [], "p99": []}
                    samples[key][variant].append(row["ns_per_op"])
                    if variant == "on":
                        latency[key]["p50"].append(row["p50"])
                        latency[key]["p99"].append(row["p99"])
                peak_rss[variant].append(outcome["peak_rss"])
                raw.extend(f"{rep}\t{line}" for line in outcome["raw"])
                if variant == "on":
                    created = outcome["created"]
                    # The worker's shutdown flushes before it exits, but the
                    # last batch can still be in flight when wait() returns.
                    deadline = time.monotonic() + 15.0
                    while (len(collector.spans()) < created
                           and time.monotonic() < deadline):
                        time.sleep(0.2)
                    delivery.append((rep, created, len(collector.spans())))
                    # Drop the received protobufs: only the count matters here
                    # and the driver would otherwise grow by a repetition's
                    # worth of spans every repetition. Safe unlocked — the
                    # worker has exited, so no handler is appending.
                    collector.span_messages.clear()
    finally:
        collector.stop()

    if args.raw:
        with open(args.raw, "w", encoding="utf-8") as out:
            out.write("rep\tvariant\tscenario\tthreads\tns_per_op\tp50\tp99"
                      "\ttotal_ops\n")
            out.write("\n".join(raw) + "\n")
        print(f"[driver] raw rows -> {args.raw}", file=sys.stderr)

    print()
    print(f"repeats={args.repeats} ops={args.ops} warmup={args.warmup} "
          f"drain={args.drain_ms}ms  python={sys.version.split()[0]}  "
          f"medians in ns/op")
    print(f"{'scenario':<22}{'thr':>4}{'off':>9}{'on':>10}{'added':>10}"
          f"{'on p50':>9}{'on p99':>9}{'on min-max':>18}")
    added_single = []
    for key in order:
        name, threads = key
        off = statistics.median(samples[key]["off"])
        on = statistics.median(samples[key]["on"])
        on_all = samples[key]["on"]
        if threads == 1:
            added_single.append(on - off)
        print(f"{name:<22}{threads:>4}{off:>9.0f}{on:>10.0f}{on - off:>10.0f}"
              f"{statistics.median(latency[key]['p50']):>9.0f}"
              f"{statistics.median(latency[key]['p99']):>9.0f}"
              f"{min(on_all):>9.0f}-{max(on_all):<9.0f}")

    print()
    print(f"median added cost, single-threaded scenarios: "
          f"{statistics.median(added_single) / 1000.0:.2f} us/op")
    off_rss = statistics.median(peak_rss["off"]) / 1024.0
    on_rss = statistics.median(peak_rss["on"]) / 1024.0
    print(f"peak RSS: off {off_rss:.1f} MiB, on {on_rss:.1f} MiB "
          f"(+{on_rss - off_rss:.1f})")

    invalid = [d for d in delivery if d[2] < d[1]]
    for rep, created, delivered in delivery:
        state = "ok" if delivered >= created else "DROPPED"
        print(f"delivery: rep{rep} created={created} delivered={delivered} "
              f"{state}")
    if invalid:
        print("\nINVALID: a repetition delivered fewer spans than it created. "
              "The agent took the cheap drop path, so the 'on' column "
              "understates the real cost. Lower --ops or raise the queue size "
              "and re-run.", file=sys.stderr)
        return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--worker", action="store_true",
                        help="run one variant in this process (the driver "
                             "spawns these)")
    parser.add_argument("--variant", choices=("on", "off"), default="on",
                        help="worker mode: agent enabled or disabled")
    parser.add_argument("--repeats", type=int, default=5,
                        help="driver mode: interleaved repetitions per variant")
    parser.add_argument("--host", default=None,
                        help="worker mode: collector host (default "
                             "127.0.0.1); the driver hosts its own")
    parser.add_argument("--raw", default="",
                        help="driver mode: write every repetition's rows to "
                             "this TSV, so a min-max outlier can be traced "
                             "to a repetition or a scenario")
    parser.add_argument("--collector-port", type=int, default=9991,
                        help="agent/span/stat ports all point here; the "
                             "driver's mock collector serves all three on "
                             "one ephemeral port")
    parser.add_argument("--ops", type=int, default=2500)
    parser.add_argument("--warmup", type=int, default=2000,
                        help="Python needs more warmup than the op count "
                             "suggests; s1_span_lifecycle and s6_threads_1 "
                             "run the same workload, so a gap between them "
                             "means the run never reached steady state")
    parser.add_argument("--drain-ms", type=int, default=3000)
    parser.add_argument("--scenario", default="",
                        help="run only this scenario")
    args = parser.parse_args(argv)

    # The native agent reads PINPOINT_PY_* itself and they override the config
    # the worker composes, so a leaked dev.sh default (INFO logging, the
    # internal collector host) would silently change what this run measures —
    # or register this benchmark against a real collector. Stripped here so it
    # holds however the benchmark was launched; workers inherit this env.
    for key in [k for k in os.environ if k.startswith("PINPOINT_PY_")]:
        del os.environ[key]

    if args.worker:
        args.host = args.host or "127.0.0.1"
        return run_worker(args)
    if args.host is not None:
        # The driver's collector is in-process; honouring another host would
        # point the workers away from the one counting their spans.
        raise SystemExit("[driver] --host is a worker-mode option; the driver "
                         "hosts its own collector on 127.0.0.1")
    return run_driver(args)


if __name__ == "__main__":
    raise SystemExit(main())
