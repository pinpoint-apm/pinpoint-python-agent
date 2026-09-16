# API overhead: instrumented vs not

What one traced operation costs, measured against the same operation with the
agent switched off. Scenarios s1–s7 each drive the public API the way an
instrumented application does — root span, child events, annotations, SQL,
context propagation — and every one runs under two variants:

| variant | agent | what the calls do |
|---|---|---|
| `on` | `enabled=True`, sampling 1/1, collector attached | spans built, queued, serialized, delivered |
| `off` | `enabled=False` | `init()` returns `_NullAgent`, so the same calls hit the Python no-op span and never cross pybind11 |

`on − off` per operation is the number to quote. The `off` column is the
residual cost of leaving instrumentation calls in the source with the agent
turned off.

Agent state is process-wide, so the driver runs each variant as a subprocess,
interleaved per repetition, and reports medians with the min–max spread. It
hosts the mock collector from `tests/integration/_collector.py` in-process
(never in a worker — Python gRPC handler threads would contend for the GIL
with the measured loop) and uses it as the span-delivery gate.

Measured numbers: [RESULTS.md](RESULTS.md).

Server-level overhead — CPU%, RSS and RPS of a real FastAPI app with the agent
on and off — is a different measurement: `tests/e2e/run-compare.sh`.

## Running

```bash
cmake --build --preset release
scripts/dev.sh release
benchmark/api_overhead/run.sh --repeats 5
```

A Debug `_native` invalidates every number, so the run asserts on the build
type of the resolved artifact. `scripts/dev.sh <preset>` re-points the package
symlinks (docs/development.md §4); switch back to your debug preset afterwards.

A single variant can be run by hand with `--worker --variant on|off`
(`--scenario` narrows it to one row). `--variant on` then needs a collector on
`--host/--collector-port`; `--variant off` needs none.

## Reading the results

- **Check the delivery lines before any timing.** The agent drops spans once
  the queue saturates, and the drop path is cheaper than the send path — a
  saturated run measures as faster while delivering less. Each `on` repetition
  reports spans created and delivered; the driver exits non-zero if any
  repetition delivered less.
- A difference smaller than the `on min-max` spread is noise. Buy precision
  with `--repeats`, not with more `--ops`. When a spread looks wrong, re-run
  with `--raw out.tsv`: the summary cannot tell one slow repetition (the
  machine) from one slow scenario (the code), and the per-repetition rows can.
- Numbers are only comparable within one record. Two runs on different
  machines or different `third_party/pinpoint-cpp-agent` commits move the
  hardware and the core underneath the measurement at once, so a diff across
  records attributes nothing. Re-measure instead of diffing.
- `s1_span_lifecycle` and `s6_threads_1` run the same workload. If they
  disagree, the run never reached steady state — raise `--warmup`.
- `s6_threads_N` measures the GIL, not cross-core scaling. Standard CPython
  with the GIL on; the growth is real cost a threaded app pays, but it is not
  a scaling curve. A free-threaded or JIT-enabled interpreter is a different
  experiment, worth doing once, on purpose.
- Peak RSS on both variants reflects `span_queue_size=65536` (sized so a whole
  scenario cannot saturate it), not the shipped 1024 default.
- Idle machine, mains power, and record the interpreter version and GIL status
  (`sys._is_gil_enabled()`) with any numbers you keep.
