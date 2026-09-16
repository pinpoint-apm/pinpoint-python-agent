# API overhead — measured results

Produced by `benchmark/api_overhead/run.sh --repeats 5 --raw raw.tsv`
(defaults otherwise); see [README.md](README.md) for what the two variants are
and how to read them.

## Environment

| | |
|---|---|
| machine | Apple M4 Pro, macOS 15.3.1 (Darwin 24.3.0 arm64) |
| core | pinpoint-cpp-agent submodule `847e30c` (v2.0.0-python-agent branch), Release (`-O3 -DNDEBUG`), Apple clang 16.0.0 |
| python | CPython 3.14.4, GIL enabled, no JIT, `.venv` |
| collector | `tests/integration/_collector.py` mock collector, hosted in the driver process |
| schedule | 5 interleaved repetitions, 2500 ops/scenario (625 for `s5*`), 2000 warmup, 3000 ms drain |
| config | sampling 1/1, `span_queue_size=65536`, stats off, log error, callstack trace and URL stat off |

Measured 2026-08-26. **This record does not continue the previous one.** That
one ran on an Apple M1 Pro against core `2be014a`; this one is a different
machine and a different core commit, so the two tables differ by hardware,
core version and Python-layer changes at once and no row-to-row diff between
them attributes anything. Every comparison below is therefore internal to this
run — `on` against `off`, both interleaved on the same machine in the same
session, which is what the harness is built to make valid.

The machine was otherwise idle but this was not a controlled-idle session.

## Validity

| check | result |
|---|---|
| span delivery | 99,751 created / 99,751 delivered, all 5 repetitions |
| steady state (`s1_span_lifecycle` vs `s6_threads_1`, same workload) | 11,767 vs 11,979 ns/op — 1.8% apart |
| sampled / unsampled probe | sampled span sampled, `Pinpoint-Sampled: s0` took the unsampled path |
| repetition stability (`--raw`) | per-repetition totals within 0.9% of each other |

Nothing was dropped, so no row measures the cheap drop path, and the run
reached steady state before it was timed.

## Results

Medians over 5 repetitions, ns/op. `added` is `on − off`: what tracing an
operation of this shape costs. `off` is the residual cost of the same calls
with the agent disabled. `calls` counts Python-level public-API calls per
operation, `per call` divides `added` by it.

| scenario | thr | off | on | added | spread (on) | calls | per call | on p50 | on p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| s1_span_lifecycle | 1 | 528 | 11,767 | +11,239 | ±3% | 15 | 749 | 11,625 | 32,458 |
| s2_unsampled | 1 | 232 | 5,028 | +4,796 | ±50%※ | 5 | 959 | 5,125 | 6,667 |
| s3_annotation_heavy | 1 | 489 | 10,847 | +10,359 | ±2% | 14 | 740 | 10,291 | 20,833 |
| s4a_sql_hit | 1 | 348 | 9,608 | +9,259 | ±8% | 8 | 1,157 | 8,708 | 22,500 |
| s4b_sql_hit_binds | 1 | 352 | 9,737 | +9,385 | ±2% | 8 | 1,173 | 9,083 | 25,083 |
| s4c_sql_miss | 1 | 364 | 12,285 | +11,920 | ±28%※ | 8 | 1,490 | 11,083 | 29,500 |
| s5a_deep_events (30 ev) | 1 | 1,591 | 39,072 | +37,481 | ±1% | 63 | 595 | 39,375 | 45,917 |
| s5b_wide_events (100 ev) | 1 | 3,696 | 121,528 | +117,832 | ±1% | 203 | 580 | 120,667 | 149,708 |
| s7_propagation | 1 | 364 | 10,184 | +9,820 | ±2% | 8 | 1,228 | 9,833 | 23,917 |
| s6_threads_1 | 1 | 517 | 11,979 | +11,462 | ±4% | 15 | 764 | 11,583 | 25,458 |
| s6_threads_2 | 2 | 1,023 | 25,537 | +24,514 | ±3% | 15 | | 11,750 | 244,375 |
| s6_threads_4 | 4 | 2,073 | 57,072 | +54,999 | ±2% | 15 | | 13,333 | 624,583 |
| s6_threads_8 | 8 | 4,116 | 115,378 | +111,262 | ±1% | 15 | | 13,625 | 1,318,583 |

※ Both wide spreads are one-sided excursions in repetition 5 alone
(`s2` 2,510 against 4,977–5,170 elsewhere; `s4c` 8,883 against 12,074–12,739),
while that repetition's total across all rows sits within 0.9% of the others.
So they are scenario-local, not a slow or fast repetition, and the medians —
which four repetitions agree on — are sound. The spread column overstates the
uncertainty on exactly these two rows.

Peak RSS: off 42.5 MiB, on 70.6 MiB (+28.0 MiB) — dominated by
`span_queue_size=65536`, not the shipped 1024 default. Per-op allocation
counts are not measured (no cheap cumulative counter in CPython).

## Reading the numbers

**Cost splits cleanly into per-span and per-event.** `s5a` and `s5b` share one
span shape and differ only in event count, and both are the tightest rows in
the table (±1%), so the two points fit a line worth quoting: **≈1.15 µs per
span event, on a ≈3.0 µs per-span floor.** The floor covers root-span creation
including the `Pinpoint-*` header extraction `new_span` now runs per call
(measured on its own at 487 ns for this 5-key inbound dict), and the
end-of-span flush.

**Per public-API call the median is 0.86 µs, but the range is real** (0.58–1.49
µs), so call-counting is a weaker predictor here than the span/event split
above. The cheap end is the event-dense shapes (s5a/s5b at 0.58–0.60 µs) whose
calls are event create/end pairs replayed in one batch. The expensive end is
the calls that do work rather than wrapping: `set_sql_query` normalizes the
statement (s4a/s4b 1.16–1.17 µs, s4c's cache miss 1.49 µs) and s7's 1.23 µs
folds in `propagator.inject_items` building the outbound pairs.

**The disabled variant costs 232–528 ns per operation** for the same calls
(1,591–3,696 ns for the 30/100-event shapes) — 18–46 ns per no-op call, which
is Python call dispatch and nothing else: `_NullAgent` hands out a no-op span
that never crosses pybind11. Leaving instrumentation in the source with the
agent off costs 22–34× less than tracing.

**Sampling is the cheapest lever.** `s2_unsampled` adds 4.8 µs against s1's
11.2 µs. With sampling 1-in-N, N−1 of every N requests take that path, so the
`UnSampledSpan` design (annotations never cross the boundary) is what most
requests actually pay.

**In application terms:** a request traced with an s1-shaped span (root plus
three events) pays 11.2 µs. Served in 10 ms that is 0.11%; in 1 ms, 1.1%.
Server-level confirmation of that ratio — CPU%, RSS and RPS of a FastAPI app
with the agent on and off — is a separate measurement:
`tests/e2e/run-compare.sh`.

**Threads cost close to plain GIL serialization.** The `off` column grows
1.98–2.03× per thread doubling — pure serialization, so uninstrumented
aggregate throughput is flat (1.93 M ops/s at 1 thread, 1.94 M at 8). The `on`
column grows 2.13×, 2.23×, 2.02×, so traced aggregate throughput falls only
1.20× across that range (83.5 K ops/s single-threaded against 69.3 K at 8
threads). Tracing therefore adds a small extra threading penalty on top of the
GIL rather than a compounding one. The per-op p50 stays flat at 11.6–13.6 µs
while p99 climbs to 1.3 ms: threads run batches of ops between GIL handoffs
and then pay one long wait, so read `s6_threads_N` as throughput cost, not as
a latency curve, and never as a cross-core scaling curve — this is standard
CPython with the GIL on. A free-threaded interpreter is a different
experiment, worth running once, on purpose.
