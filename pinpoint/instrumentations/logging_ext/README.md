# logging (log correlation)

Attaches the live trace and span IDs to every stdlib `logging` record, so logs
can be correlated with traces in the Pinpoint UI. This is the only integration
that records nothing itself — it enriches your logs instead.

| | |
|---|---|
| Target module | `logging` (stdlib) |
| Hook point | record factory — every `LogRecord` |
| Adds | `record.PtxId`, `record.PspanId` (same keys as the Go agent's log plugins) |
| Opt-out alias | `logging` |

## What it does

Both attributes are populated on **every** record. Inside a sampled Pinpoint
transaction they carry the live IDs; everywhere else — startup logs, unsampled
requests, background threads — they carry the placeholder `"-"`.

That "always set" property is deliberate: a format string referencing
`%(PtxId)s` would otherwise raise and drop the line for records
originating outside a transaction.

Stamping live IDs also marks the span as logged (`Span.set_logging()`), which
is the flag the Pinpoint UI checks before offering a span's log lines. Only the
flag reaches the native agent, with the rest of the span at `end()`; the IDs
are written by this integration on the Python side.

## Logging the IDs yourself

If a logger bypasses the stdlib record factory (a structured logger with its
own event dict, say), add the IDs from the current span and set the flag:

```python
import pinpoint

span = pinpoint.current_span()
if span is not None and span.sampled:
    span.set_logging()
    log.info("handling request",
             PtxId=span.trace_id, PspanId=span.span_id_str)
```

`set_logging()` only sets a wrapper flag, so calling it once per record costs
nothing measurable.

## Usage

Add the fields to your log format:

```python
import logging

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] "
           "[%(PtxId)s:%(PspanId)s] %(name)s: %(message)s",
)

log = logging.getLogger("myapp")
log.info("handling request")
# 2026-08-21 10:00:00 [INFO] [<agent-id>^1755000000000^42:1234] myapp: handling request
# JSON formatters: emit the two attributes as fields named exactly PtxId and
# PspanId; that is what the Pinpoint UI keys a log line to a span by.

# Outside any transaction:
# 2026-08-21 10:00:00 [INFO] [-:-] myapp: starting up
```

`dictConfig` / `fileConfig` and structured formatters work the same way — the
attributes are on the record, so any formatter can read them.

## Disable

```bash
PINPOINT_PY_DISABLED_INSTRUMENTATIONS=logging
```

Note the alias is `logging` (the target module), not `logging_ext` (the package
directory). A format string referencing the fields will then fail — remove it
too.

## See also

- Unit tests: [`test_logging_ext_instrumentation.py`](../../../tests/unit/instrumentations/test_logging_ext_instrumentation.py)
- [Configuration Guide](../../../docs/config.md) — agent log level and output
