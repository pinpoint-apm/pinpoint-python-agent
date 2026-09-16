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

"""stdlib `logging` integration: attaches trace/span IDs to every record.

We populate `record.PtxId` and `record.PspanId` on *every* record, the keys the
Pinpoint web UI links a log line to a span by. When a Python log record is
emitted within a sampled Pinpoint transaction they carry the live IDs;
otherwise (startup logs, unsampled requests, background threads) they carry a
placeholder (`"-"`). Users can then put `%(PtxId)s` in their log format to
correlate logs with traces in the Pinpoint UI — and the format string never
drops a line for a missing attribute, no matter where the record originates.

Stamping the live IDs also flags the span as logged (`Span.set_logging`), which
is what makes the Pinpoint UI offer the span's log lines. The IDs themselves
never cross into native: the span wrapper already holds them.
"""

from __future__ import annotations

import logging as _stdlog
import threading
from ...context import current_span
from ...instrumentor import BaseInstrumentor
from .._util import replace_arg, wrap

# Record attribute names, as the Pinpoint UI's log-to-span lookup expects them.
LOG_TRANSACTION_ID_KEY = "PtxId"
LOG_SPAN_ID_KEY = "PspanId"

# Placeholder written when no sampled span is current, so the documented
# ``%(PtxId)s`` format never raises ``KeyError`` and drops the line.
_NO_TRACE_ID = "-"
_NO_SPAN_ID = "-"


class LoggingInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        _install_factory()
        # ``Logger.makeRecord`` raises ``KeyError("Attempt to overwrite ...")``
        # when an ``extra`` key already sits on the record — and our factory
        # puts PtxId/PspanId on every record. Without this a user's
        # ``extra={"PtxId": ...}`` or a LoggerAdapter carrying the same keys as
        # placeholders would turn every log call into an exception.
        wrap("logging", "Logger.makeRecord", _make_record_wrapper)

    def _uninstrument(self) -> None:
        _uninstall_factory()


_previous_factory = None
# Per-thread re-entrancy guard. If a third party chained our factory into their
# own and we later re-instrument, ``_previous_factory`` can end up being a
# wrapper that calls back into ``_factory`` — this breaks that loop.
_reentry = threading.local()
# The guard costs three thread-local ops on every record the app emits, and a
# loop is only possible when the captured factory is a third-party wrapper.
# Computed once at install so the stock-``LogRecord`` case skips it entirely.
_needs_reentry_guard = False


def _install_factory() -> None:
    """Install our record factory, capturing the previous one to chain.

    Idempotent and re-instrument-safe: if our factory is already the current
    one we do nothing, so we never wrap ourselves nor capture our own wrapper
    as ``_previous_factory``.
    """
    global _previous_factory, _needs_reentry_guard
    if _stdlog.getLogRecordFactory() is _factory:
        return
    _previous_factory = _stdlog.getLogRecordFactory()
    _needs_reentry_guard = _previous_factory is not _stdlog.LogRecord
    _stdlog.setLogRecordFactory(_factory)


def _uninstall_factory() -> None:
    """Restore whatever factory was active before ours.

    Restores ``_previous_factory`` (not the bare ``logging.LogRecord``) so a
    pre-existing custom factory survives, and clears the guard so a later
    re-instrument actually installs again. If a third party chained its own
    factory after ours, leave it alone — swapping it out from under them
    would break their chain.
    """
    global _previous_factory, _needs_reentry_guard
    if _stdlog.getLogRecordFactory() is _factory:
        _stdlog.setLogRecordFactory(_previous_factory or _stdlog.LogRecord)
    _previous_factory = None
    _needs_reentry_guard = False


def _factory(*args, **kwargs):
    if not _needs_reentry_guard:
        # The captured factory is the stock ``LogRecord`` (or gone): it cannot
        # chain back into us, so skip the per-record thread-local dance.
        record = (_previous_factory or _stdlog.LogRecord)(*args, **kwargs)
    elif getattr(_reentry, "active", False):
        # A captured factory chained back into us: a third party wrapped ours,
        # then we stored that wrapper as ``_previous_factory``. Break the loop with
        # a bare record; the outermost call still stamps the attributes below.
        return _stdlog.LogRecord(*args, **kwargs)
    else:
        _reentry.active = True
        try:
            record = (_previous_factory or _stdlog.LogRecord)(*args, **kwargs)
        finally:
            _reentry.active = False
    # Always set both attributes so the documented format string never drops a
    # line; fall back to the placeholder when no sampled span is current.
    trace_id = _NO_TRACE_ID
    span_id = _NO_SPAN_ID
    # Logging must never raise because of us: trace_id/span_id cross the
    # pybind11 boundary on first access, and a stale span left on some
    # context could fail there. Swallow everything.
    try:
        span = current_span()
        if span is not None and getattr(span, "sampled", False):
            trace_id = span.trace_id
            span_id = span.span_id_str
            # The ids are on their way into a log line: flag the span so the
            # UI links the line to it (a wrapper bool, flushed at end()).
            span.set_logging()
    except Exception:  # noqa: BLE001
        pass
    setattr(record, LOG_TRANSACTION_ID_KEY, trace_id)
    setattr(record, LOG_SPAN_ID_KEY, span_id)
    return record


# makeRecord(name, level, fn, lno, msg, args, exc_info, func, extra, sinfo) —
# wrapt passes the wrapper its args without ``self``.
_EXTRA_INDEX = 8
_OUR_KEYS = (LOG_TRANSACTION_ID_KEY, LOG_SPAN_ID_KEY)


def _make_record_wrapper(wrapped, instance, args, kwargs):
    extra = args[_EXTRA_INDEX] if len(args) > _EXTRA_INDEX else kwargs.get("extra")
    if not extra or not any(key in extra for key in _OUR_KEYS):
        return wrapped(*args, **kwargs)
    # Strip our keys so stdlib's overwrite check passes; the factory has
    # already stamped the record. A live sampled span wins over the caller's
    # value — that is the correlation the keys exist for — while the caller's
    # value fills in where we would only have written the placeholder.
    ours = {key: extra[key] for key in _OUR_KEYS if key in extra}
    rest = {key: value for key, value in extra.items() if key not in ours}
    args, kwargs = replace_arg(args, kwargs, _EXTRA_INDEX, "extra", rest or None)
    record = wrapped(*args, **kwargs)
    for key, value in ours.items():
        if getattr(record, key, _NO_TRACE_ID) == _NO_TRACE_ID:
            setattr(record, key, value)
    return record


def instrument() -> None:
    LoggingInstrumentor().instrument()
