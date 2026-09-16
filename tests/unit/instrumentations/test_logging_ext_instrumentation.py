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

"""stdlib logging instrumentation."""

from __future__ import annotations

import logging

import pytest

from _fakes import FakeNativeSpan
from pinpoint import context as ppctx
from pinpoint.instrumentations import logging_ext
from pinpoint.tracer import Span


@pytest.fixture(autouse=True)
def _restore_log_record_factory():
    original_factory = logging.getLogRecordFactory()
    original_previous = logging_ext._previous_factory
    logging.setLogRecordFactory(logging.LogRecord)
    logging_ext._previous_factory = None
    yield
    logging.setLogRecordFactory(original_factory)
    logging_ext._previous_factory = original_previous


def _record():
    return logging.getLogRecordFactory()(
        "pinpoint.test",
        logging.INFO,
        __file__,
        1,
        "message",
        (),
        None,
    )


def test_logging_stamps_current_span_ids_on_every_record():
    span = Span(FakeNativeSpan(), trace_id="trace-id", span_id=7)
    token = ppctx.set_current_span(span)
    try:
        logging_ext._install_factory()

        records = [_record(), _record(), _record()]

        assert [r.PtxId for r in records] == ["trace-id"] * 3
        assert [r.PspanId for r in records] == ["7"] * 3
        # Stamping the ids flags the span as logged (Go ppslog.NewAttrs).
        assert span._logging is True
    finally:
        ppctx.reset_current_span(token)


def test_logging_flag_reaches_native_at_span_end():
    native = FakeNativeSpan()
    span = Span(native, trace_id="trace-id", span_id=7)
    token = ppctx.set_current_span(span)
    try:
        logging_ext._install_factory()
        _record()
    finally:
        ppctx.reset_current_span(token)
    assert native.logging is False, "the flag is buffered until end()"
    span.end()
    assert native.logging is True


def test_span_without_log_records_is_not_flagged():
    native = FakeNativeSpan()
    Span(native, trace_id="trace-id", span_id=7).end()
    assert native.logging is False


def test_logging_uses_placeholder_for_unsampled_span():
    class _UnsampledSpan:
        sampled = False

        @property
        def trace_id(self):
            raise AssertionError("unsampled span must not read trace_id")

        @property
        def span_id(self):
            raise AssertionError("unsampled span must not read span_id")

    token = ppctx.set_current_span(_UnsampledSpan())  # type: ignore[arg-type]
    try:
        logging_ext._install_factory()

        record = _record()

        assert record.PtxId == logging_ext._NO_TRACE_ID
        assert record.PspanId == logging_ext._NO_SPAN_ID
    finally:
        ppctx.reset_current_span(token)


def test_null_span_set_logging_is_a_chainable_noop():
    from pinpoint.agent import _NullSpan

    span = _NullSpan()
    assert span.set_logging() is span


def test_logging_uses_placeholder_when_no_span_active():
    """A record logged outside any span (startup, background thread) must still
    carry the attributes so ``%(PtxId)s`` formats cleanly."""
    logging_ext._install_factory()

    record = _record()

    assert record.PtxId == logging_ext._NO_TRACE_ID
    assert record.PspanId == logging_ext._NO_SPAN_ID
    # The documented format string must not drop the line.
    formatter = logging.Formatter("%(PtxId)s %(PspanId)s %(message)s")
    assert formatter.format(record) == "- - message"


def test_factory_swallows_native_failures():
    """User logging must never raise because of us — a stale span whose
    accessors blow up gets skipped, not propagated."""

    class _ExplodingSpan:
        sampled = True

        @property
        def trace_id(self):
            raise RuntimeError("span gone")

        @property
        def span_id_str(self):
            raise RuntimeError("span gone")

    logging_ext._install_factory()
    token = ppctx.set_current_span(_ExplodingSpan())  # type: ignore[arg-type]
    try:
        record = _record()  # must not raise
    finally:
        ppctx.reset_current_span(token)
    # The native failure is swallowed and the placeholder is used instead.
    assert record.PtxId == logging_ext._NO_TRACE_ID
    assert record.PspanId == logging_ext._NO_SPAN_ID


def test_uninstrument_restores_previous_factory_and_allows_reinstall():
    """Uninstrument must restore the *pre-existing* factory (not the bare
    ``logging.LogRecord``) and clear the guard so a re-install works."""

    def custom_factory(*args, **kwargs):
        return logging.LogRecord(*args, **kwargs)

    logging.setLogRecordFactory(custom_factory)

    logging_ext._install_factory()
    assert logging.getLogRecordFactory() is logging_ext._factory

    logging_ext._uninstall_factory()
    assert logging.getLogRecordFactory() is custom_factory
    assert logging_ext._previous_factory is None

    # Re-install actually installs again (guard was cleared).
    logging_ext._install_factory()
    assert logging.getLogRecordFactory() is logging_ext._factory
    logging_ext._uninstall_factory()


def test_uninstall_leaves_third_party_factory_chained_after_ours():
    """If someone chained their own factory after ours, uninstall must not
    yank it out from under them."""
    logging_ext._install_factory()

    def third_party(*args, **kwargs):
        return logging.LogRecord(*args, **kwargs)

    logging.setLogRecordFactory(third_party)
    logging_ext._uninstall_factory()
    assert logging.getLogRecordFactory() is third_party


def test_install_is_idempotent_when_already_current():
    """Calling install twice must not wrap our own factory as the previous
    one (which would recurse on every record)."""
    logging_ext._install_factory()
    assert logging.getLogRecordFactory() is logging_ext._factory
    prev = logging_ext._previous_factory

    logging_ext._install_factory()
    assert logging.getLogRecordFactory() is logging_ext._factory
    # Previous factory unchanged — we did not capture ourselves.
    assert logging_ext._previous_factory is prev
    assert logging_ext._previous_factory is not logging_ext._factory

    _record()  # must not RecursionError


def test_reinstrument_after_third_party_chain_does_not_recurse():
    """instrument -> third party chains our factory -> uninstrument ->
    instrument again must not cause RecursionError on the next record."""
    logging_ext._install_factory()

    # A third party captures the current factory (ours) and chains it.
    chained = logging.getLogRecordFactory()

    def third_party(*args, **kwargs):
        return chained(*args, **kwargs)

    logging.setLogRecordFactory(third_party)

    # Uninstrument leaves the third-party wrapper in place, then re-instrument.
    logging_ext._uninstall_factory()
    logging_ext._install_factory()

    record = _record()  # must not RecursionError
    assert record.PtxId == logging_ext._NO_TRACE_ID
    assert record.PspanId == logging_ext._NO_SPAN_ID


def _installed_logger(name):
    """A logger with the full instrumentation (factory + makeRecord wrapper)
    installed, torn down by the caller via ``uninstrument``."""
    inst = logging_ext.LoggingInstrumentor()
    inst.instrument()
    return inst, logging.getLogger(name)


def test_extra_carrying_our_keys_does_not_raise(caplog):
    """stdlib makeRecord raises KeyError when an ``extra`` key already sits on
    the record, and our factory puts PtxId/PspanId on every record — so a
    caller's own placeholder (``extra`` or a LoggerAdapter) must not break
    their log call."""
    inst, log = _installed_logger("pinpoint.test.extra")
    try:
        with caplog.at_level(logging.INFO, logger=log.name):
            log.info("direct", extra={"PtxId": "manual", "PspanId": "m"})
            logging.LoggerAdapter(log, {"PtxId": "-", "PspanId": "-"}).info("adapter")
    finally:
        inst.uninstrument()
    direct, adapter = caplog.records
    # No span current: the caller's value fills in for the placeholder.
    assert direct.PtxId == "manual" and direct.PspanId == "m"
    assert adapter.PtxId == "-" and adapter.PspanId == "-"


def test_live_span_ids_win_over_extra_placeholders(caplog):
    span = Span(FakeNativeSpan(), trace_id="trace-id", span_id=7)
    token = ppctx.set_current_span(span)
    inst, log = _installed_logger("pinpoint.test.extra_live")
    try:
        with caplog.at_level(logging.INFO, logger=log.name):
            log.info("hello", extra={"PtxId": "-", "PspanId": "-", "other": 1})
    finally:
        inst.uninstrument()
        ppctx.reset_current_span(token)
    (record,) = caplog.records
    assert record.PtxId == "trace-id"
    assert record.PspanId == span.span_id_str
    assert record.other == 1                      # unrelated extra keys survive
