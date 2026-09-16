# pinpoint-python-agent
# Copyright (c) 2026-present NAVER Corp.
# Licensed under the Apache License, Version 2.0.

"""Native logger queue and Python consumer contracts."""

from __future__ import annotations

import logging
import subprocess
import sys
import threading
import time

import pytest

from pinpoint import _native
from pinpoint._native_log import NativeLogConsumer


def _wait_for(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not reached before timeout")


@pytest.fixture
def native_logger():
    logger = logging.getLogger("pinpoint.native")
    parent = logging.getLogger("pinpoint")
    saved = (logger.level, list(logger.handlers), logger.propagate,
             parent.level, list(parent.handlers), parent.propagate)
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)
    logger.propagate = True
    parent.handlers.clear()
    parent.setLevel(logging.DEBUG)
    parent.propagate = False
    yield parent
    (logger.level, logger.handlers[:], logger.propagate,
     parent.level, parent.handlers[:], parent.propagate) = saved


def test_queue_copies_records_and_preserves_fifo_after_source_lifetime():
    bridge = _native.NativeLogBridge(4)
    level = "info"
    message = "owned-message"
    bridge._enqueue_for_test(level, message)
    del level, message
    bridge._enqueue_for_test("warning", "second")

    assert bridge.drain(4) == [
        ("info", "owned-message"),
        ("warning", "second"),
    ]


def test_full_queue_drops_without_blocking_and_counts_exactly():
    bridge = _native.NativeLogBridge(2)
    bridge._enqueue_for_test("info", "one")
    bridge._enqueue_for_test("info", "two")
    started = time.monotonic()
    for _ in range(1000):
        bridge._enqueue_for_test("error", "dropped")

    assert time.monotonic() - started < 1.0
    assert bridge.dropped() == 1000
    assert bridge.take_dropped() == 1000
    assert bridge.dropped() == 0


def test_empty_and_long_utf8_message_are_valid_and_bounded():
    bridge = _native.NativeLogBridge(4)
    bridge._enqueue_for_test("info", "")
    bridge._enqueue_for_test(
        "info", "가" * (_native.NATIVE_LOG_MAX_MESSAGE_BYTES + 10))

    records = bridge.drain(4)
    assert records[0] == ("info", "")
    encoded = records[1][1].encode("utf-8")
    assert len(encoded) <= _native.NATIVE_LOG_MAX_MESSAGE_BYTES
    assert encoded.decode("utf-8") == records[1][1]


def test_queue_byte_budget_drops_even_before_entry_capacity():
    bridge = _native.NativeLogBridge(2048)
    payload = "x" * _native.NATIVE_LOG_MAX_MESSAGE_BYTES
    for _ in range(1100):
        bridge._enqueue_for_test("info", payload)
    records = bridge.drain(2048)
    dropped = bridge.take_dropped()
    assert 0 < len(records) < 1100
    assert dropped == 1100 - len(records)


def test_concurrent_native_producers_keep_every_record_and_per_producer_order():
    bridge = _native.NativeLogBridge(4096)
    bridge._enqueue_many_for_test(8, 300)

    records = bridge.drain(4096)
    assert len(records) == 2400
    assert bridge.dropped() == 0
    for producer in range(8):
        seen = [int(message.rsplit("-", 1)[1]) for _level, message in records
                if message.startswith(f"producer-{producer}-")]
        assert seen == list(range(300))


def test_native_producer_does_not_wait_for_gil_subprocess():
    code = """
from pinpoint import _native
b = _native.NativeLogBridge(2)
b._enqueue_from_native_thread_for_test('info', 'no-gil')
assert b.drain(1) == [('info', 'no-gil')]
"""
    completed = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=5)
    assert completed.returncode == 0, completed.stderr


def test_consumer_maps_levels_messages_and_unknown_fallback(native_logger):
    records = []

    class Capture(logging.Handler):
        def emit(self, record):
            records.append(record)

    native_logger.addHandler(Capture())
    bridge = _native.NativeLogBridge(8)
    consumer = NativeLogConsumer(bridge)
    consumer.start()
    for level, message in [
        ("debug", "debug-message"),
        ("info", "info-message"),
        ("warning", "warning-message"),
        ("error", "error-message"),
        ("future", "unknown-message"),
    ]:
        bridge._enqueue_for_test(level, message)
    _wait_for(lambda: len(records) == 5)
    consumer.stop()

    assert [r.name for r in records] == ["pinpoint.native"] * 5
    assert [r.levelno for r in records] == [
        logging.DEBUG, logging.INFO, logging.WARNING,
        logging.ERROR, logging.WARNING,
    ]
    assert [r.getMessage() for r in records] == [
        "debug-message", "info-message", "warning-message",
        "error-message", "unknown-message",
    ]


def test_handler_exception_does_not_stop_consumer(native_logger):
    seen = []

    class RaisesOnce(logging.Handler):
        def emit(self, record):
            if not seen:
                seen.append("raised")
                raise RuntimeError("broken handler")
            seen.append(record.getMessage())

    native_logger.addHandler(RaisesOnce())
    bridge = _native.NativeLogBridge(4)
    consumer = NativeLogConsumer(bridge)
    consumer.start()
    bridge._enqueue_for_test("info", "first")
    bridge._enqueue_for_test("info", "second")
    _wait_for(lambda: seen == ["raised", "second"])
    consumer.stop()


def test_consumer_reports_drops_once_and_keeps_cumulative_count(native_logger):
    warnings = []

    class Capture(logging.Handler):
        def emit(self, record):
            if "native log bridge dropped" in record.getMessage():
                warnings.append(record.getMessage())

    native_logger.addHandler(Capture())
    bridge = _native.NativeLogBridge(2)
    consumer = NativeLogConsumer(bridge)
    for _ in range(20):
        bridge._enqueue_for_test("info", "first-burst")
    consumer._report_drops()
    assert bridge.drain(2) == [
        ("info", "first-burst"), ("info", "first-burst")]
    for _ in range(20):
        bridge._enqueue_for_test("info", "second-burst")
    consumer._report_drops()
    bridge.deactivate()

    assert len(warnings) == 1
    assert consumer.dropped == 36


def test_slow_handler_never_blocks_native_enqueue(native_logger):
    entered = threading.Event()
    release = threading.Event()

    class Slow(logging.Handler):
        def emit(self, record):
            entered.set()
            release.wait(2)

    native_logger.addHandler(Slow())
    bridge = _native.NativeLogBridge(256)
    consumer = NativeLogConsumer(bridge)
    consumer.start()
    bridge._enqueue_for_test("info", "hold-consumer")
    assert entered.wait(1)
    started = time.monotonic()
    bridge._enqueue_many_for_test(4, 50)
    elapsed = time.monotonic() - started
    release.set()
    consumer.stop()

    assert elapsed < 1.0


def test_logger_level_and_filter_control_delivery(native_logger):
    seen = []

    class Capture(logging.Handler):
        def emit(self, record):
            seen.append(record.getMessage())

    handler = Capture()
    handler.addFilter(lambda record: "keep" in record.getMessage())
    native_logger.addHandler(handler)
    logging.getLogger("pinpoint.native").setLevel(logging.WARNING)
    bridge = _native.NativeLogBridge(8)
    consumer = NativeLogConsumer(bridge)
    consumer.start()
    bridge._enqueue_for_test("info", "keep-info-filtered-by-level")
    bridge._enqueue_for_test("warning", "discard-warning")
    bridge._enqueue_for_test("error", "keep-error")
    _wait_for(lambda: seen == ["keep-error"])
    consumer.stop()


def test_deactivate_drops_post_shutdown_callbacks_without_counting():
    bridge = _native.NativeLogBridge(4)
    bridge._enqueue_for_test("info", "before")
    bridge.deactivate()
    bridge._enqueue_many_for_test(4, 100)
    assert bridge.drain(4) == [("info", "before")]
    assert bridge.dropped() == 0


def test_concurrent_producers_can_be_deactivated_during_enqueue():
    bridge = _native.NativeLogBridge(256)
    start = threading.Event()

    def produce(producer):
        start.wait()
        for record in range(2000):
            bridge._enqueue_for_test(
                "info", f"producer-{producer}-{record}")

    threads = [threading.Thread(target=produce, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    start.set()
    time.sleep(0.005)
    bridge.deactivate()
    for thread in threads:
        thread.join(2)
        assert not thread.is_alive()
    assert len(bridge.drain(256)) <= 256


def test_logging_handler_can_call_shutdown_without_deadlock_subprocess():
    code = r"""
import logging, os
for key in list(os.environ):
    if key.startswith('PINPOINT_PY_'):
        os.environ.pop(key)
import pinpoint
agent = pinpoint.init(
    application_name='handler-shutdown', native_log_to_python=True,
    collector_host='127.0.0.1', collector_agent_port=1,
    collector_span_port=1, collector_stat_port=1, log_level='INFO')
called = []
class ShutdownHandler(logging.Handler):
    def emit(self, record):
        if 'agent shutdown' in record.getMessage() and not called:
            called.append(True)
            pinpoint.shutdown()
logger = logging.getLogger('pinpoint.native')
logger.handlers[:] = [ShutdownHandler()]
logger.setLevel(logging.INFO)
logger.propagate = False
pinpoint.shutdown()
print('DONE', bool(called), agent.native_log_dropped)
"""
    completed = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True,
        timeout=10)
    assert completed.returncode == 0, completed.stderr
    assert "DONE True" in completed.stdout
