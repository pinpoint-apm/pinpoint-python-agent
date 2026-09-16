# pinpoint-python-agent
# Copyright (c) 2026-present NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0

"""Drain the native logger bridge into the standard Python logging pipeline."""

from __future__ import annotations

import logging
import sys
import threading
from typing import Any

from ._log import get_logger

_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}
_UNKNOWN_LEVEL = logging.WARNING
_DRAIN_BATCH_SIZE = 256
# 100 ms: the bridge is a bounded queue drained in batches, so a longer poll
# only adds delivery latency on an idle process, while 10 ms cost ~100 wakeups
# per second per worker for nothing.
_POLL_INTERVAL_SECONDS = 0.1
_JOIN_TIMEOUT_SECONDS = 1.0


class NativeLogConsumer:
    """Own one bridge and its daemon logging consumer.

    The bridge itself owns no Python references. Native callbacks only enqueue
    copied bytes in C++; this thread is the first place a record reaches the
    interpreter and Python logging handlers. Shutdown is intentionally bounded:
    a user handler that never returns cannot hold up agent shutdown or process
    exit, and the daemon retains the bridge until that handler eventually exits.
    """

    def __init__(self, bridge: Any):
        self._bridge = bridge
        self._logger = get_logger("native")
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="pinpoint-native-log",
            daemon=True,
        )
        self._dropped = 0
        self._drop_warning_emitted = False
        self._abandoned_after_fork = False

    def start(self) -> None:
        self._thread.start()

    @property
    def dropped(self) -> int:
        if self._abandoned_after_fork:
            return self._dropped
        try:
            return self._dropped + int(self._bridge.dropped())
        except Exception:  # noqa: BLE001
            return self._dropped

    def stop(self) -> None:
        """Deactivate, bounded-drain, and join unless called by the consumer."""
        if self._abandoned_after_fork:
            return
        try:
            self._bridge.deactivate()
        except Exception:  # noqa: BLE001
            pass
        self._stop.set()
        if threading.current_thread() is self._thread:
            # A logging handler may call pinpoint.shutdown(). The loop performs
            # its final drain after this handler returns; joining self would fail.
            return
        self._thread.join(_JOIN_TIMEOUT_SECONDS)

    def abandon_after_fork(self) -> None:
        """Make an inherited bridge inert without touching Python thread locks."""
        self._abandoned_after_fork = True
        try:
            # Atomic C++ store only. The inherited Event/Thread locks may have
            # been owned by vanished parent threads and must not be acquired.
            self._bridge.deactivate()
        except Exception:  # noqa: BLE001
            pass

    def _run(self) -> None:
        try:
            # Explicitly clear any context a future/free-threaded Python build
            # might inherit into new threads. Logging handlers run outside an
            # application trace and cannot recursively create Pinpoint events.
            from .context import _current_span
            _current_span.set(None)
            while not self._stop.wait(_POLL_INTERVAL_SECONDS):
                if getattr(sys, "is_finalizing", lambda: False)():
                    try:
                        self._bridge.deactivate()
                    except Exception:  # noqa: BLE001
                        pass
                    return
                self._drain_once()

            # Producers are inactive before stop is set. Capacity is bounded,
            # so at most capacity records can remain and this drain is bounded.
            while self._drain_once():
                pass
            self._report_drops()
        except Exception:  # noqa: BLE001
            # No consumer failure may escape a daemon thread or affect native
            # workers. One malformed handler/record only costs Python delivery.
            return
        finally:
            # On the normal shutdown path native has already cleared its
            # std::function sink. Releasing this last Python-side owner frees
            # the queue; on startup failure/finalization a retained native
            # callback still owns the inert C++ state and remains safe.
            try:
                self._bridge.deactivate()
            except Exception:  # noqa: BLE001
                pass
            self._bridge = None

    def _drain_once(self) -> bool:
        try:
            records = self._bridge.drain(_DRAIN_BATCH_SIZE)
        except Exception:  # noqa: BLE001
            return False
        for level, message in records:
            try:
                self._logger.log(
                    _LEVELS.get(str(level).lower(), _UNKNOWN_LEVEL),
                    message,
                )
            except Exception:  # noqa: BLE001
                # logging.Handler.emit implementations are user code and may
                # raise. Continue with later native records.
                continue
        self._report_drops()
        return bool(records)

    def _report_drops(self) -> None:
        try:
            dropped = int(self._bridge.take_dropped())
        except Exception:  # noqa: BLE001
            return
        if not dropped:
            return
        self._dropped += dropped
        if self._drop_warning_emitted:
            return
        self._drop_warning_emitted = True
        try:
            self._logger.warning(
                "native log bridge dropped %d record(s) because its bounded "
                "queue was full",
                dropped,
            )
        except Exception:  # noqa: BLE001
            pass
