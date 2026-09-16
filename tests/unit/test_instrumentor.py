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

"""Tests for pinpoint.instrumentor.BaseInstrumentor."""

import types

import pytest

from pinpoint.instrumentations._util import (
    already_wrapped,
    mark_pinpoint_wrapper,
    record_wrapper_target,
    wrap,
)
from pinpoint.instrumentor import BaseInstrumentor, _global_installed


@pytest.fixture(autouse=True)
def _isolate_global_registry():
    """Installation is process-wide and class-keyed; drop test-local entries."""
    before = set(_global_installed)
    yield
    for cls in set(_global_installed) - before:
        _global_installed.pop(cls, None)


class _Counter(BaseInstrumentor):
    def __init__(self):
        super().__init__()
        self.install_calls = 0
        self.uninstall_calls = 0

    def _instrument(self):
        self.install_calls += 1

    def _uninstrument(self):
        self.uninstall_calls += 1


def test_instrument_calls_instrument_exactly_once():
    inst = _Counter()
    inst.instrument()
    assert inst.install_calls == 1
    assert inst._installed is True


def test_instrument_is_idempotent():
    inst = _Counter()
    inst.instrument()
    inst.instrument()
    inst.instrument()
    assert inst.install_calls == 1


def test_first_party_instrumentor_is_idempotent_across_instances():
    calls = []

    class FirstParty(BaseInstrumentor):
        __module__ = "pinpoint.instrumentations.fake"

        def _instrument(self):
            calls.append("install")

        def _uninstrument(self):
            calls.append("uninstall")

    first = FirstParty()
    second = FirstParty()
    try:
        first.instrument()
        second.instrument()
        assert calls == ["install"]
    finally:
        first.uninstrument()


def test_uninstrument_calls_uninstrument_exactly_once():
    inst = _Counter()
    inst.instrument()
    inst.uninstrument()
    assert inst.uninstall_calls == 1
    assert inst._installed is False


def test_uninstrument_is_idempotent():
    inst = _Counter()
    inst.instrument()
    inst.uninstrument()
    inst.uninstrument()
    assert inst.uninstall_calls == 1


def test_uninstrument_without_instrument_is_noop():
    inst = _Counter()
    inst.uninstrument()
    assert inst.uninstall_calls == 0


def test_instrument_then_reinstrument_after_uninstrument():
    inst = _Counter()
    inst.instrument()
    inst.uninstrument()
    inst.instrument()
    assert inst.install_calls == 2
    assert inst._installed is True


def test_instrument_exception_leaves_installed_false():
    class Broken(BaseInstrumentor):
        def _instrument(self):
            raise RuntimeError("oops")

    inst = Broken()
    inst.instrument()  # must not raise (safe_try swallows)
    assert inst._installed is False


def test_uninstrument_exception_still_resets_installed():
    class Broken(BaseInstrumentor):
        def _instrument(self):
            pass

        def _uninstrument(self):
            raise RuntimeError("oops")

    inst = Broken()
    inst.instrument()
    inst.uninstrument()  # must not raise
    assert inst._installed is False


def test_default_uninstrument_without_wrappers_is_safe():
    class MinimalInst(BaseInstrumentor):
        def _instrument(self):
            pass

    inst = MinimalInst()
    inst.instrument()
    inst.uninstrument()  # default _uninstrument returns None — must not raise
    assert inst._installed is False


# ---------------------------------------------------------------------------
# Idempotent wrap() — a re-instrument cycle / partial-install retry must never
# stack wrappers on the same target.
# ---------------------------------------------------------------------------

def _fake_target_module(monkeypatch):
    """Register a throwaway module holding a wrappable class."""
    import sys

    mod = types.ModuleType("pinpoint_fake_wrap_target")

    class Widget:
        def run(self, x):
            return x

    mod.Widget = Widget
    monkeypatch.setitem(sys.modules, mod.__name__, mod)
    return mod


def test_wrap_reports_and_skips_repeated_installs(monkeypatch):
    pytest.importorskip("wrapt")
    mod = _fake_target_module(monkeypatch)
    calls = []

    def wrapper(wrapped, instance, args, kwargs):
        calls.append("wrap")
        return wrapped(*args, **kwargs)

    assert already_wrapped(mod.__name__, "Widget.run") is False
    wrap(mod.__name__, "Widget.run", wrapper)
    assert already_wrapped(mod.__name__, "Widget.run") is True

    # Repeated instrument(), or instrument→uninstrument→instrument, re-runs the
    # same wrap — it must be a no-op, not a second wrapper layer.
    wrap(mod.__name__, "Widget.run", wrapper)
    wrap(mod.__name__, "Widget.run", wrapper)

    assert mod.Widget().run(7) == 7
    # One wrapper layer → exactly one wrapper invocation per call.
    assert calls == ["wrap"]


def test_wrap_partial_install_retry_does_not_double_wrap(monkeypatch):
    """A mid-install failure leaves early targets wrapped; a retry must skip
    those already-wrapped targets rather than stacking a second layer."""
    pytest.importorskip("wrapt")
    import sys

    mod = types.ModuleType("pinpoint_fake_partial_target")

    class Widget:
        def first(self, x):
            return x

        def second(self, x):
            return x

    mod.Widget = Widget
    monkeypatch.setitem(sys.modules, mod.__name__, mod)

    counts = {"first": 0, "second": 0}

    def make(name):
        def _w(wrapped, instance, args, kwargs):
            counts[name] += 1
            return wrapped(*args, **kwargs)
        return _w

    # First install "succeeds" on `first`, then the caller's `_instrument`
    # aborts before wrapping `second` (simulated by simply not calling it).
    wrap(mod.__name__, "Widget.first", make("first"))

    # Retry runs the full install again: `first` is already wrapped (skip),
    # `second` gets wrapped for the first time.
    wrap(mod.__name__, "Widget.first", make("first"))
    wrap(mod.__name__, "Widget.second", make("second"))

    w = mod.Widget()
    w.first(1)
    w.second(2)
    assert counts == {"first": 1, "second": 1}


# ---------------------------------------------------------------------------
# Symmetric uninstrument — BaseInstrumentor owns every common wrap() target.
# ---------------------------------------------------------------------------

def test_default_uninstrument_restores_all_owned_wrappers_and_keeps_foreign(
        monkeypatch):
    wrapt = pytest.importorskip("wrapt")
    mod = _fake_target_module(monkeypatch)
    original_run = mod.Widget.__dict__["run"]

    def other(x):
        return x + 1

    mod.other = other
    original_other = mod.other

    class Registered(BaseInstrumentor):
        __module__ = "pinpoint.instrumentations.fake_registered"

        def _instrument(self):
            wrap(mod.__name__, "Widget.run", _pinpoint)
            wrap(mod.__name__, "other", _pinpoint)

    def _pinpoint(wrapped, instance, args, kwargs):
        return wrapped(*args, **kwargs)

    def _foreign(wrapped, instance, args, kwargs):
        return "foreign", wrapped(*args, **kwargs)

    inst = Registered()
    try:
        inst.instrument()
        assert mod.Widget.__dict__["run"] is not original_run
        assert mod.other is not original_other

        # Simulate another APM installing after Pinpoint.
        wrapt.wrap_function_wrapper(mod.Widget, "run", _foreign)
        foreign_layer = mod.Widget.__dict__["run"]

        # First-party installation is process-wide, so any instance of the
        # same instrumentor class must be able to uninstall the retained owner.
        Registered().uninstrument()

        assert mod.Widget.__dict__["run"] is foreign_layer
        assert mod.Widget().run(7) == ("foreign", 7)
        assert mod.other is original_other
        assert inst._installed is False

        # A fresh cycle installs one Pinpoint layer and removes it again while
        # preserving the foreign layer.
        again = Registered()
        again.instrument()
        again.uninstrument()
        assert mod.Widget.__dict__["run"] is foreign_layer
        assert mod.Widget().run(8) == ("foreign", 8)
    finally:
        _global_installed.pop(Registered, None)


def test_partial_install_failure_rolls_back_earlier_wrappers(monkeypatch):
    pytest.importorskip("wrapt")
    mod = _fake_target_module(monkeypatch)
    original = mod.Widget.__dict__["run"]
    cleanup_calls = []

    class Broken(BaseInstrumentor):
        __module__ = "pinpoint.instrumentations.fake_broken"

        def _instrument(self):
            wrap(mod.__name__, "Widget.run", _wrapper)
            raise RuntimeError("install failed")

        def _uninstrument(self):
            cleanup_calls.append("cleanup")

    def _wrapper(wrapped, instance, args, kwargs):
        return wrapped(*args, **kwargs)

    inst = Broken()
    inst.instrument()

    assert inst._installed is False
    assert Broken not in _global_installed
    assert mod.Widget.__dict__["run"] is original
    assert inst._wrapper_targets == []
    assert cleanup_calls == ["cleanup"]


def test_custom_uninstrument_failure_still_restores_common_wrappers(monkeypatch):
    pytest.importorskip("wrapt")
    mod = _fake_target_module(monkeypatch)
    original = mod.Widget.__dict__["run"]

    class BrokenCleanup(BaseInstrumentor):
        __module__ = "pinpoint.instrumentations.fake_cleanup"

        def _instrument(self):
            wrap(mod.__name__, "Widget.run", _wrapper)

        def _uninstrument(self):
            raise RuntimeError("custom cleanup failed")

    def _wrapper(wrapped, instance, args, kwargs):
        return wrapped(*args, **kwargs)

    inst = BrokenCleanup()
    try:
        inst.instrument()
        inst.uninstrument()

        assert mod.Widget.__dict__["run"] is original
        assert inst._installed is False
    finally:
        _global_installed.pop(BrokenCleanup, None)


def test_deferred_wrapper_is_owned_and_cannot_install_after_uninstrument(
        monkeypatch):
    pytest.importorskip("wrapt")
    mod = _fake_target_module(monkeypatch)
    original = mod.Widget.__dict__["run"]

    class Lazy(BaseInstrumentor):
        __module__ = "pinpoint.instrumentations.fake_lazy"

        def _instrument(self):
            pass

        def install_deferred(self):
            return self._wrap_target(mod.__name__, "Widget.run", _wrapper)

    def _wrapper(wrapped, instance, args, kwargs):
        return wrapped(*args, **kwargs)

    inst = Lazy()
    try:
        inst.instrument()
        assert inst.install_deferred() is True
        assert mod.Widget.__dict__["run"] is not original
        inst.uninstrument()
        assert mod.Widget.__dict__["run"] is original

        # A post-import callback retained by wrapt may fire late; once the
        # instrumentor is inactive it must not reapply the monkey patch.
        assert inst.install_deferred() is False
        assert mod.Widget.__dict__["run"] is original
    finally:
        _global_installed.pop(Lazy, None)


def test_direct_wrapt_install_can_join_common_uninstrument_registry(monkeypatch):
    wrapt = pytest.importorskip("wrapt")
    mod = _fake_target_module(monkeypatch)
    original = mod.Widget.__dict__["run"]

    class Direct(BaseInstrumentor):
        __module__ = "pinpoint.instrumentations.fake_direct"

        def _instrument(self):
            wrapt.wrap_function_wrapper(
                mod.__name__,
                "Widget.run",
                mark_pinpoint_wrapper(_wrapper),
            )
            record_wrapper_target(mod.__name__, "Widget.run")

    def _wrapper(wrapped, instance, args, kwargs):
        return wrapped(*args, **kwargs)

    inst = Direct()
    try:
        inst.instrument()
        assert mod.Widget.__dict__["run"] is not original
        inst.uninstrument()
        assert mod.Widget.__dict__["run"] is original
    finally:
        _global_installed.pop(Direct, None)


def test_global_lock_is_replaced_after_fork():
    """A post-import hook holding _global_lock at fork time leaves it locked in
    the child with no owning thread; the after-fork hook installs a fresh lock
    so the child's deferred instrumentation imports cannot deadlock."""
    from pinpoint import instrumentor as instr_mod

    old = instr_mod._global_lock
    old.acquire()  # stand-in for a parent thread holding it across fork
    try:
        instr_mod._reset_lock_after_fork()
        assert instr_mod._global_lock is not old
        assert instr_mod._global_lock.acquire(blocking=False)
        instr_mod._global_lock.release()
    finally:
        old.release()
