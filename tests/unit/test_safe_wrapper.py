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

"""``safe_wrapper`` contract.

The shim must swallow instrumentation bugs and fall back to the target,
*but never re-run user code*. Almost every synchronous wrapper calls the
target itself and lets its exception propagate; a naive fallback would
execute the user's function a second time and hide the original error
(double INSERTs, views running twice, lost messages). These tests pin the
three outcomes:

- instrumentation fails before entering the target  -> fall back (1 call)
- the target itself raises                           -> propagate, no re-run
- instrumentation fails *after* the target returns   -> swallow, return value
"""

from __future__ import annotations

import pytest

from pinpoint.instrumentations._util import safe_wrapper


def _install(fn):
    """Apply safe_wrapper and return a plain callable mimicking wrapt's
    ``wrapper(wrapped, instance, args, kwargs)`` dispatch."""
    wrapped_shim = safe_wrapper(fn)

    def call(target, *args, **kwargs):
        return wrapped_shim(target, None, args, kwargs)

    return call


def test_falls_back_when_instrumentation_fails_before_target():
    """A bug in the wrapper *before* it reaches the target: the target must
    still run exactly once via the fallback."""
    calls = []

    def target(*args, **kwargs):
        calls.append(args)
        return "real-result"

    def wrapper(wrapped, instance, args, kwargs):
        raise RuntimeError("instrumentation bug before target")

    call = _install(wrapper)
    assert call(target, 1, 2) == "real-result"
    assert calls == [(1, 2)]


def test_target_exception_propagates_and_runs_once():
    """When the user's own code raises, the exception propagates verbatim and
    the target is *not* invoked a second time."""
    calls = []

    class UserError(Exception):
        pass

    def target(*args, **kwargs):
        calls.append(args)
        raise UserError("payment failed")

    def wrapper(wrapped, instance, args, kwargs):
        # Typical sync wrapper shape: call the target inside a trace scope
        # and let its exception propagate.
        return wrapped(*args, **kwargs)

    call = _install(wrapper)
    with pytest.raises(UserError, match="payment failed"):
        call(target, "charge")
    assert len(calls) == 1


def test_post_call_instrumentation_bug_returns_target_result():
    """If instrumentation blows up *after* the target already returned, the
    value the target produced is handed back and the target is not re-run."""
    calls = []

    def target(*args, **kwargs):
        calls.append(args)
        return 42

    def wrapper(wrapped, instance, args, kwargs):
        result = wrapped(*args, **kwargs)
        raise RuntimeError("bug while recording the span after the call")

    call = _install(wrapper)
    assert call(target) == 42
    assert len(calls) == 1


def test_successful_wrapper_returns_value_and_runs_once():
    calls = []

    def target(*args, **kwargs):
        calls.append(args)
        return "ok"

    def wrapper(wrapped, instance, args, kwargs):
        return wrapped(*args, **kwargs)

    call = _install(wrapper)
    assert call(target, "x") == "ok"
    assert len(calls) == 1


def test_stopiteration_from_target_is_not_retried():
    """kafka consumer polling raises StopIteration on timeout; it must not be
    caught and retried (which would double the loop-exit latency)."""
    calls = []

    def target(*args, **kwargs):
        calls.append(1)
        raise StopIteration

    def wrapper(wrapped, instance, args, kwargs):
        record = wrapped(*args, **kwargs)
        return record

    call = _install(wrapper)
    with pytest.raises(StopIteration):
        call(target)
    assert len(calls) == 1


def test_keyboard_interrupt_always_propagates():
    def target(*args, **kwargs):
        return "unused"

    def wrapper(wrapped, instance, args, kwargs):
        raise KeyboardInterrupt

    call = _install(wrapper)
    with pytest.raises(KeyboardInterrupt):
        call(target)


def test_keyboard_interrupt_from_target_propagates_without_rerun():
    calls = []

    def target(*args, **kwargs):
        calls.append(1)
        raise KeyboardInterrupt

    def wrapper(wrapped, instance, args, kwargs):
        return wrapped(*args, **kwargs)

    call = _install(wrapper)
    with pytest.raises(KeyboardInterrupt):
        call(target)
    assert len(calls) == 1


def test_precheck_true_bypasses_wrapper_machinery():
    """A truthy precheck routes straight to the target — the wrapper body
    (and its sentinel) must never run."""
    calls = []

    def target(*args, **kwargs):
        calls.append(args)
        return "real-result"

    def wrapper(wrapped, instance, args, kwargs):
        raise AssertionError("wrapper must not run when precheck is true")

    shim = safe_wrapper(wrapper, precheck=lambda: True)
    assert shim(target, None, (1,), {}) == "real-result"
    assert calls == [(1,)]


def test_precheck_false_runs_wrapper():
    seen = []

    def target(*args, **kwargs):
        return "real-result"

    def wrapper(wrapped, instance, args, kwargs):
        seen.append(True)
        return wrapped(*args, **kwargs)

    shim = safe_wrapper(wrapper, precheck=lambda: False)
    assert shim(target, None, (), {}) == "real-result"
    assert seen == [True]


def test_raising_precheck_falls_through_to_guarded_wrapper():
    """A precheck bug must never break the user's call: fall through to the
    normal guarded wrapper path."""
    def target(*args, **kwargs):
        return "real-result"

    def wrapper(wrapped, instance, args, kwargs):
        return wrapped(*args, **kwargs)

    def bad_precheck():
        raise RuntimeError("precheck bug")

    shim = safe_wrapper(wrapper, precheck=bad_precheck)
    assert shim(target, None, (), {}) == "real-result"


def test_precheck_fast_path_propagates_target_exception_without_rerun():
    """A target exception on the fast path is the user's own failure: it must
    propagate, and the target must never be re-run via the guarded wrapper
    (double send/publish)."""
    calls = []

    def target(*args, **kwargs):
        calls.append(args)
        raise ValueError("broker down")

    def wrapper(wrapped, instance, args, kwargs):
        raise AssertionError("wrapper must not run when precheck is true")

    shim = safe_wrapper(wrapper, precheck=lambda: True)
    with pytest.raises(ValueError, match="broker down"):
        shim(target, None, (1,), {})
    assert calls == [(1,)]


# ---------------------------------------------------------------------------
# Interceptor call tracing.
# ---------------------------------------------------------------------------


@pytest.fixture
def traced(monkeypatch):
    """Turn instrumentation call tracing on and collect the messages."""
    from pinpoint import _log as log_module
    from pinpoint.instrumentations import _util

    lines = []
    monkeypatch.setattr(log_module, "debug_enabled", True)
    monkeypatch.setattr(_util._log, "debug",
                        lambda msg, *args, **kw: lines.append(msg % args))
    return lines


def test_call_tracing_is_silent_when_debug_is_off(monkeypatch):
    from pinpoint import _log as log_module
    from pinpoint.instrumentations import _util

    lines = []
    monkeypatch.setattr(log_module, "debug_enabled", False)
    monkeypatch.setattr(_util._log, "debug",
                        lambda msg, *args, **kw: lines.append(msg % args))

    def wrapper(wrapped, instance, args, kwargs):
        return wrapped(*args, **kwargs)

    assert _install(wrapper)(lambda: "ok") == "ok"
    assert lines == []


def test_call_tracing_brackets_the_callback(traced):
    def wrapper(wrapped, instance, args, kwargs):
        return wrapped(*args, **kwargs)

    assert _install(wrapper)(lambda: "ok") == "ok"
    # __qualname__ carries the enclosing scope, so match the ends.
    assert [line.split()[1] for line in traced] == ["before", "after"]
    assert all(line.endswith("wrapper") for line in traced)


def test_call_tracing_shows_a_callback_that_never_finished(traced):
    """The entry log without a matching exit is the point: it separates 'the
    hook never ran' from 'the hook ran and blew up'."""
    def wrapper(wrapped, instance, args, kwargs):
        raise RuntimeError("instrumentation bug")

    assert _install(wrapper)(lambda: "real") == "real"   # fallback still runs
    bracket = [line for line in traced if line.startswith("interceptor ")]
    assert len(bracket) == 1 and bracket[0].startswith("interceptor before ")
    # The failure itself is still reported on the existing debug line.
    assert any(line.startswith("instrumentation exception") for line in traced)
