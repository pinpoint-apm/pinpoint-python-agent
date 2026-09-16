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

"""Tests for pinpoint.callstack — Python call-stack capture for set_error."""

from __future__ import annotations

import pytest

import pinpoint.callstack as cs
from pinpoint.callstack import (
    _MAX_FRAMES,
    _describe,
    _is_internal,
    frames_for,
)


def _dump_frames(error_or_name):
    """Capture frames through the public frames_for path."""
    return frames_for(error_or_name, enabled=True)


# ---------------------------------------------------------------------------
# frames_for — disabled path
# ---------------------------------------------------------------------------


def test_frames_for_returns_none_when_disabled():
    assert frames_for(RuntimeError("boom"), enabled=False) is None
    assert frames_for("error_name", enabled=False) is None


# ---------------------------------------------------------------------------
# frames_for — enabled path
# ---------------------------------------------------------------------------


def test_frames_for_exception_returns_typed_frames_when_enabled():
    try:
        raise ValueError("test")
    except ValueError as exc:
        frames = _dump_frames(exc)
    assert len(frames) >= 1
    module, function, filepath, line = frames[0]
    assert isinstance(module, str)
    assert isinstance(function, str)
    assert isinstance(filepath, str)
    assert isinstance(line, int) and line > 0


def test_frames_for_string_returns_frames_when_enabled():
    frames = _dump_frames("SomeError")
    assert frames is not None


def test_frames_for_exception_without_traceback_returns_empty_list():
    # Still a list (not None): an empty stack is recorded natively as an
    # Exception of its own.
    assert _dump_frames(ValueError("no traceback")) == []


# ---------------------------------------------------------------------------
# _describe
# ---------------------------------------------------------------------------


def test_describe_returns_four_tuple():
    import sys
    frame = sys._getframe(0)
    result = _describe(frame, 42)
    assert len(result) == 4
    module, function, filepath, line = result
    assert isinstance(module, str)
    assert isinstance(function, str)
    assert isinstance(filepath, str)
    assert line == 42


def test_describe_captures_current_module_name():
    import sys
    frame = sys._getframe(0)
    module, *_ = _describe(frame, 1)
    assert module == __name__


# ---------------------------------------------------------------------------
# _is_internal
# ---------------------------------------------------------------------------


def test_is_internal_recognizes_callstack_module():
    assert _is_internal(("pinpoint.callstack", "frames_for", "callstack.py", 1))


def test_is_internal_recognizes_tracer_module():
    assert _is_internal(("pinpoint.tracer", "set_error", "tracer.py", 1))


def test_is_internal_false_for_user_module():
    assert not _is_internal(("myapp.views", "index", "views.py", 10))


def test_is_internal_false_for_plain_pinpoint():
    assert not _is_internal(("pinpoint", "init", "__init__.py", 1))


def test_is_internal_recognizes_instrumentation_wrapper_module():
    # A synthesized (non-exception) set_error from an instrumentation must not
    # leave the agent's own wrapper frames at the top of the user-visible stack.
    assert _is_internal(
        ("pinpoint.instrumentations.wsgi", "_trace", "wsgi/__init__.py", 1))
    assert _is_internal(
        ("pinpoint.http_helper", "record_exception_on_span", "http_helper.py", 1))


# ---------------------------------------------------------------------------
# frame capture — exception traceback path
# ---------------------------------------------------------------------------


def test_exception_frames_empty_when_no_traceback():
    exc = RuntimeError("bare")
    assert _dump_frames(exc) == []


def test_exception_frames_returns_frames():
    try:
        raise RuntimeError("raised")
    except RuntimeError as exc:
        frames = _dump_frames(exc)
    assert len(frames) >= 1
    assert all(len(f) == 4 for f in frames)


def test_exception_frames_cap_at_max_frames():
    def recurse(n):
        if n == 0:
            raise RecursionError("deep")
        recurse(n - 1)

    try:
        recurse(_MAX_FRAMES + 10)
    except RecursionError as exc:
        frames = _dump_frames(exc)
    assert len(frames) <= _MAX_FRAMES


def test_exception_frames_keep_the_raise_site_past_the_cap():
    # walk_tb yields outermost-first, so a cap applied to the *head* silently
    # drops the frame that actually raised — the one the reader came for.
    def recurse(n):
        if n == 0:
            raise RecursionError("deep")
        recurse(n - 1)

    try:
        recurse(_MAX_FRAMES + 10)
    except RecursionError as exc:
        frames = _dump_frames(exc)

    # The raise happened in `recurse`, and the last frame is the deepest one.
    assert frames, "no frames captured"
    assert frames[-1][1].endswith("recurse")
    assert frames[-1][3] == recurse.__code__.co_firstlineno + 2


# ---------------------------------------------------------------------------
# frame capture — current-stack path (non-exception argument)
# ---------------------------------------------------------------------------


def test_current_stack_frames_returns_non_empty():
    frames = _dump_frames("SyntheticError")
    assert len(frames) >= 1


def test_current_stack_frames_drop_internal_frames():
    frames = _dump_frames("SyntheticError")
    for f in frames:
        assert not _is_internal(f), f"internal frame leaked: {f}"


def test_current_stack_frames_cap_at_max_frames():
    frames = _dump_frames("SyntheticError")
    assert len(frames) <= _MAX_FRAMES


# ---------------------------------------------------------------------------
# exception chains — causes_for
# ---------------------------------------------------------------------------


def _chained(explicit: bool):
    try:
        try:
            raise KeyError("root")
        except KeyError as root:
            if explicit:
                raise ValueError("middle") from root
            raise ValueError("middle")
    except ValueError as exc:
        return exc


@pytest.fixture
def chain_depth():
    saved = cs._chain_max_depth
    yield cs.set_chain_max_depth
    cs._chain_max_depth = saved


def test_causes_for_none_without_cause_or_context():
    try:
        raise RuntimeError("alone")
    except RuntimeError as exc:
        assert cs.causes_for(exc) is None


def test_causes_for_walks_explicit_cause():
    causes = cs.causes_for(_chained(explicit=True))
    assert len(causes) == 1
    name, message, frames = causes[0]
    assert (name, message) == ("KeyError", "'root'")
    assert frames and all(len(f) == 4 for f in frames)


def test_causes_for_walks_implicit_context():
    causes = cs.causes_for(_chained(explicit=False))
    assert [c[0] for c in causes] == ["KeyError"]


def test_causes_for_respects_suppress_context():
    try:
        try:
            raise KeyError("hidden")
        except KeyError:
            raise ValueError("shown") from None
    except ValueError as exc:
        assert exc.__context__ is not None
        assert cs.causes_for(exc) is None


def test_causes_for_stops_on_cycle():
    a, b = ValueError("a"), KeyError("b")
    a.__cause__, b.__cause__ = b, a
    assert [c[0] for c in cs.causes_for(a)] == ["KeyError"]


def test_causes_for_caps_total_entries(chain_depth):
    exc = ValueError("0")
    cur = exc
    for i in range(1, 10):
        cur.__cause__ = RuntimeError(str(i))
        cur = cur.__cause__
    chain_depth(3)
    assert [c[1] for c in cs.causes_for(exc)] == ["1", "2"]
    chain_depth(1)
    assert cs.causes_for(exc) is None
    chain_depth(0)
    assert len(cs.causes_for(exc)) == 9


def test_causes_for_survives_throwing_str():
    class Loud(Exception):
        def __str__(self):
            raise RuntimeError("no str for you")

    exc = ValueError("outer")
    exc.__cause__ = Loud()
    assert cs.causes_for(exc) == [("Loud", "", [])]
