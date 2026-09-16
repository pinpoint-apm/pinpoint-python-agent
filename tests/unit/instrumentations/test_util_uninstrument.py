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

"""Ownership-aware uninstrument helpers.

``restore_pinpoint_target`` / ``unwrap_pinpoint`` must remove *only* pinpoint's
own wrapt layer from a target's ``__wrapped__`` chain — never a wrapper another
library layered on the same target. The fastapi/starlette/aiohttp
``_uninstrument`` paths all funnel through these two helpers, so exercising
them here covers every caller once.

Tests drive the real install path (``_util.wrap`` stamps pinpoint's wrapper) and
a bare ``wrapt.wrap_function_wrapper`` for the "foreign" library, against a
throwaway module registered in ``sys.modules`` so nothing real is mutated.
"""

from __future__ import annotations

import sys
import types

import pytest
import wrapt

from pinpoint.instrumentations import _util

_MOD = "pp_medium03_fake"


@pytest.fixture
def fake_module():
    mod = types.ModuleType(_MOD)

    class Target:
        def method(self, x):
            return f"base({x})"

    def func(x):
        return f"fbase({x})"

    mod.Target = Target
    mod.func = func
    sys.modules[_MOD] = mod
    try:
        yield mod
    finally:
        sys.modules.pop(_MOD, None)


def _pinpoint_wrap_method(prefix="pp"):
    """Install pinpoint's wrapper via the real ``_util.wrap`` (which stamps)."""
    def _w(wrapped, instance, args, kwargs):
        return prefix + ":" + wrapped(*args, **kwargs)
    _util.wrap(_MOD, "Target.method", _w)


def _foreign_wrap_method(cls, prefix="otel"):
    """A second library's wrapt wrapper — deliberately *not* stamped."""
    def _w(wrapped, instance, args, kwargs):
        return prefix + ":" + wrapped(*args, **kwargs)
    wrapt.wrap_function_wrapper(cls, "method", _w)


def _chain_has_pinpoint(obj) -> bool:
    for _ in range(64):
        if getattr(getattr(obj, "_self_wrapper", None),
                   "__pinpoint_wrapper__", False) is True:
            return True
        obj = getattr(obj, "__wrapped__", None)
        if obj is None:
            return False
    return False


def test_restore_when_pinpoint_outermost_restores_original(fake_module):
    Target = fake_module.Target
    original = Target.__dict__["method"]
    _pinpoint_wrap_method()
    assert Target.__dict__["method"] is not original

    _util.restore_pinpoint_target(_MOD, "Target.method")

    assert Target.__dict__["method"] is original
    assert Target().method("x") == "base(x)"


def test_restore_module_level_function(fake_module):
    """The ``attr=None`` path (fastapi's ``run_endpoint_function``)."""
    original = fake_module.func

    def _w(wrapped, instance, args, kwargs):
        return "pp:" + wrapped(*args, **kwargs)

    _util.wrap(_MOD, "func", _w)
    assert fake_module.func is not original
    assert fake_module.func("x") == "pp:fbase(x)"

    _util.restore_pinpoint_target(_MOD, "func")

    assert fake_module.func is original
    assert fake_module.func("x") == "fbase(x)"


def test_restore_splices_pinpoint_out_when_foreign_on_top(fake_module):
    """A foreign wrapper is layered on top of pinpoint's: uninstrument must
    keep it installed and functional and remove only ours."""
    Target = fake_module.Target
    _pinpoint_wrap_method()
    _foreign_wrap_method(Target)
    foreign_fw = Target.__dict__["method"]
    assert Target().method("x") == "otel:pp:base(x)"  # both layers active

    _util.restore_pinpoint_target(_MOD, "Target.method")

    # Foreign wrapper is still the installed outermost object ...
    assert Target.__dict__["method"] is foreign_fw
    # ... pinpoint's layer is gone from the chain ...
    assert not _chain_has_pinpoint(Target.__dict__["method"])
    # ... and the foreign wrapper still runs over the untouched original.
    assert Target().method("x") == "otel:base(x)"


def test_restore_is_noop_when_only_foreign_wrapper(fake_module):
    """Pinpoint never wrapped, a foreign library did — leave it be."""
    Target = fake_module.Target
    _foreign_wrap_method(Target)
    foreign_fw = Target.__dict__["method"]

    _util.restore_pinpoint_target(_MOD, "Target.method")

    assert Target.__dict__["method"] is foreign_fw
    assert Target().method("x") == "otel:base(x)"


def test_restore_removes_pinpoint_layered_over_foreign(fake_module):
    """pinpoint wrapped *on top of* a pre-existing foreign wrapper: drop pinpoint,
    leave the foreign wrapper as the outermost layer."""
    Target = fake_module.Target
    _foreign_wrap_method(Target)
    foreign_fw = Target.__dict__["method"]
    _pinpoint_wrap_method()
    assert Target().method("x") == "pp:otel:base(x)"

    _util.restore_pinpoint_target(_MOD, "Target.method")

    assert Target.__dict__["method"] is foreign_fw
    assert Target().method("x") == "otel:base(x)"


def test_restore_missing_module_or_attr_is_noop():
    # Module absent entirely.
    _util.restore_pinpoint_target("pp_medium03_absent_module", "X.y")
    # Module present, attribute absent.
    mod = types.ModuleType("pp_medium03_empty")
    sys.modules["pp_medium03_empty"] = mod
    try:
        _util.restore_pinpoint_target("pp_medium03_empty", "Missing.attr")
    finally:
        sys.modules.pop("pp_medium03_empty", None)


def test_unwrap_pinpoint_leaves_chain_intact_when_splice_refused():
    """If the enclosing layer refuses the ``__wrapped__`` re-point, the whole
    chain is left untouched — a foreign wrapper is never stripped."""
    def original():
        return "base"

    pinpoint_layer = wrapt.FunctionWrapper(
        original, mark_pinpoint := _util.mark_pinpoint_wrapper(
            lambda w, i, a, k: w(*a, **k),
        ),
    )
    assert getattr(mark_pinpoint, "__pinpoint_wrapper__", False) is True

    class FrozenOuter:
        """A foreign proxy that exposes ``__wrapped__`` but refuses assignment."""

        def __init__(self, inner):
            self._inner = inner

        @property
        def __wrapped__(self):
            return self._inner

    outer = FrozenOuter(pinpoint_layer)

    result = _util.unwrap_pinpoint(outer)

    # No reassignment signalled, and the pinpoint layer is still reachable.
    assert result is outer
    assert outer.__wrapped__ is pinpoint_layer
