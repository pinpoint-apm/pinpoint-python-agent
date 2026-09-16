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

"""BaseInstrumentor — the install/uninstall contract every integration keeps.

An Instrumentor:
1. `_instrument()` — installs wrapt wrappers. Called lazily via a
   post-import hook so user programs pay no cost if the module never loads.
2. `_uninstrument()` — undoes #1 (tests + live reload).

All `_instrument()` / `_uninstrument()` calls are routed through `safe_try`,
so a bug in one integration never kills the agent or user code. Installation
is process-wide: the class-keyed `_global_installed` registry makes
instrument/uninstrument idempotent across instances.
"""

from __future__ import annotations

import os
import threading
from typing import Any, Callable, Dict, List, Type

from ._log import get_logger
from .errors import safe_try

_log = get_logger("instrumentor")

_global_lock = threading.RLock()
_global_installed: Dict[Type["BaseInstrumentor"], "BaseInstrumentor"] = {}


def _reset_lock_after_fork() -> None:
    # A post-import hook on another thread may hold _global_lock at fork time;
    # the child would inherit it locked with no owner and deadlock on its next
    # deferred instrumentation import. Registered here, where the lock lives.
    global _global_lock
    _global_lock = threading.RLock()


if hasattr(os, "register_at_fork"):  # absent on Windows
    try:
        os.register_at_fork(after_in_child=_reset_lock_after_fork)
    except Exception:  # noqa: BLE001
        _log.debug("os.register_at_fork(after_in_child) failed", exc_info=True)


class BaseInstrumentor:
    def __init__(self) -> None:
        self._installed = False
        self._installing = False
        self._wrapper_targets: List[tuple[str, str]] = []

    # ---- public API --------------------------------------------------------
    def instrument(self) -> None:
        with _global_lock:
            if type(self) in _global_installed:
                return
            ok = _safe_install(self)
            self._installed = bool(ok)
            if self._installed:
                _global_installed[type(self)] = self

    def uninstrument(self) -> None:
        with _global_lock:
            installed = _global_installed.get(type(self))
            if installed is None:
                return
            _safe_uninstall(installed)
            installed._installed = False
            self._installed = False
            _global_installed.pop(type(self), None)

    # ---- subclass hooks ----------------------------------------------------
    def _instrument(self) -> None:  # pragma: no cover
        raise NotImplementedError

    def _uninstrument(self) -> None:  # pragma: no cover
        # Common wrapt patches are restored by _safe_uninstall(). Subclasses
        # only override this for non-wrap resources (listeners, route swaps,
        # logging factories, etc.).
        return None

    # ---- common wrapper ownership -----------------------------------------
    def _record_wrapper_target(self, module: str, target: str) -> None:
        item = (module, target)
        if item not in self._wrapper_targets:
            self._wrapper_targets.append(item)

    def _restore_wrapper_targets(self) -> None:
        from .instrumentations._util import restore_pinpoint_target

        targets, self._wrapper_targets = self._wrapper_targets, []
        for module, target in reversed(targets):
            restore_pinpoint_target(module, target)

    def _wrap_target(
        self,
        module: str,
        target: str,
        wrapper: Callable[..., Any],
    ) -> bool:
        """Install and register a wrapper from a deferred import hook.

        Post-import callbacks may run after ``_instrument`` returns, outside
        its automatic registration scope. They must also become no-ops after
        uninstrument, since wrapt import hooks cannot be unregistered.
        """
        with _global_lock:
            if not (self._installing or self._installed):
                return False
            from .instrumentations._util import (
                begin_wrapper_install,
                end_wrapper_install,
                wrap,
            )

            token = begin_wrapper_install(self)
            try:
                return wrap(module, target, wrapper)
            finally:
                end_wrapper_install(token)


@safe_try
def _safe_install(inst: BaseInstrumentor) -> bool:
    _log.debug("installing %s", type(inst).__name__)
    from .instrumentations._util import begin_wrapper_install, end_wrapper_install

    inst._installing = True
    token = begin_wrapper_install(inst)
    try:
        inst._instrument()
        return True
    except BaseException:
        # Roll back both custom resources and wrappers installed before a
        # partial failure. Otherwise the global guard reports "not installed"
        # while listeners or monkey patches remain active.
        _safe_uninstall(inst)
        raise
    finally:
        end_wrapper_install(token)
        inst._installing = False


@safe_try
def _safe_uninstall(inst: BaseInstrumentor) -> None:
    _log.debug("uninstalling %s", type(inst).__name__)
    try:
        inst._uninstrument()
    finally:
        # Always remove every wrap() target, even when a subclass's custom
        # listener/route cleanup raises part-way through.
        inst._restore_wrapper_targets()
