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

"""Python call-stack capture for SpanEvent.set_error.

`SpanEvent.set_error` passes the resolved `EnableCallstackTrace` value pinned
to its parent span and dumps Python frames into plain `(module, function,
file, line)` tuples only when that flag is true. They flush with the buffered
error at `end()`, where native `SetError` consumes the list in one call — no
reader callback ever re-enters the interpreter. Native attaches the frames to
the span event as a separate Exception, which the UI's call-stack panel
renders.
"""

from __future__ import annotations

import traceback
from collections import deque
from itertools import islice
from typing import List, Optional, Tuple

# Cap how many frames we ship per error: deep recursion would otherwise
# inflate the span event payload. Both capture paths keep the frames nearest
# the failure and drop the outermost ones — see frames_for.
_MAX_FRAMES = 64

# Frame tuple shape used internally and across the native boundary.
_Frame = Tuple[str, str, str, int]  # (module, function, file, line)

# Total entries (thrown exception + causes) recorded per exception chain;
# 0 = unlimited. Set from Config.exception_chain_max_depth at agent.init().
_chain_max_depth: int = 5


def set_chain_max_depth(depth: int) -> None:
    global _chain_max_depth
    _chain_max_depth = max(0, int(depth))


def _cause(exc: BaseException) -> Optional[BaseException]:
    # Python's own traceback rule: an explicit ``raise X from Y`` wins;
    # otherwise the implicit context unless ``raise X from None`` hid it.
    cause = exc.__cause__
    if cause is None and not exc.__suppress_context__:
        cause = exc.__context__
    return cause


def causes_for(exc: BaseException) -> Optional[List[Tuple[str, str, List[_Frame]]]]:
    """Dump the causes behind ``exc`` as ``(name, message, frames)`` entries,
    outermost first, for the chained native ``SetError``.

    Returns None when ``exc`` has no recorded cause or context (the common
    case, kept allocation-free), or when the chain cap admits only ``exc``
    itself. Stops at the cap, at a cycle, or when the chain ends.
    """
    cause = _cause(exc)
    if cause is None:
        return None
    seen = {id(exc)}
    causes: List[Tuple[str, str, List[_Frame]]] = []
    limit = _chain_max_depth
    while cause is not None and id(cause) not in seen and (limit <= 0 or len(causes) + 1 < limit):
        seen.add(id(cause))
        try:
            message = str(cause)
        except Exception:  # noqa: BLE001 — a throwing __str__ must not lose the chain
            message = ""
        causes.append((type(cause).__name__, message, frames_for(cause, enabled=True) or []))
        cause = _cause(cause)
    return causes or None


def frames_for(error_or_name, *, enabled: bool) -> Optional[List[_Frame]]:
    """Dump the call stack frames that describe this error.

    - If `error_or_name` is a `BaseException` with a non-None __traceback__,
      walk the traceback (where the error was actually raised).
    - Otherwise, snapshot the current Python stack — useful when callers
      synthesize an error name/message without raising (e.g. an HTTP 5xx
      handler that decided to record a failure).
    ``enabled`` is the span-local gate: the parent span resolves it from the
    native config snapshot for the generation it was created in. Returns None
    when callstack tracing is disabled; an empty list (still recorded as an
    Exception natively) when there are no frames to show.
    """
    if not enabled:
        return None
    if isinstance(error_or_name, BaseException):
        tb = getattr(error_or_name, "__traceback__", None)
        if tb is None:
            return []
        # walk_tb yields outermost-first, so the *tail* holds the raise site —
        # the one frame the reader needs. Taking the leading _MAX_FRAMES would
        # drop it on any stack deeper than the cap (a Django/DRF request path
        # clears 64 frames on its own), leaving only framework bootstrap. A
        # bounded deque keeps the last _MAX_FRAMES, in order, in O(1) memory.
        return _frames(deque(traceback.walk_tb(tb), maxlen=_MAX_FRAMES))
    # walk_stack yields innermost-first, so _frames' islice already keeps the
    # frames nearest the call site. Agent UIs expect the order
    # traceback.print_stack produces (outermost-first), so reverse, then
    # drop the innermost agent frames so the user's call site is on top.
    frames = _frames(traceback.walk_stack(None))
    frames.reverse()
    while frames and _is_internal(frames[-1]):
        frames.pop()
    return frames


def _frames(walker) -> List[_Frame]:
    return [_describe(f, lineno) for f, lineno in islice(walker, _MAX_FRAMES)]


def _describe(frame, lineno: int) -> _Frame:
    code = frame.f_code
    module = ""
    g = frame.f_globals
    if isinstance(g, dict):
        m = g.get("__name__")
        if isinstance(m, str):
            module = m
    # co_qualname needs 3.11, which is this package's minimum.
    return (module, code.co_qualname, code.co_filename, int(lineno))


def _is_internal(frame: _Frame) -> bool:
    # Strip leading agent frames so the user's own call site is on top: this helper,
    # ``pinpoint.tracer``'s set_error, and any instrumentation wrapper that
    # synthesized the error. Submodules are all internal, but the bare public
    # ``pinpoint`` package is a call the user made — and ``startswith("pinpoint.")``
    # matches the submodules without matching bare ``"pinpoint"``.
    return frame[0].startswith("pinpoint.")


__all__ = ["set_chain_max_depth", "frames_for", "causes_for"]
