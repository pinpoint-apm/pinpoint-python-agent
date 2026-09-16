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

"""Pythonic span wrappers over the native Span type.

Users never construct these directly — they're handed out by `Agent.new_span()`
or `trace()`. Only the span itself is native: span events are created, timed
and stacked entirely in Python, and `Span.end()` replays every finished event
through one native call before the C++ shared_ptr drains the span into the
gRPC sender.
"""

from __future__ import annotations

import _thread
import contextvars
import itertools
import operator
import random
import time
from typing import Any, Optional, Self

from . import _native  # type: ignore[import-not-found]
from . import callstack as _callstack
from . import context as _ctx
from ._log import get_logger
from .annotation import ANNOTATION_API
from .propagator import (
    HEADER_FLAG,
    HEADER_HOST,
    HEADER_PARENT_SPAN_ID,
    HEADER_SPAN_ID,
    HEADER_TRACE_ID,
)
from .service_type import SERVICE_TYPE_PYTHON_METHOD

_log = get_logger("tracer")

# C-implemented sort key for the per-flush finished-events ordering; a lambda
# here would be allocated and invoked once per buffered event on every span end.
_EVENT_SORT_KEY = operator.itemgetter(0)

# Cap on Python-side buffered annotations per span/event: the native
# ``span_max_event_sequence`` bounds events, not these wrapper-side buffers, so a
# long-lived span annotating per iteration would grow one without bound.
_MAX_ANNOTATIONS = 5000

# Verdict-only errors buffered for overflow placeholders. Past the bound each
# excess error crosses immediately, so memory stays bounded without letting a
# run of ignored names hide a later real failure.
_MAX_ERROR_VERDICTS = 100

# Native drops raw SQL over 1 MiB, then normalizes and abbreviates metadata.
# Never cut raw SQL: a large literal/comment can normalize to a tiny query,
# and cutting first changes its SQL id or leaves an unterminated token.
_MAX_SQL_BYTES = 1024 * 1024

# Cap on every other buffered annotation string, pinned until the span ends.
# The unbounded inputs are outbound URLs (batch APIs build megabyte-long
# id-list query strings) and recorded header values, neither of which native
# cuts for us. Far above any real URL or header.
_MAX_ANNOTATION_CHARS = 64 * 1024

# Annotation buffer tags: each selects a native ``SetAnnotation`` overload
# (``_ANN_SQL`` selects ``SetSqlQuery``, ``_ANN_ERROR`` selects ``SetError``).
# Must stay in sync with ``marshal_annotations`` in ``src/_native.cpp``.
_ANN_INT = 0
_ANN_STRING = 1
_ANN_STRING_STRING = 2
_ANN_LONG = 3
_ANN_SQL = 4  # (tag, sql, args) — no int key, SpanEvent only
_ANN_ERROR = 5  # (tag, message) | (tag, name, message[, frames[, causes]]) — no int key
_ANN_LONG_IIBBS = 6  # (tag, key, long, int, int, byte, byte, str) — Span only
_ANN_IGNORED_ERROR = 7  # _ANN_ERROR shape (arity >= 2): recorded, never marks the trace failed

# Span.IgnoreErrors rules whose subclass/cause matching only Python can do:
# (name, message_contains, match_subclasses, match_cause). Empty by default, so
# the default path costs one truthiness test. Set from Config at agent.init().
# ponytail: inline-kwarg rules only — the native snapshot does not expose the
# resolved IgnoreErrors, so file/profile rules cannot carry the flags; expose
# them in SpanConfigSnapshot to lift that.
_IGNORE_RULES: tuple = ()
_IGNORE_CHAIN_MAX = 8

# Floor for the extra grace the implicit-task lifetime timer grants a span whose
# SpanEvent scopes are still open (see _fork_for_async_task._finish). Module-level
# so tests can shrink it.
_TASK_SPAN_GRACE_MIN = 60.0

# Event-position limit defaults, mirroring config.py's span_max_event_depth /
# span_max_event_sequence. Agent.new_span passes the configured values; these
# cover test-built spans.
_DEFAULT_MAX_EVENT_DEPTH = 64
_DEFAULT_MAX_EVENT_SEQUENCE = 5000

# Process-wide async-id source: the id is assigned here and flushed with the
# parent event. ``count().__next__`` is one GIL-protected op.
_ASYNC_ID_GEN = itertools.count(1)


def _next_async_id() -> int:
    # Fold into positive int32 (the wire field), never 0 (NONE_ASYNC_ID).
    return (next(_ASYNC_ID_GEN) - 1) % 0x7FFFFFFF + 1


def _now_ms() -> int:
    """Epoch milliseconds."""
    return time.time_ns() // 1_000_000


# Private generator rather than the module-global one: fork() does not reseed a
# PRNG, so prefork workers would otherwise emit identical span-id sequences, and
# reseeding ``random`` itself would silently break an application that seeded it
# for reproducible behaviour (see _reseed_span_ids).
_span_id_random = random.Random()


def _reseed_span_ids() -> None:
    """Re-seed the span-id generator, called from the after-fork hook.

    Sibling workers forked from one master inherit the parent's PRNG state, so
    without this they hand out the same outbound child span ids.
    """
    _span_id_random.seed()


def _generate_span_id() -> int:
    """Random child span id for outbound propagation: a full-range int64,
    excluding the native -1 sentinel and 0 ("never injected")."""
    while True:
        value = _span_id_random.getrandbits(64)
        if value and value != (1 << 64) - 1:
            return value - (1 << 64) if value >= (1 << 63) else value


class _DiscardAnnotations(list):
    """Annotation sink for overflow placeholder events (``sequence is None``).

    Everything recorded on a placeholder is guaranteed to be dropped at
    ``_finalize`` (no sequence, no record), so buffering it would only pin
    payloads — full SQL text included — until the event is popped. A shared
    empty list whose ``append`` discards keeps every ``annotate_*`` gate and
    ``_buffer_annotation`` untouched for real events.
    """

    __slots__ = ()

    def append(self, item) -> None:  # noqa: ARG002
        pass


_DISCARD_ANNOTATIONS = _DiscardAnnotations()


# The types the native bind marshaller keeps typed instead of routing through
# str(); all of them immutable, so they can sit in the buffer as they are.
_SQL_BIND_SCALARS = (str, bool, int, float)
# The marshaller's int range (int64 min .. uint64 max); an int past it travels
# as text so the exact digits survive.
_SQL_BIND_INT_MIN = -(1 << 63)
_SQL_BIND_INT_MAX = (1 << 64) - 1


def _snapshot_sql_bind(value: Any) -> Any:
    """Freeze one bind value at call time.

    Anything the native marshaller cannot keep typed it records as
    ``str(value)``. Taking that text here rather than at the span-end flush
    keeps a mutable bind from reporting its *later* state — one dict of named
    params reused across a request's queries would otherwise give every query
    the last one's values — and releases the object right away.
    """
    if value is None or isinstance(value, _SQL_BIND_SCALARS):
        if isinstance(value, int) and not _SQL_BIND_INT_MIN <= value <= _SQL_BIND_INT_MAX:
            return str(value)
        return value
    try:
        return str(value)
    except Exception:  # noqa: BLE001
        # Same degradation as the native fallback: a failing __str__ must not
        # raise into the traced query.
        return "<unrepresentable>"


def _snapshot_sql_binds(args: Any) -> Any:
    """``args`` for :meth:`SpanEvent.set_sql_query`, frozen at call time.

    Mirrors how the marshaller reads the container: a list/tuple is one bind
    per item, and any other object (a dict of named params, bytes, a cursor's
    own parameter holder) is a single bind value.
    """
    if isinstance(args, (list, tuple)):
        return tuple(_snapshot_sql_bind(v) for v in args)
    return _snapshot_sql_bind(args)


# One WARNING per process for the single call that carries a whole trace. A
# failure here costs every event, annotation and status the transaction
# collected, and the buffers are already cleared, so nothing retries; at DEBUG a
# persistently broken binding would drop every trace while the agent looks
# healthy.
_flush_failure_warned = False


def _warn_flush_failed() -> None:
    """Report a failed span flush: loudly once, quietly after that.

    Kept to once because the cause is a broken binding, not a per-request
    condition — repeating it per span would bury the rest of the log.
    """
    global _flush_failure_warned
    if _flush_failure_warned:
        _log.debug("end_span_with_data failed", exc_info=True)
        return
    _flush_failure_warned = True
    _log.warning(
        "end_span_with_data failed: this transaction's span events, "
        "annotations and status were all dropped. Further occurrences log "
        "at DEBUG.", exc_info=True)


def _capped(value) -> str:
    """``str(value)``, bounded by ``_MAX_ANNOTATION_CHARS``."""
    text = str(value)
    return text if len(text) <= _MAX_ANNOTATION_CHARS \
        else text[:_MAX_ANNOTATION_CHARS]


def _error_name_message(error_or_name: Any,
                        message: Optional[str]) -> tuple[str, str]:
    """Normalize ``set_error`` for verdict-only paths.

    The one-string overload means ``SetError(message)`` in C++, whose error
    name is ``Error``. Exception instances retain Python's established
    ``type(error).__name__`` / ``str(error)`` mapping.
    """
    if isinstance(error_or_name, BaseException):
        return type(error_or_name).__name__, str(error_or_name)
    if message is None:
        return "Error", str(error_or_name)
    return str(error_or_name), str(message)


def set_ignore_rules(rules: Any) -> None:
    """Keep the ``span_ignore_errors`` rules that need Python-side matching."""
    global _IGNORE_RULES
    kept = []
    for rule in rules or ():
        if not isinstance(rule, dict):
            continue
        sub, cause = bool(rule.get("match_subclasses")), bool(rule.get("match_cause"))
        if sub or cause:
            kept.append((str(rule.get("name") or ""), str(rule.get("message_contains") or ""),
                         sub, cause))
    _IGNORE_RULES = tuple(kept)


def _rule_matches(rule: tuple, exc: BaseException) -> bool:
    name, contains, sub, _cause = rule
    if name:
        if sub:
            if not any(k.__name__ == name for k in type(exc).__mro__[:-1]):
                return False
        elif type(exc).__name__ != name:
            return False
    if contains:
        try:
            return contains in str(exc)
        except Exception:  # noqa: BLE001
            return False
    return True


def _python_ignored(error: Any) -> bool:
    """True when ``error`` matches a rule via subclass or cause: the error is
    recorded but must not fail the trace. Exact-name rules stay native."""
    if not _IGNORE_RULES or not isinstance(error, BaseException):
        return False
    try:
        for rule in _IGNORE_RULES:
            exc, seen, hops = error, {id(error)}, 0
            while exc is not None:
                if _rule_matches(rule, exc):
                    return True
                if not rule[3] or hops >= _IGNORE_CHAIN_MAX:
                    break
                exc = _callstack._cause(exc)
                if exc is None or id(exc) in seen:
                    break
                seen.add(id(exc))
                hops += 1
    except Exception:  # noqa: BLE001 — a hostile exception object must not break set_error
        return False
    return False


def _as_ignored(ann: tuple) -> tuple:
    """Retag a buffered error so native records it without failing the trace.

    ``SetIgnoredError`` has no one-argument form, so the bare-message shape
    ``(tag, message)`` — which native ``SetError(message)`` would have named
    ``"Error"`` — is promoted to that explicit pair.
    """
    if len(ann) == 2:
        return (_ANN_IGNORED_ERROR, "Error", ann[1])
    return (_ANN_IGNORED_ERROR,) + ann[1:]


def _buffer_annotation(anns: Optional[list], ann: tuple) -> list:
    """Append ``ann`` to a per-span/event annotation buffer, enforcing
    ``_MAX_ANNOTATIONS``, and return the (possibly newly created) list.

    Callers store the result back on ``self._annotations`` so the common
    no-annotation object never allocates a list (``anns is None`` stays until
    the first annotation). Beyond the cap the overflow is dropped and a single
    marker annotation records that truncation happened — see ``_MAX_ANNOTATIONS``.
    """
    if anns is None:
        return [ann]
    n = len(anns)
    if n < _MAX_ANNOTATIONS:
        anns.append(ann)
    elif n == _MAX_ANNOTATIONS:
        # Appended exactly once: the next call sees n > _MAX_ANNOTATIONS.
        anns.append((
            _ANN_STRING, ANNOTATION_API,
            f"pinpoint: annotations truncated at {_MAX_ANNOTATIONS}",
        ))
    return anns


class _Annotatable:
    """Buffered setters/annotations shared verbatim by :class:`Span` and
    :class:`SpanEvent`. Both subclasses keep the touched state
    (``_ended``, ``_service_type``, ``_end_point``, ``_annotations``) in
    their own ``__slots__``; ``Self`` keeps the fluent chaining typed as
    the concrete subclass."""

    __slots__ = ()

    def set_service_type(self, t: int) -> Self:
        if not self._ended:
            self._service_type = int(t)
        return self

    def set_end_point(self, endpoint: str) -> Self:
        if not self._ended:
            self._end_point = str(endpoint)
        return self

    def annotate_int(self, key: int, value: int) -> Self:
        if self._ended:
            return self
        self._annotations = _buffer_annotation(
            self._annotations, (_ANN_INT, int(key), int(value)))
        return self

    def annotate_long(self, key: int, value: int) -> Self:
        """64-bit integer annotation (native ``SetAnnotation(int64)``).

        Use for values that can exceed the int32 range — Kafka offsets, byte
        counts, timestamps — which would overflow :meth:`annotate_int` and get
        dropped at finalize."""
        if self._ended:
            return self
        self._annotations = _buffer_annotation(
            self._annotations, (_ANN_LONG, int(key), int(value)))
        return self

    def annotate_string(self, key: int, value: str) -> Self:
        if self._ended:
            return self
        self._annotations = _buffer_annotation(
            self._annotations, (_ANN_STRING, int(key), _capped(value)))
        return self

    def annotate_string_string(self, key: int, s1: str, s2: str) -> Self:
        """Two-string annotation (e.g. ``ANNOTATION_MONGO_JSON_DATA``).

        Mirrors the native two-string ``SetAnnotation`` overload — used by
        annotations whose Pinpoint UI rendering expects a paired value
        (data + bindings, normalized + original, …)."""
        if self._ended:
            return self
        self._annotations = _buffer_annotation(
            self._annotations,
            (_ANN_STRING_STRING, int(key), _capped(s1), _capped(s2)))
        return self


class SpanEvent(_Annotatable):
    """Pure-Python span event.

    Owned by its parent :class:`Span`, which assigns its position (sequence and
    depth) and keeps it on the Python event stack until :meth:`end`. No native
    call happens per event: everything recorded here is buffered on the
    wrapper, and the parent span replays all of its finished events through one
    native call at ``Span.end()``.

    Inherits the span's thread-sharing contract (see the ``Span``
    docstring): use it on the thread that drives the parent span. Shared
    across threads anyway, the same guarantee applies — crash-safety only
    (latched ``end()``, liveness guards), not trace accuracy.
    """

    __slots__ = (
        "_span", "_native_lock", "_annotations", "_ended",
        "_service_type", "_operation_name", "_destination", "_end_point",
        "_next_span_id", "_sequence", "_depth", "_start_time",
        "_async_id", "_async_seq",
    )

    def __init__(
        self,
        span: "Span",
        operation: str = "",
        service_type: int = SERVICE_TYPE_PYTHON_METHOD,
        sequence: Optional[int] = 0,
        depth: int = 1,
    ):
        self._span = span
        # All wrappers of one Span share its lock: the native paths release the
        # GIL, so only this serializes crossings into a span being drained. It
        # buys crash-safety, not trace accuracy.
        self._native_lock = span._native_lock
        # append is one GIL-protected C op; `+= 1` on an int would race. The
        # entry is this wrapper itself: it is both the event-stack slot
        # (back() = innermost open scope, which outbound injection rides) and
        # what Span.end() drains for events user code never ended.
        span._active_events.append(self)
        # Claimed under _native_lock by the first end(): makes finalize (and
        # the flush-record append) one-shot.
        self._ended = False
        # (tag, key, *values) tuples, flushed with the event record at
        # Span.end(). None until the first annotate_* so the common event
        # allocates no list. Overflow placeholders discard instead of
        # buffering — their record is dropped at _finalize anyway.
        self._annotations: Optional[list] = (
            None if sequence is not None else _DISCARD_ANNOTATIONS)
        # Already normalized by the sole caller (Span.new_span_event).
        self._service_type = service_type
        self._operation_name = operation
        self._destination = ""
        self._end_point = ""
        # Child span id generated at inject time (0 = never injected); flushed
        # with the other buffered fields so native records it as nextSpanId.
        self._next_span_id = 0
        # Position assigned by the parent span. ``None`` marks an overflow
        # placeholder past max depth/sequence: it stays on the stack so nesting
        # and outbound injection keep working, but it consumed no
        # sequence/depth and is dropped at flush.
        self._sequence = sequence
        self._depth = depth
        self._start_time = _now_ms()
        # Async linkage, assigned by Span.new_async_span on first use and
        # flushed with the event so the collector links the async children
        # recorded against it.
        self._async_id = 0
        self._async_seq = 0

    # ---- annotation helpers (shared ones live on _Annotatable) --------------
    def set_operation_name(self, name: str) -> "SpanEvent":
        if not self._ended:
            self._operation_name = str(name)
        return self

    def set_destination(self, dest: str) -> "SpanEvent":
        if not self._ended:
            self._destination = str(dest)
        return self

    def set_error(self, error_or_name: Any, message: Optional[str] = None,
                  mark_error: bool = True) -> "SpanEvent":
        """Retain the error effect and replay it onto native at span end.

        For recording events, the call stack is captured *here* — ``frames_for``
        dumps the traceback into plain tuples — so deferred native ``SetError``
        records the same frames. Overflow placeholders retain only name and
        message for the transaction verdict and collect no frames or metadata.

        ``mark_error=False`` records the error on this step without failing
        the transaction. An overflow placeholder then has nothing left to keep
        — it records no step and was only holding the verdict — so the call is
        a no-op there.
        """
        if self._ended:
            return self
        if self._sequence is None:
            if not mark_error:
                return self
            # Overflow retains only the transaction verdict. In particular, do
            # not call frames_for(): a placeholder drops call stacks and
            # exception metadata along with the event itself.
            if _python_ignored(error_or_name):
                return self
            name, msg = _error_name_message(error_or_name, message)
            with self._native_lock:
                if self._ended or self._span is None:
                    return self
                self._span._record_error_verdict_locked(name, msg)
            return self
        if isinstance(error_or_name, BaseException):
            name = type(error_or_name).__name__
            msg = str(error_or_name)
        else:
            name = str(error_or_name)
            msg = "" if message is None else str(message)

        # The parent span owns the native-resolved flag for the generation it
        # was created in. Never enter frames_for on the disabled path: that
        # keeps traceback walkers and frame/string allocation cold.
        span = self._span
        frames = (_callstack.frames_for(error_or_name, enabled=True)
                  if span is not None and span._enable_callstack_trace
                  else None)
        if frames is not None:
            # Chained exceptions (__cause__/__context__) ride along as a 5th
            # element so the UI can show the root cause; None when absent, so
            # the plain path allocates nothing extra.
            causes = (_callstack.causes_for(error_or_name)
                      if isinstance(error_or_name, BaseException) else None)
            ann = ((_ANN_ERROR, name, msg, frames, causes) if causes
                   else (_ANN_ERROR, name, msg, frames))
        elif not isinstance(error_or_name, BaseException) and message is None:
            # Preserve the 1- vs 2-arg convention: native error metadata differs.
            ann = (_ANN_ERROR, name)
        else:
            ann = (_ANN_ERROR, name, msg)
        if not mark_error or _python_ignored(error_or_name):
            ann = _as_ignored(ann)
        self._annotations = _buffer_annotation(self._annotations, ann)
        return self

    def set_sql_query(self, sql: str, args: Any = "") -> "SpanEvent":
        """Record a SQL statement and its bound parameters on this event.

        ``args`` may be a list or tuple of ``None``, ``str``, ``bool``,
        ``int``, and ``float`` values.  The native agent preserves those
        scalar types while formatting and size-limiting the annotation.  Any
        other element type (``datetime``, ``Decimal``, ...) is recorded as
        ``str(value)``, and any other ``args`` object (a dict of named
        params, bytes, ...) is recorded whole as a single ``str(value)`` —
        bind conversion never raises into the traced query.  A single string
        remains supported for compatibility with older instrumentations.

        Buffered like the ``annotate_*`` family and flushed to native in one
        call at :meth:`end`, so this never crosses into the extension on the
        query hot path.  Every ``str(value)`` above is therefore taken *here*
        rather than at the flush: see :func:`_snapshot_sql_binds`.
        """
        if self._ended:
            return self
        sql_text = str(sql)
        if len(sql_text) > _MAX_SQL_BYTES:
            return self
        if (not sql_text.isascii()
                and len(sql_text.encode("utf-8", errors="replace")) > _MAX_SQL_BYTES):
            return self
        args = _snapshot_sql_binds(args)
        self._annotations = _buffer_annotation(
            self._annotations, (_ANN_SQL, sql_text, args))
        return self

    # ---- lifecycle ---------------------------------------------------------
    def end(self) -> None:
        """End the span event: stamp its end time and buffer its completed
        record on the parent span, to be flushed in one batch at ``Span.end()``.

        Idempotent and thread-safe: the first caller to take the lock claims
        ``_ended`` (via ``_finalize``) and every later caller — a manual
        ``end()`` followed by ``with``-exit, or a cross-thread done-callback
        race — observes the flag and no-ops, so the record can't be buffered
        twice. The stack pop (including the out-of-order unwind) lives on the
        parent.
        """
        # No lock here: ``_finish_span_event`` re-checks ``_ended`` under the
        # shared span lock itself, so taking it first would only re-enter the
        # same RLock on every event end. The unlocked ``_ended`` read is a fast
        # path only — a stale False just proceeds into the locked check.
        if self._ended:
            return
        parent = self._span
        try:
            finish = parent._finish_span_event
        except AttributeError:
            # Test-double or detached parent (``_finalize`` nulled ``_span``
            # concurrently): latch and drop the payloads.
            with self._native_lock:
                if not self._ended:
                    self._finalize(None, 0)
            return
        finish(self)

    def _finalize(self, span: Optional["Span"], end_time: int) -> None:
        """Latch this event ended, buffer its completed record on ``span``,
        and drop the payload references.

        Called with the span lock held (by ``Span._finish_span_event`` or the
        ``Span.end()`` drain). ``span`` is ``None`` when the record must be
        dropped instead: a late end after the span already flushed, or a
        detached/test-double parent.
        """
        self._ended = True
        annotations = self._annotations or ()
        if span is not None and self._sequence is not None:
            # The depth reservation is released as the event leaves the
            # stack. Overflow placeholders reserved nothing and record nothing
            # (the guard above skips both).
            span._event_depth -= 1
            span._finished_events.append((
                self._sequence, self._depth, self._start_time, end_time,
                self._service_type, self._operation_name,
                self._destination, self._end_point,
                self._next_span_id, self._async_id, annotations,
            ))
        # An ended wrapper has no retry path, so drop the payloads now rather
        # than retaining them until an app-held SpanEvent is collected.
        self._annotations = None
        self._span = None  # type: ignore[assignment]
        self._operation_name = ""
        self._destination = ""
        self._end_point = ""

    # ---- context-manager semantics (event scope) ---------------------------
    def __enter__(self) -> "SpanEvent":
        return self

    def __exit__(self, exc_type, exc_val, tb) -> None:
        try:
            if exc_val is not None:
                # set_error can raise (broken __str__, native shutdown failure);
                # never let it replace the in-flight exception or skip end().
                self.set_error(exc_val)
        except Exception:  # noqa: BLE001
            _log.debug("set_error failed in SpanEvent.__exit__", exc_info=True)
        finally:
            self.end()


class Span(_Annotatable):
    """Wrapper around `_native.Span`. One per inbound transaction.

    ``Agent.new_span()`` already short-circuits unsampled transactions to
    :class:`pinpoint.agent.UnSampledSpan`, so any live ``Span`` here is by
    construction sampled — ``sampled`` is a constant ``True`` and no
    native ``is_sampled()`` call is needed.

    Thread-sharing contract: a span — including the :class:`SpanEvent` scopes
    it hands out — is meant to be driven by a single thread for its whole
    lifetime. When a span IS
    shared across threads anyway, the baseline assumption is that the trace
    may come out wrong; what this wrapper defends is only that the process
    must not crash and no exception may leak into user code:

    * ``end()`` claims a one-shot flag under the span's native lock, so
      racing ends finalize the native span exactly once;
    * every native-touching method is serialized through one per-span lock;
      ``end()`` nulls the handle under that lock, so late/racing calls wait
      and then degrade to a no-op instead of reaching the drained span;
    * annotation buffers and the live-event counter use single-op list
      mutations, so cross-thread interleavings lose data at worst;
    * the native layer's own ``finished_``/atomic guards remain the final
      defense for direct native-handle misuse outside this wrapper.

    None of this makes concurrent use *correct*: annotations can drop,
    events can mis-nest, a trace can be truncated. To trace work on another
    thread, do NOT share this span — create a child with
    :meth:`new_async_span` on the owning thread (or use the
    ``pinpoint.async_trace`` helper) and hand that child over instead;
    ``context.py`` applies the
    same rule to implicitly inherited spans (asyncio-task fork, detached
    copied worker contexts).
    """

    __slots__ = (
        "_native", "_native_lock", "_ended", "_tokens", "_annotations",
        "_trace_id", "_span_id", "_span_id_str", "_parent_span_id", "_collect_url_stat",
        "_async_task_span_timeout", "_active_events", "_finished_events",
        "_event_sequence", "_event_depth",
        "_max_event_depth", "_max_event_sequence",
        "_config_snapshot", "_enable_callstack_trace",
        "_error_verdicts", "_async_root",
        "_service_type", "_remote_address", "_end_point",
        "_acceptor_host", "_status_code", "_url_stat",
        "_flags_str", "_inject_base", "_event_flush_size", "_logging",
    )

    def __init__(self, native_span: "_native.Span",
                 collect_url_stat: bool = True,
                 async_task_span_timeout: float = 300.0,
                 flags: int = 0,
                 inject_base: tuple = (),
                 trace_id: Optional[str] = None,
                 span_id: Optional[int] = None,
                 max_event_depth: int = _DEFAULT_MAX_EVENT_DEPTH,
                 max_event_sequence: int = _DEFAULT_MAX_EVENT_SEQUENCE,
                 config_snapshot: tuple = (),
                 enable_callstack_trace: bool = False,
                 async_root: bool = False,
                 parent_span_id: int = -1,
                 acceptor_host: str = "",
                 event_flush_size: int = 0):
        self._native = native_span
        # Finished-event count at which the buffer is replayed to native
        # mid-span, so a long transaction streams SpanChunks instead of pinning
        # every event until end(). 0 keeps the single end() replay.
        self._event_flush_size = max(0, int(event_flush_size))
        # The C++ wrapper releases the GIL around create/finalize, so this lock
        # protects native object lifetime when callers violate the single-thread
        # contract. It deliberately does not make event ordering correct.
        self._native_lock = _thread.RLock()
        # See SpanEvent: makes "a second end() is a no-op" hold across threads.
        self._ended = False
        # One reset token per active ``with`` scope, so re-entry is safe.
        self._tokens: list[contextvars.Token] = []
        self._annotations: Optional[list] = None
        # Identity, captured by Agent.new_span from the creation call's
        # return. Async children inherit the parent's values.
        self._trace_id: Optional[str] = trace_id
        self._span_id: Optional[int] = span_id
        self._span_id_str: Optional[str] = None
        self._parent_span_id = int(parent_span_id)
        self._collect_url_stat = bool(collect_url_stat)
        self._async_task_span_timeout = max(
            0.0, float(async_task_span_timeout))
        # The Python event stack: live SpanEvent scopes, back() being the
        # innermost — what outbound injection rides and what end() pops (with
        # out-of-order unwinding). Mutated with one GIL-protected list op each
        # so cross-thread start/end pairs stay atomic. The implicit-task
        # timeout waits for this to drain, so it can't finalize under an
        # instrumentation suspended at an await.
        self._active_events: list = []
        # Completed event records, replayed to native in one batch by end() —
        # the only per-event native cost.
        self._finished_events: list = []
        # Event position counters. An async child's native side already holds
        # its root async event at sequence 0 / depth 1, so its Python counters
        # start past that slot.
        self._event_sequence = 1 if async_root else 0
        self._event_depth = 2 if async_root else 1
        self._max_event_depth = int(max_event_depth)
        self._max_event_sequence = int(max_event_sequence)
        # Native-resolved config generation captured with this span. Header
        # recording reads it directly, so config-file and hot-reload values
        # stay aligned with native event processing for the span's lifetime.
        self._config_snapshot = config_snapshot
        self._enable_callstack_trace = bool(enable_callstack_trace)
        # Verdict-only errors from Python-side overflow placeholders. None on
        # the normal path, so a span that never overflows pays no list allocation.
        self._error_verdicts: Optional[list] = None
        # An async child may outlive the root object. Its error must reach the
        # shared root at set_error time, before the root can serialize PSpan;
        # ordinary/root spans safely batch the same verdict until their own end.
        self._async_root = bool(async_root)
        # Outbound propagation inputs: the inbound Pinpoint-Flags value and
        # the agent's constant pairs (pAppName/pAppType/...), both set by
        # Agent.new_span. See inject_context_items.
        self._flags_str = str(int(flags))
        self._inject_base = (inject_base if all(value for _, value in inject_base)
                             else tuple((key, value) for key, value in inject_base if value))
        self._service_type: Optional[int] = None
        self._remote_address: Optional[str] = None
        self._end_point: Optional[str] = None
        self._acceptor_host: Optional[str] = acceptor_host or None
        self._status_code: Optional[int] = None
        self._url_stat: Optional[tuple[str, str, int]] = None
        # Set once the trace/span ids were written to an application log (see
        # set_logging); flushed at end().
        self._logging = False

    # ---- span metadata -----------------------------------------------------
    @property
    def trace_id(self) -> str:
        # Identity arrives from the creation call (Agent.new_span); the native
        # binding has no per-field getters.
        return self._trace_id or ""

    @property
    def span_id(self) -> int:
        return self._span_id or 0

    @property
    def span_id_str(self) -> str:
        """String form of :attr:`span_id`, cached per span.

        The logging integration stamps this on every log record emitted within
        the transaction, so cache the conversion instead of re-running
        ``str(span_id)`` per record."""
        s = self._span_id_str
        if s is None:
            s = self._span_id_str = str(self.span_id)
        return s

    @property
    def sampled(self) -> bool:
        return True

    def inject_context_items(self) -> tuple:
        """Outbound propagation headers for the innermost open span event.

        The child span id is generated here and buffered on that event (flushed
        as its ``nextSpanId`` at ``end()``), and every header value comes from
        wrapper state — no native call beyond the first (cached) trace/span-id
        read.
        """
        with self._native_lock:
            if self._ended or self._native is None:
                # A finished span injects nothing.
                return ()
            events = self._active_events
            event = events[-1] if events else None
            if event is None:
                # Nothing to inject against, and nothing to record the
                # generated child span id on.
                _log.warning(
                    "inject_context_items: span has no open span event")
                return ()
            # Stamped under the same lock that guarded the liveness check: a
            # racing end() must not finalize this event in between, or the id
            # injected downstream would never be recorded as the event's
            # nextSpanId and the downstream span would dangle with no parent.
            next_span_id = _generate_span_id()
            while next_span_id in (self._span_id, self._parent_span_id, event._next_span_id):
                next_span_id = _generate_span_id()
            event._next_span_id = next_span_id
            destination = event._destination
        return (
            (HEADER_TRACE_ID, self.trace_id),
            (HEADER_SPAN_ID, str(next_span_id)),
            (HEADER_PARENT_SPAN_ID, self.span_id_str),
            (HEADER_FLAG, self._flags_str),
            *self._inject_base,
            *(((HEADER_HOST, destination),) if destination else ()),
        )

    def set_remote_address(self, address: str) -> "Span":
        if not self._ended:
            self._remote_address = str(address)
        return self

    def set_end_point(self, endpoint: str) -> "Span":
        """The first non-empty endpoint wins.

        A transaction has one endpoint, and the outermost instrumentation is
        the one that knows it: when a framework hook runs inside a transport
        hook (ASGI under WSGI, a router under a server middleware), last-wins
        would let the inner one relabel the whole transaction. An empty string
        never claims the slot — callers pass ``endpoint or ""`` and the flush
        drops empty values anyway, so an empty first write must not block the
        real one.

        :class:`SpanEvent` keeps the inherited last-wins setter: its endpoint
        describes one outbound step, not the transaction.
        """
        if not self._ended and not self._end_point:
            self._end_point = str(endpoint)
        return self

    def set_acceptor_host(self, host: str) -> "Span":
        if not self._ended:
            self._acceptor_host = str(host)
        return self

    def set_status_code(self, code: int) -> "Span":
        if not self._ended:
            self._status_code = int(code)
        return self

    def set_error(self, error_or_name: Any, message: Optional[str] = None,
                  mark_error: bool = True) -> "Span":
        """Buffered like ``annotate_*`` and replayed onto native at :meth:`end`.

        ``mark_error=False`` records the error but leaves the transaction
        successful. Use it for a failure the application handles as a normal
        outcome (a retried call, an expected lookup miss) that is still worth
        seeing in the trace. The trace is also left successful when the error
        matches a configured ignore rule; see :func:`_python_ignored`.
        """
        if self._ended:
            return self
        if isinstance(error_or_name, BaseException):
            ann = (_ANN_ERROR, type(error_or_name).__name__, str(error_or_name))
        elif message is None:
            ann = (_ANN_ERROR, str(error_or_name))
        else:
            ann = (_ANN_ERROR, str(error_or_name), str(message))
        if not mark_error or _python_ignored(error_or_name):
            ann = _as_ignored(ann)
        self._annotations = _buffer_annotation(self._annotations, ann)
        return self

    def set_logging(self) -> "Span":
        """Mark this span as logged: its trace id and span id were written
        to an application log record.

        The Pinpoint web UI lists a span's log lines only for spans carrying
        this flag. The stdlib ``logging`` integration calls it whenever it
        stamps :attr:`trace_id` / :attr:`span_id_str` on a record; call it
        yourself when a logger outside that integration writes the ids. Sets a
        wrapper flag only, flushed with the rest of the span at :meth:`end`, so
        it is free to call per record.
        """
        self._logging = True
        return self

    def set_url_stat(self, url_pattern: str, method: str, status_code: int) -> "Span":
        if self._ended or not self._collect_url_stat:
            return self
        self._url_stat = (str(url_pattern) or "/NULL", str(method), int(status_code))
        return self

    def annotate_long_iibbs(self, key: int, long_value: int, int1: int,
                            int2: int, byte1: int, byte2: int, s: str) -> "Span":
        """Composite long/int/int/byte/byte/string annotation — the
        ``ANNOTATION_HTTP_PROXY_HEADER`` payload shape (http_helper only)."""
        if self._ended:
            return self
        self._annotations = _buffer_annotation(
            self._annotations,
            (_ANN_LONG_IIBBS, int(key), int(long_value), int(int1), int(int2),
             int(byte1), int(byte2), str(s)))
        return self

    # ---- child span events -------------------------------------------------
    def new_span_event(self, operation: str,
                       service_type: int = SERVICE_TYPE_PYTHON_METHOD) -> SpanEvent:
        """Create a child span event, managed entirely in Python.

        This span assigns the event's sequence and depth and pushes it onto the
        Python event stack — no native call. Past the configured max
        depth/sequence an overflow placeholder is handed out instead: it
        records nothing and is dropped at flush, but stays on the stack so
        nesting and outbound context propagation keep working — overflow is a
        profiling depth limit, not a sampling decision.
        """
        # Liveness guard — see trace_id. Public API paths (trace(),
        # @spanevent) reach here unprotected by safe_wrapper, so a late call must
        # degrade to a no-op event rather than push onto a drained event stack.
        with self._native_lock:
            if self._native is None:
                from .agent import _NULL_SPAN_EVENT
                return _NULL_SPAN_EVENT
            # Normalize once here for both the record and the wrapper state.
            op = str(operation)
            st = int(service_type)
            seq = self._event_sequence
            depth = self._event_depth
            if depth - 1 > self._max_event_depth or seq >= self._max_event_sequence:
                # Debug, not warning: this fires once per discarded event.
                _log.debug("span event maximum depth/sequence exceeded "
                           "(depth:%d, seq:%d)", depth, seq)
                # Real events are bounded by max_event_sequence, but these
                # placeholders are not: a long-lived span whose events are
                # opened and never ended would grow the stack one wrapper per
                # call for its whole life (the annotation buffers got the same
                # cap via _MAX_ANNOTATIONS). Past the bound, reuse an existing
                # owner-bound overflow placeholder instead of allocating one
                # more. A process-wide null event would lose set_error(), and
                # storing an owner on that singleton would mix requests.
                if (len(self._active_events)
                        >= self._max_event_sequence + self._max_event_depth):
                    for event in reversed(self._active_events):
                        if event._sequence is None:
                            return event
                    # Defensive only: real events are bounded below the sum,
                    # so reaching the cap implies at least one placeholder.
                    from .agent import _NULL_SPAN_EVENT
                    return _NULL_SPAN_EVENT
                return SpanEvent(self, operation=op, service_type=st,
                                 sequence=None, depth=depth)
            self._event_sequence = seq + 1
            self._event_depth = depth + 1
            return SpanEvent(self, operation=op, service_type=st,
                             sequence=seq, depth=depth)

    def _record_error_verdict_locked(self, name: str, message: str) -> None:
        """Keep an overflow error's native-policy verdict and no profiling data.

        Called with ``_native_lock`` held. Async spans apply the verdict at
        once, because a child can end after the shared root it must mark; root
        spans batch it into end_span_with_data.
        """
        native = self._native
        if self._ended or native is None:
            return
        errors = self._error_verdicts
        if self._async_root or (
                errors is not None and len(errors) >= _MAX_ERROR_VERDICTS):
            try:
                native.mark_error(name, message)
            except Exception:  # noqa: BLE001
                _log.debug("native overflow error verdict failed", exc_info=True)
            return
        if errors is None:
            self._error_verdicts = [(name, message)]
        else:
            errors.append((name, message))

    def _finish_span_event(self, event: SpanEvent) -> None:
        """Pop the event stack down to and including ``event``, buffering each
        popped event's completed record.

        A well-nested trace has ``event`` on top, so this is a single pop.
        When user code ends events out of order (parent before child), the
        events above ``event`` are implicitly finished — as ``end()`` does —
        rather than stranding them on the stack.
        """
        with self._native_lock:
            if event._ended:
                return
            if self._ended:
                # end() already drained the stack and flushed; a late event
                # end has nowhere to record.
                event._finalize(None, 0)
                return
            stack = self._active_events
            if stack and stack[-1] is event:
                # Well-nested end — effectively every event; skip the O(depth)
                # membership scan below.
                stack.pop()
                event._finalize(self, _now_ms())
                self._flush_finished_events_locked()
                return
            if event not in stack:
                _log.warning("abnormal span - ended span event not on the "
                             "event stack")
                event._finalize(None, 0)
                return
            _log.warning("span event ended out of order; implicitly "
                         "finishing nested events")
            end_time = _now_ms()
            while stack:
                top = stack.pop()
                top._finalize(self, end_time)
                if top is event:
                    self._flush_finished_events_locked()
                    return

    def _flush_finished_events_locked(self) -> None:
        """Replay the finished-event buffer to native once it reaches the
        chunk size, which emits a SpanChunk. Called with ``_native_lock`` held.
        Ordering across flushes matches the native path: nested children ship
        before their still-open parent."""
        n = self._event_flush_size
        finished = self._finished_events
        if not n or len(finished) < n:
            return
        record = getattr(self._native, "record_span_events", None)
        if record is None:
            return  # binding without the batch call: keep the end() replay
        self._finished_events = []
        finished.sort(key=_EVENT_SORT_KEY)
        try:
            record(finished)
        except Exception:  # noqa: BLE001
            _log.debug("record_span_events failed; %d event(s) dropped",
                       len(finished), exc_info=True)

    # ---- async / background-work spans -------------------------------------
    def new_async_span(self, operation: str) -> "Span":
        """Create an async child span linked to this span's current span event.

        The returned :class:`Span` is the handle the caller passes to a thread,
        coroutine, or any other deferred-execution context. In that context the
        recipient uses it as a context manager (``with async_span: ...``) so
        :func:`current_span` resolves to it and ``end()`` runs at exit; child
        :meth:`trace` calls then attach to the async span exactly like they
        would on a synchronous one.

        The async link is recorded against the parent's *currently active* span
        event, so this method must be called inside an enclosing
        ``with span.new_span_event(...):`` block. Without an active event a
        no-op span is returned and the async work is silently untracked.

        The link ids are assigned here: ``async_id`` is stamped on the
        innermost open event — flushed with it at ``Span.end()`` — and passed
        to native together with the per-event async sequence.
        """
        # Liveness guard — see trace_id. A no-op child keeps the
        # caller's ``with async_span:`` protocol working after a parent end.
        with self._native_lock:
            native = self._native
            if native is None:
                from .agent import _NullSpan
                return _NullSpan()
            events = self._active_events
            event = events[-1] if events else None
            if event is None:
                _log.warning("new_async_span: span has no open span event")
                from .agent import _NullSpan
                return _NullSpan()
            if event._sequence is None:
                # An overflow placeholder is never recorded, so an async child
                # would dangle from an event that is never sent.
                from .agent import _NullSpan
                return _NullSpan()
            if event._async_id == 0:
                event._async_id = _next_async_id()
            event._async_seq += 1
            child = native.new_async_span(
                str(operation), event._async_id, event._async_seq)
        # The async child carries the parent's trace id and span id, so hand
        # down this wrapper's cached identity too. async_root=True: the child's
        # native side holds the root async event, so its Python event counters
        # start past sequence 0 / depth 1.
        return Span(
            child,
            collect_url_stat=self._collect_url_stat,
            async_task_span_timeout=self._async_task_span_timeout,
            flags=0,
            inject_base=self._inject_base,
            trace_id=self._trace_id,
            span_id=self._span_id,
            parent_span_id=self._parent_span_id,
            max_event_depth=self._max_event_depth,
            max_event_sequence=self._max_event_sequence,
            config_snapshot=self._config_snapshot,
            enable_callstack_trace=self._enable_callstack_trace,
            async_root=True,
            event_flush_size=self._event_flush_size,
        )

    def _fork_for_async_task(self, task: Any) -> Optional["Span"]:
        """Create the task-local span used for implicit ContextVar hand-off.

        A child asyncio task inherits the parent's ContextVar value, but it
        must not inherit the parent's event stack. Open and close the
        linking event synchronously (there is no task switch without an
        ``await``), then let the child task own a separate async span. The done
        callback covers normal completion, failure, and cancellation before
        the child records another event. A lifetime timer also finalizes the
        native span when inherited fire-and-forget work never completes.
        """
        if self._ended:
            return None
        # Pure-Python test doubles may not expose the native async-span
        # primitive; the production binding always does.
        if not callable(getattr(self._native, "new_async_span", None)):
            return self

        operation = "asyncio.task"
        async_span: Optional[Span] = None
        timeout_handle: Any = None
        try:
            with self.new_span_event(operation):
                created = self.new_async_span(operation)
                async_span = created

            # One-element ownership latch: whichever callback fires first takes
            # and ends the span, and the loser drops its strong reference.
            owned_span = [created]
            grace_deadline: Optional[float] = None

            def _finish(_task: Any = None) -> None:
                nonlocal timeout_handle, grace_deadline
                try:
                    span = owned_span[-1]
                except IndexError:
                    return
                # The timer must not finalize the native stack under a SpanEvent
                # scope suspended at an await, so it re-checks instead. Bounded by
                # a grace deadline: a leaked event would otherwise re-arm at 1 Hz
                # and pin the span forever — the leak this timer exists to reap.
                # The task done path never defers; no user code remains.
                if _task is None and span._active_events:
                    now = loop.time()
                    if grace_deadline is None:
                        grace_deadline = now + max(
                            timeout, _TASK_SPAN_GRACE_MIN)
                    if now < grace_deadline:
                        timeout_handle = loop.call_later(
                            min(timeout, 1.0), _finish)
                        return
                owned_span.pop()
                handle = timeout_handle
                timeout_handle = None
                if _task is not None and handle is not None:
                    handle.cancel()
                span.end()

            timeout = self._async_task_span_timeout
            loop = task.get_loop()
            if timeout > 0:
                timeout_handle = loop.call_later(timeout, _finish)
            task.add_done_callback(_finish)
            return created
        except Exception:  # noqa: BLE001
            _log.debug("asyncio task span hand-off failed", exc_info=True)
            if timeout_handle is not None:
                timeout_handle.cancel()
            if async_span is not None:
                try:
                    async_span.end()
                except Exception:  # noqa: BLE001
                    pass
            return None

    def _detached_context_span(self) -> "Span":
        """Return a native-free view for a Context copied to another thread.

        Keeping a non-None placeholder preserves nested-server deduplication,
        while every recording operation on the foreign thread becomes a no-op.
        Explicit hand-off helpers use a real async child instead.
        """
        from .agent import _NullSpan
        return _NullSpan()

    # ---- lifecycle ---------------------------------------------------------
    def end(self) -> None:
        with self._native_lock:
            if self._ended:
                return
            self._ended = True
            # Null the handle first, then do everything else under one try:
            # the latch above is one-shot, so anything raising between it and
            # the native call would leave the native span un-ended for good.
            # A drain/sort failure must still reach end_span_with_data.
            native = self._native
            self._native = None
            try:
                self._end_locked(native)
            except Exception:  # noqa: BLE001
                _warn_flush_failed()

    def _end_locked(self, native) -> None:
        """Body of :meth:`end`, with ``_native_lock`` held and ``_ended`` set."""
        # Drain events user code never ended: LIFO, all stamped with the
        # span's end time. Runs before the payload snapshot so the
        # finished-event records land in this flush.
        end_time = _now_ms()
        stack = self._active_events
        if stack:
            _log.debug("%d span event(s) not ended by user code; "
                       "finished implicitly", len(stack))
        while stack:
            stack.pop()._finalize(self, end_time)
        service_type = self._service_type
        remote_address = self._remote_address
        end_point = self._end_point
        acceptor_host = self._acceptor_host
        status_code = self._status_code
        url_stat = self._url_stat
        annotations = self._annotations or ()
        finished_events = self._finished_events
        error_verdicts = self._error_verdicts or ()
        logging = self._logging

        # Finalization is one-shot, failure path included. Release the payloads
        # now; the local snapshots keep them alive for the native call below.
        self._service_type = None
        self._remote_address = None
        self._end_point = None
        self._acceptor_host = None
        self._status_code = None
        self._url_stat = None
        self._annotations = None
        self._finished_events = []
        self._error_verdicts = None
        if native is None:
            return

        url_pattern = ""
        method = ""
        resolved_status_code = 0
        if url_stat is not None:
            url_pattern, method, resolved_status_code = url_stat
        if status_code is not None:
            resolved_status_code = status_code

        # Native expects the records sequence-ordered, and nested events
        # finish innermost-first, so order once at flush (timsort on a
        # mostly-sorted list).
        finished_events.sort(key=_EVENT_SORT_KEY)
        native.end_span_with_data(
            int(service_type or 0),
            remote_address or "",
            end_point or "",
            acceptor_host or "",
            int(resolved_status_code),
            url_pattern,
            method,
            error_verdicts,
            annotations,
            finished_events,
            logging,
        )

    # ---- context-manager semantics (root span scope) -----------------------
    def __enter__(self) -> "Span":
        # Re-entrant: one token per scope, so nested ``with span:`` (or exiting
        # out of order) can't strand the contextvar on an ended span.
        self._tokens.append(_ctx.set_current_span(self))
        return self

    def __exit__(self, exc_type, exc_val, tb) -> None:
        try:
            if exc_val is not None:
                self.set_error(exc_val)
        except Exception:  # noqa: BLE001
            # As in SpanEvent.__exit__: never mask the in-flight exception.
            _log.debug("set_error failed in Span.__exit__", exc_info=True)
        finally:
            if self._tokens:
                token = self._tokens.pop()
                try:
                    _ctx.reset_current_span(token)  # type: ignore[attr-defined]
                except Exception:  # noqa: BLE001
                    pass
            # Only the outermost scope ends: an inner exit would leave later work
            # in the outer scope on a dead span.
            if not self._tokens:
                self.end()
