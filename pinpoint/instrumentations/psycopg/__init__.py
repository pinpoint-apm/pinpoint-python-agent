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

"""psycopg2 / psycopg3 (PostgreSQL) instrumentation.

psycopg3's ``Cursor`` / ``AsyncCursor`` are pure Python and take the regular
class wrappers. psycopg2's are immutable C extension types, so this module
wraps ``psycopg2.connect`` and installs a traced ``cursor_factory`` — a real
subclass of the C cursor — on the *raw* connection instead.

Two constraints hold that design in place:

- Never hand a ``wrapt.ObjectProxy`` back to the caller. It satisfies only
  Python-level ``isinstance``, not psycopg2's C ``PyObject_TypeCheck``, so
  ``register_type`` / ``quote_ident`` / Django and SQLAlchemy connection setup
  would raise outright.
- A real subclass means only the execute family is overridden — fetch,
  iteration and attribute reads stay at native C speed, which matters because
  fetch runs once per row.

Known limitation (a per-call ``conn.cursor(cursor_factory=...)`` is untraced)
and the rest: see ``README.md``. The dbapi base does the SQL recording and
connection-target extraction for both versions.
"""

from __future__ import annotations

from typing import Any

from ..._log import get_logger
from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_POSTGRESQL
from .._util import wrap
from ..dbapi import (
    trace_cursor_call,
    wrap_async_cursor_class,
    wrap_cursor_class,
)

_log = get_logger("psycopg")


class Psycopg2Instrumentor(BaseInstrumentor):
    # Hook ``psycopg2`` itself, not ``psycopg2.extensions``: the parent package
    # imports ``extensions`` *before* it defines ``connect()``, so a hook there fires
    # mid-import and the wrap fails silently and permanently.

    def _instrument(self) -> None:
        # psycopg2: connect-level hook (see module docstring — the C extension
        # cursor class rejects monkeypatching). ``wrap`` is idempotent, so an
        # instrument→uninstrument→instrument cycle doesn't stack connect
        # wrappers; a failed wrap raises so BaseInstrumentor's guard doesn't
        # lock out a retry once the target exists.
        if not wrap("psycopg2", "connect", _psycopg2_connect_wrapper):
            raise RuntimeError("psycopg2.connect could not be wrapped")


class Psycopg3Instrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        # psycopg3: sync + async cursors are top-level under ``psycopg``.
        wrap_cursor_class(
            "psycopg", "Cursor",
            service_type=SERVICE_TYPE_POSTGRESQL,
        )
        wrap_async_cursor_class(
            "psycopg", "AsyncCursor",
            service_type=SERVICE_TYPE_POSTGRESQL,
        )


# ---------------------------------------------------------------- psycopg2

# Cached per base cursor class: identity must stay stable for ``cursor_factory``
# comparisons and our double-instrument guard, and each user-supplied factory
# (``cursor``, ``DictCursor``, …) needs its own subclass to keep base behaviour.
_traced_cursor_cache: dict = {}


def _traced_cursor_factory(base_cursor_cls):
    """Return a traced subclass of ``base_cursor_cls``.

    The subclass is a *real* subclass of psycopg2's C cursor type, so it
    passes ``PyObject_TypeCheck`` and inherits the C cursor's own iterator
    protocol — ``for row in cur`` and ``next(cur)`` both work unchanged.
    Only the execute family is overridden to record a span event.
    """
    cached = _traced_cursor_cache.get(base_cursor_cls)
    if cached is not None:
        return cached

    class _TracedCursor(base_cursor_cls):
        __pinpoint_traced_cursor__ = True

        def execute(self, *args, **kwargs):
            return trace_cursor_call(
                super().execute, self, args, kwargs,
                operation="psycopg2.extensions.cursor.execute",
                op_kind="execute", service_type=SERVICE_TYPE_POSTGRESQL,
            )

        def executemany(self, *args, **kwargs):
            return trace_cursor_call(
                super().executemany, self, args, kwargs,
                operation="psycopg2.extensions.cursor.executemany",
                op_kind="executemany", service_type=SERVICE_TYPE_POSTGRESQL,
            )

        def callproc(self, *args, **kwargs):
            return trace_cursor_call(
                super().callproc, self, args, kwargs,
                operation="psycopg2.extensions.cursor.callproc",
                op_kind="callproc", service_type=SERVICE_TYPE_POSTGRESQL,
            )

    # setdefault, not a plain store: two threads opening connections at once
    # can both miss the lookup above and each build a subclass, and handing
    # them different classes would break the ``cursor_factory`` identity this
    # cache exists to keep stable. The first publisher wins and the loser
    # returns that same class, discarding its own.
    if len(_traced_cursor_cache) >= 1024:
        # An app generating cursor-factory classes per connection would grow
        # the cache (and its generated heap types) without bound; past the cap
        # the identity guarantee is dropped rather than the memory.
        return _TracedCursor
    return _traced_cursor_cache.setdefault(base_cursor_cls, _TracedCursor)


def _default_cursor_cls(conn):
    """Discover the cursor class ``conn.cursor()`` produces by default.

    ``conn.cursor_factory is None`` does *not* imply psycopg2's plain C
    ``cursor``: ``connection_factory`` subclasses
    (``psycopg2.extras.RealDictConnection``, ``NamedTupleConnection``,
    ``DictConnection``, ``LoggingConnection``, …) override ``cursor()`` to fall
    back to their own cursor class via ``self.cursor_factory or <Special>``.
    Pinning ``cursor_factory`` to a plain-cursor traced factory would make that
    ``or`` pick our factory instead of the special one, silently turning the
    rows those connections return (dicts / namedtuples) back into plain tuples
    and breaking user code such as ``row['col']``.

    Rather than guess, open one throwaway cursor to learn the real default
    class, then trace a subclass of *that*. Creating (and closing) a cursor is
    a local, side-effect-free operation — it opens no transaction. Any failure
    (custom connection whose ``cursor()`` needs args or state, e.g. an
    uninitialised ``LoggingConnection``) returns ``None`` so the caller leaves
    the connection untraced rather than risk breaking it."""
    try:
        probe = conn.cursor()
    except Exception:  # noqa: BLE001
        _log.debug("psycopg2 cursor probe failed", exc_info=True)
        return None
    try:
        return type(probe)
    finally:
        try:
            probe.close()
        except Exception:  # noqa: BLE001
            pass


def _install_traced_cursor_factory(conn) -> None:
    """Point ``conn.cursor_factory`` at a traced subclass of whatever cursor
    class the connection would otherwise produce, leaving ``conn`` itself
    untouched (still a genuine psycopg2 connection)."""
    base = getattr(conn, "cursor_factory", None)
    if base is None:
        # No explicit factory: discover the connection's real default cursor
        # class (see _default_cursor_cls) — not necessarily the plain cursor.
        base = _default_cursor_cls(conn)
        if base is None:
            return
    # Already instrumented (e.g. a pooled connection handed back through
    # connect again, or a probe that returned an already-traced cursor) —
    # don't wrap a traced factory in another.
    if getattr(base, "__pinpoint_traced_cursor__", False):
        return
    conn.cursor_factory = _traced_cursor_factory(base)


def _agent_permanently_disabled() -> bool:
    """True only when an agent exists but is configured off for good, so it
    can never open a span.

    Unlike the sibling drivers — which patch a *shared* cursor class once and
    gate purely at runtime via ``current_span()`` — the psycopg2 hook mutates
    the ``cursor_factory`` of *every* host connection. When the agent is
    config-disabled that mutation buys nothing, so we skip it and leave each
    connection's cursors 100% native.

    A missing agent (``None``) is deliberately *not* treated as a permanent
    disable: unit tests drive the wrapper directly, and installing the factory
    is harmless anyway (every ``execute`` is still runtime-gated). A live agent
    whose gRPC registration is merely still pending has ``config.enabled=True``
    and is likewise never skipped — otherwise connections opened during the
    registration window would never trace.
    """
    from ...agent import get_agent

    agent = get_agent()
    if agent is None:
        return False
    try:
        return not bool(agent.config.enabled)
    except Exception:  # noqa: BLE001
        return False


def _psycopg2_connect_wrapper(wrapped, instance, args, kwargs):
    conn = wrapped(*args, **kwargs)
    # aiopg passes async_=True and drives the raw connection's poll loop
    # itself (and has its own instrumentation) — instrumenting those would
    # double-trace every query. Leave them untouched.
    if kwargs.get("async") or kwargs.get("async_"):
        return conn
    # Agent configured off for good — don't touch the host connection at all.
    if _agent_permanently_disabled():
        return conn
    try:
        _install_traced_cursor_factory(conn)
    except Exception:  # noqa: BLE001
        # Never break the host connection: fall back to the untraced raw
        # connection rather than raising out of psycopg2.connect().
        _log.debug("psycopg2 cursor_factory install failed", exc_info=True)
    return conn


def instrument_psycopg2(*_args: Any, **_kwargs: Any) -> None:
    Psycopg2Instrumentor().instrument()


def instrument_psycopg3(*_args: Any, **_kwargs: Any) -> None:
    Psycopg3Instrumentor().instrument()
