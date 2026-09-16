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

"""SQLAlchemy instrumentation using the built-in ``before_cursor_execute`` /
``after_cursor_execute`` engine events. This plugs in at the correct layer
(the DB-API driver) so we see the real SQL plus the connection endpoint.
"""

from __future__ import annotations

from ..._log import get_logger
from ...context import current_span
from ...errors import safe_try
from ...instrumentor import BaseInstrumentor
from ...service_type import (
    SERVICE_TYPE_MSSQL,
    SERVICE_TYPE_MYSQL,
    SERVICE_TYPE_ORACLE,
    SERVICE_TYPE_POSTGRESQL,
    SERVICE_TYPE_UNKNOWN_DB,
)
from .._util import span_is_sampled, sql_bind_values_enabled
from ..dbapi import _format_params, enter_sqlalchemy_execute, exit_sqlalchemy_execute

_log = get_logger("sqlalchemy")
_OPERATION_CURSOR_EXECUTE = "sqlalchemy.engine.Engine.before_cursor_execute"

_DIALECT_TO_TYPE = {
    "mysql": SERVICE_TYPE_MYSQL,
    "mariadb": SERVICE_TYPE_MYSQL,
    "postgresql": SERVICE_TYPE_POSTGRESQL,
    "postgres": SERVICE_TYPE_POSTGRESQL,
    "mssql": SERVICE_TYPE_MSSQL,
    "oracle": SERVICE_TYPE_ORACLE,
}


class SQLAlchemyInstrumentor(BaseInstrumentor):

    _LISTENERS = (
        ("before_cursor_execute", "_before"),
        ("after_cursor_execute", "_after"),
        ("handle_error", "_on_error"),
    )

    def _instrument(self) -> None:
        try:
            from sqlalchemy import event
            from sqlalchemy.engine import Engine
        except Exception:  # noqa: BLE001
            _log.debug("sqlalchemy import failed", exc_info=True)
            return
        # ``event.contains`` guard: a second registration would fire two ``_before``
        # callbacks per query, and the second stash overwriting the first leaks a
        # never-ended event per statement.
        for hook_name, fn_name in self._LISTENERS:
            fn = globals()[fn_name]
            if not event.contains(Engine, hook_name, fn):
                event.listen(Engine, hook_name, fn)

    def _uninstrument(self) -> None:
        try:
            from sqlalchemy import event
            from sqlalchemy.engine import Engine
        except Exception:  # noqa: BLE001
            return
        for hook_name, fn_name in self._LISTENERS:
            fn = globals()[fn_name]
            try:
                if event.contains(Engine, hook_name, fn):
                    event.remove(Engine, hook_name, fn)
            except Exception:  # noqa: BLE001
                _log.debug("sqlalchemy event.remove(%s) failed", hook_name,
                           exc_info=True)


@safe_try
def _before(conn, cursor, statement, parameters, context, executemany):
    # Suppress the driver-level wrapper *before* the cursor executes: SQLAlchemy
    # owns the trace for cursors it drives. Set regardless of sampling so it always
    # pairs with the reset in ``_after``/``_on_error`` — an unpaired set would leak
    # suppression onto the ambient context and drop later driver traces.
    token = enter_sqlalchemy_execute()
    event_ = None
    try:
        span = current_span()
        if span is None or not span_is_sampled(span):
            _stash_event(context, None, token)
            return
        event_ = span.new_span_event(
            _OPERATION_CURSOR_EXECUTE,
            service_type=_dialect_service_type(conn),
        )
        # The suppressed driver-level event is what would carry the SQL text and
        # connection endpoint, so record both here — otherwise ORM-driven queries
        # show up as empty events with no DB node.
        _annotate_connection(event_, conn)
        _record_statement(event_, statement, parameters)
        _stash_event(context, event_, token)
    except Exception:  # noqa: BLE001
        # Raised after the suppression flag was set. ``@safe_try`` would swallow
        # it with the token never stashed, pinning ``_in_sqlalchemy`` True on this
        # context and dropping every later raw-driver query on the worker. Reset
        # here so every ``enter_`` pairs with an ``exit_``, then re-raise to log.
        try:
            if event_ is not None:
                event_.end()
        finally:
            exit_sqlalchemy_execute(token)
        raise


@safe_try
def _after(conn, cursor, statement, parameters, context, executemany):
    event_, token = _pop_event(context)
    if token is not None:
        exit_sqlalchemy_execute(token)
    if event_ is not None:
        event_.end()


@safe_try
def _on_error(ctx) -> None:
    event_, token = _pop_event(ctx.execution_context)
    if token is not None:
        exit_sqlalchemy_execute(token)
    if event_ is not None:
        try:
            original_exception = getattr(ctx, "original_exception", None)
            if original_exception is not None:
                event_.set_error(original_exception)
        finally:
            # Error recording can itself fail (notably an exception with a
            # broken __str__). The SQLAlchemy payload has already been popped,
            # so this finally is the last chance to close the native event.
            event_.end()


def _dialect_service_type(conn) -> int:
    dialect = getattr(getattr(conn, "dialect", None), "name", "") or ""
    return _DIALECT_TO_TYPE.get(dialect, SERVICE_TYPE_UNKNOWN_DB)


def _engine_target(conn):
    """(endpoint, destination) for the connection's Engine, memoized on the
    Engine: its immutable URL would otherwise be re-walked per ORM statement."""
    engine = getattr(conn, "engine", None)
    target = getattr(engine, "_pinpoint_target", None)
    if target is not None:
        return target
    url = getattr(engine, "url", None)
    host = getattr(url, "host", "") or ""
    port = getattr(url, "port", None)
    endpoint = f"{host}:{port}" if host and port else host
    database = getattr(url, "database", "") or ""
    target = (endpoint, database or endpoint)
    if engine is not None:
        try:
            engine._pinpoint_target = target
        except Exception:  # noqa: BLE001
            pass
    return target


@safe_try
def _annotate_connection(event_, conn) -> None:
    endpoint, destination = _engine_target(conn)
    if endpoint:
        event_.set_end_point(endpoint)
    if destination:
        event_.set_destination(destination)


@safe_try
def _record_statement(event_, statement, parameters) -> None:
    sql = statement if isinstance(statement, str) else str(statement or "")
    params_text = ""
    # Bound values routinely contain PII and credentials. Keep the SQL
    # template, but only attach literal values when the same explicit opt-in
    # used by the driver-level integrations is enabled.
    if parameters and sql_bind_values_enabled(event_):
        params_text = _format_params(parameters)
    event_.set_sql_query(sql, params_text)


def _stash_event(context, event_, token) -> None:
    context.__pinpoint_event__ = (event_, token)


def _pop_event(context):
    """Return ``(event, suppression_token)`` for the current statement, or
    ``(None, None)`` if nothing was stashed — ``handle_error`` can fire with
    ``execution_context`` None for a failure raised before SQLAlchemy built
    one, i.e. before ``_before`` ever ran."""
    payload = getattr(context, "__pinpoint_event__", None)
    if payload is not None:
        try:
            del context.__pinpoint_event__
        except Exception:  # noqa: BLE001
            pass
        return payload
    return None, None


def instrument() -> None:
    SQLAlchemyInstrumentor().instrument()
