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

"""mysql-connector-python instrumentation.

Oracle's official Python connector ships several Cursor classes —
``MySQLCursor``, ``MySQLCursorBuffered``, ``MySQLCursorPrepared``,
``MySQLCursorDict``, etc. — all subclasses of ``MySQLCursor`` defined in
``mysql.connector.cursor``. The C-extension implementation
(``mysql.connector.cursor_cext``) mirrors the same hierarchy under
``CMySQLCursor``.

We instrument the base ``MySQLCursor.execute`` / ``executemany`` /
``callproc`` once: subclasses pick the wrapping up via Python's normal
method resolution. The C-extension cursor needs its own pass because it
overrides ``execute`` directly.

Connection target lookup goes through the dbapi base, with a small
override for the ``_host`` / ``_port`` / ``_database`` underscore-prefixed
attributes that mysql-connector uses internally.
"""

from __future__ import annotations

import re

from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_MYSQL
from .._util import wrap
from ..dbapi import TargetInfo, wrap_cursor_class

# Connection classes whose schema-change funnels we hook so the recorded
# destination schema tracks runtime ``USE`` / ``conn.database = ...``. Both the
# pure-python and C-extension drivers expose the same method names.
_CONNECTION_CLASSES = (
    ("mysql.connector.connection", "MySQLConnection"),
    ("mysql.connector.connection_cext", "CMySQLConnection"),
)

# Stamped on a live connection to remember a runtime schema switch. Absent until
# the first ``USE``/``cmd_init_db``, when the extractor falls back to the
# connect-time ``_database``.
_LIVE_SCHEMA_ATTR = "_pinpoint_live_schema"

# MySQL caps schema names at 64 chars, so a short prefix always suffices to
# classify a statement — a multi-megabyte INSERT is never copied or decoded just
# to be rejected as "not a USE".
_MAX_USE_HEAD = 128


class MySQLConnectorInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        # Pure-Python cursor: covers MySQLCursorBuffered, MySQLCursorDict, etc.
        wrap_cursor_class(
            "mysql.connector.cursor", "MySQLCursor",
            service_type=SERVICE_TYPE_MYSQL,
            extract_target=_extract_mysql_target,
        )
        # C-extension cursor (mysql-connector built against libmysqlclient).
        wrap_cursor_class(
            "mysql.connector.cursor_cext", "CMySQLCursor",
            service_type=SERVICE_TYPE_MYSQL,
            extract_target=_extract_mysql_target,
        )
        # ``_database`` is frozen at connect time and never updated by ``USE`` or
        # ``conn.database = ...``, so without these hooks every span after a switch
        # reports the stale schema. See ``_extract_mysql_target`` on why we cache.
        for module, cls in _CONNECTION_CLASSES:
            # ``cursor.execute("USE <db>")`` funnels through ``cmd_query`` on both
            # drivers. Every statement crosses this, so the observer fast-rejects
            # non-USE ones after decoding a short prefix at most.
            wrap(module, f"{cls}.cmd_query", _cmd_query_wrapper)
            # Covers ``conn.database = <db>`` on the pure-python driver (its setter
            # calls ``cmd_init_db``) and direct ``cmd_init_db`` calls on both. The
            # C-extension setter instead goes straight to ``_cmysql.select_db``, so
            # that one path keeps reporting the connect-time schema until the next
            # ``USE``.
            wrap(module, f"{cls}.cmd_init_db", _cmd_init_db_wrapper)


def _extract_mysql_target(cursor) -> TargetInfo | None:
    conn = (
        getattr(cursor, "_connection", None)
        or getattr(cursor, "connection", None)
    )
    if conn is None:
        return None
    # IMPORTANT: read only inert attributes. ``database`` is a property that runs a
    # live ``SELECT DATABASE()`` round trip (and recurses through the wrapped
    # ``execute`` on the pure-python driver). Prefer the private connect-time
    # fields; ``server_host``/``server_port`` are plain getters over them.
    host = (
        getattr(conn, "_host", None)
        or getattr(conn, "server_host", None)
        or ""
    )
    port = (
        getattr(conn, "_port", None)
        or getattr(conn, "server_port", None)
        or 0
    )
    # Schema is the one field that changes over a connection's life, so unlike
    # host/port neither ``_database`` nor a plain cached TargetInfo can serve
    # it. Prefer the schema our ``cmd_query``/``cmd_init_db`` hooks stamped at
    # the last switch, falling back to connect-time ``_database`` (empty when
    # connected server-wide). Never the public ``database`` property: its
    # round trip is exactly what the cmd_query/cmd_init_db hooks exist to
    # avoid.
    database = (getattr(conn, _LIVE_SCHEMA_ATTR, None)
                or getattr(conn, "_database", None) or "")
    # host/port (and the endpoint TargetInfo derives) are connect-time
    # constants, so the built TargetInfo is cached keyed by the raw schema
    # value and rebuilt only when the schema actually switches.
    cached = getattr(conn, "_pinpoint_mysql_target", None)
    if cached is not None and cached[0] == database:
        return cached[1]
    schema_key = database
    if isinstance(database, (bytes, bytearray)):
        database = database.decode("utf-8", "replace")
    try:
        port = int(port) if port else 0
    except Exception:  # noqa: BLE001
        port = 0
    target = TargetInfo(host=str(host), port=port, database=str(database))
    try:
        conn._pinpoint_mysql_target = (schema_key, target)
    except Exception:  # noqa: BLE001
        pass
    return target


# ``USE <db>``: the verb, then a back-tick / quote-delimited identifier (a
# doubled delimiter escapes itself, e.g. ``USE `odd``name```) or a bare one.
_USE_RE = re.compile(
    r"""\s*use\s+(?:(?P<q>[`'"])(?P<quoted>(?:(?P=q){2}|(?!(?P=q)).)*)(?P=q)"""
    r"""|(?P<bare>[^\s;`'"]+))""",
    re.IGNORECASE | re.DOTALL,
)


def _parse_use_target(statement) -> str | None:
    """Return the schema named by a ``USE <db>`` statement, else ``None``.

    Inspects only a bounded prefix (:data:`_MAX_USE_HEAD`) so a large statement
    is never fully copied or decoded on the hot path."""
    if isinstance(statement, (bytes, bytearray)):
        head = bytes(statement[:_MAX_USE_HEAD]).decode("utf-8", "replace")
    elif isinstance(statement, str):
        head = statement[:_MAX_USE_HEAD]
    else:
        return None
    m = _USE_RE.match(head)
    if m is None:
        return None
    bare = m.group("bare")
    if bare is not None:
        return bare
    q = m.group("q")
    return m.group("quoted").replace(q + q, q) or None


def _set_live_schema(conn, name) -> None:
    """Stamp the observed runtime schema on ``conn`` for the extractor to read.

    Best-effort: a proxied/slotted connection that rejects the attribute just
    keeps the connect-time fallback."""
    if isinstance(name, (bytes, bytearray)):
        name = name.decode("utf-8", "replace")
    try:
        setattr(conn, _LIVE_SCHEMA_ATTR, str(name))
    except Exception:  # noqa: BLE001
        pass


def _cmd_query_wrapper(wrapped, instance, args, kwargs):
    """Observe ``USE <db>`` statements issued via ``cursor.execute`` (which
    both drivers route through ``connection.cmd_query``) and refresh the cached
    schema — only after the statement succeeds, so a failed ``USE`` never
    poisons the cache. Non-USE statements pay just one bounded prefix check.
    ``safe_wrapper`` (installed by ``wrap``) guarantees a failure in the
    observer can't break the query: the value ``cmd_query`` returned is handed
    back regardless."""
    result = wrapped(*args, **kwargs)
    statement = args[0] if args else kwargs.get("query")
    name = _parse_use_target(statement)
    if name:
        _set_live_schema(instance, name)
    return result


def _cmd_init_db_wrapper(wrapped, instance, args, kwargs):
    """Refresh the cached schema when the app switches via ``cmd_init_db``
    (the pure-python ``conn.database = <db>`` setter routes here). Updates only
    on success, mirroring ``_cmd_query_wrapper``."""
    result = wrapped(*args, **kwargs)
    database = args[0] if args else kwargs.get("database")
    if database:
        _set_live_schema(instance, database)
    return result


def instrument() -> None:
    MySQLConnectorInstrumentor().instrument()
