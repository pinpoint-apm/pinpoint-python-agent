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

"""MySQL drivers (pymysql / mysql-connector / aiomysql / SQLAlchemy) against
one real MySQL container."""

from __future__ import annotations

import asyncio

import pytest

pymysql = pytest.importorskip("pymysql")

from pinpoint.service_type import SERVICE_TYPE_MYSQL

_OP_PYMYSQL_EXECUTE = "pymysql.cursors.Cursor.execute"
_OP_PYMYSQL_EXECUTEMANY = "pymysql.cursors.Cursor.executemany"
_OP_SQLALCHEMY = "sqlalchemy.engine.Engine.before_cursor_execute"


@pytest.fixture(scope="module", autouse=True)
def _instrument():
    from pinpoint.instrumentations import pymysql as pymysql_instr

    pymysql_instr.instrument()


@pytest.fixture(scope="module")
def conn(mysql_container):
    c = pymysql.connect(
        host=mysql_container["host"],
        port=mysql_container["port"],
        user=mysql_container["user"],
        password=mysql_container["password"],
        database=mysql_container["database"],
        autocommit=True,
    )
    with c.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS it_users ("
            "id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64))")
    yield c
    c.close()


def test_pymysql_execute_renders_sql_and_target(conn, mysql_container, traced,
                                                sql_bind_values_on):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO it_users (name) VALUES (%s)", ("alice",))
        cur.execute("SELECT name FROM it_users WHERE name = %s", ("alice",))
        assert cur.fetchone() == ("alice",)

    events = [e for e in traced.events_named(_OP_PYMYSQL_EXECUTE) if e.ended]
    assert len(events) == 2
    insert_ev, select_ev = events
    assert insert_ev.service_type == SERVICE_TYPE_MYSQL
    endpoint = f"{mysql_container['host']}:{mysql_container['port']}"
    assert insert_ev.endpoint == endpoint
    assert insert_ev.destination == mysql_container["database"]
    # PyMySQL cursors have mogrify — the trace shows the rendered statement.
    assert insert_ev.sql == [("INSERT INTO it_users (name) VALUES ('alice')", "")]
    assert select_ev.sql == [
        ("SELECT name FROM it_users WHERE name = 'alice'", "")]


def test_pymysql_bind_values_off_by_default(conn, traced):
    """The secure default: without ``sql_trace_bind_values`` the template goes
    out as-is — no mogrify rendering, no captured params."""
    with conn.cursor() as cur:
        cur.execute("SELECT %s", ("sensitive-value",))
        assert cur.fetchone() == ("sensitive-value",)

    ev = traced.single(_OP_PYMYSQL_EXECUTE)
    assert ev.ended
    assert ev.sql == [("SELECT %s", "")]


def test_pymysql_executemany_yields_single_event(conn, traced,
                                                 sql_bind_values_on):
    rows = [(f"bulk-{i}",) for i in range(5)]
    with conn.cursor() as cur:
        cur.executemany("INSERT INTO it_users (name) VALUES (%s)", rows)

    ev = traced.single(_OP_PYMYSQL_EXECUTEMANY)
    assert ev.ended
    # Re-entrant per-row execute()s are suppressed — exactly one event total.
    assert traced.events_named(_OP_PYMYSQL_EXECUTE) == []
    sql, params = ev.sql[0]
    assert sql.startswith("INSERT INTO it_users (name) VALUES ('bulk-0')")
    assert "+ 4 more rows" in sql
    assert params == ""


def test_pymysql_error_recorded(conn, traced):
    with pytest.raises(pymysql.err.ProgrammingError):
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM no_such_table_it")

    events = traced.events_named(_OP_PYMYSQL_EXECUTE)
    assert events and events[-1].ended
    assert events[-1].error is not None
    assert events[-1].error[0] == "ProgrammingError"


def test_mysql_connector_execute(mysql_container, traced):
    mysql_connector = pytest.importorskip("mysql.connector")
    from pinpoint.instrumentations import mysql as mysql_instr

    mysql_instr.instrument()

    # use_pure pins the pure-python cursor class so the asserted op name is
    # deterministic whether or not the C extension is installed.
    c = mysql_connector.connect(
        host=mysql_container["host"],
        port=mysql_container["port"],
        user=mysql_container["user"],
        password=mysql_container["password"],
        database=mysql_container["database"],
        use_pure=True,
    )
    try:
        cur = c.cursor()
        cur.execute("SELECT %s + %s", (1, 2))
        assert cur.fetchone() == (3,)
        cur.close()
    finally:
        c.close()

    # mysql-connector issues internal statements (e.g. SELECT @@session.sql_mode)
    # through the same cursor class — pick the test's own statement.
    events = [e for e in traced.events_named(
        "mysql.connector.cursor.MySQLCursor.execute")
        if e.sql and "%s + %s" in e.sql[0][0]]
    assert len(events) == 1
    ev = events[0]
    assert ev.ended
    assert ev.service_type == SERVICE_TYPE_MYSQL
    assert ev.endpoint == (
        f"{mysql_container['host']}:{mysql_container['port']}")
    assert ev.destination == mysql_container["database"]
    assert ev.sql and "SELECT" in ev.sql[0][0]


def test_mysql_connector_tracks_runtime_use_schema(mysql_container, traced):
    """Connect without a default schema, switch with a real USE statement,
    and verify later events report the live schema instead of the frozen
    connect-time value."""
    mysql_connector = pytest.importorskip("mysql.connector")
    from pinpoint.instrumentations import mysql as mysql_instr

    mysql_instr.instrument()
    c = mysql_connector.connect(
        host=mysql_container["host"],
        port=mysql_container["port"],
        user=mysql_container["user"],
        password=mysql_container["password"],
        use_pure=True,
    )
    try:
        cur = c.cursor()
        cur.execute("SELECT DATABASE()")
        assert cur.fetchone() == (None,)
        cur.execute(f"USE `{mysql_container['database']}`")
        cur.execute("SELECT DATABASE()")
        assert cur.fetchone() == (mysql_container["database"],)
        cur.close()
    finally:
        c.close()

    operation = "mysql.connector.cursor.MySQLCursor.execute"
    selects = [
        event for event in traced.events_named(operation)
        if event.sql == [("SELECT DATABASE()", "")]
    ]
    assert len(selects) == 2
    endpoint = f"{mysql_container['host']}:{mysql_container['port']}"
    assert selects[0].destination == endpoint
    assert selects[1].destination == mysql_container["database"]
    assert all(event.ended for event in selects)


def test_aiomysql_async_execute(mysql_container, traced, sql_bind_values_on):
    aiomysql = pytest.importorskip("aiomysql")
    from pinpoint.instrumentations import aiomysql as aiomysql_instr

    aiomysql_instr.instrument()

    async def main():
        conn = await aiomysql.connect(
            host=mysql_container["host"],
            port=mysql_container["port"],
            user=mysql_container["user"],
            password=mysql_container["password"],
            db=mysql_container["database"],
        )
        try:
            async with conn.cursor() as cur:
                await cur.execute("SELECT %s", (42,))
                return await cur.fetchone()
        finally:
            conn.close()

    assert asyncio.run(main()) == (42,)

    ev = traced.single("aiomysql.cursors.Cursor.execute")
    assert ev.ended
    assert ev.service_type == SERVICE_TYPE_MYSQL
    assert ev.destination == mysql_container["database"]
    assert ev.sql == [("SELECT 42", "")]


def test_sqlalchemy_suppresses_driver_event(mysql_container, traced):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    from pinpoint.instrumentations import sqlalchemy as sa_instr

    sa_instr.instrument()

    url = ("mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
           .format(**mysql_container))
    engine = sqlalchemy.create_engine(url)
    try:
        with engine.connect() as sa_conn:
            result = sa_conn.execute(sqlalchemy.text("SELECT 7"))
            assert result.scalar() == 7
    finally:
        engine.dispose()

    # SQLAlchemy owns the trace for statements it drives: the SELECT shows up
    # as a sqlalchemy event and the pymysql cursor wrapper stays silent for
    # it. (Dialect-init statements like SET NAMES run on the raw driver
    # outside the event pipeline and legitimately keep their driver events.)
    sa_events = [e for e in traced.events_named(_OP_SQLALCHEMY) if e.ended]
    assert len(sa_events) >= 1
    driver_events = traced.events_named(_OP_PYMYSQL_EXECUTE)
    assert not [e for e in driver_events
                if any("SELECT 7" in sql for sql, _ in e.sql)]


def test_sqlalchemy_error_ends_event_and_restores_driver_tracing(
        mysql_container, traced):
    """The real handle_error listener must close the failed event and release
    DBAPI suppression so both a later ORM query and a later raw query trace."""
    sqlalchemy = pytest.importorskip("sqlalchemy")
    from pinpoint.instrumentations import sqlalchemy as sa_instr

    sa_instr.instrument()
    url = ("mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
           .format(**mysql_container))
    engine = sqlalchemy.create_engine(url)
    try:
        with engine.connect() as sa_conn:
            with pytest.raises(sqlalchemy.exc.DBAPIError):
                sa_conn.execute(sqlalchemy.text(
                    "SELECT * FROM no_such_sqlalchemy_table_it"))

            assert sa_conn.execute(sqlalchemy.text("SELECT 11")).scalar() == 11

        # A raw driver query after SQLAlchemy's error path must not inherit the
        # contextvar suppression flag from the failed statement.
        raw = engine.raw_connection()
        try:
            with raw.cursor() as cursor:
                cursor.execute("SELECT 12")
                assert cursor.fetchone() == (12,)
        finally:
            raw.close()
    finally:
        engine.dispose()

    sa_events = traced.events_named(_OP_SQLALCHEMY)
    failed = [e for e in sa_events if e.error is not None]
    successful = [e for e in sa_events if e.error is None and e.ended]
    assert len(failed) == 1 and failed[0].ended
    assert successful, "the ORM query after the failure must still be traced"

    raw_events = [
        e for e in traced.events_named(_OP_PYMYSQL_EXECUTE)
        if any("SELECT 12" in sql for sql, _ in e.sql)
    ]
    assert len(raw_events) == 1 and raw_events[0].ended
