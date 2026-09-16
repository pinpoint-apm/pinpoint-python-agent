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

"""PostgreSQL drivers (psycopg3 sync/async, asyncpg) against one real
PostgreSQL container."""

from __future__ import annotations

import asyncio

import pytest

psycopg = pytest.importorskip("psycopg")

from pinpoint.service_type import SERVICE_TYPE_POSTGRESQL


@pytest.fixture(scope="module", autouse=True)
def _instrument():
    from pinpoint.instrumentations.psycopg import instrument_psycopg3

    instrument_psycopg3()


def _conninfo(pg):
    return ("host={host} port={port} user={user} password={password} "
            "dbname={database}".format(**pg))


def test_psycopg3_execute(postgres_container, traced, sql_bind_values_on):
    with psycopg.connect(_conninfo(postgres_container)) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT %s::int + %s::int", (1, 2))
            assert cur.fetchone() == (3,)

    ev = traced.single("psycopg.Cursor.execute")
    assert ev.ended
    assert ev.service_type == SERVICE_TYPE_POSTGRESQL
    assert ev.endpoint == (
        f"{postgres_container['host']}:{postgres_container['port']}")
    assert ev.destination == postgres_container["database"]
    # Plain psycopg3 cursors have no mogrify — template + repr(params).
    assert ev.sql == [("SELECT %s::int + %s::int", "(1, 2)")]


def test_psycopg3_bind_values_off_by_default(postgres_container, traced):
    """The secure default: without ``sql_trace_bind_values`` only the SQL
    template is recorded — never the bound values."""
    with psycopg.connect(_conninfo(postgres_container)) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT %s::text", ("sensitive-value",))
            assert cur.fetchone() == ("sensitive-value",)

    ev = traced.single("psycopg.Cursor.execute")
    assert ev.ended
    assert ev.sql == [("SELECT %s::text", "")]


def test_psycopg3_error_recorded(postgres_container, traced):
    with psycopg.connect(_conninfo(postgres_container)) as conn:
        with pytest.raises(psycopg.errors.UndefinedTable):
            conn.execute("SELECT * FROM no_such_table_it")

    events = [e for e in traced.events if e.operation == "psycopg.Cursor.execute"]
    assert events and events[-1].ended
    assert events[-1].error is not None
    assert events[-1].error[0] == "UndefinedTable"


def test_psycopg3_async_execute(postgres_container, traced):
    async def main():
        conn = await psycopg.AsyncConnection.connect(
            _conninfo(postgres_container))
        try:
            cur = await conn.execute("SELECT %s::text", ("hello",))
            return await cur.fetchone()
        finally:
            await conn.close()

    assert asyncio.run(main()) == ("hello",)

    ev = traced.single("psycopg.AsyncCursor.execute")
    assert ev.ended
    assert ev.service_type == SERVICE_TYPE_POSTGRESQL
    assert ev.destination == postgres_container["database"]


def test_asyncpg_fetch_and_execute(postgres_container, traced,
                                   sql_bind_values_on):
    asyncpg = pytest.importorskip("asyncpg")
    from pinpoint.instrumentations import asyncpg as asyncpg_instr

    asyncpg_instr.instrument()

    async def main():
        conn = await asyncpg.connect(
            host=postgres_container["host"],
            port=postgres_container["port"],
            user=postgres_container["user"],
            password=postgres_container["password"],
            database=postgres_container["database"],
        )
        try:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS it_kv (k text, v int)")
            await conn.execute("INSERT INTO it_kv VALUES ($1, $2)", "a", 1)
            return await conn.fetch("SELECT v FROM it_kv WHERE k = $1", "a")
        finally:
            await conn.close()

    rows = asyncio.run(main())
    assert [r["v"] for r in rows] == [1]

    executes = [e for e in traced.events_named(
        "asyncpg.connection.Connection.execute") if e.ended]
    assert len(executes) == 2
    insert_ev = executes[-1]
    assert insert_ev.service_type == SERVICE_TYPE_POSTGRESQL
    assert insert_ev.endpoint == (
        f"{postgres_container['host']}:{postgres_container['port']}")
    assert insert_ev.destination == postgres_container["database"]
    assert insert_ev.sql == [("INSERT INTO it_kv VALUES ($1, $2)", "('a', 1)")]

    fetch_ev = traced.single("asyncpg.connection.Connection.fetch")
    assert fetch_ev.ended
    assert fetch_ev.sql == [("SELECT v FROM it_kv WHERE k = $1", "('a',)")]


def test_asyncpg_batch_scalar_and_cursor_paths(postgres_container, traced,
                                               sql_bind_values_on):
    """Exercise the independently wrapped asyncpg methods, including cursor(),
    which is a synchronous factory even though iteration is asynchronous."""
    asyncpg = pytest.importorskip("asyncpg")
    from pinpoint.instrumentations import asyncpg as asyncpg_instr

    asyncpg_instr.instrument()

    async def main():
        conn = await asyncpg.connect(
            host=postgres_container["host"],
            port=postgres_container["port"],
            user=postgres_container["user"],
            password=postgres_container["password"],
            database=postgres_container["database"],
        )
        try:
            await conn.execute("DROP TABLE IF EXISTS it_asyncpg_methods")
            await conn.execute(
                "CREATE TABLE it_asyncpg_methods (k text PRIMARY KEY, v int)")
            await conn.executemany(
                "INSERT INTO it_asyncpg_methods VALUES ($1, $2)",
                [("a", 1), ("b", 2)],
            )
            row = await conn.fetchrow(
                "SELECT v FROM it_asyncpg_methods WHERE k = $1", "a")
            value = await conn.fetchval(
                "SELECT v FROM it_asyncpg_methods WHERE k = $1", "b")
            async with conn.transaction():
                cursor = conn.cursor(
                    "SELECT v FROM it_asyncpg_methods ORDER BY v",
                    prefetch=1,
                )
                cursor_values = [record["v"] async for record in cursor]
            return row["v"], value, cursor_values
        finally:
            await conn.close()

    assert asyncio.run(main()) == (1, 2, [1, 2])

    executemany = traced.single(
        "asyncpg.connection.Connection.executemany")
    assert executemany.ended
    assert executemany.service_type == SERVICE_TYPE_POSTGRESQL
    assert executemany.destination == postgres_container["database"]
    assert executemany.sql
    assert executemany.sql[0][0] == (
        "INSERT INTO it_asyncpg_methods VALUES ($1, $2)")
    assert "('a', 1)" in executemany.sql[0][1]

    fetchrow = traced.single("asyncpg.connection.Connection.fetchrow")
    assert fetchrow.ended
    assert fetchrow.sql == [
        ("SELECT v FROM it_asyncpg_methods WHERE k = $1", "('a',)")]

    fetchval = traced.single("asyncpg.connection.Connection.fetchval")
    assert fetchval.ended
    assert fetchval.sql == [
        ("SELECT v FROM it_asyncpg_methods WHERE k = $1", "('b',)")]

    cursor = traced.single("asyncpg.connection.Connection.cursor")
    assert cursor.ended
    assert cursor.sql == [
        ("SELECT v FROM it_asyncpg_methods ORDER BY v", "")]


def test_asyncpg_awaited_error_ends_and_marks_event(postgres_container,
                                                    traced):
    asyncpg = pytest.importorskip("asyncpg")
    from pinpoint.instrumentations import asyncpg as asyncpg_instr

    asyncpg_instr.instrument()

    async def main():
        conn = await asyncpg.connect(
            host=postgres_container["host"],
            port=postgres_container["port"],
            user=postgres_container["user"],
            password=postgres_container["password"],
            database=postgres_container["database"],
        )
        try:
            await conn.fetch("SELECT * FROM no_such_asyncpg_table_it")
        finally:
            await conn.close()

    with pytest.raises(asyncpg.UndefinedTableError):
        asyncio.run(main())

    event = traced.single("asyncpg.connection.Connection.fetch")
    assert event.ended
    assert event.error is not None
    assert event.error[0] == "UndefinedTableError"
