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

Validates that the dbapi base hooks are installed correctly on a fake
mysql-connector cursor and that the underscore-prefixed connection
attribute layout is recognized by the target extractor.
"""

from __future__ import annotations

import pytest

from pinpoint.instrumentations import mysql as mysql_instr
from pinpoint.instrumentations._util import already_wrapped, wrap


class _MNConn:
    """Mimics mysql.connector.connection.MySQLConnection — uses the
    underscore-prefixed attribute names the connector exposes internally."""
    def __init__(self, host="db.test", port=3306, database="appdb"):
        self._host = host
        self._port = port
        self._database = database


class _MNCursor:
    def __init__(self, conn):
        self._connection = conn


def test_extract_mysql_target_reads_underscore_attrs():
    target = mysql_instr._extract_mysql_target(_MNCursor(_MNConn()))
    assert target.host == "db.test"
    assert target.port == 3306
    assert target.database == "appdb"


def test_extract_mysql_target_handles_bytes_database():
    """Some configurations return database name as bytes."""
    conn = _MNConn(database=b"orders")
    target = mysql_instr._extract_mysql_target(_MNCursor(conn))
    assert target.database == "orders"


def test_extract_mysql_target_no_connection_returns_none():
    class _Stub: pass
    assert mysql_instr._extract_mysql_target(_Stub()) is None


def test_extract_mysql_target_handles_string_port():
    """mysql-connector lets users pass ports as strings; we coerce."""
    conn = _MNConn(port="3307")
    target = mysql_instr._extract_mysql_target(_MNCursor(conn))
    assert target.port == 3307


class _QueryingConn:
    """Mimics MySQLConnectionAbstract where ``database`` is a property that
    runs a live ``SELECT DATABASE()`` round trip. Connecting server-wide with
    no default schema leaves ``_database`` empty. The extractor must never read
    the ``database`` property — doing so would issue a query (and recurse on the
    pure-python driver) before every traced statement."""

    def __init__(self):
        self._host = "db.test"
        self._port = 3306
        self._database = ""  # connected server-wide, no default schema
        self.database_property_reads = 0

    @property
    def database(self):
        self.database_property_reads += 1
        raise AssertionError(
            "extractor read the query-executing `database` property"
        )

    # server_host / server_port are plain inert getters over the same fields.
    @property
    def server_host(self):
        return self._host

    @property
    def server_port(self):
        return self._port


def test_extract_mysql_target_never_touches_database_property():
    """No default schema -> no round trip; the ``database`` property is never read."""
    conn = _QueryingConn()
    target = mysql_instr._extract_mysql_target(_MNCursor(conn))
    assert conn.database_property_reads == 0
    assert target.host == "db.test"
    assert target.port == 3306
    assert target.database == ""


def test_extract_mysql_target_falls_back_to_server_host_port():
    """When only the public inert getters exist, still extract host/port."""
    class _NoUnderscore:
        _database = "appdb"

        @property
        def server_host(self):
            return "srv.test"

        @property
        def server_port(self):
            return 3308

    target = mysql_instr._extract_mysql_target(_MNCursor(_NoUnderscore()))
    assert target.host == "srv.test"
    assert target.port == 3308
    assert target.database == "appdb"


# ---------------------------------------------------------------------------
# Runtime schema switches: ``_database`` is frozen at connect time, so
# after ``USE <db>`` / ``conn.database = <db>`` the extractor must report the
# NEW schema — without ever running ``SELECT DATABASE()``.
# ---------------------------------------------------------------------------

class _SwitchableConn:
    """Models a real mysql-connector connection.

    ``_database`` is set once at connect time and never updated by the driver;
    the public ``database`` getter is query-backed (``SELECT DATABASE()``) and
    must never be read on the hot path; and ``cursor.execute("USE <db>")`` /
    ``conn.database = <db>`` change the server's current schema by routing
    through ``cmd_query`` / ``cmd_init_db`` respectively — leaving ``_database``
    stale. A statement mentioning ``boom`` models a server-rejected switch.
    """

    def __init__(self, host="db.test", port=3306, database="a"):
        self._host = host
        self._port = port
        self._database = database  # connect-time snapshot; never updated
        self.database_property_reads = 0

    @property
    def database(self):
        self.database_property_reads += 1
        raise AssertionError(
            "extractor read the query-executing `database` property"
        )

    def cmd_query(self, statement, *args, **kwargs):
        text = (
            statement.decode() if isinstance(statement, (bytes, bytearray))
            else statement
        )
        if "boom" in text:
            raise RuntimeError("server rejected statement")
        return {"ok": True}

    def cmd_init_db(self, database):
        if not database or "boom" in database:
            raise RuntimeError("server rejected schema switch")
        return {"ok": True}


# Install the real instrumentation observers on the fake connection class, the
# same way MySQLConnectorInstrumentor._instrument() wires the real driver
# classes — so the tests below drive the actual wrapped code path.
wrap(__name__, "_SwitchableConn.cmd_query", mysql_instr._cmd_query_wrapper)
wrap(__name__, "_SwitchableConn.cmd_init_db", mysql_instr._cmd_init_db_wrapper)


def test_schema_observers_install_via_wrap():
    """The observers wire up through the shared ``wrap`` primitive."""
    assert already_wrapped(__name__, "_SwitchableConn.cmd_query")
    assert already_wrapped(__name__, "_SwitchableConn.cmd_init_db")


def test_extract_reflects_runtime_use_via_execute():
    """After ``cursor.execute("USE b")`` (routed through ``cmd_query``) the
    extractor reports ``b``, not the connect-time ``a`` — and never queries."""
    conn = _SwitchableConn(database="a")
    cursor = _MNCursor(conn)
    assert mysql_instr._extract_mysql_target(cursor).database == "a"

    conn.cmd_query("USE b")

    assert mysql_instr._extract_mysql_target(cursor).database == "b"
    assert conn.database_property_reads == 0


def test_extract_reflects_backtick_quoted_use():
    conn = _SwitchableConn(database="a")
    conn.cmd_query("use `Reporting`;")
    assert mysql_instr._extract_mysql_target(_MNCursor(conn)).database == "Reporting"
    assert conn.database_property_reads == 0


def test_extract_reflects_cmd_init_db_switch():
    """``conn.database = "c"`` on the pure-python driver routes through
    ``cmd_init_db``; the extractor tracks it."""
    conn = _SwitchableConn(database="a")
    conn.cmd_init_db("c")
    assert mysql_instr._extract_mysql_target(_MNCursor(conn)).database == "c"


def test_failed_use_does_not_poison_cache():
    """A rejected ``USE`` must not overwrite the last known-good schema."""
    conn = _SwitchableConn(database="a")
    conn.cmd_query("USE b")
    with pytest.raises(RuntimeError):
        conn.cmd_query("USE boom")
    assert mysql_instr._extract_mysql_target(_MNCursor(conn)).database == "b"


def test_parse_use_target_variants():
    parse = mysql_instr._parse_use_target
    assert parse("USE shopdb") == "shopdb"
    assert parse("  use   shopdb  ") == "shopdb"
    assert parse(b"USE orders") == "orders"
    assert parse("USE `weird db`") == "weird db"
    assert parse("SELECT 1") is None
    assert parse("USER_ACCOUNTS") is None  # 'use' prefix but not the USE verb
    assert parse("USE") is None
