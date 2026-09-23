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

"""aiopg instrumentation.

Validates the target-extractor walks through aiopg.Connection.raw to the
underlying psycopg2 connection (so dbapi's get_dsn_parameters() path
applies) and the async cursor wrappers are wired up.
"""

from __future__ import annotations

from unittest import mock

from pinpoint.instrumentations import aiopg as aiopg_instr
from pinpoint.service_type import SERVICE_TYPE_POSTGRESQL


def test_instrumentor_uses_async_dbapi_wrappers():
    with mock.patch.object(aiopg_instr, "wrap_async_cursor_class") as wrap_call:
        aiopg_instr.AiopgInstrumentor()._instrument()
    wrap_call.assert_called_once()
    args, kwargs = wrap_call.call_args
    assert args == ("aiopg.connection", "Cursor")
    assert kwargs["service_type"] == SERVICE_TYPE_POSTGRESQL


def test_extract_target_walks_through_aiopg_connection_raw():
    """aiopg's Cursor.connection is an aiopg Connection wrapping psycopg2."""
    class _Psycopg2Conn:
        def get_dsn_parameters(self):
            return {"host": "pg.test", "port": "5432", "dbname": "shop"}

    class _AiopgConn:
        raw = _Psycopg2Conn()

    class _Cursor:
        connection = _AiopgConn()

    target = aiopg_instr._extract_aiopg_target(_Cursor())
    assert target.host == "pg.test"
    assert target.port == 5432
    assert target.database == "shop"


def test_extract_target_falls_back_to_underscore_conn():
    """Older aiopg versions stash the underlying connection on ``_conn``."""
    class _Psycopg2Conn:
        host = "old.test"
        port = 5432
        database = "legacy"

    class _AiopgConn:
        _conn = _Psycopg2Conn()
    setattr(_AiopgConn, "raw", None)

    class _Cursor:
        connection = _AiopgConn()

    target = aiopg_instr._extract_aiopg_target(_Cursor())
    assert target.host == "old.test"
    assert target.database == "legacy"


def test_extract_target_returns_none_without_connection():
    class _Stub:
        pass
    assert aiopg_instr._extract_aiopg_target(_Stub()) is None
