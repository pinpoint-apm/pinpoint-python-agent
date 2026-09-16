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

"""aiomysql instrumentation."""

from __future__ import annotations

from unittest import mock

from pinpoint.instrumentations import aiomysql as aiomysql_instr
from pinpoint.service_type import SERVICE_TYPE_MYSQL


def test_instrumentor_targets_aiomysql_cursor_with_async_dbapi_helper():
    with mock.patch.object(aiomysql_instr, "wrap_async_cursor_class") as wrap:
        aiomysql_instr.AiomysqlInstrumentor()._instrument()
    wrap.assert_called_once()
    args, kwargs = wrap.call_args
    assert args == ("aiomysql.cursors", "Cursor")
    assert kwargs["service_type"] == SERVICE_TYPE_MYSQL


def test_target_extraction_via_default_dbapi_helper():
    """aiomysql cursors expose ``connection`` directly with host/port/db
    — the default dbapi extractor handles them with no overrides."""
    from pinpoint.instrumentations.dbapi import extract_default_target

    class _AiomysqlConn:
        host = "db.test"
        port = 3306
        db = b"orders"  # aiomysql uses the legacy ``db`` name

    class _Cur:
        connection = _AiomysqlConn()

    target = extract_default_target(_Cur())
    assert target.host == "db.test"
    assert target.port == 3306
    assert target.database == "orders"
