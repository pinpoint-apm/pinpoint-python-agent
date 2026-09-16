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

"""mysqlclient (MySQLdb) instrumentation.

Smoke-test that the instrumentor calls into the dbapi base with the
expected arguments — the heavy lifting (SQL recording, error handling,
target extraction) is exercised by the dbapi tests themselves.
"""

from __future__ import annotations

from unittest import mock

from pinpoint.instrumentations import mysqlclient as mc_instr


def test_instrumentor_wraps_basecursor_via_dbapi_base():
    """``_instrument`` should ask the dbapi helper to wrap MySQLdb's
    BaseCursor with the mysql dialect + service type."""
    with mock.patch.object(mc_instr, "wrap_cursor_class") as wrap_call:
        mc_instr.MysqlClientInstrumentor()._instrument()

    wrap_call.assert_called_once()
    args, kwargs = wrap_call.call_args
    assert args == ("MySQLdb.cursors", "BaseCursor")
    # MySQL service type code (2101) — using the import to stay in sync.
    from pinpoint.service_type import SERVICE_TYPE_MYSQL
    assert kwargs["service_type"] == SERVICE_TYPE_MYSQL


def test_instrument_module_function_constructs_instrumentor():
    """The autoload entry point ``instrument()`` instantiates and runs
    ``MysqlClientInstrumentor.instrument()``."""
    with mock.patch.object(mc_instr, "MysqlClientInstrumentor") as cls:
        mc_instr.instrument()
    cls.assert_called_once_with()
    cls.return_value.instrument.assert_called_once_with()
