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

mysqlclient is the C-extension fork of MySQL-python that powers Django's
default MySQL backend. The distribution is ``mysqlclient``; the import name
is ``MySQLdb``, with that capitalization.

The cursor hierarchy is in ``MySQLdb.cursors``: ``BaseCursor`` defines
``execute`` / ``executemany``; the public ``Cursor``,
``DictCursor``, ``SSCursor`` etc. inherit from it. Wrapping
``BaseCursor.execute`` covers every subclass.

Connection-level attribute lookup uses the dbapi base: mysqlclient stores
host/port/db on the ``Connection`` instance (no underscore prefix), and
the connection is reachable from the cursor via ``cursor.connection``.
"""

from __future__ import annotations

from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_MYSQL
from ..dbapi import wrap_cursor_class



class MysqlClientInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        # BaseCursor is the abstract parent shared by Cursor / DictCursor /
        # SSCursor — wrapping its methods covers every subclass.
        wrap_cursor_class(
            "MySQLdb.cursors", "BaseCursor",
            service_type=SERVICE_TYPE_MYSQL,
        )


def instrument() -> None:
    MysqlClientInstrumentor().instrument()
