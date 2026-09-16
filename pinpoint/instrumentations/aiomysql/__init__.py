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

"""aiomysql (async MySQL) instrumentation.

aiomysql is a port of pymysql to asyncio. ``aiomysql.cursors.Cursor``
exposes ``execute()``, ``executemany()`` and ``callproc()`` as
coroutines; subclasses (``DictCursor``, ``SSCursor``, ``SSDictCursor``)
inherit from it.

The cursor's ``connection`` attribute is the aiomysql ``Connection``,
which stores host/port/db on plain attributes — same as pymysql — so
the dbapi base's default extractor works without an override.
"""

from __future__ import annotations

from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_MYSQL
from ..dbapi import wrap_async_cursor_class



class AiomysqlInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        wrap_async_cursor_class(
            "aiomysql.cursors", "Cursor",
            service_type=SERVICE_TYPE_MYSQL,
        )


def instrument() -> None:
    AiomysqlInstrumentor().instrument()
