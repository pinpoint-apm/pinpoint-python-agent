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

"""PyMySQL DB-API driver instrumentation."""

from __future__ import annotations

from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_MYSQL
from ..dbapi import wrap_cursor_class


class PyMySQLInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        wrap_cursor_class(
            "pymysql.cursors",
            "Cursor",
            service_type=SERVICE_TYPE_MYSQL,
        )


def instrument() -> None:
    PyMySQLInstrumentor().instrument()
