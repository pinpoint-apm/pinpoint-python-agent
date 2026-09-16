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

"""aiopg (async PostgreSQL) instrumentation.

aiopg wraps psycopg2 in an asyncio-friendly facade. The cursor type is
``aiopg.connection.Cursor``; ``execute()``, ``executemany()`` and
``callproc()`` are coroutines.

Underneath, aiopg holds a real psycopg2 connection in
``cursor._impl`` / ``cursor.raw`` — that's where host/port/database live.
Our target extractor walks through to it and then defers to the dbapi
base, which already knows how to read psycopg2's
``get_dsn_parameters()``.
"""

from __future__ import annotations

from typing import Optional

from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_POSTGRESQL
from ..dbapi import TargetInfo, connection_target, wrap_async_cursor_class



class AiopgInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        wrap_async_cursor_class(
            "aiopg.connection", "Cursor",
            service_type=SERVICE_TYPE_POSTGRESQL,
            extract_target=_extract_aiopg_target,
        )


def _extract_aiopg_target(cursor) -> Optional[TargetInfo]:
    """Resolve the underlying psycopg2 connection. aiopg exposes it via
    ``cursor.connection`` (an aiopg ``Connection``) whose ``raw`` attribute
    holds the wrapped psycopg2 connection. Older versions used ``_conn``."""
    aio_conn = (
        getattr(cursor, "connection", None)
        or getattr(cursor, "_connection", None)
    )
    if aio_conn is None:
        return None
    raw = (
        getattr(aio_conn, "raw", None)
        or getattr(aio_conn, "_conn", None)
        or aio_conn  # last resort: maybe it *is* the raw connection
    )
    return connection_target(raw)


def instrument() -> None:
    AiopgInstrumentor().instrument()
