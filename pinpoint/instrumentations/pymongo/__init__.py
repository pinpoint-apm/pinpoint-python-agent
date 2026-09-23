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

"""pymongo instrumentation via the official monitoring API.

Rather than wrap individual operations on ``Collection`` / ``Database``,
pymongo provides a built-in monitoring layer (PEP-style) that fires
``CommandStartedEvent`` / ``CommandSucceededEvent`` / ``CommandFailedEvent``
for every wire-protocol command — find, insert, update, delete,
aggregate, ``hello``, you name it. Hooking this gives full coverage with
one listener and stays compatible across pymongo versions.

Each command becomes a span event named ``mongo.<command>`` (e.g.
``mongo.find``, ``mongo.insert``) on the *current* span. We use the command's
request id, connection id, and callback-thread id to match started →
succeeded/failed without ever ending another thread's span event.
"""

from __future__ import annotations

import functools
import _thread
import json
from typing import Any

from ..._log import get_logger
from ...annotation import (
    ANNOTATION_MONGO_COLLECTION_OPTION,
    ANNOTATION_MONGO_COLLECTION_INFO,
    ANNOTATION_MONGO_JSON_DATA,
)
from ...context import current_span
from ...errors import safe_try
from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_MONGO
from .._util import (
    limited_repr,
    limited_structure,
    span_is_sampled,
    sql_bind_values_enabled,
    truncate_text,
)

_log = get_logger("pymongo")

# Hard cap on the marshaled command text: the annotation transport accepts 64 KiB
# per string slot, and an insert/update can carry a large document array.
_MONGO_JSON_MAX_BYTES = 64 * 1024
_MONGO_JSON_MAX_ITEMS = 32
_MONGO_JSON_MAX_DEPTH = 6
_MONGO_JSON_MAX_STRING = 4096


# (request_id, connection_id, executor ident) -> span event. PyMongo request ids
# are connection-scoped, not process-global, so concurrent clients can reuse one;
# the connection and thread identity stop a callback from ending another thread's
# event (which would drive its parent Span from the wrong thread too). Normal
# started→succeeded/failed callbacks run on the same driver thread, so the key pairs
# without synchronization. The stored SpanEvent retains its parent Span itself.
_InflightKey = tuple[int, Any, int]
_inflight: dict[_InflightKey, Any] = {}

# PyMongo's monitoring contract delivers exactly one succeeded/failed per
# started, so entries normally clear themselves. A cap covers the case where one
# does not arrive -- every other buffer in this agent is bounded, and an
# unbounded one here pins a SpanEvent, its parent Span, and that span's native
# handle for the life of the process. Eviction drops our reference only: the
# event is still on its parent's stack, so the parent's end() finalizes it, and
# calling end() here could touch an event another thread owns.
_MAX_INFLIGHT = 4096


# ``monitoring.register`` has no unregister counterpart, so register at most once
# per process and toggle ``_listener_enabled``: a second listener would fire
# duplicate ``started`` callbacks, and the second stash overwriting the first
# ``_inflight`` entry leaks a never-ended event per command.
_listener_registered = False
_listener_enabled = False


class PymongoInstrumentor(BaseInstrumentor):
    def _instrument(self) -> None:
        global _listener_registered, _listener_enabled
        try:
            from pymongo import monitoring
        except Exception:  # noqa: BLE001
            _log.debug("pymongo.monitoring unavailable", exc_info=True)
            return
        _listener_enabled = True
        if _listener_registered:
            return
        listener = _PinpointMongoListener()
        try:
            monitoring.register(listener)
            _listener_registered = True
        except Exception:  # noqa: BLE001
            _log.debug("pymongo monitoring.register failed", exc_info=True)

    def _uninstrument(self) -> None:
        global _listener_enabled
        _listener_enabled = False


def _try_import_listener_base():
    """Return ``pymongo.monitoring.CommandListener`` or ``object`` as a
    fallback so the class definition still works when pymongo isn't
    installed (e.g. during agent unit tests)."""
    try:
        from pymongo.monitoring import CommandListener
        return CommandListener
    except Exception:  # noqa: BLE001
        return object


class _PinpointMongoListener(_try_import_listener_base()):
    """``CommandListener`` implementation. pymongo invokes:

    - ``started(event)`` when a command is dispatched.
    - ``succeeded(event)`` when the server replies OK.
    - ``failed(event)`` when the server replies with an error or the
      transport breaks.
    """

    def started(self, event):  # type: ignore[override]
        _on_started(event)

    def succeeded(self, event):  # type: ignore[override]
        _on_succeeded(event)

    def failed(self, event):  # type: ignore[override]
        _on_failed(event)


@safe_try
def _on_started(event) -> None:
    # Only ``started`` is gated: in-flight commands started before an
    # uninstrument still get drained by succeeded/failed so they don't leak.
    if not _listener_enabled:
        return
    span = current_span()
    if span is None:
        return
    if not span_is_sampled(span):
        return
    command_name = getattr(event, "command_name", "command") or "command"
    op_name = f"mongo.{command_name}"
    span_event = span.new_span_event(op_name, service_type=SERVICE_TYPE_MONGO)
    _annotate_started(span_event, event)
    key = _event_key(event)
    if key is None:
        # No correlator → end immediately so we don't leak. The trace
        # still shows the command was issued.
        span_event.end()
        return
    stale = _inflight.pop(key, None)
    if stale is not None:
        # Same connection/thread key re-entered without a terminal callback.
        # The ident proves this thread owns the stale event, so cleanup cannot
        # touch another thread's live Span.
        stale.end()
    if len(_inflight) >= _MAX_INFLIGHT:
        _evict_oldest_inflight()
    _inflight[key] = span_event


def _evict_oldest_inflight() -> None:
    """Drop the oldest tracked command to keep ``_inflight`` bounded.

    Insertion-ordered dict, so the first key is the oldest. Reference dropped
    without ending the event -- see _MAX_INFLIGHT.
    """
    try:
        key = next(iter(_inflight))
    except (StopIteration, RuntimeError):
        # RuntimeError: another driver thread resized the dict between iter()
        # and next(). Eviction is best-effort — skipping one beat is fine;
        # raising here would unwind _on_started before it stashes the event.
        return
    _inflight.pop(key, None)
    _log.debug("pymongo in-flight command table full (%d); dropped the oldest "
               "entry -- its span event is still finalized by its parent span",
               _MAX_INFLIGHT)


@safe_try
def _on_succeeded(event) -> None:
    span_event = _pop(event)
    if span_event is None:
        return
    span_event.end()


@safe_try
def _on_failed(event) -> None:
    span_event = _pop(event)
    if span_event is None:
        return
    failure = getattr(event, "failure", None)
    if isinstance(failure, BaseException):
        if isinstance(failure, Exception):
            try:
                span_event.set_error(failure)
            except Exception:  # noqa: BLE001
                pass
    elif isinstance(failure, dict):
        # MongoDB returns a {errmsg, code} dict in the wire reply on
        # command-level failures. Surface it as a string error.
        try:
            span_event.set_error("MongoCommandError", str(failure))
        except Exception:  # noqa: BLE001
            pass
    span_event.end()


def _event_key(event) -> _InflightKey | None:
    request_id = getattr(event, "request_id", None)
    if request_id is None:
        return None
    connection_id = getattr(event, "connection_id", None)
    try:
        hash(connection_id)
    except Exception:  # noqa: BLE001
        connection_id = repr(connection_id)
    return int(request_id), connection_id, _thread.get_ident()


def _pop(event):
    # succeeded/failed fire for every command even when nothing was tracked
    # (agent disabled, unsampled); skip the key build when there is nothing
    # to pop.
    if not _inflight:
        return None
    key = _event_key(event)
    if key is None:
        return None
    return _inflight.pop(key, None)


@safe_try
def _annotate_started(span_event, event) -> None:
    db = getattr(event, "database_name", "") or ""
    if db:
        span_event.set_destination(str(db))

    addr = getattr(event, "connection_id", None)
    endpoint = _format_address(addr)
    if endpoint:
        span_event.set_end_point(endpoint)

    command_name = getattr(event, "command_name", "") or ""
    command = getattr(event, "command", None) or {}
    collection = _extract_collection(command_name, command)
    if collection:
        # Collection only — the database is already on the span event via
        # set_destination, so prefixing it here would just duplicate.
        span_event.annotate_string(ANNOTATION_MONGO_COLLECTION_INFO, collection)

    collection_option = _extract_collection_option(command)
    if collection_option:
        span_event.annotate_string(
            ANNOTATION_MONGO_COLLECTION_OPTION,
            collection_option,
        )

    # The marshaled command carries literal field VALUES (insert payloads, filter
    # operands) — Mongo's equivalent of SQL bind values, just as likely to hold
    # PII/secrets. Gated on Config.sql_trace_bind_values (off by default); the
    # annotations above still show the command shape and target.
    if sql_bind_values_enabled(span_event):
        marshaled = _marshal_command(command)
        if marshaled:
            # ``ANNOTATION_MONGO_JSON_DATA`` is a string-string slot; the first
            # field carries the rendered command JSON, the second is reserved
            # for normalized/bind data and stays empty for mongo.
            span_event.annotate_string_string(
                ANNOTATION_MONGO_JSON_DATA, marshaled, "",
            )


@functools.cache
def _bson_json_util():
    """``bson.json_util`` (ships with pymongo), or ``None`` when it is not
    importable — either way resolved once, not per command."""
    try:
        from bson import json_util  # type: ignore[import-not-found]
    except Exception:  # noqa: BLE001
        return None
    return json_util


def _marshal_command(command: dict) -> str:
    """Render the wire command as JSON, truncated to the annotation limit.

    Mongo commands routinely include BSON-only types (``ObjectId``,
    ``datetime``, ``Binary``, ``Decimal128`` …) that ``json.dumps`` can't
    encode natively. Prefer ``bson.json_util.dumps`` (ships with pymongo)
    when available; fall back to stdlib ``json`` with ``default=str`` so
    untrusted/unfamiliar types degrade to their ``repr`` rather than
    blowing up the listener.
    """
    if not command:
        return ""
    command = _limit_command(command, _MONGO_JSON_MAX_BYTES)
    text = None
    json_util = _bson_json_util()
    if json_util is not None:
        try:
            text = json_util.dumps(command)
        except Exception:  # noqa: BLE001
            text = None
    if text is None:
        try:
            text = json.dumps(command, default=str, ensure_ascii=False)
        except Exception:  # noqa: BLE001
            return ""
    return _truncate_utf8(text, _MONGO_JSON_MAX_BYTES)


def _limit_command(value, max_chars: int):
    """Bounded copy of the command with mongo's caps and a ``max_chars``
    global budget (see :func:`limited_structure`)."""
    return limited_structure(
        value, max_chars=max_chars, max_depth=_MONGO_JSON_MAX_DEPTH,
        max_items=_MONGO_JSON_MAX_ITEMS, max_string=_MONGO_JSON_MAX_STRING,
    )


def _truncate_utf8(text: str, max_bytes: int) -> str:
    """Cut ``text`` at the last whole UTF-8 codepoint that fits in
    ``max_bytes`` so the annotation never lands mid-multibyte sequence."""
    # UTF-8 needs at most 4 bytes per code point, and ASCII exactly one —
    # both checks are O(n) C loops that skip the full encode() (and its
    # up-to-4x transient allocation) for the overwhelmingly common case.
    if len(text) * 4 <= max_bytes or (len(text) <= max_bytes and text.isascii()):
        return text
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def _format_address(addr) -> str:
    """``CommandStartedEvent.connection_id`` is a ``(host, port)`` tuple
    on TCP servers and a path-like string for Unix sockets."""
    if addr is None:
        return ""
    if isinstance(addr, tuple) and len(addr) >= 2:
        host, port = addr[0], addr[1]
        if host and port:
            return f"{host}:{port}"
        return str(host or "")
    return str(addr)


def _extract_collection(command_name: str, command: dict) -> str:
    """Most CRUD commands stash the collection name as the *value* of the
    command key in the BSON document — e.g. ``{"find": "users", ...}``,
    ``{"insert": "events", ...}``. The pymongo monitoring API exposes the
    full BSON; pulling the collection out gives us the
    ``mongo_collection_info`` annotation users expect in Pinpoint."""
    if not command_name or not isinstance(command, dict):
        return ""
    coll = command.get(command_name)
    if isinstance(coll, str):
        return coll
    return ""


def _extract_collection_option(command: dict) -> str:
    """Return read preference or write concern when pymongo exposes it.

    The monitoring API hands over the wire command, not the collection object,
    so the options are visible only when the caller set them explicitly and
    they ride along as ``$readPreference`` / ``writeConcern``. Record nothing
    otherwise.
    """
    if not isinstance(command, dict):
        return ""
    for key in _COLLECTION_OPTION_KEYS:
        value = command.get(key)
        text = _format_collection_option(value)
        if text:
            return text
    return ""


_COLLECTION_OPTION_KEYS = ("writeConcern", "$readPreference", "readPreference")


def _format_collection_option(value) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, dict):
        mode = value.get("mode")
        if isinstance(mode, str) and len(value) == 1:
            return truncate_text(mode, _MONGO_JSON_MAX_STRING)
        try:
            limited = _limit_command(value, _MONGO_JSON_MAX_STRING)
            return truncate_text(
                json.dumps(limited, default=str, ensure_ascii=False, sort_keys=True),
                _MONGO_JSON_MAX_STRING,
            )
        except Exception:  # noqa: BLE001
            return limited_repr(value, _MONGO_JSON_MAX_STRING)
    return truncate_text(str(value), _MONGO_JSON_MAX_STRING)


def instrument() -> None:
    PymongoInstrumentor().instrument()
