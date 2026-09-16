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

"""pymemcache instrumentation.

pymemcache exposes its commands as methods on three client classes in
``pymemcache.client.base``:

- ``Client`` — single-server text protocol.
- ``PooledClient`` — connection pool over ``Client``.
- ``HashClient`` — sharded across multiple servers (in
  ``pymemcache.client.hash``).

We wrap the canonical command methods on the base ``Client`` —
``PooledClient`` delegates to ``Client`` so it inherits the wrappers,
and ``HashClient`` constructs ``Client`` instances on demand which also
inherit. Each command becomes a span event named after the wrapped API
method and annotated with the destination server (``host:port`` or unix
socket path) and the cache key when relevant.
"""

from __future__ import annotations

from typing import Iterable

from ...annotation import ANNOTATION_ARG0
from ...context import current_span
from ...errors import safe_try
from ...instrumentor import BaseInstrumentor
from ...service_type import SERVICE_TYPE_MEMCACHED
from .._util import cached_endpoint, span_event_scope, span_is_sampled, wrap



# Command methods on pymemcache.Client, grouped by related op. Keyless methods
# (``stats``, ``version``, ``flush_all``, ``quit``) are wrapped too, for their RTT.
_CLIENT_METHODS: Iterable[str] = (
    "get",
    "get_many",
    "gets",
    "gets_many",
    "set",
    "set_many",
    "set_multi",
    "add",
    "replace",
    "append",
    "prepend",
    "cas",
    "delete",
    "delete_many",
    "incr",
    "decr",
    "touch",
    "stats",
    "version",
    "flush_all",
    "quit",
)


class PymemcacheInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        for method in _CLIENT_METHODS:
            wrap(
                "pymemcache.client.base",
                f"Client.{method}",
                _make_wrapper(method),
            )


def _make_wrapper(command: str):
    # The operation string is fixed per wrapper; build it once, not per call.
    operation = f"pymemcache.client.base.Client.{command}"

    def _wrapper(wrapped, instance, args, kwargs):
        return _trace_command(wrapped, instance, args, kwargs,
                              command=command, operation=operation)
    _wrapper.__qualname__ = f"_pymemcache_{command}_wrapper"
    return _wrapper


def _trace_command(wrapped, instance, args, kwargs, *, command: str,
                   operation: str):
    span = current_span()
    if span is None or not span_is_sampled(span):
        return wrapped(*args, **kwargs)

    event = span.new_span_event(
        operation,
        service_type=SERVICE_TYPE_MEMCACHED,
    )
    _annotate(event, instance, command, args, kwargs)

    with span_event_scope(event):
        return wrapped(*args, **kwargs)


@safe_try
def _annotate(event, client, command: str, args, kwargs) -> None:
    endpoint = _server_endpoint(client)
    event.set_end_point(endpoint or "UNKNOWN")
    event.set_destination("MEMCACHED")

    key_repr = _summarize_key(command, args, kwargs)
    if key_repr:
        event.annotate_string(ANNOTATION_ARG0, key_repr)


def _resolve_server_endpoint(client):
    server = getattr(client, "server", None)
    if isinstance(server, tuple) and len(server) == 2:
        return server
    if isinstance(server, str):
        return server, None
    return None, None


def _server_endpoint(client) -> str:
    """``Client.server`` is either a ``(host, port)`` tuple for TCP or a
    str path for Unix sockets. ``HashClient`` doesn't have a single server
    — it stores the per-key client elsewhere — so this returns "" for it.
    The server is fixed for the client's life, so the joined endpoint is
    memoized on the client via cached_endpoint (runs per command)."""
    return cached_endpoint(client, _resolve_server_endpoint) or ""


# A set literal assigned to a local is rebuilt (BUILD_SET) on every command;
# only a bare ``x in {...}`` gets constant-folded, so hoist it.
_MULTI_KEY_CMDS = frozenset(
    ("get_many", "gets_many", "set_many", "set_multi", "delete_many"))


def _summarize_key(command: str, args, kwargs) -> str:
    """Render a short representation of the cache key(s) for the trace.
    Multi-key commands (``get_many``, ``set_many``, ``delete_many``)
    receive a sequence or dict; we join their keys with ``,`` and
    truncate the joined string to keep span size bounded."""
    if not args and not kwargs:
        return ""
    first = args[0] if args else (kwargs.get("keys") or kwargs.get("values"))
    if command in _MULTI_KEY_CMDS:
        return _join_keys(first)
    if first is None:
        return ""
    if isinstance(first, (bytes, bytearray)):
        first = first.decode("utf-8", "replace")
    rendered = str(first)
    if len(rendered) > 80:
        return rendered[:80] + "..."
    return rendered


def _join_keys(keys) -> str:
    """Render the keys of a multi-key command as a comma-joined string,
    capped at 80 chars (with ``...`` suffix on overflow). Accepts a dict
    (``set_many`` / ``set_multi``) or any iterable of str/bytes."""
    if keys is None:
        return ""
    try:
        iterable = keys.keys() if isinstance(keys, dict) else iter(keys)
    except TypeError:
        return ""
    rendered: list[str] = []
    total = 0
    for key in iterable:
        if isinstance(key, (bytes, bytearray)):
            key = key.decode("utf-8", "replace")
        text = str(key)
        rendered.append(text)
        # Stop once the cap is blown: a get_many with thousands of keys shouldn't
        # decode and join them all to keep 80 chars. ``total`` overestimates the join
        # by exactly one (trailing comma), so >81 already takes the "..." branch.
        total += len(text) + 1
        if total > 81:
            break
    joined = ",".join(rendered)
    if len(joined) > 80:
        return joined[:80] + "..."
    return joined


def instrument() -> None:
    PymemcacheInstrumentor().instrument()
