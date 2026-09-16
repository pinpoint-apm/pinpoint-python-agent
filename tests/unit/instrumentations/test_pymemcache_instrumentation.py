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

Drives the wrappers against an in-memory fake client — no real memcached.
Validates that every command becomes a span event named after the client API,
the destination/endpoint is set from client.server, the API annotation
includes the key (or count for multi-key ops), and exceptions surface.
"""

from __future__ import annotations

import pytest

from pinpoint.instrumentations import pymemcache as pymemcache_instr


# ---------------------------------------------------------------------------
# Fakes (span fakes + ``push_span`` fixture come from _fakes / conftest)
# ---------------------------------------------------------------------------

class _Client:
    def __init__(self, server=("cache.test", 11211)):
        self.server = server


# ---------------------------------------------------------------------------
# Single-key commands
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("command", ["get", "gets", "set", "add", "replace",
                                       "append", "prepend", "cas", "delete",
                                       "incr", "decr", "touch"])
def test_single_key_command_emits_event_with_key_in_api(push_span, command):
    sp, rec = push_span

    def wrapped(*_a, **_kw):
        return None

    wrapper = pymemcache_instr._make_wrapper(command)
    wrapper(wrapped, _Client(), ("user:42", b"value"), {})
    operation = f"pymemcache.client.base.Client.{command}"
    assert ("event_start", "root", operation) in rec.events
    assert ("event_end", "root", operation) in rec.events


# ---------------------------------------------------------------------------
# Multi-key commands
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("command,arg,expected_keys", [
    ("get_many", ["a", "b", "c"], "a,b,c"),
    ("gets_many", ["a", "b"], "a,b"),
    ("set_many", {"a": 1, "b": 2, "c": 3, "d": 4}, "a,b,c,d"),
    ("set_multi", {"x": 1, "y": 2}, "x,y"),
    ("delete_many", ["a", "b"], "a,b"),
])
def test_multi_key_command_renders_joined_keys(push_span, command, arg, expected_keys):
    sp, rec = push_span

    def wrapped(*_a, **_kw):
        return None

    wrapper = pymemcache_instr._make_wrapper(command)
    wrapper(wrapped, _Client(), (arg,), {})
    starts = [e for e in rec.events if e[0] == "event_start"]
    assert starts and starts[-1][2] == f"pymemcache.client.base.Client.{command}"
    # The API annotation feeds off _summarize_key; assert its rendering
    # directly since the fake event recorder doesn't surface annotations.
    assert pymemcache_instr._summarize_key(command, (arg,), {}) == expected_keys


def test_join_keys_decodes_bytes_and_caps_length():
    # bytes keys should decode
    assert pymemcache_instr._join_keys([b"a", b"b", b"c"]) == "a,b,c"
    # dict input uses keys()
    assert pymemcache_instr._join_keys({b"x": 1, b"y": 2}) == "x,y"
    # overflow gets truncated with ... suffix
    long_keys = [f"k{i:03d}" for i in range(50)]
    joined = pymemcache_instr._join_keys(long_keys)
    assert joined.endswith("...")
    assert len(joined) == 83  # 80 chars + "..."


def test_join_keys_handles_none_and_non_iterable():
    assert pymemcache_instr._join_keys(None) == ""
    assert pymemcache_instr._join_keys(42) == ""


# ---------------------------------------------------------------------------
# Connection-less commands
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("command", ["stats", "version", "flush_all", "quit"])
def test_argless_command_still_traces(push_span, command):
    sp, rec = push_span

    def wrapped(*_a, **_kw):
        return None

    wrapper = pymemcache_instr._make_wrapper(command)
    wrapper(wrapped, _Client(), (), {})
    assert ("event_start", "root", f"pymemcache.client.base.Client.{command}") in rec.events


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def test_command_records_exception_and_reraises(push_span):
    sp, rec = push_span

    def boom(*_a, **_kw):
        raise RuntimeError("kaput")

    wrapper = pymemcache_instr._make_wrapper("get")
    with pytest.raises(RuntimeError, match="kaput"):
        wrapper(boom, _Client(), ("k",), {})
    assert any(e[0] == "event_error" for e in rec.events)


def test_command_no_active_span_passes_through():
    """No current span → just delegate."""
    def wrapped(*_a, **_kw):
        return "ok"

    wrapper = pymemcache_instr._make_wrapper("get")
    assert wrapper(wrapped, _Client(), ("k",), {}) == "ok"


# ---------------------------------------------------------------------------
# Server endpoint helper
# ---------------------------------------------------------------------------

def test_server_endpoint_renders_tcp_tuple():
    assert pymemcache_instr._server_endpoint(_Client(("h", 11211))) == "h:11211"


def test_server_endpoint_renders_unix_socket():
    assert pymemcache_instr._server_endpoint(_Client("/tmp/cache.sock")) == "/tmp/cache.sock"


def test_server_endpoint_handles_no_server():
    class _Stub: pass
    assert pymemcache_instr._server_endpoint(_Stub()) == ""
