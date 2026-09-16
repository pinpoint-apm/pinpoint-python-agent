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

"""Size bounds in ``_util``.

Annotation payloads are capped so one pathological value cannot inflate a span.
The caps only hold if the helpers enforcing them respect their own limit.
"""

from __future__ import annotations

import pytest

from pinpoint.instrumentations._util import limited_repr, truncate_text


@pytest.mark.parametrize("max_len", [0, 1, 2, 3, 4, 5, 16, 1024])
def test_truncate_text_never_exceeds_its_cap(max_len):
    # Below four characters there is no room for "..." within the cap, so the
    # marker has to be dropped rather than overrun the caller's budget.
    assert len(truncate_text("abcdefghijklmnop", max_len)) <= max_len


def test_truncate_text_keeps_short_text_verbatim():
    assert truncate_text("abc", 8) == "abc"
    assert truncate_text("abc", 3) == "abc"


def test_truncate_text_ellipsizes_when_there_is_room():
    assert truncate_text("abcdefghij", 8) == "abcde..."
    assert truncate_text("abcdefghij", 4) == "a..."


def test_truncate_text_hard_clips_below_the_marker_width():
    assert truncate_text("abcdefghij", 3) == "abc"
    assert truncate_text("abcdefghij", 2) == "ab"
    assert truncate_text("abcdefghij", 1) == "a"
    assert truncate_text("abcdefghij", 0) == ""


@pytest.mark.parametrize("max_len", [1, 2, 3, 4, 32])
def test_limited_repr_never_exceeds_its_cap(max_len):
    # limited_repr truncates through truncate_text, so it inherits the bound.
    assert len(limited_repr(b"\xff" * 4096, max_len)) <= max_len
    assert len(limited_repr("x" * 4096, max_len)) <= max_len


def test_cached_endpoint_memoizes_slotted_connections():
    """Connections that reject attribute assignment (asyncpg's Connection is
    __slots__) must fall back to the weak-key cache instead of silently
    recomputing the endpoint on every command, forever."""
    from pinpoint.instrumentations._util import cached_endpoint

    class _Slotted:
        __slots__ = ("__weakref__",)

    conn = _Slotted()
    calls = []

    def resolve(c):
        calls.append(c)
        return "db.host", 5432

    assert cached_endpoint(conn, resolve) == "db.host:5432"
    assert cached_endpoint(conn, resolve) == "db.host:5432"
    assert len(calls) == 1


def test_limited_structure_walks_a_non_dict_mapping():
    """A driver's own document type (pymongo's RawBSONDocument, SON) is a
    Mapping but not a dict. Falling through to limited_repr would repr the
    *whole* value first — RawBSONDocument's embeds its entire raw payload —
    so the walk must recurse structurally and stay inside its budgets."""
    from collections.abc import Mapping

    from pinpoint.instrumentations._util import limited_structure

    class _Doc(Mapping):
        """Stands in for RawBSONDocument: a Mapping whose repr is O(payload)."""

        def __init__(self, data):
            self._data = data

        def __getitem__(self, key):
            return self._data[key]

        def __iter__(self):
            return iter(self._data)

        def __len__(self):
            return len(self._data)

        def __repr__(self):
            raise AssertionError("the whole document must not be repr'd")

    out = limited_structure(
        {"insert": "c", "documents": [_Doc({"_id": 1, "blob": "x" * 40})]},
        max_chars=4096, max_depth=6, max_items=16, max_string=64,
    )

    assert out["documents"][0]["_id"] == 1
    assert out["documents"][0]["blob"] == "x" * 40


def test_end_server_span_always_ends_the_span():
    """A failing annotation step must cost only the annotation: the callers
    are @safe_try, so an exception escaping before ``span.end()`` would leave
    the root span open for good."""
    from pinpoint.instrumentations._util import end_server_span

    class _Span:
        ended = False

        def set_url_stat(self, *_a):
            raise RuntimeError("boom")

        def end(self):
            self.ended = True

    span = _Span()
    end_server_span(span, "/x", "GET", "not-an-int", sampled=False)
    assert span.ended
