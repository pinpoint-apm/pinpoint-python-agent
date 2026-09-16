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

"""pymemcache against a real memcached container."""

from __future__ import annotations

import pytest

pymemcache = pytest.importorskip("pymemcache")

from pinpoint.annotation import ANNOTATION_ARG0
from pinpoint.service_type import SERVICE_TYPE_MEMCACHED

_OP = "pymemcache.client.base.Client.{}"


@pytest.fixture(scope="module", autouse=True)
def _instrument():
    from pinpoint.instrumentations import pymemcache as instr

    instr.instrument()


@pytest.fixture(scope="module")
def client(memcached_container):
    from pymemcache.client.base import Client

    c = Client((memcached_container["host"], memcached_container["port"]))
    yield c
    c.close()


def test_set_get_record_events(client, memcached_container, traced):
    assert client.set("it-key", b"value") is True
    assert client.get("it-key") == b"value"

    set_ev = traced.single(_OP.format("set"))
    get_ev = traced.single(_OP.format("get"))
    for ev in (set_ev, get_ev):
        assert ev.ended
        assert ev.service_type == SERVICE_TYPE_MEMCACHED
        assert ev.destination == "MEMCACHED"
        assert ev.endpoint == (
            f"{memcached_container['host']}:{memcached_container['port']}")
        assert ev.ann(ANNOTATION_ARG0) == ["it-key"]


def test_get_many_joins_keys(client, traced):
    client.set_many({"it-a": b"1", "it-b": b"2"})
    got = client.get_many(["it-a", "it-b"])
    assert got == {b"it-a": b"1", b"it-b": b"2"} or got == {"it-a": b"1", "it-b": b"2"}

    ev = traced.single(_OP.format("get_many"))
    assert ev.ended
    assert ev.ann(ANNOTATION_ARG0) == ["it-a,it-b"]


def test_error_recorded_on_event(client, traced):
    from pymemcache.exceptions import MemcacheIllegalInputError

    # A key with whitespace is rejected by the client library itself — the
    # failure still happens inside the wrapped method, so it must land on the
    # span event like a server-side error would.
    with pytest.raises(MemcacheIllegalInputError):
        client.set("bad key", b"x")

    ev = traced.single(_OP.format("set"))
    assert ev.ended
    assert ev.error is not None
    assert ev.error[0] == "MemcacheIllegalInputError"


def test_no_current_span_passes_through(client, recorder):
    client.set("it-untraced", b"x")
    assert recorder.events == []
