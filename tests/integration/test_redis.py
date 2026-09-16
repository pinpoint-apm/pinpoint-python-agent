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

"""redis-py against a real Redis container."""

from __future__ import annotations

import asyncio

import pytest

redis = pytest.importorskip("redis")

from pinpoint.annotation import ANNOTATION_ARG0
from pinpoint.service_type import SERVICE_TYPE_REDIS

_OP_EXECUTE = "redis.client.Redis.execute_command"
_OP_PIPELINE = "redis.client.Pipeline.execute"
_OP_ASYNC_EXECUTE = "redis.asyncio.client.Redis.execute_command"
_OP_ASYNC_PIPELINE = "redis.asyncio.client.Pipeline.execute"


@pytest.fixture(scope="module", autouse=True)
def _instrument():
    from pinpoint.instrumentations import redis as instr

    instr.instrument()
    instr.instrument_async()


@pytest.fixture(scope="module")
def client(redis_container):
    c = redis.Redis(host=redis_container["host"],
                    port=redis_container["port"])
    yield c
    c.close()


def test_execute_command_records_event(client, redis_container, traced):
    assert client.set("it:key", "v") is True
    assert client.get("it:key") == b"v"

    events = [e for e in traced.events_named(_OP_EXECUTE) if e.ended]
    assert len(events) == 2
    set_ev, get_ev = events
    assert set_ev.service_type == SERVICE_TYPE_REDIS
    assert set_ev.destination == "REDIS"
    endpoint = f"{redis_container['host']}:{redis_container['port']}"
    assert set_ev.endpoint == endpoint
    assert set_ev.ann(ANNOTATION_ARG0) == ["SET"]
    assert get_ev.ann(ANNOTATION_ARG0) == ["GET"]


def test_pipeline_execute_joins_command_names(client, traced):
    pipe = client.pipeline()
    pipe.incr("it:counter")
    pipe.expire("it:counter", 60)
    assert pipe.execute() == [1, True]

    ev = traced.single(_OP_PIPELINE)
    assert ev.ended
    assert ev.service_type == SERVICE_TYPE_REDIS
    # redis-py implements incr() via INCRBY.
    assert ev.ann(ANNOTATION_ARG0) == ["INCRBY,EXPIRE"]


def test_error_recorded_on_event(client, traced):
    with pytest.raises(redis.ResponseError):
        client.execute_command("NOSUCHCOMMAND")

    events = traced.events_named(_OP_EXECUTE)
    assert events and events[-1].ended
    assert events[-1].error is not None
    assert events[-1].error[0] == "ResponseError"


def test_no_current_span_passes_through(client, recorder):
    # No span in context: the wrapped command must still work, untraced.
    assert client.ping() is True
    assert recorder.events == []


def test_async_commands_and_pipeline_record_events(redis_container, traced):
    """The asyncio client uses distinct Redis/Pipeline classes, so exercise
    both real async wrappers instead of assuming the sync hooks cover them."""
    async def main():
        client = redis.asyncio.Redis(
            host=redis_container["host"], port=redis_container["port"])
        try:
            assert await client.set("it:async-key", "v") is True
            assert await client.get("it:async-key") == b"v"

            pipe = client.pipeline()
            pipe.incr("it:async-counter")
            pipe.expire("it:async-counter", 60)
            assert await pipe.execute() == [1, True]
        finally:
            await client.aclose()

    asyncio.run(main())

    commands = [
        event for event in traced.events_named(_OP_ASYNC_EXECUTE)
        if event.ended
    ]
    assert len(commands) == 2
    endpoint = f"{redis_container['host']}:{redis_container['port']}"
    for event in commands:
        assert event.service_type == SERVICE_TYPE_REDIS
        assert event.destination == "REDIS"
        assert event.endpoint == endpoint
    assert commands[0].ann(ANNOTATION_ARG0) == ["SET"]
    assert commands[1].ann(ANNOTATION_ARG0) == ["GET"]

    pipeline = traced.single(_OP_ASYNC_PIPELINE)
    assert pipeline.ended
    assert pipeline.service_type == SERVICE_TYPE_REDIS
    assert pipeline.endpoint == endpoint
    assert pipeline.ann(ANNOTATION_ARG0) == ["INCRBY,EXPIRE"]


def test_async_error_ends_and_marks_event(redis_container, traced):
    """An error raised from the awaited coroutine must still close the async
    span event; a wrapper around coroutine creation alone would miss this."""
    async def main():
        client = redis.asyncio.Redis(
            host=redis_container["host"], port=redis_container["port"])
        try:
            await client.execute_command("NOSUCHCOMMAND")
        finally:
            await client.aclose()

    with pytest.raises(redis.ResponseError):
        asyncio.run(main())

    event = traced.single(_OP_ASYNC_EXECUTE)
    assert event.ended
    assert event.error is not None
    assert event.error[0] == "ResponseError"


def test_async_no_current_span_passes_through(redis_container, recorder):
    async def main():
        client = redis.asyncio.Redis(
            host=redis_container["host"], port=redis_container["port"])
        try:
            return await client.ping()
        finally:
            await client.aclose()

    assert asyncio.run(main()) is True
    assert recorder.events == []
