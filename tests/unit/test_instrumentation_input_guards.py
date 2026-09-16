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

"""Instrumentation must not break a call that works untraced.

Two host-visible failures the wrappers have to absorb: a caller argument the
tracing code inspects but the driver never would, and a client that cannot
carry the headers we want to inject.
"""

from __future__ import annotations

import asyncio

import _fakes
import pytest

from pinpoint import context as ppctx
from pinpoint.instrumentations.dbapi import _trace_query_async
from pinpoint.instrumentations.kafka import (
    _SEND_HEADERS_POS, _producer_send_wrapper, _supports_headers,
)
from pinpoint.service_type import SERVICE_TYPE_PYTHON_METHOD
from pinpoint.tracer import Span


def _root_span():
    return Span(_fakes.FakeNativeSpan("root", recorder=_fakes.Recorder()))


class _RaisingBool:
    """Stand-in for numpy arrays / pandas objects: legal as a driver param,
    but raises the moment anything evaluates its truthiness."""

    def __bool__(self):
        raise ValueError("truth value of an array ... is ambiguous")


# ---------------------------------------------------------------------------
# dbapi async: params are the caller's, and tracing must not evaluate them
# outside its guard
# ---------------------------------------------------------------------------

def test_async_execute_survives_params_whose_bool_raises():
    """The extractor evaluates the params object's truthiness. A raising
    __bool__ must cost the trace, never the query."""
    executed = []

    async def execute(*args, **kwargs):
        executed.append((args, kwargs))
        return "rows"

    kwargs = {"parameters": _RaisingBool()}

    async def run():
        return await _trace_query_async(
            execute, object(), ("INSERT INTO t VALUES (%s)",), kwargs,
            operation="execute", op_kind="execute",
            service_type=SERVICE_TYPE_PYTHON_METHOD, extract=None,
        )

    token = ppctx.set_current_span(_root_span())
    try:
        result = asyncio.run(run())
    finally:
        ppctx.reset_current_span(token)

    assert result == "rows"
    assert executed == [(("INSERT INTO t VALUES (%s)",), kwargs)]


# ---------------------------------------------------------------------------
# kafka-python: record headers need message format v2
# ---------------------------------------------------------------------------

class _FakeProducer:
    def __init__(self, api_version):
        self.config = {"api_version": api_version, "bootstrap_servers": "b:9092"}


def _send(producer, args=("topic",), **kwargs):
    """Run the producer wrapper over a send that records what it received."""
    sent = []

    def wrapped(*a, **kw):
        sent.append((a, kw))
        return "future"

    token = ppctx.set_current_span(_root_span())
    try:
        _producer_send_wrapper(wrapped, producer, args, kwargs)
    finally:
        ppctx.reset_current_span(token)
    return sent[0]


def _pinpoint_keys(headers):
    return [k for k, _ in headers or [] if k.startswith("Pinpoint-")]


@pytest.mark.parametrize("api_version", [(0, 10), (0, 10, 2), (0, 9)])
def test_legacy_broker_send_carries_no_injected_headers(api_version):
    """Below (0, 11) kafka-python's legacy record builder asserts on any
    headers, so a traced send must reach it exactly as the caller wrote it."""
    _args, kwargs = _send(_FakeProducer(api_version), value=b"v")

    assert "headers" not in kwargs


def test_legacy_broker_leaves_caller_headers_untouched():
    caller_headers = [("x", b"1")]
    _args, kwargs = _send(_FakeProducer((0, 10)), headers=caller_headers)

    assert kwargs["headers"] is caller_headers


def test_modern_broker_send_still_gets_trace_headers():
    _args, kwargs = _send(_FakeProducer((2, 5)), value=b"v")

    assert _pinpoint_keys(kwargs["headers"])


def test_unreadable_api_version_keeps_injecting():
    """A mock or subclass we cannot read is not the real legacy producer;
    injecting is the behaviour that predates the gate."""
    assert _supports_headers(object()) is True
    assert _supports_headers(_FakeProducer(None)) is True


def test_modern_broker_injects_into_positional_headers():
    """The gate must not disturb the positional slot the shared helper
    writes when injection does apply."""
    caller_headers = [("x", b"1")]
    args, kwargs = _send(_FakeProducer((2, 5)),
                         args=("topic", b"v", None, caller_headers))

    assert "headers" not in kwargs
    assert _pinpoint_keys(args[_SEND_HEADERS_POS])
    assert caller_headers == [("x", b"1")]  # caller's list not mutated
