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

"""PyMySQL instrumentation delegates to the shared DB-API base."""

from __future__ import annotations

from unittest import mock

import pytest

from pinpoint.instrumentations import pymysql as pymysql_instr
from pinpoint.instrumentor import _global_installed
from pinpoint.service_type import SERVICE_TYPE_MYSQL


def _pinpoint_wrapper_layers(cls, name):
    """Count the pinpoint wrapper layers installed on ``cls.name`` by walking
    wrapt's ``__wrapped__`` chain and tallying our ``__pinpoint_wrapper__`` tag."""
    obj = cls.__dict__.get(name)
    count = 0
    seen = set()
    while obj is not None and id(obj) not in seen:
        seen.add(id(obj))
        wrapper = getattr(obj, "_self_wrapper", None)
        if getattr(wrapper, "__pinpoint_wrapper__", False) is True:
            count += 1
        obj = getattr(obj, "__wrapped__", None)
    return count


def _unwrap_pinpoint(cls, names):
    """Strip pinpoint wrapper layers off ``cls`` so a test leaves the real
    module untouched for others in the session."""
    for name in names:
        obj = cls.__dict__.get(name)
        original = obj
        while original is not None and getattr(
            getattr(original, "_self_wrapper", None), "__pinpoint_wrapper__", False,
        ) is True:
            original = getattr(original, "__wrapped__", None)
        if original is not None and original is not obj:
            setattr(cls, name, original)


def test_instrumentor_wraps_cursor_via_dbapi_base():
    with mock.patch.object(pymysql_instr, "wrap_cursor_class") as wrap_call:
        pymysql_instr.PyMySQLInstrumentor()._instrument()

    wrap_call.assert_called_once_with(
        "pymysql.cursors",
        "Cursor",
        service_type=SERVICE_TYPE_MYSQL,
    )


def test_instrument_module_function_constructs_instrumentor():
    with mock.patch.object(pymysql_instr, "PyMySQLInstrumentor") as cls:
        pymysql_instr.instrument()
    cls.assert_called_once_with()
    cls.return_value.instrument.assert_called_once_with()


def test_reinstrument_cycle_keeps_single_wrapper():
    """instrument→uninstrument→instrument on the real ``pymysql.cursors.Cursor``
    must leave exactly one pinpoint wrapper per method — not a growing stack."""
    cursors = pytest.importorskip("pymysql.cursors")
    methods = ("execute", "executemany", "callproc")
    inst = pymysql_instr.PyMySQLInstrumentor()
    try:
        inst.instrument()
        inst.uninstrument()
        inst.instrument()
        for name in methods:
            assert _pinpoint_wrapper_layers(cursors.Cursor, name) == 1, (
                f"{name} should carry exactly one pinpoint wrapper"
            )
    finally:
        _unwrap_pinpoint(cursors.Cursor, methods)
        _global_installed.pop(pymysql_instr.PyMySQLInstrumentor, None)
