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

"""Span-event names in the Java agent's API descriptor form.

Pinpoint Web fills the call tree's Class column only for an API string shaped
like the Java agent's ``ApiUtils.mergeApiDescriptor`` output,
``<class>.<method>(<params>)``. ``_web_split`` ports the part of Web's
``ApiDescriptionParser`` + ``DefaultApiParser`` that decides the two columns,
so these tests check what the call tree actually shows.
"""

from __future__ import annotations

import functools
import importlib
import pkgutil

import pytest

import pinpoint.instrumentations as _instrumentations
from pinpoint._api_name import api_descriptor, callable_api_name
from pinpoint.instrumentations.grpc import _event_name as grpc_event_name


def _web_split(api_info: str) -> tuple[str, str]:
    """(Class column, Method column) Pinpoint Web shows for a DEFAULT-type
    API string — commons-server ApiDescriptionParser.parse(): the first '(',
    the first ')' after it, the last '.' before the '('; the Class column is
    the class name's last segment. A failed parse (no parentheses) shows the
    whole string as the method, with an empty class."""
    start = api_info.find("(")
    end = api_info.find(")", start) if start != -1 else -1
    if start == -1 or end == -1:
        return "", api_info
    dot = api_info.rfind(".", 0, start)
    class_name = api_info[:dot] if dot != -1 else ""
    method = api_info[dot + 1:start] + api_info[start:end + 1]
    return class_name[class_name.rfind(".") + 1:], method


# ---- the Web side, against the Java agent's own output --------------------

def test_web_split_matches_java_agent_descriptor():
    # DefaultMethodDescriptor("clazz", "method", null, null, 0), as in the
    # Java agent's DefaultApiMetaDataServiceTest, sends "clazz.method()".
    assert _web_split("clazz.method()") == ("clazz", "method()")
    assert _web_split(
        "org.springframework.web.servlet.FrameworkServlet.doGet()"
    ) == ("FrameworkServlet", "doGet()")


def test_name_without_parentheses_leaves_class_column_empty():
    # Why the agent appends "()": the pre-change names failed Web's parse.
    assert _web_split("redis.client.Redis.execute_command") == (
        "", "redis.client.Redis.execute_command")


# ---- api_descriptor --------------------------------------------------------

def test_api_descriptor_joins_module_qualname_and_method():
    assert api_descriptor("myapp.handlers", "UserHandler", "get") == (
        "myapp.handlers.UserHandler.get()")
    assert api_descriptor("myapp.views", "index") == "myapp.views.index()"


def test_api_descriptor_drops_locals_segments():
    assert api_descriptor("myapp", "create_app.<locals>.index") == (
        "myapp.create_app.index()")


@pytest.mark.parametrize("module", [None, "", 42])
def test_api_descriptor_omits_unusable_module(module):
    assert api_descriptor(module, "Cls.meth") == "Cls.meth()"


# ---- callable_api_name -----------------------------------------------------

def _module_function():
    pass


class _Resource:
    def on_get(self):
        pass

    class Nested:
        def run(self):
            pass


class _CallableApp:
    def __call__(self, environ, start_response):
        pass


def test_function_is_module_qualified():
    name = callable_api_name(_module_function)
    assert name == f"{__name__}._module_function()"
    assert _web_split(name) == (__name__.rsplit(".", 1)[-1], "_module_function()")


def test_method_shows_its_class():
    name = callable_api_name(_Resource().on_get)
    assert name == f"{__name__}._Resource.on_get()"
    assert _web_split(name) == ("_Resource", "on_get()")


def test_nested_class_method_shows_innermost_class():
    name = callable_api_name(_Resource.Nested.run)
    assert _web_split(name) == ("Nested", "run()")


def test_local_function_names_enclosing_scope_not_locals():
    def create_app():
        def index():
            pass
        return index

    name = callable_api_name(create_app())
    assert "<locals>" not in name
    assert _web_split(name) == ("create_app", "index()")


def test_callable_object_is_named_after_its_call_method():
    name = callable_api_name(_CallableApp())
    assert name == f"{__name__}._CallableApp.__call__()"
    assert _web_split(name) == ("_CallableApp", "__call__()")


def test_partial_and_wraps_are_unwrapped():
    @functools.wraps(_module_function)
    def wrapper(*a, **kw):
        pass

    assert callable_api_name(functools.partial(wrapper)) == (
        f"{__name__}._module_function()")


def test_django_class_based_view_names_view_class_dispatch():
    # Django's as_view() returns a closure whose __qualname__ is
    # "View.as_view.<locals>.view"; Django documents view_class as the name.
    class UserView:
        pass

    def view(request):
        pass
    view.view_class = UserView

    assert _web_split(callable_api_name(view)) == ("UserView", "dispatch()")


def test_unresolvable_returns_default():
    assert callable_api_name(None, default="fallback") == "fallback"

    class _Raises:
        def __getattribute__(self, name):
            raise RuntimeError(name)

    assert callable_api_name(_Raises(), default="fallback") == "fallback"


# ---- gRPC method paths -----------------------------------------------------

def test_grpc_method_path_becomes_service_and_method():
    name = grpc_event_name("/helloworld.Greeter/SayHello")
    assert name == "helloworld.Greeter.SayHello()"
    assert _web_split(name) == ("Greeter", "SayHello()")


@pytest.mark.parametrize("method", ["", None, b"/svc/Method"])
def test_grpc_unusable_method_is_left_alone(method):
    assert grpc_event_name(method) == method


# ---- every instrumentation's fixed span-event name -------------------------

def _span_event_constants():
    # Root-span operations (… HTTP Server, … Invocation, gRPC Server) are
    # WEB_REQUEST/INVOCATION-type APIs Web shows verbatim; asyncpg's _PREFIX is
    # completed with the method at wrap time.
    for info in pkgutil.iter_modules(_instrumentations.__path__):
        module = importlib.import_module(
            f"{_instrumentations.__name__}.{info.name}")
        for attr, value in vars(module).items():
            if ("OPERATION" not in attr or not isinstance(value, str)
                    or "SERVER" in attr or "INVOCATION" in attr
                    or attr.endswith("PREFIX")):
                continue
            yield pytest.param(value, id=f"{info.name}.{attr}")


@pytest.mark.parametrize("operation", list(_span_event_constants()))
def test_instrumentation_span_event_names_fill_class_column(operation):
    class_column, method_column = _web_split(operation)
    assert class_column, operation
    assert method_column.endswith("()"), operation
