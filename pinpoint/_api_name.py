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

The Java agent names every span event ``<class>.<method>(<params>)``
(``ApiUtils.mergeApiDescriptor``), e.g.
``org.springframework.web.servlet.FrameworkServlet.doGet(...)``. Pinpoint Web's
``ApiDescriptionParser`` relies on that shape for a DEFAULT-type API: the text
up to the last ``.`` before ``(`` is the class, whose last segment fills the
call tree's Class column, and ``method(params)`` fills the Method column. A
name with no ``(`` fails that parse, so the call tree shows it whole in the
Method column and leaves the Class column empty.

Python has no parameter types to report, so the parameter list is ``()`` —
what the Java agent itself sends for a method descriptor without parameter
information (``ApiUtils.mergeParameterVariableNameDescription(null, null)``).
"""

from __future__ import annotations

import functools
from typing import Any

# A function defined inside another one has ``outer.<locals>.inner`` as its
# __qualname__ (Flask's app-factory routes, closures). Kept, the Class column
# would read ``<locals>``; dropped, it names the enclosing scope instead.
_LOCALS = "<locals>."


def api_descriptor(module: Any, qualname: str, method: str = "") -> str:
    """``module.qualname[.method]()``, the form Pinpoint Web parses into a
    class and a method. An empty or non-string ``module`` is left out."""
    if _LOCALS in qualname:
        qualname = qualname.replace(_LOCALS, "")
    name = f"{module}.{qualname}" if isinstance(module, str) and module else qualname
    if method:
        name = f"{name}.{method}"
    return name + "()"


def callable_api_name(fn: Any, default: str = "") -> str:
    """API descriptor naming the code a call of ``fn`` runs.

    - function or method: ``module.qualname()``
      (``myapp.views.UserView.get()``, ``myapp.views.index()``);
    - class: ``module.qualname()``, the instantiation;
    - Django class-based view (``as_view()`` returns a closure carrying
      ``view_class``, the name Django says to use): ``module.Class.dispatch()``;
    - callable object: ``module.Class.__call__()``.

    ``functools.partial`` layers and a wrapt/``functools.wraps`` wrapper are
    stripped first, so the user's own function is named rather than
    ``decorator.<locals>._wrapper``. Returns ``default`` for ``None`` or when
    nothing resolves.
    """
    if fn is None:
        return default
    try:
        while isinstance(fn, functools.partial):
            fn = fn.func
        fn = getattr(fn, "__wrapped__", fn)
        view_class = getattr(fn, "view_class", None)
        if isinstance(view_class, type):
            return api_descriptor(view_class.__module__, view_class.__qualname__,
                                  "dispatch")
        name = getattr(fn, "__qualname__", None) or getattr(fn, "__name__", None)
        if isinstance(name, str) and name:
            return api_descriptor(getattr(fn, "__module__", None), name)
        cls = type(fn)
        name = getattr(cls, "__qualname__", None) or getattr(cls, "__name__", None)
        if isinstance(name, str) and name:
            return api_descriptor(cls.__module__, name, "__call__")
    except Exception:  # noqa: BLE001 — naming must never fail the caller
        pass
    return default
