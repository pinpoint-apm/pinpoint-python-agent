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

"""The universal `safe_try` guard used by every instrumentation."""

import functools
from typing import Any, TypeVar
from collections.abc import Callable

from ._log import get_logger

_log = get_logger("errors")

F = TypeVar("F", bound=Callable[..., Any])


def safe_try(fn: F) -> F:
    """Decorator that swallows every exception so instrumentation bugs never
    bubble up to user code. Errors are logged at DEBUG.

    Only wrap *instrumentation* callbacks — never wrap the user's code itself.
    """
    @functools.wraps(fn)
    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except Exception:  # noqa: BLE001
            _log.debug("pinpoint instrumentation error in %s", fn.__qualname__, exc_info=True)
            return None
    return _wrapped  # type: ignore[return-value]
