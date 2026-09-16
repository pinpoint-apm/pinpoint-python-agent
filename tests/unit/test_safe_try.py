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

"""safe_try must never propagate exceptions. This is load-bearing: it protects
user code from every instrumentation bug."""

from pinpoint.errors import safe_try


def test_safe_try_swallows_exception():
    @safe_try
    def boom():
        raise RuntimeError("instrumentation bug")

    # Must return None and not raise.
    assert boom() is None


def test_safe_try_returns_value_on_success():
    @safe_try
    def ok():
        return 42

    assert ok() == 42


def test_safe_try_passes_through_kwargs():
    @safe_try
    def fn(a, b=2):
        return a + b

    assert fn(1, b=3) == 4
