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

"""Bootstrap for `pinpoint-run`: prepends the `_pythonpath` subdirectory to
PYTHONPATH so the `sitecustomize.py` there fires at interpreter startup, and
sets `PINPOINT_PY_AUTOLOAD=1` so it activates autoload.

Only `_pythonpath` goes on PYTHONPATH, never this directory — everything
beside `sitecustomize.py` in a PYTHONPATH entry shadows a top-level module
name in the user's application."""
