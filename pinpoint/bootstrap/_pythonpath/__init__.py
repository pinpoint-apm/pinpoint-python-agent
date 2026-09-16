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

"""The directory ``pinpoint-run`` prepends to ``PYTHONPATH``.

**Keep ``sitecustomize.py`` the only module here.** This directory goes
*first* on ``sys.path`` for the launched process — and, because ``PYTHONPATH``
is inherited, for every Python child process it spawns — so every module in it
becomes importable under its bare top-level name and shadows any same-named
module in the user's application. Shadowing ``sitecustomize`` is the whole
point; shadowing anything else is a bug.

``sitecustomize`` drops this directory back off ``sys.path`` once it has run,
but that cleanup cannot be relied on for safety: it does not happen under
``PINPOINT_PY_PREV_SITECUSTOMIZE``, nor in a child started with ``python -S``,
where ``site`` never runs the module at all.

This file exists only so ``setuptools``' package discovery ships the directory;
it must stay empty of behaviour.
"""
