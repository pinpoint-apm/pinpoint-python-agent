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

"""Post-import autoloader for every first-party instrumentation.

We register a `wrapt.importer` post-import hook per target module. The hook
fires exactly once the target is imported, at which point we activate the
corresponding instrumentor. Modules that are never imported pay zero cost.

Users can restrict or disable specific integrations via:

    PINPOINT_PY_DISABLED_INSTRUMENTATIONS=flask,redis
"""

from __future__ import annotations

import os
from importlib import import_module
from typing import Callable, Dict

import wrapt  # type: ignore[import-not-found]

from ._log import get_logger
from .errors import safe_try

_log = get_logger("autoload")

# target module -> dotted path "package:callable"
_REGISTRY: Dict[str, str] = {
    "flask.app": "pinpoint.instrumentations.flask:instrument",
    # Trigger after the concrete transport module has finished importing.
    # Hooking ``base`` fires while WSGIHandler/ASGIHandler may still be
    # undefined in their parent module, permanently missing the outer wrapper.
    "django.core.handlers.wsgi": (
        "pinpoint.instrumentations.django:instrument_wsgi"
    ),
    "django.core.handlers.asgi": (
        "pinpoint.instrumentations.django:instrument_asgi"
    ),
    # Django's ASGI stack (and Channels) adapts every synchronous view,
    # middleware, and ORM call through asgiref's SyncToAsync; the hand-off
    # wrapper keeps those sync sections traced on their worker thread.
    "asgiref.sync": "pinpoint.instrumentations.asgiref:instrument",
    "requests.sessions": "pinpoint.instrumentations.requests:instrument",
    "httpx._client": "pinpoint.instrumentations.httpx:instrument",
    "urllib3.connectionpool": "pinpoint.instrumentations.urllib3:instrument",
    "sqlalchemy.engine": "pinpoint.instrumentations.sqlalchemy:instrument",
    "pymysql.cursors": "pinpoint.instrumentations.pymysql:instrument",
    "redis.client": "pinpoint.instrumentations.redis:instrument",
    "redis.asyncio.client": "pinpoint.instrumentations.redis:instrument_async",
    "grpc": "pinpoint.instrumentations.grpc:instrument",
    "kafka": "pinpoint.instrumentations.kafka:instrument",
    "logging": "pinpoint.instrumentations.logging_ext:instrument",
    "pymongo.monitoring": "pinpoint.instrumentations.pymongo:instrument",
    "pymemcache.client.base": "pinpoint.instrumentations.pymemcache:instrument",
    "cassandra.cluster": "pinpoint.instrumentations.cassandra:instrument",
    "asyncpg.connection": "pinpoint.instrumentations.asyncpg:instrument",
    "aiomysql.cursors": "pinpoint.instrumentations.aiomysql:instrument",
    "aiopg.connection": "pinpoint.instrumentations.aiopg:instrument",
    # ``psycopg2`` itself, not ``psycopg2.extensions`` — see the psycopg
    # instrumentation: a hook on ``extensions`` fires before ``connect`` exists.
    "psycopg2": "pinpoint.instrumentations.psycopg:instrument_psycopg2",
    "psycopg": "pinpoint.instrumentations.psycopg:instrument_psycopg3",
    "MySQLdb.cursors": "pinpoint.instrumentations.mysqlclient:instrument",
    "mysql.connector": "pinpoint.instrumentations.mysql:instrument",
    "confluent_kafka": "pinpoint.instrumentations.confluent_kafka:instrument",
    "aiokafka.producer.producer": "pinpoint.instrumentations.aiokafka:instrument",
    "pika.channel": "pinpoint.instrumentations.pika:instrument",
    "aio_pika.exchange": "pinpoint.instrumentations.aio_pika:instrument",
    "tornado.web": "pinpoint.instrumentations.tornado:instrument",
    "starlette.applications": "pinpoint.instrumentations.starlette:instrument",
    # ``fastapi.applications``, not ``fastapi.routing``: the instrumentation wraps
    # targets in both, and only applications finishes loading after routing. Hooking
    # routing would fire mid-import, before the ``FastAPI`` symbol exists.
    "fastapi.applications": "pinpoint.instrumentations.fastapi:instrument",
    "pyramid.router": "pinpoint.instrumentations.pyramid:instrument",
    # Distinct instrumentor classes per side: ``aiohttp.client`` is imported before
    # the server modules, so one shared guard would let the client hook consume the
    # server installation and break transport-specific opt-out.
    "aiohttp.web_protocol": (
        "pinpoint.instrumentations.aiohttp:instrument_server"
    ),
    "aiohttp.client": "pinpoint.instrumentations.aiohttp:instrument_client",
    "falcon.app": "pinpoint.instrumentations.falcon:instrument",
    "elasticsearch": "pinpoint.instrumentations.elasticsearch:instrument",
}

# Short alias for the env var: the target's top-level package, case-folded. An
# alias owning several transport hooks (django, redis, aiohttp) disables all of
# them; a full module name opts out one. "aio-pika" folds to underscores.
_ALIASES: Dict[str, set] = {}
for _module in _REGISTRY:
    _ALIASES.setdefault(_module.split(".")[0].lower(), set()).add(_module)

# The other accepted token form — a full hook module name, case-folded for the
# same reason the aliases are ("MySQLdb.cursors").
_HOOK_MODULES = {_module.lower() for _module in _REGISTRY}


def autoload() -> None:
    """Register post-import hooks for every enabled instrumentation."""
    disabled_env = os.environ.get("PINPOINT_PY_DISABLED_INSTRUMENTATIONS", "")
    disabled = {s.strip().lower() for s in disabled_env.split(",") if s.strip()}
    # User tokens were case-folded above, so alias and registry matching must
    # be case-insensitive too — some registry keys are mixed-case module names
    # ("MySQLdb.cursors") that would otherwise never match a folded token.
    disabled_modules = set()
    for name in disabled:
        modules = _ALIASES.get(name) or _ALIASES.get(name.replace("-", "_"))
        if modules:
            disabled_modules.update(m.lower() for m in modules)
        elif name in _HOOK_MODULES:
            disabled_modules.add(name)
        else:
            # Matches neither form, so it disables nothing. Say so: an opt-out
            # that stopped matching — a distribution name rather than the
            # import package ("mysqlclient" for MySQLdb), or a hook module
            # since renamed or split — otherwise re-enables instrumentation on
            # upgrade with no sign of it outside the trace itself.
            _log.warning(
                "PINPOINT_PY_DISABLED_INSTRUMENTATIONS: '%s' matches no "
                "instrumentation and disabled nothing — expected a package "
                "alias ('redis') or a hook module ('redis.asyncio.client')",
                name)

    for module_name, target in _REGISTRY.items():
        if module_name.lower() in disabled_modules:
            _log.info("pinpoint instrumentation disabled: %s", module_name)
            continue
        wrapt.register_post_import_hook(_make_hook(target), module_name)


def _make_hook(target: str) -> Callable[[object], None]:
    @safe_try
    def _hook(_module: object) -> None:
        pkg, _, fn_name = target.partition(":")
        module = import_module(pkg)
        fn = getattr(module, fn_name, None)
        if fn is None:
            _log.debug("autoload target missing: %s", target)
            return
        fn()
    return _hook
