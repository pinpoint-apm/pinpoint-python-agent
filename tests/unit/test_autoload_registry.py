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

"""The autoload registry must point at callables that actually exist. A typo
here silently disables an integration, so pin it with a static check."""

from importlib import import_module

import pinpoint.autoload as autoload_mod
from pinpoint.autoload import _REGISTRY


def test_every_registered_target_resolves():
    for module_name, target in _REGISTRY.items():
        pkg, _, fn_name = target.partition(":")
        mod = import_module(pkg)
        assert hasattr(mod, fn_name), f"{target} missing ({module_name})"


def test_autoload_skips_disabled_instrumentations(monkeypatch):
    """``PINPOINT_PY_DISABLED_INSTRUMENTATIONS`` opts modules out of autoload.
    The value is comma-split, lowercased, and mapped through ``_ALIASES``, so
    the short alias ``flask`` disables the ``flask.app`` hook while other
    instrumentations still register. A break in the parse/alias/skip logic
    would silently disable a documented opt-out, so pin it here."""
    registered = []
    monkeypatch.setattr(
        autoload_mod.wrapt, "register_post_import_hook",
        lambda hook, module_name: registered.append(module_name),
    )
    # Mixed case + whitespace + multiple entries, using short aliases.
    monkeypatch.setenv("PINPOINT_PY_DISABLED_INSTRUMENTATIONS", "flask, Redis")

    autoload_mod.autoload()

    # Disabled via alias (case-insensitive): their real module hooks are skipped.
    assert "flask.app" not in registered
    assert "redis.client" not in registered
    # The ``redis`` alias covers both the sync and asyncio clients.
    assert "redis.asyncio.client" not in registered
    # A non-disabled instrumentation still registers.
    assert "httpx._client" in registered
    # Only real registry modules are ever registered (no stray names).
    assert set(registered) <= set(_REGISTRY)
    # And exactly the disabled modules were dropped.
    assert set(_REGISTRY) - set(registered) == {
        "flask.app", "redis.client", "redis.asyncio.client",
    }


def test_autoload_registers_all_when_none_disabled(monkeypatch):
    """With the env var unset, every registry module gets a post-import hook."""
    registered = []
    monkeypatch.setattr(
        autoload_mod.wrapt, "register_post_import_hook",
        lambda hook, module_name: registered.append(module_name),
    )
    monkeypatch.delenv("PINPOINT_PY_DISABLED_INSTRUMENTATIONS", raising=False)

    autoload_mod.autoload()

    assert set(registered) == set(_REGISTRY)


def test_django_alias_disables_both_transport_hooks(monkeypatch):
    registered = []
    monkeypatch.setattr(
        autoload_mod.wrapt, "register_post_import_hook",
        lambda hook, module_name: registered.append(module_name),
    )
    monkeypatch.setenv("PINPOINT_PY_DISABLED_INSTRUMENTATIONS", "django")

    autoload_mod.autoload()

    assert "django.core.handlers.wsgi" not in registered
    assert "django.core.handlers.asgi" not in registered
    assert set(_REGISTRY) - set(registered) == {
        "django.core.handlers.wsgi",
        "django.core.handlers.asgi",
    }


import pytest


@pytest.mark.parametrize("token", [
    "MySQLdb",          # the driver's real (mixed-case) package name
    "mysqldb",          # its case-folded form
    "MySQLdb.cursors",  # the full registry module name
])
def test_mysqlclient_opt_out_accepts_all_spellings(monkeypatch, token):
    """User tokens are case-folded before matching, so mixed-case alias keys
    and registry module names ("MySQLdb.cursors") must be matched
    case-insensitively — otherwise ``PINPOINT_PY_DISABLED_INSTRUMENTATIONS=MySQLdb``
    is silently ignored and the instrumentation installs despite the opt-out."""
    registered = []
    monkeypatch.setattr(
        autoload_mod.wrapt, "register_post_import_hook",
        lambda hook, module_name: registered.append(module_name),
    )
    monkeypatch.setenv("PINPOINT_PY_DISABLED_INSTRUMENTATIONS", token)

    autoload_mod.autoload()

    assert "MySQLdb.cursors" not in registered
    assert set(_REGISTRY) - set(registered) == {"MySQLdb.cursors"}


def test_redis_alias_disables_both_sync_and_async_clients(monkeypatch):
    registered = []
    monkeypatch.setattr(
        autoload_mod.wrapt, "register_post_import_hook",
        lambda hook, module_name: registered.append(module_name),
    )
    monkeypatch.setenv("PINPOINT_PY_DISABLED_INSTRUMENTATIONS", "redis")

    autoload_mod.autoload()

    assert "redis.client" not in registered
    assert "redis.asyncio.client" not in registered
    assert set(_REGISTRY) - set(registered) == {
        "redis.client", "redis.asyncio.client",
    }


def test_redis_full_module_name_disables_only_selected_client(monkeypatch):
    registered = []
    monkeypatch.setattr(
        autoload_mod.wrapt, "register_post_import_hook",
        lambda hook, module_name: registered.append(module_name),
    )
    monkeypatch.setenv(
        "PINPOINT_PY_DISABLED_INSTRUMENTATIONS", "redis.asyncio.client",
    )

    autoload_mod.autoload()

    assert "redis.client" in registered
    assert "redis.asyncio.client" not in registered


def test_aiohttp_alias_disables_both_server_and_client(monkeypatch):
    registered = []
    monkeypatch.setattr(
        autoload_mod.wrapt, "register_post_import_hook",
        lambda hook, module_name: registered.append(module_name),
    )
    monkeypatch.setenv("PINPOINT_PY_DISABLED_INSTRUMENTATIONS", "aiohttp")

    autoload_mod.autoload()

    assert "aiohttp.web_protocol" not in registered
    assert "aiohttp.client" not in registered
    assert set(_REGISTRY) - set(registered) == {
        "aiohttp.web_protocol", "aiohttp.client",
    }


def test_aiohttp_full_module_name_disables_only_client(monkeypatch):
    registered = []
    monkeypatch.setattr(
        autoload_mod.wrapt, "register_post_import_hook",
        lambda hook, module_name: registered.append(module_name),
    )
    monkeypatch.setenv("PINPOINT_PY_DISABLED_INSTRUMENTATIONS", "aiohttp.client")

    autoload_mod.autoload()

    assert "aiohttp.web_protocol" in registered
    assert "aiohttp.client" not in registered


def test_aiohttp_transports_use_independent_autoload_entry_points():
    """The package imports its client before server modules. Distinct targets
    ensure the first post-import hook cannot consume the other's global guard."""
    assert _REGISTRY["aiohttp.web_protocol"] == (
        "pinpoint.instrumentations.aiohttp:instrument_server"
    )
    assert _REGISTRY["aiohttp.client"] == (
        "pinpoint.instrumentations.aiohttp:instrument_client"
    )


def test_django_full_module_name_disables_only_selected_transport(monkeypatch):
    registered = []
    monkeypatch.setattr(
        autoload_mod.wrapt, "register_post_import_hook",
        lambda hook, module_name: registered.append(module_name),
    )
    monkeypatch.setenv(
        "PINPOINT_PY_DISABLED_INSTRUMENTATIONS",
        "django.core.handlers.asgi",
    )

    autoload_mod.autoload()

    assert "django.core.handlers.wsgi" in registered
    assert "django.core.handlers.asgi" not in registered


def test_opt_out_token_matching_nothing_warns(monkeypatch):
    """A token matching neither an alias nor a hook module disables nothing,
    and a silent no-op is how an opt-out gets lost: "mysqlclient" is the
    distribution name (the import package is MySQLdb), and
    "django.core.handlers.base" is not a hook module (the WSGI and ASGI
    handlers are). Each must say so rather than let the instrumentation
    install as if nothing was configured."""
    # Keep the token apart from the rendered text: the guidance in the message
    # names example instrumentations, so substring-matching the text alone
    # cannot tell which token a warning is about.
    warned = []
    monkeypatch.setattr(autoload_mod._log, "warning",
                        lambda msg, *args: warned.append((msg % args, args[0])))
    registered = []
    monkeypatch.setattr(
        autoload_mod.wrapt, "register_post_import_hook",
        lambda hook, module_name: registered.append(module_name),
    )
    monkeypatch.setenv(
        "PINPOINT_PY_DISABLED_INSTRUMENTATIONS",
        # Two that match nothing, alongside one of each valid form.
        "mysqlclient, django.core.handlers.base, redis, aiohttp.client",
    )

    autoload_mod.autoload()

    # Warned about exactly the two dead tokens — the valid forms stay silent.
    assert {token for _, token in warned} == {"mysqlclient",
                                              "django.core.handlers.base"}
    # Each warning names the token it is about.
    assert all(f"'{token}'" in text for text, token in warned)
    # The valid forms still take effect.
    assert "redis.client" not in registered
    assert "aiohttp.client" not in registered
    assert "aiohttp.web_protocol" in registered
    # An unmatched token disables nothing else by accident.
    assert "MySQLdb.cursors" in registered
    assert "django.core.handlers.wsgi" in registered
