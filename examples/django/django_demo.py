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

"""Minimal Django demo — single-file project to exercise the Pinpoint
``django`` instrumentation.

Run directly:

    pinpoint-run --app-name demo-django --agent-name demo-django \
        --collector localhost -- python examples/django/django_demo.py

Or without ``pinpoint-run``:

    python examples/django/django_demo.py

Then hit it:

    curl http://localhost:8000/ping
    curl http://localhost:8000/items/42
    curl http://localhost:8000/items/42/edit
    curl http://localhost:8000/db/mongo        # pymongo SELECT-style
    curl http://localhost:8000/mongo/crud      # pymongo insert/find/update/delete
    curl http://localhost:8000/boom

The Pinpoint django instrumentation wraps ``WSGIHandler.__call__`` and
``ASGIHandler.__call__`` so each root span covers the complete response-body
lifecycle, including streaming responses. The /items routes demonstrate
URL-pattern-based stats; /boom raises so the agent reports the error on the
root span.

The /db/mongo and /mongo/crud endpoints exercise the pymongo
instrumentation through the official monitoring API: every wire command
(``find``, ``insert``, ``update``, ``delete``, ``count``, ``hello``, …)
becomes a ``mongo.<command>`` span event under the Django request span.
Defaults match ``docker run --rm -p 27017:27017 mongo:7``. Override via
``MONGO_HOST``, ``MONGO_PORT``, ``MONGO_DATABASE``.
"""

from __future__ import annotations

import os
import sys

try:
    import pymongo  # type: ignore[import-not-found]
except ImportError:
    pymongo = None  # type: ignore[assignment]


MONGO_CONFIG = {
    "host": os.environ.get("MONGO_HOST", "127.0.0.1"),
    "port": int(os.environ.get("MONGO_PORT", "27017")),
    "database": os.environ.get("MONGO_DATABASE", "demo"),
}


def _configure_django() -> None:
    """Stand up a project-less Django app in a single file.

    ``settings.configure()`` is the documented way to use Django without
    a ``settings.py`` file; ``DEBUG=True`` makes uncaught view errors
    render a 500 page so the agent still gets a status to report.
    """
    from django.conf import settings

    if settings.configured:
        return

    settings.configure(
        DEBUG=True,
        SECRET_KEY="pinpoint-demo-not-a-secret",
        ROOT_URLCONF=__name__,
        ALLOWED_HOSTS=["*"],
        MIDDLEWARE=[],
        DATABASES={},
        INSTALLED_APPS=[],
    )

    import django

    django.setup()


_configure_django()

from django.http import JsonResponse  # noqa: E402  (configured above)
from django.urls import path  # noqa: E402


def ping(_request):
    return JsonResponse({"service": "demo-django", "ok": True})


def items(_request, item_id: int, action: str = "view"):
    """Route template — the django agent reports url_stat against the
    matched path (``/items/<int:item_id>``) so the Pinpoint UI groups
    requests per endpoint rather than per concrete id."""
    return JsonResponse({"item_id": int(item_id), "action": action})


def boom(_request):
    """Raise so ``set_error`` is exercised on the root span. With
    ``DEBUG=True`` Django still returns a 500 — the agent annotates it."""
    raise RuntimeError("simulated django failure")


def _mongo_client():
    """Short-lived MongoClient with a tight server-selection timeout so a
    missing mongo doesn't hang the demo for 30s."""
    return pymongo.MongoClient(
        host=MONGO_CONFIG["host"],
        port=MONGO_CONFIG["port"],
        serverSelectionTimeoutMS=2000,
    )


def db_mongo(_request):
    """Single-command pymongo path: ping + server build info.

    The pymongo monitoring API fires one ``CommandStartedEvent`` per
    wire command, so this endpoint produces ``mongo.ping`` and
    ``mongo.buildInfo`` span events under the Django request span.
    """
    if pymongo is None:
        return JsonResponse(
            {"error": "pymongo not installed (pip install pymongo)"},
            status=503,
        )
    try:
        client = _mongo_client()
        admin = client[MONGO_CONFIG["database"]].command("ping")
        info = client.server_info()
    except Exception as exc:  # noqa: BLE001
        return JsonResponse({"error": f"mongo failed: {exc}"}, status=503)
    finally:
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass
    return JsonResponse({
        "ping_ok": int(admin.get("ok", 0)) == 1,
        "mongo_version": info.get("version"),
    })


def mongo_crud(_request):
    """Exercise insert/find/update/delete so each pymongo codepath emits a span.

    Mirrors the ``/crud`` pattern in ``flask_demo.py`` but on a
    ``demo_items`` collection rather than a SQL table. The pymongo
    instrumentation reports one ``mongo.<command>`` span event per wire
    command (``insert``, ``find``, ``update``, ``delete``, ``count``),
    each annotated with the database and collection.
    """
    if pymongo is None:
        return JsonResponse(
            {"error": "pymongo not installed (pip install pymongo)"},
            status=503,
        )
    try:
        client = _mongo_client()
        coll = client[MONGO_CONFIG["database"]]["demo_items"]
        coll.delete_many({})
        insert_res = coll.insert_many([
            {"name": "alpha", "value": 1},
            {"name": "beta", "value": 2},
            {"name": "gamma", "value": 3},
        ])
        rows = [
            {"name": d["name"], "value": d["value"]}
            for d in coll.find({}, {"_id": 0}).sort("value")
        ]
        update_res = coll.update_many({"name": "beta"}, {"$mul": {"value": 10}})
        delete_res = coll.delete_many({"value": {"$lt": 15}})
        remaining = coll.count_documents({})
    except Exception as exc:  # noqa: BLE001
        return JsonResponse({"error": f"mongo crud failed: {exc}"}, status=503)
    finally:
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass
    return JsonResponse({
        "inserted": len(insert_res.inserted_ids),
        "selected": rows,
        "updated": update_res.modified_count,
        "deleted": delete_res.deleted_count,
        "remaining": remaining,
    })


urlpatterns = [
    path("ping", ping),
    path("items/<int:item_id>", items),
    path("items/<int:item_id>/<str:action>", items),
    path("db/mongo", db_mongo),
    path("mongo/crud", mongo_crud),
    path("boom", boom),
]


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    # HTTP header tracing — same keys as Http.{Server,Client}.Record*Header
    # in pinpoint-cpp-agent's pinpoint-config.yaml. Equivalent env vars are
    # PINPOINT_PY_HTTP_{SERVER,CLIENT}_RECORD_{REQUEST,RESPONSE}_{HEADER,COOKIE}.
    pinpoint.init(
        application_name="python-demo-django",
        agent_name="python-demo-django-1",
        server_info="Django",
        http_server_record_request_header=[
            "User-Agent", "Content-Type", "Accept", "Host",
            "X-Request-ID", "X-Forwarded-For",
        ],
        http_server_record_request_cookie=["sessionid", "csrftoken"],
        http_server_record_response_header=[
            "Content-Type", "X-Response-Time", "X-Request-ID",
        ],
    )
    autoload()

    # Django's runserver wants to be invoked via manage.py. Use the
    # equivalent management entry point with the same CLI.
    from django.core.management import execute_from_command_line

    argv = sys.argv[:1] + ["runserver", "--noreload",
                           os.environ.get("BIND", "0.0.0.0:8000")]
    execute_from_command_line(argv)


if __name__ == "__main__":
    main()
