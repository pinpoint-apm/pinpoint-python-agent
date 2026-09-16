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

"""Minimal Tornado demo — exercises the Pinpoint ``tornado`` server
instrumentation.

Run with pinpoint-run:

    pinpoint-run --app-name demo-tornado --agent-name demo-tornado \
        --collector localhost -- python examples/tornado/tornado_demo.py

Or directly:

    python examples/tornado/tornado_demo.py

Hit it:

    curl http://localhost:8888/ping
    curl http://localhost:8888/items/42
    curl http://localhost:8888/items/42/edit
    curl http://localhost:8888/boom

What this shows
---------------
- ``tornado.web.RequestHandler._execute`` is wrapped so every request
  opens a Pinpoint root span around the full handler lifecycle
  (``prepare`` → ``get/post/...`` → ``finish``).
- The agent emits a Python-method span event named after the resolved
  handler method — for example, ``ItemsHandler.get`` — so the Pinpoint
  UI shows the user's handler rather than just the URL.
- ``log_exception`` is wrapped to forward ``HTTPError`` and uncaught
  exceptions onto the active span via ``set_error``.
"""

from __future__ import annotations

import os

import tornado.ioloop
import tornado.web


class PingHandler(tornado.web.RequestHandler):
    def get(self):
        self.write({"service": "demo-tornado", "ok": True})


class ItemsHandler(tornado.web.RequestHandler):
    def get(self, item_id: str, action: str = "view"):
        self.write({"item_id": int(item_id), "action": action})


class BoomHandler(tornado.web.RequestHandler):
    def get(self):
        # log_exception fires for this — the span gets set_error'd.
        raise RuntimeError("simulated tornado failure")


def build_app() -> tornado.web.Application:
    return tornado.web.Application(
        [
            (r"/ping", PingHandler),
            (r"/items/([0-9]+)", ItemsHandler),
            (r"/items/([0-9]+)/([a-zA-Z]+)", ItemsHandler),
            (r"/boom", BoomHandler),
        ]
    )


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="python-demo-tornado",
        agent_name="python-demo-tornado-1",
        server_info="Tornado",
        # See pinpoint-cpp-agent/test/it_test/pinpoint-config.yaml for the
        # YAML keys; equivalent env vars are PINPOINT_PY_HTTP_{SERVER,CLIENT}_RECORD_*.
        http_server_record_request_header=[
            "User-Agent", "Content-Type", "Accept", "Host",
            "X-Request-ID", "X-Forwarded-For",
        ],
        http_server_record_request_cookie=["session_id", "token"],
        http_server_record_response_header=[
            "Content-Type", "X-Response-Time", "X-Request-ID",
        ],
    )
    autoload()

    app = build_app()
    app.listen(int(os.environ.get("PORT", "8888")), address="0.0.0.0")
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
