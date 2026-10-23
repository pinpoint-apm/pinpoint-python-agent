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

"""Celery web producer demo.

Mirrors examples/pika/producer_demo.py, but enqueues a Celery task instead
of publishing a raw AMQP message. Each ``/save`` request calls
``save.delay(msg)`` and replies with the task id and queue, so the trace
shows the Flask root span and the ``celery.app.task.Task.apply_async`` child
event under it.

Run with pinpoint-run:

    pinpoint-run --app-name demo-celery-producer --agent-name demo-celery-producer \
        --collector localhost -- python examples/celery/producer_demo.py

Or directly:

    python examples/celery/producer_demo.py

Hit it:

    curl -XPOST 'http://localhost:5009/save?msg=hello'

What this shows
---------------
- ``pinpoint.instrumentations.flask`` opens the root span on ``/save`` /
  ``/ping``.
- ``celery.app.task.Task.apply_async`` is wrapped so a span event nests
  under the Flask span, and the ``before_task_publish`` signal receiver
  writes the Pinpoint-* trace headers into the task *message headers*,
  with the routed queue as the event's destination. The worker
  (worker_demo.py) reads those headers off ``task.request`` and continues
  the trace.

Configurable:
    CELERY_BROKER_URL / RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/
    CELERY_QUEUE / RABBITMQ_QUEUE=demo.celery
    PORT=5009
"""

from __future__ import annotations

import os

from flask import Flask, jsonify, request

from demo_tasks import QUEUE, save

app = Flask("demo-celery-producer")


@app.get("/ping")
def ping():
    return jsonify(service="demo-celery-producer", ok=True)


@app.post("/save")
def save_endpoint():
    msg = request.args.get("msg", "hello")
    result = save.delay(msg)
    return jsonify(task="demo.celery.save", task_id=result.id, queue=QUEUE, message=msg)


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="demo-celery-producer",
        uid_version="v3",
        agent_name="demo-celery-producer-1",
        server_info="Celery Producer",
    )
    autoload()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5009")))


if __name__ == "__main__":
    main()
