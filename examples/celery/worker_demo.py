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

"""Celery worker demo.

Consumes ``demo.celery`` and executes ``demo.celery.save`` for each task
message. Pair with producer_demo.py to see the distributed trace continue
from the producer's Flask span into a per-task ``Celery Worker Invocation``
root span on this side.

Run with pinpoint-run:

    pinpoint-run --app-name demo-celery-worker --agent-name demo-celery-worker \
        --collector localhost -- python examples/celery/worker_demo.py

Or directly:

    python examples/celery/worker_demo.py
    CELERY_POOL=prefork python examples/celery/worker_demo.py

What this shows
---------------
- ``task_prerun`` opens a Pinpoint root span per task execution, seeded
  from the Pinpoint-* headers the producer wrote into the message (they
  arrive as attributes of ``task.request``); ``task_postrun`` ends it. The
  whole body of ``save`` runs inside that span.
- The demo runs the ``solo`` pool (one task at a time, no child
  processes), so the plain ``init()`` applies. ``CELERY_POOL=prefork``
  switches to Celery's default pool, which forks its children from this
  master; an agent started before a fork does not survive it, so that path
  initializes with ``prefork=True``: the master makes no native call and
  each child starts its own agent from the after-fork hook (see
  docs/prefork.md). Note that billiard *spawns* rather than forks on macOS,
  where Celery's own prefork pool does not work from a script like this one;
  use the prefork variant on Linux.

Configurable:
    CELERY_BROKER_URL / RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/
    CELERY_QUEUE / RABBITMQ_QUEUE=demo.celery
    CELERY_POOL=solo
"""

from __future__ import annotations

import os

from demo_tasks import QUEUE, app

POOL = os.environ.get("CELERY_POOL", "solo")


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="demo-celery-worker",
        uid_version="v3",
        agent_name="demo-celery-worker-1",
        server_info="Celery Worker",
        prefork=(POOL == "prefork"),
    )
    autoload()
    print(f"celery worker consuming {QUEUE!r} with the {POOL} pool", flush=True)
    app.worker_main([
        "worker", "--loglevel=INFO", f"--pool={POOL}", "--concurrency=1",
        "-Q", QUEUE, "--hostname=demo-celery-worker@%h",
        "--without-gossip", "--without-mingle", "--without-heartbeat",
    ])


if __name__ == "__main__":
    main()
