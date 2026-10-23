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

"""The Celery app + task shared by producer_demo.py and worker_demo.py.

Both sides import this module, as a real deployment shares its ``tasks``
module, so the producer calls ``save.delay(...)`` on the very task object the
worker executes.

Configurable:
    CELERY_BROKER_URL=amqp://guest:guest@127.0.0.1:5672/   (RABBITMQ_URL also honoured)
    CELERY_QUEUE=demo.celery                                (RABBITMQ_QUEUE also honoured)
"""

from __future__ import annotations

import os

from celery import Celery

BROKER_URL = (
    os.environ.get("CELERY_BROKER_URL")
    or os.environ.get("RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")
)
QUEUE = os.environ.get("CELERY_QUEUE") or os.environ.get("RABBITMQ_QUEUE", "demo.celery")

app = Celery("demo-celery", broker=BROKER_URL)
app.conf.update(
    task_default_queue=QUEUE,
    task_ignore_result=True,
    broker_connection_retry_on_startup=True,
)


@app.task(name="demo.celery.save")
def save(msg: str) -> int:
    """Runs on the worker, *inside* the Pinpoint span the agent opened."""
    print(f"[task] demo.celery.save msg={msg!r}", flush=True)
    return len(msg)
