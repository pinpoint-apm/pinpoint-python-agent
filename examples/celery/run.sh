#!/usr/bin/env bash
# Bring up the Celery demo — a producer (HTTP frontend) + worker pair against
# a RabbitMQ broker.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.
# The rabbitmq demos share the ``pinpoint-demo-rabbitmq`` container — running
# one implicitly tears down the others' broker (distinct queue names, so no
# data conflict). The management UI is on :15672 (guest/guest).
# The worker runs the solo pool; CELERY_POOL=prefork (Linux) exercises the
# agent's after-fork start-up instead.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO="celery"
BROKER="rabbit"
PRODUCER_PORT=5009
CHANNEL="demo.celery"
CONSUMER_SCRIPT="celery/worker_demo.py"
PIP_PKGS="wrapt flask celery"
SAVE_NOTE="apply_async event per request; pinpoint headers flow into the task message headers"
broker_demo_main "$@"
