#!/usr/bin/env bash
# Bring up the aio-pika demo — a producer (HTTP frontend) + consumer
# (standalone) pair against a RabbitMQ broker.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.
# The two rabbitmq demos share the ``pinpoint-demo-rabbitmq`` container — running
# one implicitly tears down the other's broker (distinct queue names, so no
# data conflict). The management UI is on :15672 (guest/guest).

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO="aio_pika"
BROKER="rabbit"
PRODUCER_PORT=5008
CHANNEL="demo.aio_pika"
PIP_PKGS="wrapt fastapi uvicorn[standard] aio-pika"
SAVE_NOTE="aio_pika.publish event per request; pinpoint headers flow into AMQP message headers"
broker_demo_main "$@"
