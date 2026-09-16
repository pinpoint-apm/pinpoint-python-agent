#!/usr/bin/env bash
# Bring up the aiokafka demo — a producer (HTTP frontend) + consumer
# (standalone) pair against a single-node Apache Kafka broker in KRaft mode.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO="aiokafka"
BROKER="kafka"
PRODUCER_PORT=5006
CHANNEL="demo.aiokafka"
PIP_PKGS="wrapt fastapi uvicorn[standard] aiokafka"
SAVE_NOTE="aiokafka.send event per request; pinpoint headers flow into kafka record headers"
broker_demo_main "$@"
