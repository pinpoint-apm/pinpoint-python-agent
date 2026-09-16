#!/usr/bin/env bash
# Bring up the kafka-python demo — a producer (HTTP frontend) + consumer
# (standalone) pair against a single-node Apache Kafka broker in KRaft mode.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO="kafka"
DEMO_NAME="kafka-python demo"
BROKER="kafka"
PRODUCER_PORT=5005
CHANNEL="demo.kafka-python"
PIP_PKGS="wrapt flask kafka-python"
SAVE_NOTE="kafka.send event per request; pinpoint headers flow into kafka record headers"
broker_demo_main "$@"
