#!/usr/bin/env bash
# Bring up the confluent-kafka demo — a producer (HTTP frontend) + consumer
# (standalone) pair against a single-node Apache Kafka broker in KRaft mode.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO="confluent_kafka"
DEMO_NAME="confluent-kafka demo"
BROKER="kafka"
PRODUCER_PORT=5007
CHANNEL="demo.confluent-kafka"
PIP_PKGS="wrapt flask confluent-kafka"
SAVE_NOTE="confluent_kafka.produce event per request; pinpoint headers flow into kafka record headers"
broker_demo_main "$@"
