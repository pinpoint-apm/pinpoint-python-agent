#!/usr/bin/env bash
# Bring up tornado_demo and exercise the Pinpoint tornado server
# instrumentation. No docker backend — the demo talks to nothing.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO_NAME=tornado_demo
DEMO_PORT=8888
PIDFILES="tornado"
KILL_PORTS="$DEMO_PORT"

PIP_PKGS="wrapt tornado"
DEMO_SCRIPT=tornado/tornado_demo.py
DEMO_ENV="PORT=$DEMO_PORT PINPOINT_PY_ENABLE_CALLSTACK_TRACE=true PINPOINT_PY_HTTP_COLLECT_URL_STAT=true"

test_endpoints() {
    echo
    hit "GET /ping"                        "http://127.0.0.1:$DEMO_PORT/ping"
    hit_items "$DEMO_PORT"
    hit "GET /boom (log_exception forwards the error onto the root span)" \
                                           "http://127.0.0.1:$DEMO_PORT/boom"
}

demo_main "$@"
