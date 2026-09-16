#!/usr/bin/env bash
# Bring up asgi_demo and exercise PinpointASGIMiddleware wrapped around a
# bare ASGI app (served by uvicorn). No docker backend, no framework.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO_NAME=asgi_demo
DEMO_PORT=8000
PIDFILES="asgi"
KILL_PORTS="$DEMO_PORT"

PIP_PKGS="wrapt uvicorn[standard]"
DEMO_SCRIPT=asgi/asgi_demo.py
DEMO_ENV="PORT=$DEMO_PORT PINPOINT_PY_ENABLE_CALLSTACK_TRACE=true PINPOINT_PY_HTTP_COLLECT_URL_STAT=true"

test_endpoints() {
    echo
    hit "GET /ping"                        "http://127.0.0.1:$DEMO_PORT/ping"
    hit_items "$DEMO_PORT"
    hit "GET /boom (middleware set_error, then re-raise for the ASGI server)" \
                                           "http://127.0.0.1:$DEMO_PORT/boom"
}

demo_main "$@"
