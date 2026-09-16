#!/usr/bin/env bash
# Bring up wsgi_demo and exercise PinpointWSGIMiddleware wrapped around a
# bare WSGI app (served by wsgiref). No docker backend, no framework.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO_NAME=wsgi_demo
DEMO_PORT=8000
PIDFILES="wsgi"
KILL_PORTS="$DEMO_PORT"

PIP_PKGS="wrapt"
DEMO_SCRIPT=wsgi/wsgi_demo.py
DEMO_ENV="PORT=$DEMO_PORT PINPOINT_PY_ENABLE_CALLSTACK_TRACE=true PINPOINT_PY_HTTP_COLLECT_URL_STAT=true"

test_endpoints() {
    echo
    hit "GET /ping"                        "http://127.0.0.1:$DEMO_PORT/ping"
    hit_items "$DEMO_PORT"
    hit "GET /boom (middleware set_error, then re-raise for the WSGI server)" \
                                           "http://127.0.0.1:$DEMO_PORT/boom"
}

demo_main "$@"
