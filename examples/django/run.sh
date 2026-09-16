#!/usr/bin/env bash
# Bring up django_demo + MongoDB and exercise the pymongo instrumentation
# alongside the existing Django request / url_stat paths.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO_NAME=django
CONTAINER="pinpoint-demo-mongo"
DEMO_PORT=8000
MONGO_PORT=27017
PIDFILES="django"
KILL_PORTS="$DEMO_PORT"

IMAGE="mongo:7"
DOCKER_ARGS=(-p "$MONGO_PORT:27017")
WAIT_READY=wait_mongo_ready
PIP_PKGS="wrapt django pymongo"
DEMO_SCRIPT=django/django_demo.py
DEMO_ENV="PINPOINT_PY_ENABLE_CALLSTACK_TRACE=true PINPOINT_PY_HTTP_COLLECT_URL_STAT=true BIND=0.0.0.0:$DEMO_PORT"

# mongo:7 logs `"msg":"Waiting for connections"` once the listener is
# accepting traffic. Match on that rather than a TCP probe alone so a
# half-initialized server doesn't get hit before it can answer commands.
wait_mongo_ready() { wait_log "Waiting for connections" 1 60; }

test_endpoints() {
    echo
    hit "GET /ping (django_demo)"                       "http://127.0.0.1:$DEMO_PORT/ping"
    hit "GET /db/mongo (pymongo ping + buildInfo)"      "http://127.0.0.1:$DEMO_PORT/db/mongo" 10
    hit "GET /mongo/crud (pymongo insert/find/update/delete)" \
                                                        "http://127.0.0.1:$DEMO_PORT/mongo/crud" 10
    hit_items "$DEMO_PORT"
    hit "GET /boom (django view raises; agent annotates the root span)" \
                                                        "http://127.0.0.1:$DEMO_PORT/boom"
}

demo_main "$@"
