#!/usr/bin/env bash
# Bring up starlette_demo against a local Cassandra container and exercise
# the starlette + cassandra-driver instrumentations.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.
# Cassandra is slow to boot — readiness wait is generous (180s) by design.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO_NAME=starlette_demo
CONTAINER="pinpoint-demo-cassandra"
DEMO_PORT=8000
CASSANDRA_PORT=9042
PIDFILES="starlette"
KILL_PORTS="$DEMO_PORT"
HIT_TIMEOUT=10

IMAGE="cassandra:5"
DOCKER_ARGS=(-p "$CASSANDRA_PORT:9042")
WAIT_READY=wait_cassandra_ready
PIP_PKGS="wrapt starlette uvicorn cassandra-driver"
DEMO_SCRIPT=starlette/starlette_demo.py
DEMO_ENV="PORT=$DEMO_PORT PINPOINT_PY_HTTP_COLLECT_URL_STAT=true"

wait_cassandra_ready() {
    # Native protocol opens before the cluster is actually queryable — gate
    # on a real CQL round-trip via cqlsh inside the container.
    local deadline=$((SECONDS + 180))
    until docker exec "$CONTAINER" \
            cqlsh -e "SELECT now() FROM system.local" >/dev/null 2>&1; do
        (( SECONDS > deadline )) && { warn "cassandra did not become ready within 180s"; return 1; }
        sleep 3
    done
}

test_endpoints() {
    echo
    hit "GET /ping"                                       "http://127.0.0.1:$DEMO_PORT/ping"
    hit "GET /items/42 (template /items/{item_id:int})"   "http://127.0.0.1:$DEMO_PORT/items/42"
    hit "GET /items/42/edit (template /items/{item_id:int}/{action})" \
                                                          "http://127.0.0.1:$DEMO_PORT/items/42/edit"
    hit "GET /cassandra (INSERT + SELECT + execute_async SELECT)" \
                                                          "http://127.0.0.1:$DEMO_PORT/cassandra"
    hit "GET /boom (root span set_error)"                 "http://127.0.0.1:$DEMO_PORT/boom"
}

demo_main "$@"
