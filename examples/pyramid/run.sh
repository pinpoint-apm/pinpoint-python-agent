#!/usr/bin/env bash
# Bring up pyramid_demo against a local memcached container and exercise
# the pyramid + pymemcache instrumentations.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO_NAME=pyramid_demo
CONTAINER="pinpoint-demo-memcached"
DEMO_PORT=6543
MEMCACHED_PORT=11211
PIDFILES="pyramid"
KILL_PORTS="$DEMO_PORT"

IMAGE="memcached:1.6-alpine"
DOCKER_ARGS=(-p "$MEMCACHED_PORT:11211")
WAIT_READY=wait_memcached_ready
PIP_PKGS="wrapt pyramid pymemcache"
DEMO_SCRIPT=pyramid/pyramid_demo.py
DEMO_ENV="PORT=$DEMO_PORT PINPOINT_PY_HTTP_COLLECT_URL_STAT=true"

wait_memcached_ready() {
    local deadline=$((SECONDS + 30))
    until printf 'version\r\n' | nc -w 1 127.0.0.1 "$MEMCACHED_PORT" 2>/dev/null | grep -q '^VERSION'; do
        (( SECONDS > deadline )) && { warn "memcached did not become ready within 30s"; return 1; }
        sleep 1
    done
}

test_endpoints() {
    echo
    hit "GET /ping"                                       "http://127.0.0.1:$DEMO_PORT/ping"
    hit "GET /items/42 (template /items/{item_id})"       "http://127.0.0.1:$DEMO_PORT/items/42"
    hit "GET /items/42/edit (template /items/{item_id}/{action})" \
                                                          "http://127.0.0.1:$DEMO_PORT/items/42/edit"
    hit "GET /cache (set/get/add/incr/set_many/get_many/delete)" \
                                                          "http://127.0.0.1:$DEMO_PORT/cache"
    hit "GET /boom (root span set_error)"                 "http://127.0.0.1:$DEMO_PORT/boom"
}

demo_main "$@"
