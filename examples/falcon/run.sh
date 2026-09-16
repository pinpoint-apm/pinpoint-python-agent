#!/usr/bin/env bash
# Bring up falcon_demo (WSGI + ASGI variants) against a local Redis
# container and exercise the falcon + redis instrumentations.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO_NAME=falcon
CONTAINER="pinpoint-demo-redis"
WSGI_PORT=8000
ASGI_PORT=8001
REDIS_PORT=6379
PIDFILES="falcon-wsgi falcon-asgi"
KILL_PORTS="$WSGI_PORT $ASGI_PORT"

IMAGE="redis:7-alpine"
DOCKER_ARGS=(-p "$REDIS_PORT:6379")
WAIT_READY=wait_redis_ready
PIP_PKGS="wrapt falcon uvicorn redis"

wait_redis_ready() {
    local deadline=$((SECONDS + 30))
    until docker exec "$CONTAINER" redis-cli ping 2>/dev/null | grep -q PONG; do
        (( SECONDS > deadline )) && { warn "redis did not become ready within 30s"; return 1; }
        sleep 1
    done
}

# Two processes, so override the lib's single-app default.
start_app() {
    log "starting falcon_demo (WSGI) on :$WSGI_PORT"
    start_demo falcon-wsgi falcon/falcon_demo.py \
        PORT="$WSGI_PORT" \
        PINPOINT_PY_HTTP_COLLECT_URL_STAT=true

    log "starting falcon_demo (ASGI) on :$ASGI_PORT"
    start_demo falcon-asgi falcon/falcon_demo.py \
        PORT="$ASGI_PORT" \
        FALCON_ASGI=1 \
        PINPOINT_PY_HTTP_COLLECT_URL_STAT=true
}

test_endpoints() {
    echo
    for variant in "wsgi:$WSGI_PORT" "asgi:$ASGI_PORT"; do
        local kind="${variant%%:*}" port="${variant##*:}"
        local kind_upper
        kind_upper="$(printf '%s' "$kind" | tr '[:lower:]' '[:upper:]')"
        log "--- falcon $kind_upper on :$port ---"
        hit "GET /ping ($kind)"              "http://127.0.0.1:$port/ping"
        hit "GET /items/42 ($kind, template /items/{item_id:int})" \
                                              "http://127.0.0.1:$port/items/42"
        hit "GET /items/42/edit ($kind, template /items/{item_id:int}/{action})" \
                                              "http://127.0.0.1:$port/items/42/edit"
        hit "GET /redis ($kind, SET+GET+HSET+HGETALL+pipeline)" \
                                              "http://127.0.0.1:$port/redis"
        hit "GET /boom ($kind, root span set_error)" \
                                              "http://127.0.0.1:$port/boom"
    done
}

demo_main "$@"
