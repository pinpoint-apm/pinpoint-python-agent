#!/usr/bin/env bash
# Launch the it_test HTTP frontend in the foreground: source dev.sh,
# verify .venv + _native, install runtime deps idempotently, then exec the
# python server so Ctrl+C reaches it directly.
#
#     ./tests/e2e/run-test-server.sh
#     PORT=9090 GRPC_TARGET=localhost:60051 ./tests/e2e/run-test-server.sh
#
# Pass --real-db to route /db-* through pymysql against a throwaway docker
# MySQL container (started on demand, reused if already running):
#
#     ./tests/e2e/run-test-server.sh --real-db
#
# The handlers pad themselves with a fixed sleep unit only when
# PINPOINT_E2E_SYNTHETIC_SLEEP_MS is set (=10 restores the old 10ms unit);
# leave it unset when measuring throughput.
#
# Pair with run-grpc-server.sh in a second terminal and drive both with
# fixed_rps_test.py or max_throughput_test.py.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

REAL_DB=0
while [[ $# -gt 0 ]]; do
    case $1 in
        --real-db)         REAL_DB=1; shift ;;
        -h|--help)
            sed -n '2,19p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) warn "unknown option: $1"; exit 1 ;;
    esac
done

PIP_DEPS=(wrapt grpcio fastapi 'uvicorn[standard]')

MYSQL_CONTAINER="${MYSQL_CONTAINER:-pinpoint-it-test-mysql}"
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-root}"
MYSQL_DATABASE="${MYSQL_DATABASE:-it_test}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8}"

start_mysql_container() {
    if ! command -v docker >/dev/null 2>&1; then
        warn "docker is required for --real-db (install Docker Desktop / engine)"
        exit 1
    fi

    if docker ps --format '{{.Names}}' | grep -qx "$MYSQL_CONTAINER"; then
        log "reusing running mysql container '$MYSQL_CONTAINER'"
    elif docker ps -a --format '{{.Names}}' | grep -qx "$MYSQL_CONTAINER"; then
        log "starting existing mysql container '$MYSQL_CONTAINER'"
        docker start "$MYSQL_CONTAINER" >/dev/null
    else
        log "launching mysql container '$MYSQL_CONTAINER' ($MYSQL_IMAGE) on :$MYSQL_PORT"
        docker run -d --rm \
            --name "$MYSQL_CONTAINER" \
            -p "${MYSQL_PORT}:3306" \
            -e MYSQL_ROOT_PASSWORD="$MYSQL_PASSWORD" \
            -e MYSQL_DATABASE="$MYSQL_DATABASE" \
            "$MYSQL_IMAGE" >/dev/null
    fi

    log "waiting for mysql to accept connections on ${MYSQL_HOST}:${MYSQL_PORT}"
    local i
    for i in $(seq 1 60); do
        if docker exec "$MYSQL_CONTAINER" \
                mysqladmin ping -h 127.0.0.1 -uroot \
                -p"$MYSQL_PASSWORD" --silent >/dev/null 2>&1; then
            log "mysql is ready (${i}s)"
            return
        fi
        sleep 1
    done
    warn "mysql did not become ready within 60s; check 'docker logs $MYSQL_CONTAINER'"
    exit 1
}

if [[ "$REAL_DB" -eq 1 ]]; then
    PIP_DEPS+=(pymysql)
    start_mysql_container
    export PINPOINT_IT_REAL_DB=true
    export MYSQL_HOST MYSQL_PORT MYSQL_USER MYSQL_PASSWORD MYSQL_DATABASE
fi

demo_bootstrap "${PIP_DEPS[@]}"

if [[ "$REAL_DB" -eq 1 ]]; then
    log "starting it_test_server with real-db (mysql ${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE})"
else
    log "starting it_test_server (PORT=${PORT:-8090} GRPC_TARGET=${GRPC_TARGET:-localhost:50051})"
fi
exec "$PY" -u "$REPO_ROOT/tests/e2e/test_server.py"
