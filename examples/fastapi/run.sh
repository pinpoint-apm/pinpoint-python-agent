#!/usr/bin/env bash
# Bring up the fastapi_demo + Postgres stack and exercise every
# PostgreSQL-client instrumentation (asyncpg, psycopg3, psycopg2, aiopg)
# along with the FastAPI/Starlette routing and url_stat paths.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO_NAME=fastapi
CONTAINER="pinpoint-demo-postgres"
DEMO_PORT=8000
PG_PORT=5432
PIDFILES="fastapi"
KILL_PORTS="$DEMO_PORT"

IMAGE="postgres:16"
DOCKER_ARGS=(-e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=demo -p "$PG_PORT:5432")
WAIT_READY=wait_postgres_ready
PIP_PKGS="wrapt fastapi uvicorn[standard] asyncpg psycopg[binary] psycopg2-binary aiopg"
DEMO_SCRIPT=fastapi/fastapi_demo.py
DEMO_ENV="PINPOINT_PY_SQL_ENABLE_SQL_STATS=true PINPOINT_PY_ENABLE_CALLSTACK_TRACE=true PINPOINT_PY_HTTP_COLLECT_URL_STAT=true PG_HOST=127.0.0.1 PG_PORT=$PG_PORT PG_USER=postgres PG_PASSWORD=postgres PG_DATABASE=demo"

# The postgres image runs initdb against a temporary local-socket-only
# instance before restarting with TCP listening enabled. Both phases log
# "database system is ready to accept connections", so we need to see the
# message twice before we know the externally reachable listener is up.
wait_postgres_ready() { wait_log 'database system is ready to accept connections' 2; }

test_endpoints() {
    echo
    hit "GET /ping (sanity)"                            "http://127.0.0.1:$DEMO_PORT/ping"
    hit "GET /db/asyncpg (native async postgres)"       "http://127.0.0.1:$DEMO_PORT/db/asyncpg" 10
    hit "GET /db/psycopg (psycopg3 async cursor)"       "http://127.0.0.1:$DEMO_PORT/db/psycopg" 10
    hit "GET /db/psycopg2 (psycopg2 sync, bridged via to_thread)" \
                                                        "http://127.0.0.1:$DEMO_PORT/db/psycopg2" 10
    hit "GET /db/aiopg (aiopg async wrapper around psycopg2)" \
                                                        "http://127.0.0.1:$DEMO_PORT/db/aiopg" 10
    hit "GET /db/crud (psycopg3 async; INSERT/SELECT/UPDATE/DELETE)" \
                                                        "http://127.0.0.1:$DEMO_PORT/db/crud" 10
    hit "GET /boom (root span failure path)"            "http://127.0.0.1:$DEMO_PORT/boom"
    hit_items "$DEMO_PORT"
    hit "GET /users/<name> (class-based router; span event shows UserAPI.get)" \
                                                        "http://127.0.0.1:$DEMO_PORT/users/alice"
}

demo_main "$@"
