#!/usr/bin/env bash
# Bring up the full flask_demo + flask_upstream + MySQL stack and exercise
# every MySQL-client instrumentation (pymysql, mysql-connector-python,
# mysqlclient, aiomysql) plus the distributed-trace path.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.
# Optional: ``mysqlclient`` needs system MySQL headers (``brew install mysql-client``
# + MYSQLCLIENT_CFLAGS/LDFLAGS); the script tries to install it best-effort and
# the /db/mysqlclient endpoint returns 503 if the import isn't available.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO_NAME=flask
CONTAINER="pinpoint-demo-mysql"
DEMO_PORT=5000
UPSTREAM_PORT=5001
MYSQL_PORT=3306
PIDFILES="demo upstream"
KILL_PORTS="$DEMO_PORT $UPSTREAM_PORT"

up() {
    down
    demo_bootstrap \
        wrapt flask requests cryptography \
        pymysql mysql-connector-python aiomysql

    # mysqlclient needs system MySQL headers — best-effort, skip on failure
    # so the rest of the demo still runs. The /db/mysqlclient endpoint will
    # return 503 if MySQLdb isn't importable.
    if ! "$PY" -c "import MySQLdb" 2>/dev/null; then
        # `brew --prefix <formula>` prints a path and exits 0 even when the
        # formula isn't installed, so test the directory rather than the exit
        # status — otherwise the caller's own PKG_CONFIG_PATH is replaced by a
        # path that doesn't exist.
        local pc_path="${PKG_CONFIG_PATH:-}"
        if command -v brew >/dev/null 2>&1; then
            local brew_pc
            brew_pc="$(brew --prefix mysql-client 2>/dev/null)/lib/pkgconfig"
            [[ -d "$brew_pc" ]] && pc_path="$brew_pc${pc_path:+:$pc_path}"
        fi
        PKG_CONFIG_PATH="$pc_path" \
            "$PIP" install --quiet mysqlclient \
            || warn "mysqlclient install failed (need MySQL dev headers); skipping that path"
    fi

    log "starting mysql:8 ($CONTAINER on :$MYSQL_PORT)"
    start_container \
        -e MYSQL_ROOT_PASSWORD=root \
        -e MYSQL_DATABASE=demo \
        -p "$MYSQL_PORT:3306" \
        mysql:8

    log "starting flask_upstream on :$UPSTREAM_PORT"
    start_demo upstream flask/flask_upstream.py

    log "starting flask_demo on :$DEMO_PORT (sql stats + callstack trace + url stats on, trim_path off)"
    start_demo demo flask/flask_demo.py \
        PINPOINT_PY_SQL_ENABLE_SQL_STATS=true \
        PINPOINT_PY_ENABLE_CALLSTACK_TRACE=true \
        PINPOINT_PY_HTTP_COLLECT_URL_STAT=true

    log "waiting for services to come online"
    # 3306[^0-9]: the X Plugin announces "port: 33060" moments earlier, and a
    # bare "port: 3306" matches that substring too.
    wait_log "ready for connections.*port: 3306[^0-9]"
    wait_tcp 127.0.0.1 "$UPSTREAM_PORT" flask_upstream
    wait_tcp 127.0.0.1 "$DEMO_PORT"     flask_demo
    wait_agents_ready

    test_endpoints

    demo_running "$RUN_DIR/{demo,upstream}.log"
}

test_endpoints() {
    echo
    hit "GET /ping (flask_demo -> flask_upstream)"      "http://127.0.0.1:$DEMO_PORT/ping"
    hit "GET /db (flask_demo -> mysql via pymysql)"     "http://127.0.0.1:$DEMO_PORT/db" 10
    hit "GET /db/connector (mysql-connector-python)"    "http://127.0.0.1:$DEMO_PORT/db/connector" 10
    hit "GET /db/mysqlclient (mysqlclient / MySQLdb — 503 if not installed)" \
                                                        "http://127.0.0.1:$DEMO_PORT/db/mysqlclient" 10
    hit "GET /db/aiomysql (aiomysql; async query bridged from sync handler)" \
                                                        "http://127.0.0.1:$DEMO_PORT/db/aiomysql" 10
    hit "GET /crud (flask_demo -> mysql via pymysql, INSERT/SELECT/UPDATE/DELETE)" \
                                                        "http://127.0.0.1:$DEMO_PORT/crud" 10
    hit "GET /boom (nested traceback exercising callstack capture)" \
                                                        "http://127.0.0.1:$DEMO_PORT/boom"
    hit_items "$DEMO_PORT"
    hit "GET /async/thread (run_in_thread hand-off; async sub-traces under the request)" \
                                                        "http://127.0.0.1:$DEMO_PORT/async/thread" 10
    hit "GET /async/task (create_task hand-off; asyncio sub-traces under the request)" \
                                                        "http://127.0.0.1:$DEMO_PORT/async/task" 10
    hit "GET /decorated (@spanevent-decorated helpers under the Flask span)" \
                                                        "http://127.0.0.1:$DEMO_PORT/decorated"
    hit "GET /decorated/job (@span-decorated function — separate top-level transaction)" \
                                                        "http://127.0.0.1:$DEMO_PORT/decorated/job"
}

demo_main "$@"
