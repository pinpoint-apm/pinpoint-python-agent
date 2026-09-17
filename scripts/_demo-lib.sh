#!/usr/bin/env bash
# Shared plumbing for the examples/<demo>/run.sh drivers. Source it, don't run it.
#
# Contract for a run.sh script:
#
#     . "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"  # first thing after the header
#     DEMO_NAME="flask"            # used in the default down() message
#     PIDFILES="demo upstream"     # pid-file basenames under $RUN_DIR killed by down()
#     KILL_PORTS="5000 5001"       # listener ports killed by down()
#     CONTAINER="pinpoint-demo-x"  # docker container stopped by down(); leave unset if none
#     up() { down; demo_bootstrap <pip pkgs...>; ...; wait_agents_ready; test_endpoints; demo_running "<logs>"; }
#     test_endpoints() { ... }
#     demo_main "$@"               # last line — dispatches up|down|test
#
# down() has a var-driven default below; redefine it after sourcing if a
# script ever needs custom teardown. Per-container readiness waiters that
# only one script uses stay in that script. A custom up() must call
# wait_agents_ready before test_endpoints — _demo_up()/_broker_demo_up() do it
# for you, but nothing records a span until the agent's grpc handshake lands.

set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${PINPOINT_DEMO_RUN_DIR:-/tmp/pinpoint-demo}"
PY="$REPO_ROOT/.venv/bin/python"
PIP="$REPO_ROOT/.venv/bin/pip"

mkdir -p "$RUN_DIR"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!! \033[0m %s\n' "$*" >&2; }

kill_port() {
    local port=$1 pids
    pids="$(lsof -ti :"$port" 2>/dev/null || true)"
    [[ -n "$pids" ]] && kill $pids 2>/dev/null || true
}

# Default teardown, driven by $DEMO_NAME / $PIDFILES / $KILL_PORTS / $CONTAINER.
down() {
    log "stopping $DEMO_NAME processes"
    local kind port
    for kind in $PIDFILES; do
        if [[ -f "$RUN_DIR/$kind.pid" ]]; then
            kill "$(cat "$RUN_DIR/$kind.pid")" 2>/dev/null || true
            rm -f "$RUN_DIR/$kind.pid"
        fi
    done
    for port in $KILL_PORTS; do
        kill_port "$port"
    done
    if [[ -n "${CONTAINER:-}" ]]; then
        log "stopping $CONTAINER container"
        docker stop "$CONTAINER" >/dev/null 2>&1 || true
    fi
}

wait_tcp() {
    local host=$1 port=$2 name=$3 deadline=$((SECONDS + 60))
    until nc -z "$host" "$port" 2>/dev/null; do
        (( SECONDS > deadline )) && { warn "$name did not start within 60s"; return 1; }
        sleep 1
    done
}

# wait_log <pattern> [min_count] [timeout] — poll ``docker logs $CONTAINER``
# until PATTERN has matched at least MIN_COUNT times (default 1), or fail
# after TIMEOUT seconds (default 90).
wait_log() {
    local pattern=$1 min_count=${2:-1} timeout=${3:-90}
    local deadline=$((SECONDS + timeout)) count
    while :; do
        count="$(docker logs "$CONTAINER" 2>&1 | grep -c "$pattern" || true)"
        [[ "$count" -ge "$min_count" ]] && return 0
        (( SECONDS > deadline )) && { warn "$CONTAINER did not become ready within ${timeout}s"; return 1; }
        sleep 2
    done
}

# The pinpoint agent's grpc handshake runs in a background thread —
# ``agent.enabled`` flips True only after registration, and Flask /
# FastAPI instrumentations gate span creation on that flag. Polling
# the log lets us avoid sending requests during the dead window.
wait_agent_ready() {
    local log=$1 name=$2 deadline=$((SECONDS + 30))
    until grep -q "success to register the agent" "$log" 2>/dev/null; do
        (( SECONDS > deadline )) && { warn "$name agent did not register within 30s — continuing anyway"; return 0; }
        sleep 1
    done
}

# Every app in $PIDFILES runs its own agent, so all of them have to be
# registered before the smoke test starts — a request that lands in the dead
# window is served normally but recorded nowhere.
wait_agents_ready() {
    local kind
    for kind in $PIDFILES; do
        wait_agent_ready "$RUN_DIR/$kind.log" "$kind"
    done
}

# apache/kafka prints "Kafka Server started" when the broker is fully
# initialised; we also wait for the listener port to accept TCP.
# Uses $CONTAINER and $KAFKA_PORT.
wait_kafka_ready() {
    wait_log "Kafka Server started" || return 1
    wait_tcp 127.0.0.1 "$KAFKA_PORT" kafka
}

# Even after the AMQP port opens, the broker may still be applying its
# definitions. Gate on ``rabbitmqctl status`` succeeding inside the
# container, which doesn't return 0 until the broker is fully up.
# Uses $CONTAINER.
# 180s rather than 90: the pika/aio_pika pair share one container, so the
# second demo's broker boots on a machine still busy tearing the first one
# down. A boot that never finishes at all is start_container's business.
wait_rabbit_ready() {
    local timeout=180 deadline
    deadline=$((SECONDS + timeout))
    until docker exec "$CONTAINER" rabbitmqctl status >/dev/null 2>&1; do
        (( SECONDS > deadline )) && { warn "rabbitmq did not become ready within ${timeout}s"; return 1; }
        sleep 2
    done
}

# start_container <docker run args...> — `docker run -d --rm --name $CONTAINER`
# that fails loudly. When the previous container's port forwarder is still
# holding the port, the new container dies right after create and --rm deletes
# it; `docker run` can return 0 before that surfaces, and the readiness wait
# below then just sits there until it times out. One `docker ps` turns that
# into an immediate, accurate message.
start_container() {
    docker run -d --rm --name "$CONTAINER" "$@" >/dev/null
    sleep 1
    if ! docker ps --filter "name=$CONTAINER" --format '{{.Names}}' | grep -qx "$CONTAINER"; then
        warn "$CONTAINER exited right after start — port still held by a previous container?"
        exit 1
    fi
}

# Load the dev shell, sanity-check the demo venv + native artifact, then
# pip-install the given packages (idempotent).
demo_bootstrap() {
    log "loading dev shell env"
    # shellcheck source=dev.sh
    . "$REPO_ROOT/scripts/dev.sh"

    if [[ ! -x "$PY" ]]; then
        warn "missing $REPO_ROOT/.venv — see docs/development.md §3"
        exit 1
    fi
    if ! compgen -G "$REPO_ROOT/pinpoint/_native*.so" >/dev/null; then
        warn "missing pinpoint/_native*.so symlink — see docs/development.md §4"
        exit 1
    fi

    log "ensuring python deps (idempotent)"
    "$PIP" install --quiet "$@"
}

# start_demo <name> <script path under examples/> [ENV=val ...]
# Runs the example in the background; log → $RUN_DIR/<name>.log,
# pid → $RUN_DIR/<name>.pid.
start_demo() {
    local name=$1 script=$2
    shift 2
    # Export in a subshell rather than `env VAR=val "$PY"`: macOS SIP strips
    # DYLD_LIBRARY_PATH when exec'ing the protected /usr/bin/env, and the
    # in-place native extension then fails to dlopen its transitive dylibs.
    (
        if (($#)); then export "$@"; fi
        exec "$PY" -u "$REPO_ROOT/examples/$script"
    ) >"$RUN_DIR/$name.log" 2>&1 &
    echo $! >"$RUN_DIR/$name.pid"
}

# hit <label> <url> [timeout] — curl with the standard status-code trailer.
# Timeout defaults to $HIT_TIMEOUT seconds (default 5).
hit() {
    local label=$1 url=$2 timeout=${3:-${HIT_TIMEOUT:-5}}
    log "$label"
    curl -sS -m "$timeout" -w "\nHTTP %{http_code}\n" "$url"
    echo
}

# hit_items <port> — the /items url_stat probe block shared by the
# framework demos (buckets by route template on the server side).
hit_items() {
    local port=$1 id pair
    log "GET /items/<id> x3 + /items/<id>/<action> x2 (url_stat buckets by route template)"
    for id in 1 2 3; do
        curl -sS -m 5 -w "\nHTTP %{http_code}\n" "http://127.0.0.1:$port/items/$id"
    done
    for pair in "7/edit" "9/delete"; do
        curl -sS -m 5 -w "\nHTTP %{http_code}\n" "http://127.0.0.1:$port/items/$pair"
    done
    echo
}

# tail_consumer <name> — show the last messages of $RUN_DIR/<name>.log.
tail_consumer() {
    log "tail of consumer log (last messages):"
    sleep 2
    tail -n 5 "$RUN_DIR/$1.log" || true
}

# demo_running "<logs description>" — closing hint after a successful up.
demo_running() {
    log "stack running. logs: $1"
    log "stop with: $0 down"
}

demo_main() {
    case "${1:-up}" in
        up)   up ;;
        down) down ;;
        test) test_endpoints ;;
        *)    echo "usage: $0 [up|down|test]" >&2; exit 1 ;;
    esac
}

# ---------------------------------------------------------------------------
# Broker demos (kafka / rabbitmq producer+consumer pairs).
#
# The five broker demos (kafka, aiokafka, confluent_kafka, pika, aio_pika)
# differ only in configuration, so they share one up()/test_endpoints() pair.
# A run.sh sets, after sourcing this lib:
#
#     DEMO="kafka"                  # examples/<DEMO>/{producer,consumer}_demo.py
#                                   # and the <DEMO>-{producer,consumer} pidfiles
#     DEMO_NAME="kafka-python demo" # optional; defaults to "<DEMO> demo"
#     BROKER="kafka"                # kafka | rabbit — container + env wiring
#     PRODUCER_PORT=5005
#     CHANNEL="demo.kafka-python"   # kafka topic / rabbitmq queue name
#     PIP_PKGS="wrapt flask kafka-python"
#     SAVE_NOTE="kafka.send event per request; ..."   # test-run log line
#     broker_demo_main "$@"         # last line — dispatches up|down|test
# ---------------------------------------------------------------------------

KAFKA_IMAGE="apache/kafka:latest"
KAFKA_PORT=9092
RABBIT_IMAGE="rabbitmq:3-management"
RABBIT_PORT=5672
RABBIT_UI_PORT=15672

_broker_demo_up() {
    down
    # shellcheck disable=SC2086  # deliberate word splitting of the pkg list
    demo_bootstrap $PIP_PKGS

    # BROKER_ENV values never contain spaces (host:port, amqp URL, topic), so
    # a plain string keeps this bash-3.2-safe (macOS /bin/bash).
    local broker_env
    if [[ "$BROKER" == kafka ]]; then
        log "starting kafka broker ($CONTAINER, $KAFKA_IMAGE) on :$KAFKA_PORT"
        start_container -p "$KAFKA_PORT:9092" "$KAFKA_IMAGE"
        log "waiting for kafka to come online (~10s)"
        wait_kafka_ready
        broker_env="KAFKA_BOOTSTRAP=127.0.0.1:$KAFKA_PORT KAFKA_TOPIC=$CHANNEL"
    else
        log "starting rabbitmq ($CONTAINER, $RABBIT_IMAGE) on :$RABBIT_PORT (UI :$RABBIT_UI_PORT)"
        start_container -p "$RABBIT_PORT:5672" -p "$RABBIT_UI_PORT:15672" "$RABBIT_IMAGE"
        log "waiting for rabbitmq to come online"
        wait_tcp 127.0.0.1 "$RABBIT_PORT" rabbitmq
        wait_rabbit_ready
        broker_env="RABBITMQ_URL=amqp://guest:guest@127.0.0.1:$RABBIT_PORT/ RABBITMQ_QUEUE=$CHANNEL"
    fi

    log "starting $DEMO consumer (subscribed to $CHANNEL)"
    # shellcheck disable=SC2086
    start_demo "$DEMO-consumer" "$DEMO/consumer_demo.py" $broker_env

    log "starting $DEMO producer on :$PRODUCER_PORT"
    # shellcheck disable=SC2086
    start_demo "$DEMO-producer" "$DEMO/producer_demo.py" \
        PINPOINT_PY_ENABLE_CALLSTACK_TRACE=true \
        PINPOINT_PY_HTTP_COLLECT_URL_STAT=true \
        $broker_env \
        PORT="$PRODUCER_PORT"

    log "waiting for producer to come online"
    wait_tcp 127.0.0.1 "$PRODUCER_PORT" "$DEMO-producer"
    wait_agents_ready

    test_endpoints

    tail_consumer "$DEMO-consumer"

    demo_running "$RUN_DIR/{$DEMO-producer,$DEMO-consumer}.log"
}

_broker_demo_test() {
    echo
    log "GET /ping (producer sanity)"
    curl -sS -m 5 -w "\nHTTP %{http_code}\n" "http://127.0.0.1:$PRODUCER_PORT/ping"
    echo
    log "POST /save?msg=... x3 ($SAVE_NOTE)"
    local n
    for n in 1 2 3; do
        curl -sS -m 5 -X POST -w "\nHTTP %{http_code}\n" \
            "http://127.0.0.1:$PRODUCER_PORT/save?msg=hello-$n"
    done
    echo
}

broker_demo_main() {
    DEMO_NAME="${DEMO_NAME:-$DEMO demo}"
    PIDFILES="$DEMO-producer $DEMO-consumer"
    KILL_PORTS="$PRODUCER_PORT"
    if [[ "$BROKER" == kafka ]]; then
        CONTAINER="pinpoint-demo-kafka"
    else
        CONTAINER="pinpoint-demo-rabbitmq"
    fi
    up() { _broker_demo_up; }
    test_endpoints() { _broker_demo_test; }
    demo_main "$@"
}

# ---------------------------------------------------------------------------
# Single-app demos (one example app, optionally with one docker backend).
#
# A run.sh sets, after sourcing this lib and the standard DEMO_NAME /
# PIDFILES / KILL_PORTS / CONTAINER block:
#
#     IMAGE="mongo:7"                    # docker image for the backend; leave
#                                        # unset for a demo that needs none
#     DOCKER_ARGS=(-p "27017:27017")     # docker run args (ports, -e ...)
#     WAIT_READY=wait_mongo_ready        # container readiness function
#     PIP_PKGS="wrapt django pymongo"
#     DEMO_SCRIPT=django/django_demo.py  # path under examples/, run as $PIDFILES
#     DEMO_ENV="PORT=8000 ..."           # env for start_demo; values must not
#                                        # contain spaces (bash-3.2-safe string)
#     test_endpoints() { ... }
#     demo_main "$@"
#
# Demos that start more than one process (falcon) redefine start_app instead
# of setting DEMO_SCRIPT / DEMO_ENV. up() defaults to _demo_up below; a demo
# with extra setup (flask, grpc) redefines it after sourcing this lib.
# ---------------------------------------------------------------------------

up() { _demo_up; }

start_app() {
    log "starting $DEMO_SCRIPT on :$KILL_PORTS"
    # shellcheck disable=SC2086  # deliberate word splitting of DEMO_ENV
    start_demo "$PIDFILES" "$DEMO_SCRIPT" $DEMO_ENV
}

_demo_up() {
    down
    # shellcheck disable=SC2086  # deliberate word splitting of the pkg list
    demo_bootstrap $PIP_PKGS

    if [[ -n "${IMAGE:-}" ]]; then
        log "starting $IMAGE ($CONTAINER)"
        start_container "${DOCKER_ARGS[@]}" "$IMAGE"
        log "waiting for $CONTAINER to become ready"
        "$WAIT_READY"
    fi

    start_app

    local port kind logs=""
    for port in $KILL_PORTS; do
        wait_tcp 127.0.0.1 "$port" "$DEMO_NAME"
    done
    wait_agents_ready

    test_endpoints

    for kind in $PIDFILES; do logs+="$RUN_DIR/$kind.log "; done
    demo_running "${logs% }"
}
