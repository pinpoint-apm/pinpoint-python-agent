#!/usr/bin/env bash
# Bring up aiohttp_demo against a local Elasticsearch container and exercise
# the aiohttp + elasticsearch (async transport) instrumentations.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.
# Elasticsearch is launched single-node with xpack security disabled so the
# AsyncElasticsearch client can connect without TLS / credentials.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO_NAME=aiohttp_demo
CONTAINER="pinpoint-demo-elasticsearch"
DEMO_PORT=8080
ES_PORT=9200
PIDFILES="aiohttp"
KILL_PORTS="$DEMO_PORT"
HIT_TIMEOUT=10

IMAGE="${ES_IMAGE:-docker.elastic.co/elasticsearch/elasticsearch:8.15.0}"
DOCKER_ARGS=(
    -e "discovery.type=single-node"
    -e "xpack.security.enabled=false"
    -e "ES_JAVA_OPTS=-Xms512m -Xmx512m"
    -p "$ES_PORT:9200"
)
WAIT_READY=wait_elasticsearch_ready
PIP_PKGS="wrapt aiohttp elasticsearch>=8,<9"
DEMO_SCRIPT=aiohttp/aiohttp_demo.py
DEMO_ENV="PORT=$DEMO_PORT PINPOINT_PY_HTTP_COLLECT_URL_STAT=true ELASTICSEARCH_URL=http://127.0.0.1:$ES_PORT"

wait_elasticsearch_ready() {
    # The TCP port opens before the cluster is queryable — gate on a real
    # HTTP round-trip that reports cluster health as yellow or green.
    local deadline=$((SECONDS + 180))
    until curl -sS -m 3 "http://127.0.0.1:$ES_PORT/_cluster/health" 2>/dev/null \
            | grep -Eq '"status":"(yellow|green)"'; do
        (( SECONDS > deadline )) && { warn "elasticsearch did not become ready within 180s"; return 1; }
        sleep 3
    done
}

test_endpoints() {
    echo
    hit "GET /ping"                                       "http://127.0.0.1:$DEMO_PORT/ping"
    hit "GET /items/42 (template /items/{item_id})"       "http://127.0.0.1:$DEMO_PORT/items/42"
    hit "GET /items/42/edit (template /items/{item_id}/{action})" \
                                                          "http://127.0.0.1:$DEMO_PORT/items/42/edit"
    hit "GET /elasticsearch (index + refresh + search via AsyncElasticsearch)" \
                                                          "http://127.0.0.1:$DEMO_PORT/elasticsearch"
    hit "GET /boom (root span set_error)"                 "http://127.0.0.1:$DEMO_PORT/boom"
}

demo_main "$@"
