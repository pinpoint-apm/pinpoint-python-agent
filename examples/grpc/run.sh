#!/usr/bin/env bash
# Bring up the grpc_server + grpc_client (Tornado front-end) demo stack and
# exercise every gRPC pattern the instrumentation traces (unary-unary,
# unary-stream, stream-unary, stream-stream) end to end.
# Usage (up|test|down), logs and prerequisites: see examples/README.md.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

DEMO_NAME=grpc
CLIENT_PORT=8888
GRPC_PORT=50051
PIDFILES="grpc-client grpc-server"
KILL_PORTS="$CLIENT_PORT $GRPC_PORT"
# no docker container for this demo

up() {
    down
    demo_bootstrap wrapt grpcio tornado

    log "starting grpc_server on :$GRPC_PORT"
    start_demo grpc-server grpc/grpc_server.py

    log "starting grpc_client (tornado) on :$CLIENT_PORT (callstack trace + url stats on)"
    start_demo grpc-client grpc/grpc_client.py \
        PINPOINT_PY_ENABLE_CALLSTACK_TRACE=true \
        PINPOINT_PY_HTTP_COLLECT_URL_STAT=true \
        GRPC_TARGET="127.0.0.1:$GRPC_PORT" \
        PORT="$CLIENT_PORT"

    log "waiting for services to come online"
    wait_tcp 127.0.0.1 "$GRPC_PORT"   grpc_server
    wait_tcp 127.0.0.1 "$CLIENT_PORT" grpc_client
    wait_agents_ready

    test_endpoints

    demo_running "$RUN_DIR/{grpc-server,grpc-client}.log"
}

test_endpoints() {
    echo
    hit "GET /unary (unary-unary; client event nests under tornado span, server span on the other end)" \
        "http://127.0.0.1:$CLIENT_PORT/unary"
    hit "GET /server-stream (unary-stream; event lifetime tracks the response iterator)" \
        "http://127.0.0.1:$CLIENT_PORT/server-stream"
    hit "GET /client-stream (stream-unary; request iterator drained inside the client event)" \
        "http://127.0.0.1:$CLIENT_PORT/client-stream"
    hit "GET /bidi (stream-stream; event lifetime tracks the full bidi exchange)" \
        "http://127.0.0.1:$CLIENT_PORT/bidi"
}

demo_main "$@"
