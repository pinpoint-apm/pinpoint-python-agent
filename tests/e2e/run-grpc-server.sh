#!/usr/bin/env bash
# Launch the it_test gRPC backend in the foreground: source dev.sh,
# verify .venv + _native, install runtime deps idempotently, then exec the
# python server so Ctrl+C reaches it directly.
#
#     ./tests/e2e/run-grpc-server.sh
#     GRPC_BIND='[::]:60051' ./tests/e2e/run-grpc-server.sh   # custom port
#
# Pair with run-test-server.sh in a second terminal and drive both with
# fixed_rps_test.py or max_throughput_test.py.

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

demo_bootstrap wrapt grpcio

log "starting it_test grpc_server (GRPC_BIND=${GRPC_BIND:-[::]:50051})"
exec "$PY" -u "$REPO_ROOT/tests/e2e/grpc_server.py"
