#!/usr/bin/env bash
# Wrapper for compare_overhead.py — sources dev.sh, verifies .venv
# and _native, installs runtime deps (incl. psutil), then forwards args.
#
#     ./tests/e2e/run-compare.sh                          # defaults
#     ./tests/e2e/run-compare.sh -m mixed -d 30 -c 8
#     ./tests/e2e/run-compare.sh -m grpc-all -d 60 -c 4

. "$(dirname -- "${BASH_SOURCE[0]}")/../../scripts/_demo-lib.sh"

demo_bootstrap wrapt grpcio fastapi 'uvicorn[standard]' psutil snakeviz

exec "$PY" -u "$REPO_ROOT/tests/e2e/compare_overhead.py" "$@"
