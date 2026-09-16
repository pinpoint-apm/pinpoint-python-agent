#!/usr/bin/env bash
# Runs bench_api.py with the in-place import environment set up (dev.sh, sourced
# without a preset so it never re-points pinpoint/ under you — bench_api.py
# asserts a release build instead). bench_api.py strips PINPOINT_PY_* itself, so
# dev.sh's defaults (INFO logging, remote collector host) cannot reach the
# native agent.
#
#   benchmark/api_overhead/run.sh --repeats 5
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")/../.." && pwd)"

# shellcheck source=/dev/null
source "$root/scripts/dev.sh"

exec "$root/.venv/bin/python" -u "$root/benchmark/api_overhead/bench_api.py" "$@"
