#!/usr/bin/env bash
# Run the instrumentation integration suite (testcontainers).
#
#   tests/integration/run.sh              # parallel: one xdist worker per module
#   tests/integration/run.sh -n 0        # serial
#   tests/integration/run.sh -k redis    # any extra pytest args pass through
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

# shellcheck source=/dev/null
source scripts/dev.sh

python=".venv/bin/python"
[[ -x "$python" ]] || python="python3"

# A later -n (``-n 0`` for serial) overrides the default.
exec "$python" -m pytest tests/integration -q -n auto --dist loadfile "$@"
