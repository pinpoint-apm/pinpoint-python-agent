#!/usr/bin/env bash
# Dev environment for the in-place workflow — no `pip install`, `pinpoint` is
# imported straight from the source tree (docs/development.md §4-§5). Picks a
# native build preset, wires its artifacts into pinpoint/, exports what
# `_native` needs at dlopen time, and activates .venv so `python`/`pytest`/`pip`
# are the venv's.
#
#   source scripts/dev.sh debug     # wire build/debug, then: python -m pytest tests/unit
#   source scripts/dev.sh           # env + .venv only, current wiring left alone
#   scripts/dev.sh release          # wire only (benchmarks must run against release)
#   scripts/dev.sh debug python -m pytest tests/unit -v
#   scripts/dev.sh debug python -u examples/flask/flask_demo.py
#
# Executed, $1 is always the preset. Sourced, it is optional — which keeps
# `source scripts/dev.sh` a drop-in for the run-*.sh wrappers that only want the
# environment and must not have the wiring changed under them. A sourcing
# script's own arguments are inherited here, so only a name CMakePresets.json
# defines is treated as a preset.
#
# Sourcing works from zsh too, so nothing here may rely on bash-only expansion
# (no ${!indirect}, no globs held in variables — find does that globbing).

_pp_dev_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")/.." && pwd)"

# Drop empty and repeated entries from a colon-separated search path, keeping
# the first occurrence of each. Two reasons: ld.so reads a zero-length entry as
# the current working directory, and the per-tree scans below each end with a
# separator; and re-sourcing to switch presets — the documented workflow — would
# otherwise stack another copy of every directory on every source.
# awk rather than an IFS loop: this file is sourced from zsh too, which does not
# word-split unquoted expansions.
_pp_dev_pathclean() {
    printf '%s' "$1" |
        awk -v RS=: '$0 != "" && !seen[$0]++ { printf "%s%s", (n++ ? ":" : ""), $0 }'
}

_pp_dev() {
    local preset="${1:-}" root="$_pp_dev_root" plain versioned libglob dirs lead wired

    case "$(uname -s)" in
    Darwin) plain=libpinpoint_cpp.dylib versioned='libpinpoint_cpp.*.*.*.dylib' libglob='*.dylib' ;;
    Linux)  plain=libpinpoint_cpp.so    versioned='libpinpoint_cpp.so.*.*.*'    libglob='*.so*'  ;;
    *)      echo "unsupported platform $(uname -s)" >&2; return 1 ;;
    esac

    # `source scripts/dev.sh` inside a script inherits that script's own $@ —
    # bash passes the caller's positional parameters through — so an argument
    # counts as a preset only if CMakePresets.json defines it. Otherwise
    # `tests/integration/run.sh -n 0` would try to wire a preset named '-n'.
    if [[ -n "$preset" ]] && ! grep -Eq "\"name\"[[:space:]]*:[[:space:]]*\"$preset\"" "$root/CMakePresets.json"; then
        if [[ -n "${_pp_dev_sourced:-}" ]]; then
            # An interactive shell has no inherited $@, so there it is a typo.
            case $- in
            *i*) echo "unknown preset '$preset' — ignored, wiring unchanged" >&2 ;;
            esac
            preset=""                    # not ours: the sourcing script's argument
        else
            echo "unknown preset '$preset' — see CMakePresets.json" >&2
            return 2
        fi
    fi

    if [[ -n "$preset" ]]; then
        local build="$root/build/$preset" pkg="$root/pinpoint" native lib full major
        # `|| true` on every find pipeline below: a missing dir (or head's
        # early exit) fails it, and `set -o pipefail` would abort the whole
        # script before the explicit checks could report anything.
        native="$(find "$build" -maxdepth 1 -name '_native.cpython-*.so' 2>/dev/null | head -1 || true)"
        if [[ -z "$native" ]]; then
            echo "no _native.cpython-*.so under $build — build it first:" >&2
            echo "    cmake --preset $preset && cmake --build --preset $preset" >&2
            return 1
        fi
        # Newest by mtime, not lexically first: a core version bump leaves the
        # previous libpinpoint_cpp.2.0.0 sitting beside the new 2.1.0 — ninja
        # links the new one and never removes the old — and `sort | head -1`
        # picked *that*, wiring pinpoint/ to a core the freshly built _native
        # was not compiled against. mtime rather than a version sort so that
        # going back to an older core and rebuilding wires the rebuild instead
        # of the highest version still lying in the tree.
        # -print0/xargs -0: the repo can sit under a path with spaces.
        lib="$(find "$build" -maxdepth 1 -name "$versioned" -print0 2>/dev/null |
            xargs -0 ls -t 2>/dev/null | head -1 || true)"
        # With no match, xargs still runs ls once (POSIX) and it lists the
        # working directory, handing back a bare name — so anything that is not
        # a path under $build means "none found".
        case "$lib" in "$build"/*) ;; *) lib="" ;; esac
        [[ -n "$lib" ]] || { echo "no versioned $plain under $build" >&2; return 1; }

        # Drop stale wiring first: a leftover chain from another core version
        # (e.g. libpinpoint_cpp.1.* next to a new 2.*) would otherwise survive
        # re-wiring. Done after the lookups above, so a failed switch leaves
        # the previous preset wired instead of nothing.
        find "$pkg" -maxdepth 1 \( -name '_native.cpython-*.so' -o -name 'libpinpoint_cpp*' \) -type l -delete

        full="$(basename "$lib")"       # libpinpoint_cpp.M.m.p.dylib / .so.M.m.p
        # Drop minor/patch, keeping the extension where the platform puts it.
        if [[ "$full" == *.dylib ]]; then
            major="${full%.*.*.dylib}.dylib"        # libpinpoint_cpp.M.dylib
        else
            major="${full%.*.*}"                    # libpinpoint_cpp.so.M
        fi

        ln -sf "../build/$preset/$(basename "$native")" "$pkg/$(basename "$native")"
        ln -sf "../build/$preset/$full" "$pkg/$full"
        ln -sf "$full"  "$pkg/$major"
        ln -sf "$major" "$pkg/$plain"
        echo "==> pinpoint/ wired to build/$preset ($(basename "$native"), $full)"
    fi

    export FETCHCONTENT_BASE_DIR="${FETCHCONTENT_BASE_DIR:-$HOME/.cache/cmake-fetchcontent}"

    # The transitive dylibs (libgrpc++, libabsl_*, …) stay scattered under the
    # FetchContent tree instead of next to _native, so ld needs both trees.
    # Repo-local build outputs come first: the cache keeps a copy of
    # libpinpoint_cpp that can lag behind a freshly built one and would
    # otherwise shadow it at dlopen time. The wired preset leads; after it,
    # the newest dylib's dir wins within build/.
    #
    # The lead matters because both loaders resolve a leaf name against this
    # path *before* the path they were handed — so with build/release listed
    # first, a pinpoint/ wired to debug still loads release, _native included
    # (Python dlopens it by full path). Sourcing without a preset is how every
    # run.sh wrapper asks for "environment only, leave the wiring alone", so
    # there the lead has to come from the wiring rather than from $1.
    lead="$preset"
    if [[ -z "$lead" ]]; then
        wired="$(find "$root/pinpoint" -maxdepth 1 -name '_native.cpython-*.so' -type l 2>/dev/null | head -1 || true)"
        if [[ -n "$wired" ]]; then
            wired="$(readlink "$wired")"        # ../build/<preset>/_native.…so
            if [[ "$wired" == *build/* ]]; then
                wired="${wired#*build/}"
                lead="${wired%%/*}"
            fi
        fi
    fi
    dirs="${lead:+$root/build/$lead:}"
    # -print0/-0 and one awk doing dirname+dedupe: a path with spaces would
    # otherwise be split by `xargs -n1 dirname` (the FetchContent tree, or the
    # repo itself, can sit under one). awk reads whole lines, so it is safe.
    #
    # The prefix test is what keeps a not-yet-built tree honest: with nothing to
    # match, xargs still runs ls once (POSIX) and it lists the working
    # directory, whose bare names would otherwise land in the search path as
    # relative entries — ld.so resolves those against the cwd of whatever runs
    # later. find's output always starts with the directory it was handed, so
    # anything that does not is not ours.
    dirs="$dirs$(find "$root/build" -name "$libglob" -print0 2>/dev/null |
        xargs -0 ls -t 2>/dev/null |
        awk -v pre="$root/build/" 'index($0, pre) == 1 {
            sub(/\/[^\/]*$/, ""); if (!seen[$0]++) print }' |
        tr '\n' ':' || true)"
    dirs="$dirs$(find "$FETCHCONTENT_BASE_DIR" -name "$libglob" -exec dirname {} \; 2>/dev/null | sort -u | tr '\n' ':' || true)"
    # Ours lead, whatever the caller already had follows; pathclean then drops
    # the separators the scans left behind and any dir a previous source added.
    case "$(uname -s)" in
    Darwin) export DYLD_LIBRARY_PATH="$(_pp_dev_pathclean "${dirs}${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}")" ;;
    Linux)  export LD_LIBRARY_PATH="$(_pp_dev_pathclean "${dirs}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}")" ;;
    esac

    # Guarded because switching presets means sourcing this repeatedly.
    case ":${PYTHONPATH:-}:" in
    *":$root:"*) ;;
    *) export PYTHONPATH="$root${PYTHONPATH:+:$PYTHONPATH}" ;;
    esac

    export PINPOINT_PY_LOG_LEVEL="${PINPOINT_PY_LOG_LEVEL:-INFO}"

    # python/pytest/pip resolve to the venv's — the interpreter the artifact was
    # built for — for both a sourced shell and `scripts/dev.sh <preset> python …`.
    if [[ -r "$root/.venv/bin/activate" ]]; then
        # shellcheck source=/dev/null
        . "$root/.venv/bin/activate"
    else
        echo "no $root/.venv — see docs/development.md §3" >&2
    fi
}

# Sourced: leave the caller's shell configured and stop here. Executed: fall
# through to run the command (if any) with that environment.
if [[ "${BASH_SOURCE[0]:-}" != "${0}" ]]; then
    _pp_dev_sourced=1
    _pp_dev "$@"
    return $?
fi

_pp_dev_sourced=""
set -euo pipefail
[[ $# -gt 0 ]] || {
    echo "usage: scripts/dev.sh <preset> [command ...]   (or: source scripts/dev.sh [preset])" >&2
    exit 2
}
_pp_dev "$@"
shift
if (( $# )); then exec "$@"; fi
