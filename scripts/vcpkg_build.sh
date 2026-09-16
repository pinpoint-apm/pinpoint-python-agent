#!/usr/bin/env bash
# Build the native extension against vcpkg-provided dependencies instead of the
# FetchContent source build (docs/development.md §2). gRPC, protobuf, abseil,
# yaml-cpp and fmt come from $VCPKG_ROOT rather than being cloned and compiled
# here, and the default triplets are static, so they end up linked *into*
# libpinpoint_cpp — a build that needs no LD_LIBRARY_PATH for them.
#
#   scripts/vcpkg_build.sh              # release, into build/release
#   scripts/vcpkg_build.sh debug        # any preset CMakePresets.json defines
#
# Then wire the artifacts and get a shell that can import them (§4-§5):
#
#   source scripts/dev.sh release

set -euo pipefail

preset="${1:-release}"
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")/.." && pwd)"
cd "$root"

: "${VCPKG_ROOT:?set it to a vcpkg checkout — see docs/development.md §2}"

# The debug preset takes this from the environment, and pybind11 is fetched
# even on the vcpkg path. Same default as scripts/dev.sh.
export FETCHCONTENT_BASE_DIR="${FETCHCONTENT_BASE_DIR:-$HOME/.cache/cmake-fetchcontent}"
mkdir -p "$FETCHCONTENT_BASE_DIR"

git submodule update --init --recursive third_party/pinpoint-cpp-agent

# vcpkg.json sits in the submodule, not beside the top-level CMakeLists.txt, so
# the toolchain file alone leaves vcpkg in classic mode: it installs nothing,
# VCPKG_INSTALLED_DIR points at an empty $VCPKG_ROOT/installed, and the
# submodule's find_package(Protobuf) fails at configure time.
args=(
    -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
    -DVCPKG_MANIFEST_DIR="$root/third_party/pinpoint-cpp-agent"
)

# Reuse the packages an earlier `cmake --preset vcpkg` of the submodule
# installed, rather than populating a second copy under build/$preset. vcpkg
# reports them as already installed and does nothing.
installed="$root/third_party/pinpoint-cpp-agent/build/vcpkg/vcpkg_installed"
if [[ -d "$installed" ]]; then
    echo "==> reusing $installed"
    args+=(-DVCPKG_INSTALLED_DIR="$installed")
fi

cmake --preset "$preset" "${args[@]}"
cmake --build --preset "$preset"
