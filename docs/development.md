# Development guide

**For people working on the agent itself.** If you are instrumenting an
application *with* the agent, everything you need is in the other guides — start
with [Getting Started](getting_started.md); nothing on this page is required to
use a released wheel.

Build the native extension from source and run the test suite and example apps
in-place — without `pip install`-ing the package.

The build pulls in the `pinpoint-cpp-agent` submodule and its transitive
dependencies (gRPC, abseil, protobuf, yaml-cpp, fmt) as **shared** libraries, so
dyld/ld.so has to be pointed at those siblings before `_native` will load.
[`scripts/dev.sh`](../scripts/dev.sh) (§4-§5) does that for you: it picks the
build preset to develop against and hands you a shell that can import it.

## 1. Prerequisites

- Python ≥ 3.11. The build artifact is pinned to the Python ABI you built
  against — `_native.cpython-314-darwin.so` only loads in a 3.14 interpreter.
- macOS or Linux. Windows builds are not supported.
- CMake ≥ 3.21, Ninja, a C++17 toolchain (AppleClang on macOS, gcc/clang on Linux).
- Internet access for the submodule checkout and CMake's FetchContent downloads.

```bash
git submodule update --init --recursive third_party/pinpoint-cpp-agent
```

Pass `-DPINPOINT_CPP_AGENT_SOURCE_DIR=/path/to/pinpoint-cpp-agent` at configure
time to build against a different checkout.

## 2. Build the native extension

`FETCHCONTENT_BASE_DIR` keeps the pybind11 and C++-agent dependency downloads
outside the build tree, so `rm -rf build/` doesn't force a re-clone.

```bash
export FETCHCONTENT_BASE_DIR="$HOME/.cache/cmake-fetchcontent"
mkdir -p "$FETCHCONTENT_BASE_DIR"

cmake --preset debug             # ~200s on a cold cache (gRPC etc. clone)
cmake --build --preset debug
```

Build output:

```
build/debug/_native.cpython-3XX-<platform>.so
build/debug/libpinpoint_cpp.{2.0.0,2,}.dylib   # macOS
build/debug/libpinpoint_cpp.so.{2.0.0,2,}      # Linux
```

The transitive dylibs (`libgrpc++`, `libabsl_*`, `libprotobufd`, `libyaml-cppd`,
`libfmtd`, …) stay scattered under `FETCHCONTENT_BASE_DIR`, *not* next to
`_native` — which is what §5 exists to paper over.

### Resolving the dependencies through vcpkg instead

The toolchain file on its own is not enough. `vcpkg.json` belongs to the
submodule, and vcpkg enters manifest mode only when it finds a manifest beside
the top-level `CMakeLists.txt` — so it stays in classic mode,
`VCPKG_INSTALLED_DIR` points at an empty `$VCPKG_ROOT/installed`, and configure
dies in the submodule's dependency lookup (see Troubleshooting).
`VCPKG_MANIFEST_DIR` is what puts the manifest back in view. Relative paths in
it resolve against the working directory, so run this from the repo root:

```bash
cmake --preset release \
  -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake \
  -DVCPKG_MANIFEST_DIR=third_party/pinpoint-cpp-agent
cmake --build --preset release
```

[`scripts/vcpkg_build.sh`](../scripts/vcpkg_build.sh) is those two commands plus
the submodule checkout. For pip builds, pass both `-D`s via `CMAKE_ARGS`.

The manifest installs into `build/<preset>/vcpkg_installed`. To reuse the tree
an earlier `cmake --preset vcpkg` **of the submodule** produced rather than
populate a second copy, add
`-DVCPKG_INSTALLED_DIR=third_party/pinpoint-cpp-agent/build/vcpkg/vcpkg_installed`
— vcpkg reports the packages as already installed and does nothing. The script
passes it whenever that tree exists.

vcpkg's default triplets link statically (`x64-linux` sets
`VCPKG_LIBRARY_LINKAGE static`), and that changes the dev workflow: gRPC,
protobuf, abseil, OpenSSL, yaml-cpp and fmt land **inside**
`libpinpoint_cpp.so` instead of beside it. `ldd` on the extension shows only
`libpinpoint_cpp.so.2` plus libc/libstdc++/libgcc/libm — none of the
`@rpath`/`$ORIGIN` entries §5 exists to resolve — so a vcpkg build needs no
`LD_LIBRARY_PATH`/`DYLD_LIBRARY_PATH` for the transitive libraries. §4 still
applies: `_native` reaches its sibling `libpinpoint_cpp` through those
symlinks, and `scripts/dev.sh` remains how you get `PYTHONPATH` and the venv.

## 3. Python virtual environment

Create it with the **same** Python version your build artifact targets. We don't
`pip install` the package itself — it's imported in-place from the source tree.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
```

[`requirements-dev.txt`](../requirements-dev.txt) pulls in the test runner, the
`wrapt` runtime dependency, and every client library the suites and the example
apps import — by way of
[`tests/integration/requirements.txt`](../tests/integration/requirements.txt),
which stays a separate file so the integration suite can be set up on its own.
Install the whole thing rather than picking packages off it: every driver import
is guarded, so a missing one skips its tests instead of failing the run, and the
gap shows up as quietly thinner coverage.

## 4. Wire build artifacts into the package

`_native` resolves `libpinpoint_cpp.dylib` via `@loader_path` — the `pinpoint/`
package directory — so the build outputs have to be symlinked in there. A real
`pip install` does this automatically (`CMakeBuild.build_extension` in
[`setup.py`](../setup.py)); for the in-place dev workflow,
[`scripts/dev.sh`](../scripts/dev.sh) points them at one preset:

```bash
scripts/dev.sh debug          # or: scripts/dev.sh release
```

It links the preset's `_native.cpython-*.so` plus the `libpinpoint_cpp` version
chain — `2.0.0 → 2 → plain` on macOS, the matching `.so.2.0.0 → .so.2 → .so`
SONAME chain on Linux — after removing any previous chain, which a stale
`libpinpoint_cpp.1.*` beside a new `2.*` would otherwise survive. Re-run it
with the other preset to switch; only symlinks change, so `git status` stays
clean either way.

## 5. Runtime environment

The same script, **sourced**, does §4 and then configures the shell — library
path, `PYTHONPATH`, and `.venv` activated, so `python`/`pytest`/`pip` are the
venv's:

```bash
source scripts/dev.sh debug        # once per shell; re-source to switch preset
python -m pytest tests/unit -v
python -u examples/flask/flask_demo.py
```

Executed instead of sourced, it wires the preset and runs a single command with
that environment, leaving your shell untouched:

```bash
scripts/dev.sh debug python -u examples/flask/flask_demo.py
```

Sourced **without** a preset it only sets the environment and leaves the
current wiring alone — the form the `examples/*/run.sh`, `tests/*/run*.sh` and
benchmark wrappers use, since they must not re-point symlinks under you.

It exports `DYLD_LIBRARY_PATH` (macOS) / `LD_LIBRARY_PATH` (Linux) covering every
transitive dylib, with the selected preset's build dir first and the repo's other
build outputs ahead of the fetchcontent cache: the cache keeps a copy of
`libpinpoint_cpp` that can lag behind a fresh build and would otherwise shadow it
at dlopen time. It also sets `PYTHONPATH=<repo root>` and default
`PINPOINT_PY_COLLECTOR_HOST` / `PINPOINT_PY_LOG_LEVEL` — export your own values
before sourcing to override those.

`PINPOINT_PY_*` vars are parsed by the embedded C++ agent itself (`agent.init()`
switches its env var prefix from the default `PINPOINT_CPP` to `PINPOINT_PY`) and
take precedence over the YAML rendered from `init()` kwargs. The suffixes are
pinpoint-cpp-agent's — see its `doc/config.md` — plus the Python-only
`PINPOINT_PY_AUTOLOAD`, `PINPOINT_PY_SERVER_INFO`, and
`PINPOINT_PY_DISABLED_INSTRUMENTATIONS`.

## 6. Run the tests and the example app

```bash
source scripts/dev.sh debug                  # §5; skip if already sourced

python -m pytest tests/unit -v

python -u examples/flask/flask_demo.py             # binds 0.0.0.0:5000
curl http://localhost:5000/ping              # from another terminal
```

`tests/unit/` needs the §5 environment like every other suite — most of its
files drive the real native agent, so without the library path `_native` fails
to resolve `libgrpc++` at import and collection errors out. Healthy agent
startup logs:

```
[info][pinpoint][grpc.cpp:...] success to register the agent
```

## 7. Adding a bundled auto-instrumentation

For tracing your *own* application code, or a library you maintain outside this
repo, the public API in the [Custom Instrumentation Guide](custom_instrumentation.md)
is all you need — none of this section applies. This is about adding an
integration **to the agent itself**, which means touching its source tree:

```
pinpoint/instrumentations/<name>/
├── __init__.py     the integration; exports instrument()
└── README.md       what it hooks, what it records, how to use it

tests/unit/instrumentations/test_<name>_instrumentation.py
tests/integration/test_<backend>.py     testcontainers-backed, grouped per backend
examples/<name>/                        runnable demo program + its run.sh driver
```

### Step 1 — write the instrumentor

```python
# pinpoint/instrumentations/myframework/__init__.py
"""MyFramework auto-instrumentation."""
from __future__ import annotations

from ...instrumentor import BaseInstrumentor
from .._util import wrap


class MyFrameworkInstrumentor(BaseInstrumentor):

    def _instrument(self) -> None:
        wrap("myframework.app", "App.handle_request", _request_wrapper)


def instrument() -> None:
    MyFrameworkInstrumentor().instrument()
```

`_request_wrapper` is the root-span skeleton from [Custom Instrumentation Guide
§9](custom_instrumentation.md#9-http-server-and-http-client-tracing): guard on
`get_agent()`, open a span with `headers=request.headers`, install it on the
contextvar, then record the status and end it in a `finally`.

`instrument()` is idempotent — the class-keyed registry in
[`instrumentor.py`](../pinpoint/instrumentor.py) makes repeat calls a no-op, and
`wrap()` never stacks wrappers on the same target. `uninstrument()` restores
everything installed through `wrap()` on its own; override `_uninstrument()` only
for non-wrap state (listeners, route swaps, logging factories).

### Step 2 — the `wrap` helper

`pinpoint.instrumentations._util.wrap(module, target, wrapper)` is the only
patcher you need. It uses `wrapt.wrap_function_wrapper` — which respects
descriptors and decorated methods — and routes your wrapper through
`safe_wrapper`: if your code raises before it ever invokes the target, the
exception is swallowed and the unwrapped target is called instead, so an
instrumentation bug cannot crash a user request. `KeyboardInterrupt` /
`SystemExit` always propagate.

### Step 3 — register the autoload hook

Add the entry to [`pinpoint/autoload.py`](../pinpoint/autoload.py):

```python
_REGISTRY: Dict[str, str] = {
    # ...
    "myframework.app": "pinpoint.instrumentations.myframework:instrument",
}
```

The `_REGISTRY` key is the dotted name of the module **whose import triggers your
instrumentation**. Pick the leaf module that defines the class you patch —
registering on a parent package can fire the hook before the leaf is loaded,
leaving your wrap target undefined. For libraries split across several modules
(FastAPI: `applications` + `routing`), register against the one that finishes
loading **last** in a healthy import sequence; that is why the FastAPI hook
triggers on `fastapi.applications`.

The opt-out alias is derived automatically from the registry key's top-level
package, so users disable the integration with
`PINPOINT_PY_DISABLED_INSTRUMENTATIONS=myframework` (or the full module name).

### Step 4 — the no-canonical-module case

WSGI and ASGI are protocols, not modules — every server speaks them, but there is
no single import to hook. Ship the middleware class as an opt-in API, document it
in the README, and register a no-op `instrument()` so the autoload registry stays
consistent. See [`wsgi/__init__.py`](../pinpoint/instrumentations/wsgi/__init__.py).

```python
from pinpoint.instrumentations.wsgi import PinpointWSGIMiddleware
app = PinpointWSGIMiddleware(app)
```

### Step 5 — tests, and the review checklist

The minimum bar for a new instrumentation:

- Unit-level: install the wrapper, exercise the target, assert the span/event was
  recorded with the expected service type and annotations.
- Real-library smoke test: install the actual library, run a small program through
  `pinpoint-run`, confirm spans land in the dev collector.

Then three items only a cross-language review catches:

- [ ] Operation name and service type render the same as the equivalent Go / C++
      integration.
- [ ] Distributed-tracing headers round-trip with the other agents — send a
      request to a peer service and verify the trace stitches in the UI.
- [ ] A runnable demo lives under `examples/<name>/` for the smoke test.

Everything else is the [Custom Instrumentation Guide](custom_instrumentation.md)
§3–§13, which applies unchanged to a bundled integration.

## 8. Internals with a contract

Two places where the Python and C++ sides agree on something the compiler does
not check, so a change on either side has to be made on both.

### The span config snapshot is a positional tuple

`Agent.get_config_snapshot()` / `Span.get_config_snapshot()` return the resolved
native configuration as a **tuple read positionally** by
[`http_helper.py`](../pinpoint/http_helper.py) and [`agent.py`](../pinpoint/agent.py)
(and by external test doubles). It is therefore **append-only**: never insert or
reorder a field after shipping an index. Revision is index 11,
`Sql.TraceBindValue` 12, user proxy header names 13, resolved
`EnableCallstackTrace` 14; `to_python_config` in [`src/_native.cpp`](../src/_native.cpp)
builds it. A shorter tuple from an older binding or a fake is treated as
callstack-capture *off*, rather than guessing that frame capture is safe.

### The native log callback runs without the GIL

The C++ sink installed by `native_log_to_python=True` runs inline under the
native logger mutex, so it obeys a stricter contract than an ordinary callback:

- It makes no Python API call, performs no Python reference-count operation, and
  does not acquire the GIL. It captures only shared C++ queue state.
- It copies level and message while the borrowed pointers are valid, attempts a
  lock-free MPSC enqueue, and returns. It never invokes a Pinpoint API or native
  logging recursively.
- Slots use fixed storage allocated when the bridge starts, so the callback never
  reaches a heap allocator. Exhaustion drops the new record and bumps an atomic
  counter; producers never wait for space.

Teardown is ordered: the Python `Agent` owns the consumer, the installed C++ sink
independently holds the shared queue state. Native `shutdown()` disables the sink
first and waits out any callback holding the logger mutex; only then is the
consumer deactivated, drained, and joined (≤ 1s). That ordering is what lets a
logging handler call `pinpoint.shutdown()` safely — it is outside the native
mutex by then, and the self-join is skipped. A warm-fork child atomically
deactivates the inherited C++ state *without* acquiring any inherited Python
lock, since the thread that held it did not survive the fork.

The user-visible half of both — what gets truncated, dropped, or reloaded — is in
[API Contracts §4 and §11](api_contracts.md#4-annotations-and-properties-are-buffered-until-end).

## Troubleshooting

### `Could not find a package configuration file provided by "Protobuf"`
A vcpkg build configured without `-DVCPKG_MANIFEST_DIR`: the toolchain never
entered manifest mode, so nothing was installed for
`PinpointDependencies.cmake` to find. See §2.

### `ImportError: dlopen ... Library not loaded: @rpath/libgrpc++...`
The library path doesn't cover the transitive dylibs. Re-source
`scripts/dev.sh` — the directories it scans are empty right after a clean
build, and move if you reset `FETCHCONTENT_BASE_DIR`.

### `ModuleNotFoundError: No module named 'pinpoint'`
`PYTHONPATH=$PWD` isn't exported and you're running from a subdirectory like
`examples/`, so the project root isn't on `sys.path`.

### `ModuleNotFoundError: No module named 'pinpoint._native'`
The §4 symlinks are missing, or the build artifact's Python ABI tag (e.g.
`cpython-314`) doesn't match the venv's interpreter. Check `python3 --version`,
then rebuild or recreate the venv to match.

### `Address already in use` (port 5000)
A leftover flask process — or, on macOS, "AirPlay Receiver".
```bash
lsof -ti :5000 | xargs kill -9
```

### No gRPC registration line, or `address of collector is required`
Set `PINPOINT_PY_LOG_LEVEL=INFO` and read the C++-side effective config dump on
stdout. An empty `Collector.Host` there means the YAML key mapping in
[`config.py:to_yaml`](../pinpoint/config.py) is out of sync with what
pinpoint-cpp-agent's parser expects.
