# pinpoint-python-agent
# Copyright (c) 2026-present NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Auto-activated at interpreter start when `pinpoint-run` prepends this
directory to PYTHONPATH. Idempotent and safe to import unconditionally — if
the env var isn't set, this does nothing beyond loading a handful of names.

We deliberately import as little as possible at top-level so interpreter
startup stays fast.
"""

import os as _os
import sys as _sys


def _activate() -> None:
    # Case-insensitive, and accepts the same truthy tokens as
    # config._parse_bool ("1"/"true"/"yes"/"on") so PINPOINT_PY_AUTOLOAD=TRUE
    # or =on isn't silently ignored.
    if _os.environ.get("PINPOINT_PY_AUTOLOAD", "0").strip().lower() not in (
        "1", "true", "yes", "on",
    ):
        return
    try:
        import pinpoint
        from pinpoint.autoload import autoload

        pinpoint.init(
            server_info=_os.environ.get("PINPOINT_PY_SERVER_INFO", "Python Application"),
        )
        autoload()
    except Exception as exc:  # noqa: BLE001
        print(f"pinpoint-python-agent: bootstrap failed: {exc}", file=_sys.stderr)


try:
    _activate()
except BaseException:  # noqa: BLE001
    # _activate already guards its body; what is left is its own error
    # handler — the f-string's str(exc) and the stderr write, either of which
    # can fail (broken pipe, an exception with a raising __str__). Letting
    # that escape would abort this module before the chaining block below,
    # silently dropping the user's real sitecustomize in every process.
    pass

# Chain to any sitecustomize we shadowed: being first on sys.path means Python
# imported *us* under that name and the real one never ran. Two mechanisms:
#
# 1. ``PINPOINT_PY_PREV_SITECUSTOMIZE`` names a file to exec verbatim (escape
#    hatch when auto-discovery picks the wrong one).
# 2. Auto-chaining: drop our bootstrap dir from ``sys.path``, evict ourselves
#    from ``sys.modules``, re-import ``sitecustomize``. ImportError just means
#    there was nothing to chain to.
try:
    _prev_path = _os.environ.get("PINPOINT_PY_PREV_SITECUSTOMIZE")
    if _prev_path and _os.path.isfile(_prev_path):
        with open(_prev_path, "rb") as _f:
            _code = compile(_f.read(), _prev_path, "exec")
            exec(_code, {"__name__": "sitecustomize", "__file__": _prev_path})
    else:
        # realpath on both sides so symlinked venvs / ``..`` segments still match:
        # an abspath-only compare can miss our own dir and re-import *this* module
        # until RecursionError, leaving the shadowed one unrun.
        _bootstrap_dir = _os.path.realpath(_os.path.dirname(_os.path.abspath(__file__)))
        # realpath is lstat-per-component; memoize so the finally below doesn't
        # re-resolve every sys.path entry a second time.
        _realpaths = {}

        def _real(_p):
            _key = _p or _os.curdir
            _r = _realpaths.get(_key)
            if _r is None:
                _r = _os.path.realpath(_key)
                _realpaths[_key] = _r
            return _r

        _trimmed = [_p for _p in _sys.path if _real(_p) != _bootstrap_dir]
        if len(_trimmed) != len(_sys.path):
            # Keep a handle on our own module object: after this body returns the
            # import machinery pops ``sys.modules["sitecustomize"]``, and a missing
            # entry raises KeyError *outside* our try/except — then
            # site.execsitecustomize() prints "Error in sitecustomize ..." at startup.
            _our_module = _sys.modules.get("sitecustomize")
            _sys.modules.pop("sitecustomize", None)
            try:
                _sys.path[:] = _trimmed
                import sitecustomize  # noqa: F401  (the shadowed one)
            except ImportError:
                pass
            finally:
                # A shadowed sitecustomize often exists precisely to extend sys.path
                # (site.addsitedir, vendoring), so drop only our own bootstrap dir —
                # never blanket-restore the pre-chain snapshot.
                _sys.path[:] = [
                    _p for _p in _sys.path if _real(_p) != _bootstrap_dir
                ]
                # No replacement installed (the common ImportError case): restore our
                # module object so the post-exec bookkeeping finds its entry.
                if _sys.modules.get("sitecustomize") is None and _our_module is not None:
                    _sys.modules["sitecustomize"] = _our_module
except Exception:  # noqa: BLE001
    pass
