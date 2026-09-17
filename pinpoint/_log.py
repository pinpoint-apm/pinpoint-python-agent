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

"""Internal logger. Never surfaces to users unless they opt in."""

import logging
import logging.handlers
import os
import sys
from typing import Optional

_LOGGER_NAME = "pinpoint"
_DEFAULT_LEVEL = logging.INFO
_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"

# The handler configure() installed, so a re-run (new level, after fork with a
# %pid% path) swaps it instead of stacking a second one. User-added handlers on
# the ``pinpoint`` logger are never touched.
_handler: Optional[logging.Handler] = None

# Whether ``pinpoint.*`` logging is at DEBUG, resolved once by configure().
# Instrumentation call tracing reads this instead of asking the logger on every
# wrapped call. Re-call configure() after changing the level by hand.
debug_enabled = False


def get_logger(name: str = "") -> logging.Logger:
    if name:
        return logging.getLogger(f"{_LOGGER_NAME}.{name}")
    return logging.getLogger(_LOGGER_NAME)


def _make_handler(output: str, max_file_size_mb: int, max_backups: int,
                  rotate: bool) -> logging.Handler:
    """stdout/stderr -> stream handler; anything else is a file path.

    Only one writer may rotate a shared file. When the native agent also writes
    ``output`` it owns rotation and Python follows the rename via
    WatchedFileHandler (reopens on inode change); when Python is the sole writer
    (native_log_to_python) it rotates itself.
    """
    kind = output.strip().lower()
    if kind in ("", "stdout"):
        return logging.StreamHandler(sys.stdout)
    if kind == "stderr":
        return logging.StreamHandler(sys.stderr)
    path = output.replace("%pid%", str(os.getpid()))
    if not rotate:
        return logging.handlers.WatchedFileHandler(path, encoding="utf-8")
    return logging.handlers.RotatingFileHandler(
        path, maxBytes=max(int(max_file_size_mb), 1) * 1024 * 1024,
        backupCount=max(int(max_backups), 1), encoding="utf-8")


def configure(level: str = "", output: str = "",
              max_file_size_mb: int = 10, max_backups: int = 1,
              rotate: bool = False) -> None:
    """Install the ``pinpoint`` logger's handler and level.

    ``output`` mirrors Config.log_output: ``"stdout"`` (default), ``"stderr"``
    or a file path (``%pid%`` expands to the current pid). ``rotate`` makes
    Python rotate the file at ``max_file_size_mb`` MB keeping ``max_backups``;
    leave it False while the native agent writes the same file, so it stays the
    only rotator. Safe to call again.
    """
    global debug_enabled, _handler
    # Same env vars the native agent reads (config.ENV_VAR_PREFIX + suffix);
    # spelled out to keep this module import-light.
    level = level or os.environ.get("PINPOINT_PY_LOG_LEVEL", "INFO")
    output = output or os.environ.get("PINPOINT_PY_LOG_FILE_PATH", "stdout")
    logger = logging.getLogger(_LOGGER_NAME)
    if _handler is not None:
        logger.removeHandler(_handler)
        _handler.close()
        _handler = None
    if not logger.handlers:
        open_error = None
        try:
            _handler = _make_handler(output, max_file_size_mb, max_backups, rotate)
        except OSError as exc:  # unwritable path must not break init()
            open_error = exc
            _handler = logging.StreamHandler(sys.stderr)
        _handler.setFormatter(logging.Formatter(_FORMAT))
        logger.addHandler(_handler)
        logger.propagate = False
        if open_error is not None:
            logger.warning("cannot open log file %r (%s); logging to stderr",
                           output, open_error)
    # getLevelName(name) returns an int for a known name ("WARN"/"FATAL" included) or
    # the string "Level <name>" otherwise, so a config typo falls back instead of
    # crashing init the way setLevel's ValueError would.
    numeric = logging.getLevelName(str(level).strip().upper())
    if isinstance(numeric, int):
        logger.setLevel(numeric)
    else:
        logger.setLevel(_DEFAULT_LEVEL)
        logger.warning(
            "unknown log level %r; falling back to INFO", level
        )
    debug_enabled = logger.isEnabledFor(logging.DEBUG)
