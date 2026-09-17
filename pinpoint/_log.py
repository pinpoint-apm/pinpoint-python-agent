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
import os

_LOGGER_NAME = "pinpoint"
_DEFAULT_LEVEL = logging.INFO

# Whether ``pinpoint.*`` logging is at DEBUG, resolved once by configure().
# Instrumentation call tracing reads this instead of asking the logger on every
# wrapped call. Re-call configure() after changing the level by hand.
debug_enabled = False


def get_logger(name: str = "") -> logging.Logger:
    if name:
        return logging.getLogger(f"{_LOGGER_NAME}.{name}")
    return logging.getLogger(_LOGGER_NAME)


def configure(level: str = "") -> None:
    global debug_enabled
    # Same env var the native agent reads (config.ENV_VAR_PREFIX + _LOG_LEVEL);
    # spelled out to keep this module import-light.
    level = level or os.environ.get("PINPOINT_PY_LOG_LEVEL", "INFO")
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")
        )
        logger.addHandler(handler)
        logger.propagate = False
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
