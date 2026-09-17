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

"""Tests for pinpoint._log — internal logger configuration."""

import logging
import logging.handlers
import os

import pytest

import pinpoint._log as _log_mod
from pinpoint._log import _LOGGER_NAME, configure, get_logger


@pytest.fixture(autouse=True)
def _clean_logger():
    """Restore logger to pristine state after each test."""
    logger = logging.getLogger(_LOGGER_NAME)
    original_level = logger.level
    original_handlers = list(logger.handlers)
    original_propagate = logger.propagate
    yield
    if _log_mod._handler is not None:
        _log_mod._handler.close()
        _log_mod._handler = None
    logger.setLevel(original_level)
    logger.handlers[:] = original_handlers
    logger.propagate = original_propagate


def test_configure_file_output_follows_native_rotation(tmp_path):
    logger = logging.getLogger(_LOGGER_NAME)
    logger.handlers.clear()
    path = tmp_path / "agent-%pid%.log"
    configure("INFO", str(path))
    handler = logger.handlers[0]
    assert isinstance(handler, logging.handlers.WatchedFileHandler)
    logger.info("hello file")
    live = tmp_path / f"agent-{os.getpid()}.log"
    assert "hello file" in live.read_text()
    # Native rotates by rename; Python must reopen the new live file.
    live.rename(tmp_path / "rotated.1")
    logger.info("after rotate")
    assert "after rotate" in live.read_text()
    assert "after rotate" not in (tmp_path / "rotated.1").read_text()


def test_configure_rotate_attaches_rotating_handler(tmp_path):
    logger = logging.getLogger(_LOGGER_NAME)
    logger.handlers.clear()
    configure("INFO", str(tmp_path / "agent.log"), max_file_size_mb=1,
              max_backups=3, rotate=True)
    handler = logger.handlers[0]
    assert isinstance(handler, logging.handlers.RotatingFileHandler)
    assert handler.maxBytes == 1024 * 1024 and handler.backupCount == 3


def test_configure_stream_outputs(capsys):
    logger = logging.getLogger(_LOGGER_NAME)
    logger.handlers.clear()
    configure("INFO", "stdout")
    logger.info("to-out")
    configure("INFO", "stderr")
    logger.info("to-err")
    assert len(logger.handlers) == 1
    out, err = capsys.readouterr()
    assert "to-out" in out and "to-out" not in err
    assert "to-err" in err and "to-err" not in out


def test_configure_unwritable_file_falls_back_to_stderr(tmp_path, capsys):
    logger = logging.getLogger(_LOGGER_NAME)
    logger.handlers.clear()
    configure("INFO", str(tmp_path / "missing-dir" / "agent.log"))
    assert isinstance(logger.handlers[0], logging.StreamHandler)
    assert "cannot open log file" in capsys.readouterr().err


def test_get_logger_no_name_returns_root_pinpoint_logger():
    assert get_logger().name == _LOGGER_NAME


def test_get_logger_with_name_returns_child_logger():
    assert get_logger("http").name == f"{_LOGGER_NAME}.http"


def test_get_logger_empty_string_returns_root():
    assert get_logger("").name == _LOGGER_NAME


def test_configure_sets_level():
    configure("DEBUG")
    assert logging.getLogger(_LOGGER_NAME).level == logging.DEBUG


def test_configure_reads_env_when_no_arg(monkeypatch):
    monkeypatch.setenv("PINPOINT_PY_LOG_LEVEL", "ERROR")
    configure()
    assert logging.getLogger(_LOGGER_NAME).level == logging.ERROR


def test_configure_defaults_to_info_without_env(monkeypatch):
    monkeypatch.delenv("PINPOINT_PY_LOG_LEVEL", raising=False)
    configure()
    assert logging.getLogger(_LOGGER_NAME).level == logging.INFO


def test_configure_adds_handler_when_none_present():
    logger = logging.getLogger(_LOGGER_NAME)
    logger.handlers.clear()
    configure("INFO")
    assert len(logger.handlers) == 1


def test_configure_does_not_add_duplicate_handler():
    logger = logging.getLogger(_LOGGER_NAME)
    logger.handlers.clear()
    configure("INFO")
    configure("DEBUG")
    assert len(logger.handlers) == 1


def test_configure_disables_propagation():
    configure("INFO")
    assert logging.getLogger(_LOGGER_NAME).propagate is False


def test_configure_handler_uses_expected_format():
    logger = logging.getLogger(_LOGGER_NAME)
    logger.handlers.clear()
    configure("INFO")
    fmt = logger.handlers[0].formatter._fmt
    assert "%(name)s" in fmt
    assert "%(levelname)s" in fmt
    assert "%(message)s" in fmt


def test_configure_invalid_level_falls_back_to_info_without_raising():
    # A config typo (e.g. PINPOINT_PY_LOG_LEVEL=verbose) must never crash;
    # logging.Logger.setLevel raises ValueError on unknown level names.
    configure("verbose")
    assert logging.getLogger(_LOGGER_NAME).level == logging.INFO


def test_configure_invalid_level_emits_warning():
    # configure() sets propagate=False, so pytest's caplog (which captures on
    # the root logger) can't see the record — attach our own capture handler.
    logger = logging.getLogger(_LOGGER_NAME)
    records = []

    class _Capture(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger.addHandler(_Capture())
    configure("garbage")
    assert any("garbage" in rec.getMessage() for rec in records)
    assert logger.level == logging.INFO


def test_configure_normalizes_warn_alias():
    configure("warn")
    assert logging.getLogger(_LOGGER_NAME).level == logging.WARNING


def test_configure_normalizes_fatal_alias():
    configure("fatal")
    assert logging.getLogger(_LOGGER_NAME).level == logging.CRITICAL


def test_configure_is_case_insensitive():
    configure("debug")
    assert logging.getLogger(_LOGGER_NAME).level == logging.DEBUG


def test_configure_invalid_env_level_falls_back(monkeypatch):
    monkeypatch.setenv("PINPOINT_PY_LOG_LEVEL", "loud")
    configure()
    assert logging.getLogger(_LOGGER_NAME).level == logging.INFO
