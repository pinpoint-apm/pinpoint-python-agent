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

"""Python-side span_ignore_errors matching: match_subclasses / match_cause."""

from __future__ import annotations

import pytest

import pinpoint.tracer as tracer
from pinpoint.config import Config
from pinpoint.tracer import Span, _ANN_ERROR, _ANN_IGNORED_ERROR, _python_ignored


@pytest.fixture
def rules():
    saved = tracer._IGNORE_RULES
    yield tracer.set_ignore_rules
    tracer._IGNORE_RULES = saved


def _raised(make):
    try:
        make()
    except BaseException as exc:  # noqa: BLE001
        return exc


def _from(outer, inner):
    def make():
        try:
            raise inner
        except BaseException as e:
            raise outer from e
    return _raised(make)


def test_default_has_no_rules_and_costs_nothing(rules, monkeypatch):
    rules([{"name": "ConnectionError"}])  # exact rule: native's job
    assert tracer._IGNORE_RULES == ()
    monkeypatch.setattr(tracer, "_rule_matches",
                        lambda *_: pytest.fail("no matching without flagged rules"))
    assert _python_ignored(ConnectionResetError("x")) is False


def test_match_subclasses(rules):
    rules([{"name": "ConnectionError", "match_subclasses": True}])
    assert _python_ignored(ConnectionResetError("x"))
    assert _python_ignored(ConnectionError("x"))
    assert not _python_ignored(TimeoutError("x"))
    assert not _python_ignored("ConnectionResetError")


def test_exact_rule_unchanged_without_flag(rules):
    rules([{"name": "ConnectionError", "match_cause": True}])
    assert not _python_ignored(ConnectionResetError("x"))


def test_match_cause_walks_cause_and_context(rules):
    rules([{"name": "TimeoutError", "match_cause": True}])
    assert _python_ignored(_from(RuntimeError("x"), TimeoutError("t")))

    def implicit():
        try:
            raise TimeoutError("t")
        except TimeoutError:
            raise RuntimeError("x")
    assert _python_ignored(_raised(implicit))

    def suppressed():
        try:
            raise TimeoutError("t")
        except TimeoutError:
            raise RuntimeError("x") from None
    assert not _python_ignored(_raised(suppressed))


def test_match_cause_terminates_on_cycle(rules):
    rules([{"name": "Nope", "match_cause": True}])
    a, b = RuntimeError("a"), ValueError("b")
    a.__cause__, b.__cause__ = b, a
    assert not _python_ignored(a)


def test_message_contains_tested_on_matched_link(rules):
    rules([{"name": "TimeoutError", "message_contains": "slow", "match_cause": True}])
    assert _python_ignored(_from(RuntimeError("fast"), TimeoutError("slow db")))
    assert not _python_ignored(_from(RuntimeError("slow"), TimeoutError("db")))


def test_set_error_swaps_tag_and_skips_verdicts(rules):
    rules([{"name": "ConnectionError", "match_subclasses": True}])

    class _Native:
        def end_span_with_data(self, *_):
            pass

    span = Span(_Native(), enable_callstack_trace=True)
    event = span.new_span_event("op")
    event.set_error(_raised(lambda: (_ for _ in ()).throw(ConnectionResetError("peer"))))
    tag, name, msg, frames = event._annotations[-1][:4]
    assert (tag, name, msg) == (_ANN_IGNORED_ERROR, "ConnectionResetError", "peer")
    assert frames is not None
    event.set_error(ValueError("kept"))
    assert event._annotations[-1][0] == _ANN_ERROR

    span.set_error(ConnectionResetError("root"))
    assert span._annotations[-1][0] == _ANN_IGNORED_ERROR

    overflow = Span(_Native(), max_event_sequence=0).new_span_event("overflow")
    overflow.set_error(ConnectionResetError("dropped"))
    assert overflow._span._error_verdicts is None


def test_to_yaml_strips_python_only_keys():
    cfg = Config(application_name="a", span_ignore_errors=[
        {"name": "ConnectionError", "match_subclasses": True, "match_cause": True}])
    yaml = cfg.to_yaml()
    assert '"name": "ConnectionError"' in yaml
    assert "match_subclasses" not in yaml and "match_cause" not in yaml


# ---------------------------------------------------------------------------
# mark_error=False: recorded on the trace, but the transaction stays green.
# ---------------------------------------------------------------------------


class _Native:
    def end_span_with_data(self, *_):
        pass


def test_mark_error_false_records_without_failing_the_trace():
    span = Span(_Native())
    event = span.new_span_event("op")

    event.set_error(ValueError("handled"), mark_error=False)
    assert event._annotations[-1][:3] == (
        _ANN_IGNORED_ERROR, "ValueError", "handled")

    span.set_error(ValueError("handled"), mark_error=False)
    assert span._annotations[-1][:3] == (
        _ANN_IGNORED_ERROR, "ValueError", "handled")


def test_mark_error_defaults_to_failing_the_trace():
    span = Span(_Native())
    span.set_error(ValueError("boom"))
    span.new_span_event("op").set_error(ValueError("boom"))
    assert span._annotations[-1][0] == _ANN_ERROR


def test_mark_error_false_promotes_the_bare_message_form():
    """SetIgnoredError has no one-argument overload, so the message-only call
    becomes the ("Error", message) pair native's SetError(message) implies."""
    span = Span(_Native())
    span.set_error("boom", mark_error=False)
    assert span._annotations[-1] == (_ANN_IGNORED_ERROR, "Error", "boom")

    event = Span(_Native()).new_span_event("op")
    event.set_error("boom", mark_error=False)
    assert event._annotations[-1] == (_ANN_IGNORED_ERROR, "Error", "boom")


def test_mark_error_false_on_overflow_event_keeps_no_verdict():
    """An overflow placeholder records no step and was only holding the
    verdict, so there is nothing left to keep."""
    span = Span(_Native(), max_event_sequence=0)
    span.new_span_event("overflow").set_error(ValueError("x"), mark_error=False)
    assert span._error_verdicts is None
    span.new_span_event("overflow").set_error(ValueError("x"))
    assert span._error_verdicts == [("ValueError", "x")]
