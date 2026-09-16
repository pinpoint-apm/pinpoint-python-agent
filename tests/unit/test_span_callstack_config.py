# pinpoint-python-agent
# Copyright (c) 2026-present NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0

"""Span-local resolved callstack capture gates."""

from __future__ import annotations

import threading

import pytest

import pinpoint.callstack as callstack
from pinpoint.agent import _NullSpan
from pinpoint.tracer import Span


class _Native:
    def __init__(self):
        self.children = []

    def new_async_span(self, *_args):
        child = _Native()
        self.children.append(child)
        return child

    def end_span_with_data(self, *_args):
        pass


def _error_annotation(span: Span, error, message=None):
    event = span.new_span_event("error")
    event.set_error(error, message)
    return event._annotations[-1]


def _raised_error():
    try:
        raise ValueError("boom")
    except ValueError as exc:
        return exc


def test_disabled_span_records_error_without_entering_frame_capture(monkeypatch):
    span = Span(_Native(), enable_callstack_trace=False)
    monkeypatch.setattr(
        callstack, "frames_for",
        lambda *_a, **_kw: pytest.fail("disabled span entered frame capture"),
    )

    annotation = _error_annotation(span, _raised_error())

    assert annotation == (5, "ValueError", "boom")


def test_disabled_gate_does_not_walk_traceback_or_stack(monkeypatch):
    span = Span(_Native(), enable_callstack_trace=False)
    monkeypatch.setattr(
        callstack.traceback, "walk_tb",
        lambda *_: pytest.fail("disabled span walked a traceback"),
    )
    monkeypatch.setattr(
        callstack.traceback, "walk_stack",
        lambda *_: pytest.fail("disabled span walked the current stack"),
    )

    _error_annotation(span, _raised_error())
    _error_annotation(span, "Synthetic", "message")


def test_null_event_never_enters_frame_capture(monkeypatch):
    monkeypatch.setattr(
        callstack, "frames_for",
        lambda *_a, **_kw: pytest.fail("null event entered frame capture"),
    )
    _NullSpan().new_span_event("ignored").set_error(_raised_error())


def test_enabled_span_captures_exception_and_string_frames():
    span = Span(_Native(), enable_callstack_trace=True)

    exception = _error_annotation(span, _raised_error())
    synthetic = _error_annotation(span, "Synthetic", "message")

    assert exception[:3] == (5, "ValueError", "boom")
    assert exception[3]
    assert any(frame[1].endswith("_raised_error") for frame in exception[3])
    assert synthetic[:3] == (5, "Synthetic", "message")
    assert synthetic[3]
    assert all(len(frame) == 4 for frame in synthetic[3])


def test_live_spans_with_different_gates_are_independent():
    disabled = Span(_Native(), enable_callstack_trace=False)
    enabled = Span(_Native(), enable_callstack_trace=True)

    assert len(_error_annotation(disabled, _raised_error())) == 3
    assert len(_error_annotation(enabled, _raised_error())) == 4


def test_explicit_async_child_inherits_parent_gate():
    parent = Span(_Native(), enable_callstack_trace=True)
    with parent.new_span_event("launch"):
        child = parent.new_async_span("work")

    assert child._enable_callstack_trace is True
    assert len(_error_annotation(child, _raised_error())) == 4


def test_spans_keep_thread_local_capture_decisions():
    spans = [Span(_Native(), enable_callstack_trace=False),
             Span(_Native(), enable_callstack_trace=True)]
    lengths = [None, None]

    def capture(index):
        lengths[index] = len(_error_annotation(spans[index], _raised_error()))

    threads = [threading.Thread(target=capture, args=(i,)) for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert lengths == [3, 4]


def _chained_error():
    try:
        try:
            raise KeyError("root")
        except KeyError as root:
            raise ValueError("boom") from root
    except ValueError as exc:
        return exc


def test_enabled_span_buffers_causes_as_fifth_element():
    span = Span(_Native(), enable_callstack_trace=True)

    plain = _error_annotation(span, _raised_error())
    chained = _error_annotation(span, _chained_error())

    assert len(plain) == 4
    assert len(chained) == 5
    assert chained[:3] == (5, "ValueError", "boom")
    assert [(c[0], c[1]) for c in chained[4]] == [("KeyError", "'root'")]
    assert chained[4][0][2]  # the cause's own frames


def test_disabled_and_overflow_paths_collect_no_chain(monkeypatch):
    def boom(*_a, **_k):
        raise AssertionError("causes_for must stay cold")
    monkeypatch.setattr(callstack, "causes_for", boom)

    disabled = Span(_Native(), enable_callstack_trace=False)
    assert len(_error_annotation(disabled, _chained_error())) == 3

    overflow = Span(_Native(), enable_callstack_trace=True, max_event_sequence=0)
    event = overflow.new_span_event("overflow")
    event.set_error(_chained_error())
    assert event._sequence is None and not event._annotations
