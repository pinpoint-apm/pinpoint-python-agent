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

"""Flask instrumentation.

Drives ``_wsgi_app_wrapper`` (the ``Flask.wsgi_app`` hook) against a fake
agent and a tiny WSGI app — no real Flask app/server required. Validates
root-span lifecycle, status capture, the no-upstream-context fast path
(reader-less ``new_span``), exception propagation, and disabled passthrough.
"""

from __future__ import annotations

import pytest
import wrapt
from _fakes import FakeAgent as _FakeAgent, wsgi_environ

import pinpoint
from pinpoint.instrumentations import wsgi as wsgi_instr
from pinpoint.instrumentations._util import safe_wrapper
from pinpoint.instrumentations.flask import (
    _dispatch_request_wrapper,
    _wsgi_app_wrapper,
)


def _environ(**overrides):
    return wsgi_environ(**overrides)


def _ok_app(status="200 OK"):
    def app(environ, start_response):
        start_response(status, [("content-type", "text/plain")])
        return [b"hi"]
    return app


def _run(environ, app):
    return list(_wsgi_app_wrapper(
        app, instance=object(), args=(environ, lambda *a, **k: None), kwargs={},
    ))


# ---------------------------------------------------------------------------
# _wsgi_app_wrapper
# ---------------------------------------------------------------------------

def test_wsgi_app_opens_root_span_and_captures_status(fake_agent):
    body = _run(_environ(HTTP_PINPOINT_TRACEID="T-1"), _ok_app())

    assert body == [b"hi"]
    assert ("span_start", "Flask HTTP Server", "/items/42") in fake_agent.events
    assert ("span_end", "Flask HTTP Server") in fake_agent.events
    assert fake_agent.last_native.status_code == 200
    assert ("/items/42", "GET", 200) in fake_agent.last_native.url_stats
    # Upstream Pinpoint header present -> reader-backed native call.
    assert fake_agent.last_native.headers is not None


def test_wsgi_app_skips_reader_when_no_upstream_context(fake_agent):
    """Without an HTTP_PINPOINT_* header new_span is called without a reader,
    yet the span still opens, records url_stat, and closes."""
    body = _run(_environ(), _ok_app())

    assert body == [b"hi"]
    assert fake_agent.last_native.headers is None
    assert ("span_start", "Flask HTTP Server", "/items/42") in fake_agent.events
    assert fake_agent.last_native.status_code == 200
    assert ("/items/42", "GET", 200) in fake_agent.last_native.url_stats
    assert ("span_end", "Flask HTTP Server") in fake_agent.events


def test_wsgi_app_records_exception_and_reraises(fake_agent):
    def app(environ, start_response):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        _wsgi_app_wrapper(
            app, instance=object(),
            args=(_environ(), lambda *a, **k: None), kwargs={},
        )

    assert any(e[0] == "span_error" for e in fake_agent.events)
    assert ("span_end", "Flask HTTP Server") in fake_agent.events


def test_wsgi_app_streaming_body_ends_response_child_after_drain(fake_agent):
    """A streamed body runs under a linked response child, not the root."""
    from pinpoint.context import current_span

    seen = []

    def app(environ, start_response):
        start_response("200 OK", [])

        def gen():
            seen.append(current_span())
            yield b"a"
            yield b"b"

        return gen()

    body = _wsgi_app_wrapper(
        app, instance=object(),
        args=(_environ(), lambda *a, **k: None), kwargs={},
    )
    root = fake_agent.last_native
    child = fake_agent.last_async_native
    assert root.end_called is True
    assert child.end_called is False
    assert current_span() is None

    assert list(body) == [b"a", b"b"]
    assert seen and seen[0] is not None and seen[0].sampled
    assert child.end_called is True


def test_dispatch_view_exception_runs_view_once_through_safe_wrapper(monkeypatch):
    """A view that raises (e.g. ``abort(404)``) must execute exactly once and
    its exception must propagate. ``_dispatch_request_wrapper`` calls the view
    itself, so the ``safe_wrapper`` fallback must NOT re-run it — that would
    repeat side effects like payments and swallow the original error."""
    from pinpoint import context as ppctx

    calls = []

    class Abort(Exception):
        pass

    def view(*args, **kwargs):
        calls.append(1)
        raise Abort("404")

    # A non-None current span sends the wrapper down its instrumented path;
    # no request context means _resolve_view yields (None, None) and the
    # wrapper calls the view directly — exactly where the double-run bit.
    token = ppctx.set_current_span(object())
    try:
        shim = safe_wrapper(_dispatch_request_wrapper)
        with pytest.raises(Abort, match="404"):
            shim(view, object(), (), {})
    finally:
        ppctx.reset_current_span(token)

    assert calls == [1], "view must run exactly once, not be retried"


def test_wsgi_app_ends_span_when_setup_fails_after_creation(fake_agent, monkeypatch):
    """Regression (native span leak): if span setup raises *after* new_span
    created the native span (e.g. set_current_span), the request must still run
    untraced AND the already-created native span must be ended — not leaked."""
    def _boom(_span):
        raise RuntimeError("setup boom")

    monkeypatch.setattr(wsgi_instr, "set_current_span", _boom)

    calls = []

    def app(environ, start_response):
        calls.append(1)
        start_response("200 OK", [("content-type", "text/plain")])
        return [b"hi"]

    body = list(_wsgi_app_wrapper(
        app, instance=object(),
        args=(_environ(HTTP_PINPOINT_TRACEID="T-1"), lambda *a, **k: None),
        kwargs={},
    ))

    # Request still completes, untraced, exactly once.
    assert body == [b"hi"]
    assert calls == [1]
    # The created native span was ended — no leak.
    assert fake_agent.last_native.end_called is True
    assert ("span_end", "Flask HTTP Server") in fake_agent.events


def test_wsgi_app_passes_through_when_agent_disabled(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))
    seen = {}

    def app(environ, start_response):
        seen["called"] = True
        start_response("200 OK", [])
        return [b""]

    list(_wsgi_app_wrapper(
        app, instance=object(),
        args=(_environ(), lambda *a, **k: None), kwargs={},
    ))
    assert seen["called"] is True


def test_uninstrument_restores_both_flask_hooks_and_keeps_foreign_wrapper():
    """Flask must remove both Pinpoint layers without removing a later APM."""
    flask_app = pytest.importorskip("flask.app")
    from pinpoint.instrumentations import flask as flask_instr
    from pinpoint.instrumentations._util import restore_pinpoint_target
    from pinpoint.instrumentor import _global_installed

    instrumentor_cls = flask_instr.FlaskInstrumentor
    _global_installed.pop(instrumentor_cls, None)
    restore_pinpoint_target("flask.app", "Flask.wsgi_app")
    restore_pinpoint_target("flask.app", "Flask.dispatch_request")
    original_wsgi = flask_app.Flask.__dict__["wsgi_app"]
    original_dispatch = flask_app.Flask.__dict__["dispatch_request"]

    def _foreign(wrapped, instance, args, kwargs):
        return wrapped(*args, **kwargs)

    inst = instrumentor_cls()
    try:
        inst.instrument()
        assert flask_app.Flask.__dict__["wsgi_app"] is not original_wsgi
        assert flask_app.Flask.__dict__["dispatch_request"] is not original_dispatch

        wrapt.wrap_function_wrapper(
            flask_app.Flask, "dispatch_request", _foreign
        )
        foreign_dispatch = flask_app.Flask.__dict__["dispatch_request"]

        inst.uninstrument()

        assert flask_app.Flask.__dict__["wsgi_app"] is original_wsgi
        assert flask_app.Flask.__dict__["dispatch_request"] is foreign_dispatch
        current = foreign_dispatch
        while current is not None:
            wrapper = getattr(current, "_self_wrapper", None)
            assert not getattr(wrapper, "__pinpoint_wrapper__", False)
            current = getattr(current, "__wrapped__", None)
    finally:
        flask_app.Flask.wsgi_app = original_wsgi
        flask_app.Flask.dispatch_request = original_dispatch
        _global_installed.pop(instrumentor_cls, None)


# ---------------------------------------------------------------------------
# _dispatch_request_wrapper — view span EVENT control-flow filtering
# (mirrors aiohttp's test_traced_handler_does_not_record_control_flow_*).
# ``flask.abort(404)`` raises a werkzeug ``HTTPException`` (``NotFound``) which
# propagates out of ``dispatch_request``; that is normal control flow, so the
# view span event must NOT be flagged as an error.
# ---------------------------------------------------------------------------

class _FakeWerkzeugHTTPException(Exception):
    """Mimics ``werkzeug.exceptions.HTTPException`` — the base flask's
    ``abort()`` raises. Concrete errors carry a numeric ``.code``."""
    code = -1


class _FakeNotFound(_FakeWerkzeugHTTPException):
    code = 404


class _FakeServerError(_FakeWerkzeugHTTPException):
    code = 500


# Match werkzeug's real class identity — name *and* defining module — so the
# module-gated classifier fires for these fakes exactly as for the genuine type.
_FakeWerkzeugHTTPException.__name__ = "HTTPException"
_FakeWerkzeugHTTPException.__module__ = "werkzeug.exceptions"


def _run_dispatch_raising(exc, fake_agent, monkeypatch):
    """Drive ``_dispatch_request_wrapper`` under a real root span with a view
    that raises ``exc``. Stub view resolution so the span-event path runs
    without a live flask request context."""
    from pinpoint import context as ppctx
    from pinpoint.instrumentations import flask as flask_mod

    monkeypatch.setattr(
        flask_mod, "_resolve_view",
        lambda app: ("UserView.get", "/items/<id>"),
    )
    span = fake_agent.new_span("root", "/items/42")
    token = ppctx.set_current_span(span)
    try:
        def view(*_a, **_kw):
            raise exc
        flask_mod._dispatch_request_wrapper(view, object(), (), {})
    finally:
        ppctx.reset_current_span(token)


def test_dispatch_request_does_not_record_control_flow_http_exception(
        fake_agent, monkeypatch):
    """A view raising ``abort(404)`` (werkzeug ``NotFound``) is normal control
    flow — the view span event is created but NOT marked as an error."""
    with pytest.raises(_FakeNotFound):
        _run_dispatch_raising(_FakeNotFound(), fake_agent, monkeypatch)
    assert any(e[0] == "event_start" for e in fake_agent.events)
    assert not any(e[0] == "event_error" for e in fake_agent.events)


def test_dispatch_request_records_5xx_http_exception(fake_agent, monkeypatch):
    """A 5xx ``HTTPException`` is a genuine server error and must be recorded."""
    with pytest.raises(_FakeServerError):
        _run_dispatch_raising(_FakeServerError(), fake_agent, monkeypatch)
    assert any(e[0] == "event_error" for e in fake_agent.events)


def test_dispatch_request_records_generic_exception(fake_agent, monkeypatch):
    """A non-HTTP exception is always a real failure."""
    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        _run_dispatch_raising(Boom("kaput"), fake_agent, monkeypatch)
    assert any(e[0] == "event_error" for e in fake_agent.events)


def test_is_control_flow_exception_classifies_real_werkzeug_by_status():
    """Against the genuine werkzeug types (installed): sub-500 is control flow,
    5xx is a real error."""
    we = pytest.importorskip("werkzeug.exceptions")
    from pinpoint.instrumentations import flask as flask_mod

    assert flask_mod._is_control_flow_exception(we.NotFound()) is True
    assert flask_mod._is_control_flow_exception(we.BadRequest()) is True
    assert flask_mod._is_control_flow_exception(we.InternalServerError()) is False


def test_is_control_flow_exception_ignores_foreign_and_generic():
    """A generic exception, and an unrelated class merely *named*
    ``HTTPException`` from another module, must never be swallowed."""
    from pinpoint.instrumentations import flask as flask_mod

    assert flask_mod._is_control_flow_exception(RuntimeError("x")) is False

    class HTTPException(Exception):  # not werkzeug's — must not match
        code = 404

    assert flask_mod._is_control_flow_exception(HTTPException()) is False


def test_instrument_succeeds_from_a_flask_app_post_import_hook():
    """Autoload fires on ``flask.app`` while ``flask/__init__.py`` is still
    executing ``from .app import Flask`` — ``flask.request`` is not bound yet.
    ``_instrument`` must not import it from the ``flask`` package or the whole
    integration silently rolls back (regression)."""
    import subprocess
    import sys

    code = r"""
import sys, wrapt
from pinpoint.instrumentations import flask as pf
from pinpoint.instrumentations._util import already_wrapped
wrapt.register_post_import_hook(lambda m: pf.instrument(), "flask.app")
import flask
sys.exit(0 if already_wrapped("flask.app", "Flask.wsgi_app")
         and already_wrapped("flask.app", "Flask.dispatch_request") else 1)
"""
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
