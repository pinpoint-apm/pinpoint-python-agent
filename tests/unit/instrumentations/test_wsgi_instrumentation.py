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

"""Generic WSGI middleware.

Drives ``PinpointWSGIMiddleware`` against a tiny WSGI app and a fake agent —
no real WSGI server required. Validates root-span lifecycle, status code
capture, header extraction, and error propagation.
"""

from __future__ import annotations

import asyncio
import contextvars

import pytest
from _fakes import (FakeAgent as _FakeAgent,
                    FakeNativeSpan as _FakeNativeSpan, UnsampledAgent,
                    http_scope, wsgi_environ)

import pinpoint
from pinpoint.instrumentations.asgi import PinpointASGIMiddleware
from pinpoint.instrumentations import wsgi as wsgi_instr
from pinpoint.instrumentations.wsgi import (
    PinpointWSGIMiddleware,
    _ClosingIterable,
    _end_span,
)
from pinpoint.tracer import Span


def _make_environ(**overrides):
    return wsgi_environ(path="/users/42", trace_id="T-1", **overrides)


# ---------------------------------------------------------------------------
# PinpointWSGIMiddleware
# ---------------------------------------------------------------------------

def test_middleware_creates_root_span_and_captures_status(fake_agent):
    captured = {}

    def app(environ, start_response):
        captured["environ"] = environ
        start_response("200 OK", [("content-type", "text/plain")])
        return [b"hi"]

    mw = PinpointWSGIMiddleware(app)
    body = list(mw(_make_environ(), lambda *a, **k: None))

    assert body == [b"hi"]
    assert ("span_start", "WSGI HTTP Server", "/users/42") in fake_agent.events
    assert ("span_end", "WSGI HTTP Server") in fake_agent.events
    assert fake_agent.last_native.status_code == 200
    assert fake_agent.last_native.url_stats == [("/users/42", "GET", 200)]
    # Reaches the native Http.Server.ExcludeMethod filter, which is skipped
    # for an empty method.
    assert fake_agent.last_method == "GET"
    # Upstream Pinpoint header present -> reader-backed native call.
    assert fake_agent.last_native.headers is not None


def test_middleware_forwards_start_response_arity(fake_agent):
    """A two-parameter ``start_response`` further down the chain must still work.

    PEP 3333 servers take the third argument, but hand-written middleware
    routinely declares only ``(status, headers)``. Passing an explicit
    ``exc_info=None`` through to one of those would TypeError mid-response, so
    the wrapper forwards the arity the app actually used.
    """
    seen = []

    def two_arg_start_response(status, response_headers):
        seen.append((status, response_headers))

    def app(environ, start_response):
        start_response("201 Created", [("content-type", "text/plain")])
        return [b"made"]

    mw = PinpointWSGIMiddleware(app)
    body = list(mw(_make_environ(), two_arg_start_response))

    assert body == [b"made"]
    assert seen == [("201 Created", [("content-type", "text/plain")])]
    # The status still reaches the span despite the narrower signature.
    assert fake_agent.last_native.status_code == 201


def test_middleware_forwards_exc_info_when_the_app_supplies_it(fake_agent):
    """The third argument is passed through untouched when the app sends one."""
    seen = []

    def three_arg_start_response(status, response_headers, exc_info=None):
        seen.append(exc_info)

    exc_info = (RuntimeError, RuntimeError("boom"), None)

    def app(environ, start_response):
        start_response("500 Internal Server Error", [], exc_info)
        return [b""]

    mw = PinpointWSGIMiddleware(app)
    list(mw(_make_environ(), three_arg_start_response))

    assert seen == [exc_info]


def test_middleware_skips_reader_when_no_upstream_context(fake_agent):
    """Without an HTTP_PINPOINT_* header, new_span is called without a reader,
    yet the span still opens, records url_stat, and closes."""
    def app(environ, start_response):
        start_response("200 OK", [("content-type", "text/plain")])
        return [b"hi"]

    environ = {
        "REQUEST_METHOD": "GET",
        "PATH_INFO": "/users/42",
        "HTTP_HOST": "example.test",
        "SERVER_NAME": "example.test",
        "wsgi.url_scheme": "http",
        "REMOTE_ADDR": "10.0.0.1",
        "HTTP_X_TRACE": "abc",
    }
    mw = PinpointWSGIMiddleware(app)
    body = list(mw(environ, lambda *a, **k: None))

    assert body == [b"hi"]
    assert fake_agent.last_native.headers is None
    assert ("span_start", "WSGI HTTP Server", "/users/42") in fake_agent.events
    assert fake_agent.last_native.url_stats == [("/users/42", "GET", 200)]
    assert ("span_end", "WSGI HTTP Server") in fake_agent.events


@pytest.mark.parametrize(
    ("capture_enabled", "expected_headers"),
    [(False, []), (True, [("x-response", "yes")])],
)
def test_middleware_honors_response_header_capture_gate(
        fake_agent, monkeypatch, capture_enabled, expected_headers):
    captured = []
    monkeypatch.setattr(
        wsgi_instr, "record_response_header_enabled",
        lambda _span: capture_enabled,
    )
    from pinpoint import http_helper
    # The response trace now flows through _util.end_server_span, which reads
    # trace_http_server_response off the http_helper module at call time.
    monkeypatch.setattr(
        http_helper, "trace_http_server_response",
        lambda span, url, method, status, headers: captured.extend(headers),
    )

    def app(environ, start_response):
        start_response("200 OK", [("x-response", "yes")])
        return [b"hi"]

    list(PinpointWSGIMiddleware(app)(
        _make_environ(), lambda *args, **kwargs: None,
    ))

    assert captured == expected_headers


def _run_unsampled(monkeypatch, native, collect_url_stat):
    monkeypatch.setattr(pinpoint.agent, "_instance",
                        UnsampledAgent(native, collect_url_stat))

    expected_body = [b"hi"]

    def app(environ, start_response):
        start_response("200 OK", [("content-type", "text/plain")])
        return expected_body

    mw = PinpointWSGIMiddleware(app)
    body = mw(_make_environ(), lambda *a, **k: None)
    assert body is expected_body
    list(body)


def test_middleware_unsampled_captures_status_when_url_stat_enabled(monkeypatch):
    """Unsampled + URL-stat collection on: start_response is still wrapped so
    the status reaches ``set_url_stat`` at span end."""
    native = _FakeNativeSpan()
    _run_unsampled(monkeypatch, native, collect_url_stat=True)
    assert native.url_stats == [("/users/42", "GET", 200)]


def test_middleware_unsampled_reports_500_before_start_response(monkeypatch):
    """A failing app raises before start_response, so the middleware never sees
    a status — but the server answers 500, so that is what the URL stat gets.
    Consistent with the ASGI/aiohttp transports, which force the same 500, and
    with tornado, which reads its own 500 back off the handler."""
    from pinpoint.agent import UnSampledSpan

    native = _FakeNativeSpan()

    class _NullAgentStub:
        enabled = True

        def new_span(self, operation, rpc_point, headers=None, method=""):
            return UnSampledSpan(native, collect_url_stat=True)

    monkeypatch.setattr(pinpoint.agent, "_instance", _NullAgentStub())

    def app(environ, start_response):
        raise RuntimeError("before start_response")

    with pytest.raises(RuntimeError, match="before start_response"):
        PinpointWSGIMiddleware(app)(
            _make_environ(), lambda *args, **kwargs: None,
        )

    assert native.url_stats == [("/users/42", "GET", 500)]
    assert native.end_called is True    # span still finalized


def test_middleware_unsampled_skips_start_response_wrap_without_url_stat(monkeypatch):
    """Unsampled + URL-stat collection off (default): start_response is passed
    through untouched, so no status is captured and the span ends plainly. The
    eager body is returned directly with no response-span wrapper."""
    native = _FakeNativeSpan()
    _run_unsampled(monkeypatch, native, collect_url_stat=False)
    assert native.url_stats == []
    assert native.end_called is True


def test_middleware_records_exception_and_reraises(fake_agent):
    def app(environ, start_response):
        raise RuntimeError("boom")

    mw = PinpointWSGIMiddleware(app)
    with pytest.raises(RuntimeError, match="boom"):
        mw(_make_environ(), lambda *a, **k: None)

    assert any(e[0] == "span_error" for e in fake_agent.events)
    assert ("span_end", "WSGI HTTP Server") in fake_agent.events


def test_middleware_passes_through_when_agent_disabled(monkeypatch):
    monkeypatch.setattr(pinpoint.agent, "_instance", _FakeAgent(enabled=False))

    seen = {}

    def app(environ, start_response):
        seen["called"] = True
        start_response("200 OK", [])
        return [b""]

    mw = PinpointWSGIMiddleware(app)
    body = list(mw(_make_environ(), lambda *a, **k: None))
    assert body == [b""]
    assert seen["called"] is True


def test_middleware_passes_through_when_live_span_in_context(fake_agent):
    """A live span already in the contextvar (e.g. Starlette WSGIMiddleware
    mounted under an instrumented ASGI app: fresh environ, no marker, but the
    ASGI root span is visible via the threadpool-copied context) means an
    enclosing Pinpoint layer owns the request — the WSGI runner must not open
    a second root span."""
    from pinpoint.context import reset_current_span, set_current_span

    parent_native = _FakeNativeSpan("PARENT", "/", fake_agent)
    token = set_current_span(Span(parent_native))
    try:
        def app(environ, start_response):
            start_response("200 OK", [])
            return [b""]

        mw = PinpointWSGIMiddleware(app)
        list(mw(_make_environ(), lambda *a, **k: None))
    finally:
        reset_current_span(token)

    # No new root span was opened — only the pre-existing PARENT exists.
    assert not any(
        e == ("span_start", "WSGI HTTP Server", "/users/42")
        for e in fake_agent.events
    )


def test_middleware_opens_fresh_root_span_when_context_span_already_ended(fake_agent):
    """The common leak shape — a previous request's span whose scope ended but
    whose contextvar reset was skipped, persisting on a reused worker thread —
    must NOT suppress tracing: current_span() returns None for an ended span,
    so this request opens its own root span instead of silently vanishing."""
    from pinpoint.context import reset_current_span, set_current_span

    stale_native = _FakeNativeSpan("STALE", "/old", fake_agent)
    stale = Span(stale_native)
    token = set_current_span(stale)
    stale.end()  # scope ended, but the contextvar was never reset (the leak)
    try:
        def app(environ, start_response):
            start_response("200 OK", [])
            return [b"hi"]

        mw = PinpointWSGIMiddleware(app)
        body = list(mw(_make_environ(), lambda *a, **k: None))
    finally:
        reset_current_span(token)

    assert body == [b"hi"]
    # A fresh root span WAS opened for this request despite the stale binding.
    assert ("span_start", "WSGI HTTP Server", "/users/42") in fake_agent.events
    assert ("span_end", "WSGI HTTP Server") in fake_agent.events


def test_middleware_consumes_url_pattern_when_set_by_inner_app(fake_agent):
    """If a downstream layer stashes a route template under
    ``pinpoint.url_pattern`` we record stats against the template, not the
    concrete path. This mirrors what the Flask integration does."""

    def app(environ, start_response):
        environ["pinpoint.url_pattern"] = "/users/<id>"
        start_response("200 OK", [])
        return [b""]

    mw = PinpointWSGIMiddleware(app)
    list(mw(_make_environ(), lambda *a, **k: None))
    assert fake_agent.last_native.url_stats == [("/users/<id>", "GET", 200)]


def test_middleware_close_is_invoked_to_finalize_span(fake_agent):
    closed = {"flag": False}

    class _Iterable:
        def __iter__(self):
            yield b"chunk"

        def close(self):
            closed["flag"] = True

    def app(environ, start_response):
        start_response("204 No Content", [])
        return _Iterable()

    mw = PinpointWSGIMiddleware(app)
    body = mw(_make_environ(), lambda *a, **k: None)

    # Drain the iterable then explicitly close — what a WSGI server does.
    list(body)
    body.close()
    assert closed["flag"] is True
    assert fake_agent.last_native.end_called is True


# ---------------------------------------------------------------------------
# Streaming / file_wrapper / len / cross-thread finalization
# ---------------------------------------------------------------------------

def test_streaming_body_keeps_span_active_and_ends_after_drain(fake_agent):
    """The request root ends on its owner thread; a linked async child remains
    current while the response body is drained."""
    from pinpoint.context import current_span

    seen = []

    def app(environ, start_response):
        start_response("200 OK", [])

        def gen():
            seen.append(current_span())
            yield b"a"
            seen.append(current_span())
            yield b"b"

        return gen()

    mw = PinpointWSGIMiddleware(app)
    body = mw(_make_environ(), lambda *a, **k: None)

    # Returning from __call__ ends the request root on this thread and leaves
    # the dedicated response child for the drain context.
    assert fake_agent.last_native.end_called is True
    assert fake_agent.last_async_native.end_called is False
    assert current_span() is None

    chunks = list(body)
    assert chunks == [b"a", b"b"]
    # The response child was active while the streamed body ran and ended only
    # after consumption.
    assert seen and all(s is not None and s.sampled for s in seen)
    assert fake_agent.last_async_native.end_called is True
    assert current_span() is None


def test_streaming_body_exception_records_error_and_ends_span(fake_agent):
    """An exception raised mid-stream (a generator body that fails on a later
    pull — e.g. a DB cursor dying mid-iteration) must be recorded on the span,
    end the span exactly once, and re-raise, leaving the contextvar clean. Every
    other streaming test drains to clean exhaustion, so ``__next__``'s
    ``except BaseException`` branch (record + finalize) was uncovered."""
    from pinpoint.context import current_span

    def app(environ, start_response):
        start_response("200 OK", [])

        def gen():
            yield b"a"
            raise RuntimeError("stream died")

        return gen()

    mw = PinpointWSGIMiddleware(app)
    body = mw(_make_environ(), lambda *a, **k: None)

    got = []
    raised = False
    try:
        for chunk in body:
            got.append(chunk)
    except RuntimeError:
        raised = True

    assert got == [b"a"]
    assert raised, "the mid-stream exception must propagate to the server"
    # Error recorded on the response child, both spans ended, context clean.
    assert any(e[0] == "span_error" for e in fake_agent.events)
    assert fake_agent.last_native.end_called is True
    assert fake_agent.last_async_native.end_called is True
    assert current_span() is None


def test_app_returning_non_iterable_ends_span_and_passes_result_through(fake_agent):
    """A misbehaving app that returns a non-iterable (``None``) hits the
    ``finalize_wsgi_response`` fallback: ``iter(None)`` raises, so the span is
    ended (not leaked) and the result handed back unchanged for the server to
    surface its own error. Pinpoint must not raise from the wrapper."""
    def app(environ, start_response):
        start_response("200 OK", [])
        return None

    mw = PinpointWSGIMiddleware(app)
    body = mw(_make_environ(), lambda *a, **k: None)

    assert body is None                                # handed back untouched
    assert fake_agent.last_native.end_called is True   # span ended, not leaked


def test_file_wrapper_response_returned_unwrapped(fake_agent):
    """A ``wsgi.file_wrapper`` body must be handed back unwrapped so the
    server's ``isinstance(result, file_wrapper)`` sendfile check still fires;
    the span ends immediately since the app's work is done."""
    class FileWrapper:
        def __init__(self, filelike, block_size=8192):
            self.filelike = filelike

        def __iter__(self):
            return iter([b"filedata"])

        def close(self):
            pass

    def app(environ, start_response):
        start_response("200 OK", [])
        return environ["wsgi.file_wrapper"](object())

    environ = _make_environ()
    environ["wsgi.file_wrapper"] = FileWrapper
    mw = PinpointWSGIMiddleware(app)
    body = mw(environ, lambda *a, **k: None)

    assert isinstance(body, FileWrapper)  # not masked by our wrapper
    assert fake_agent.last_native.end_called is True
    assert fake_agent.last_async_native is None
    assert list(body) == [b"filedata"]


def test_len_bearing_response_preserves_len(fake_agent):
    """A built-in eager body is returned directly, preserving identity/len."""
    expected_body = [b"onechunk"]

    def app(environ, start_response):
        start_response("200 OK", [])
        return expected_body

    mw = PinpointWSGIMiddleware(app)
    body = mw(_make_environ(), lambda *a, **k: None)

    assert body is expected_body
    assert len(body) == 1
    assert list(body) == [b"onechunk"]
    assert fake_agent.last_native.end_called is True
    assert fake_agent.last_async_native is None


def test_cross_thread_drain_uses_distinct_async_span(fake_agent):
    """The worker drains a response child and never receives the request root."""
    import threading

    from pinpoint.context import current_span

    def app(environ, start_response):
        start_response("200 OK", [])

        def gen():
            yield b"x"

        return gen()

    mw = PinpointWSGIMiddleware(app)
    body = mw(_make_environ(), lambda *a, **k: None)

    # Origin context is clean and its root is already finalized; the child has
    # not yet been driven by the worker.
    assert current_span() is None
    root_native = fake_agent.last_native
    response_native = fake_agent.last_async_native
    assert root_native.end_called is True
    assert response_native.end_called is False

    errors = []

    def drain():
        try:
            list(body)
            body.close()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    t = threading.Thread(target=drain)
    t.start()
    t.join()

    assert errors == []
    assert root_native.end_called is True
    assert response_native.end_called is True
    # Still clean on the origin thread — no stale span lingering.
    assert current_span() is None


# ---------------------------------------------------------------------------
# _ClosingIterable.close(): body teardown runs under the request span
# ---------------------------------------------------------------------------

def test_closing_iterable_close_runs_body_cleanup_under_request_span(fake_agent):
    """Cleanup that runs at ``close()`` time — a generator ``finally:`` firing
    on ``GeneratorExit``, ``werkzeug`` on-close callbacks, ``stream_with_context``
    teardown — must execute with the request span active, exactly like
    ``__next__`` activates it per pull. Drives ``_ClosingIterable.close()``
    directly for the client-disconnect shape (server closes the body before it
    is exhausted, so the span is not yet ended)."""
    from pinpoint.context import current_span

    span = fake_agent.new_span("WSGI HTTP Server", "/stream")
    seen = []

    class _Body:
        def __iter__(self):
            return iter([b"a", b"b"])

        def close(self):
            # Records the span that is current while teardown runs.
            seen.append(current_span())

    it = _ClosingIterable(_Body(), span, _make_environ(), [200, []],
                          True, _end_span)

    # Disconnect-before-drain: nothing set the span in this context, and the
    # body was never exhausted, so the span is still live when close() runs.
    assert current_span() is None
    it.close()

    # The regression: teardown ran under the request span, not with None.
    assert seen == [span]
    # Span ended once and the contextvar was reset in the drain context.
    assert fake_agent.last_native.end_called is True
    assert current_span() is None


def test_closing_iterable_close_ends_span_once_when_body_close_raises(fake_agent):
    """If the wrapped body's ``close()`` raises (an error inside a ``finally:``
    at ``GeneratorExit``), the span is still active during teardown, is ended
    exactly once, and the contextvar is reset — with the exception propagating
    to the server unchanged."""
    from pinpoint.context import current_span

    span = fake_agent.new_span("WSGI HTTP Server", "/stream")
    seen = []
    ends = []

    def _counting_end_span(*args, **kwargs):
        ends.append(args)
        return _end_span(*args, **kwargs)

    class _Body:
        def __iter__(self):
            return iter([b"a"])

        def close(self):
            seen.append(current_span())
            raise RuntimeError("cleanup boom")

    it = _ClosingIterable(_Body(), span, _make_environ(), [200, []],
                          True, _counting_end_span)

    with pytest.raises(RuntimeError, match="cleanup boom"):
        it.close()

    # Teardown still ran under the request span.
    assert seen == [span]
    # Ended exactly once despite the raising close().
    assert len(ends) == 1
    assert fake_agent.last_native.end_called is True
    # Reset ran in the finally even though close() raised — no leaked span.
    assert current_span() is None


def test_closing_iterable_next_activates_span_during_pull(fake_agent):
    """Guard that ``__next__`` still activates the span for each pull and resets
    it afterwards — unchanged by the close() fix."""
    from pinpoint.context import current_span

    span = fake_agent.new_span("WSGI HTTP Server", "/stream")
    seen = []

    def gen():
        seen.append(current_span())
        yield b"a"
        seen.append(current_span())
        yield b"b"

    it = _ClosingIterable(gen(), span, _make_environ(), [200, []],
                          True, _end_span)

    assert current_span() is None
    chunks = list(it)
    assert chunks == [b"a", b"b"]
    # Span was current during each pull, and reset after each step.
    assert seen == [span, span]
    assert current_span() is None
    # Exhaustion (StopIteration) finalized the span.
    assert fake_agent.last_native.end_called is True


# ---------------------------------------------------------------------------
# WSGI mounted inside an instrumented ASGI app (Starlette WSGIMiddleware shape)
# ---------------------------------------------------------------------------

class _FakeWSGIMiddleware:
    """Mimics Starlette's ``WSGIMiddleware``.

    It builds a *fresh* CGI-only environ from the ASGI scope — so the
    ``pinpoint.root_span_active`` marker never crosses the boundary — and runs
    the WSGI app in a threadpool thread that copies the calling context (which
    is what ``anyio.to_thread.run_sync`` does). That copied context carries the
    ASGI root span's contextvar into the WSGI thread.
    """

    def __init__(self, wsgi_app):
        self._wsgi_app = wsgi_app

    async def __call__(self, scope, receive, send):
        environ = {
            "REQUEST_METHOD": scope.get("method", "GET"),
            "PATH_INFO": scope.get("path", "/"),
            "SERVER_NAME": "testserver",
            "SERVER_PORT": "80",
            "wsgi.url_scheme": "http",
        }
        captured = {"status": 200}

        def start_response(status, headers, exc_info=None):
            try:
                captured["status"] = int(str(status).split(" ", 1)[0])
            except Exception:  # noqa: BLE001
                pass

        def run_wsgi():
            body = self._wsgi_app(environ, start_response)
            try:
                return b"".join(body)
            finally:
                close = getattr(body, "close", None)
                if close is not None:
                    close()

        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        chunks = await loop.run_in_executor(None, lambda: ctx.run(run_wsgi))

        await send({"type": "http.response.start",
                    "status": captured["status"], "headers": []})
        await send({"type": "http.response.body", "body": chunks})


def _http_scope():
    return http_scope(path="/legacy/users/42")


def _drive_asgi(app):
    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(_msg):
        pass

    async def main():
        await app(_http_scope(), receive, send)

    asyncio.run(main())


def test_wsgi_mounted_in_asgi_yields_single_root_span(fake_agent):
    """A WSGI app mounted inside an instrumented ASGI app (via WSGIMiddleware)
    must open exactly one root span — the WSGI wrapper detects the active ASGI
    transaction through the copied contextvar and skips re-rooting."""

    def wsgi_app(environ, start_response):
        start_response("200 OK", [("content-type", "text/plain")])
        return [b"legacy"]

    app = PinpointASGIMiddleware(
        _FakeWSGIMiddleware(PinpointWSGIMiddleware(wsgi_app)),
        framework_name="ASGI",
    )
    _drive_asgi(app)

    span_starts = [e for e in fake_agent.events if e[0] == "span_start"]
    span_ends = [e for e in fake_agent.events if e[0] == "span_end"]
    assert len(span_starts) == 1, span_starts
    assert span_starts[0][1] == "ASGI HTTP Server"
    assert len(span_ends) == 1, span_ends


def test_standalone_wsgi_through_threadpool_still_roots(fake_agent):
    """The contextvar guard must not suppress a legitimate standalone WSGI root
    span: driving the same WSGIMiddleware shape *without* an enclosing ASGI
    layer (no active span in the copied context) still opens one root span."""

    def wsgi_app(environ, start_response):
        start_response("200 OK", [("content-type", "text/plain")])
        return [b"legacy"]

    # No PinpointASGIMiddleware wrapper -> nothing sets current_span before the
    # threadpool context is copied.
    app = _FakeWSGIMiddleware(PinpointWSGIMiddleware(wsgi_app))
    _drive_asgi(app)

    span_starts = [e for e in fake_agent.events
                   if e[:2] == ("span_start", "WSGI HTTP Server")]
    assert len(span_starts) == 1, span_starts


def test_buffered_hint_finalizes_root_without_drain_child(fake_agent):
    """A framework-stashed ``pinpoint.response_buffered`` hint makes the root
    finalize eagerly for an opaque (non-list) body: no async drain child, and
    the body is handed back unwrapped."""
    class _OpaqueBody:
        def __init__(self):
            self._it = iter([b"hi"])

        def __iter__(self):
            return self

        def __next__(self):
            return next(self._it)

    def app(environ, start_response):
        environ["pinpoint.response_buffered"] = True
        start_response("200 OK", [])
        return _OpaqueBody()

    mw = PinpointWSGIMiddleware(app)
    result = mw(_make_environ(), lambda *a, **k: None)

    assert isinstance(result, _OpaqueBody)
    assert fake_agent.last_native.end_called is True
    assert fake_agent.last_async_native is None
    assert list(result) == [b"hi"]


def test_abandoned_response_iterable_is_reaped():
    """A server that drops a partially-drained iterable without close()
    (PEP 3333 violation) must not leak the async child span: the GC reaper
    ends it. Cooperative paths detach the reaper — no double end."""
    import gc
    from pinpoint.instrumentations.wsgi import _ClosingIterable

    ended = []

    def end_span(span, environ, status, headers, sampled):
        ended.append((span, status, sampled))

    class _Span:
        def _detached_context_span(self):
            return None

    span = _Span()
    it = _ClosingIterable(iter([b"a", b"b"]), span, {}, [200, ()], True,
                          end_span)
    next(it)
    del it
    gc.collect()
    assert ended == [(span, 200, True)]

    # Cooperative close() detaches the reaper: exactly one end.
    ended.clear()
    it = _ClosingIterable(iter([b"a"]), span, {}, [204, ()], True, end_span)
    it.close()
    del it
    gc.collect()
    assert ended == [(span, 204, True)]
