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

"""gunicorn worker classes × the real agent, over real HTTP and real gRPC.

Each case launches ``gunicorn -k <worker>`` on a Flask app whose module calls
``pinpoint.init()`` and ``autoload()`` at import time, without ``--preload``
-- so every worker starts its own native agent after gunicorn has set the
worker up (for gevent and eventlet: after the monkey-patching). The case then
sends concurrent requests whose handlers call a slow downstream through
``requests``, so a gevent/eventlet worker interleaves several transactions on
one OS thread, and asserts that

- every request is answered well within its timeout, so a worker that the
  agent wedges fails here instead of hanging a deployment;
- every outbound call carries the Pinpoint headers of its own transaction,
  so the instrumentations work under the patched sockets and the active-span
  context stays per greenlet;
- every inbound span reaches ``_collector.MockCollector`` over gRPC, linked
  to the downstream span id its outbound call announced, so the native
  agent's sender threads keep running next to the event loop.

Needs no Docker daemon (``no_docker``); a missing gunicorn, Flask, requests,
gevent or eventlet skips only the cases that need it.
"""

from __future__ import annotations

import concurrent.futures
import http.server
import os
import signal
import socket
import subprocess
import sys
import textwrap
import threading
import time
import urllib.request

import pytest

pytestmark = pytest.mark.no_docker

pytest.importorskip("grpc")
pytest.importorskip("grpc_tools")
pytest.importorskip("gunicorn")
pytest.importorskip("flask")
pytest.importorskip("requests")

from pinpoint import propagator

from tests.integration._collector import MockCollector

_APP_NAME = "gunicorn-it-app"
_CONCURRENT_REQUESTS = 8
# Long enough that a cooperative worker switches to the other requests while
# one waits on the downstream.
_DOWNSTREAM_DELAY_SECONDS = 0.2
# Generous for a local request (they take milliseconds plus the downstream
# delay), short enough that a wedged worker fails the case quickly.
_REQUEST_TIMEOUT_SECONDS = 5.0
_BOOT_TIMEOUT_SECONDS = 30.0
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))

_APP_TEMPLATE = textwrap.dedent('''\
    import pinpoint
    import pinpoint.autoload

    pinpoint.init(
        server_info="Gunicorn",
        application_name={app_name!r},
        agent_name="gunicorn-it",
        service_name="gunicorn-it-service",
        collector_host="127.0.0.1",
        collector_agent_port={collector_port},
        collector_span_port={collector_port},
        collector_stat_port={collector_port},
        span_batch_flush_interval_ms=100,
        span_batch_collect_deadline_ms=50,
        agent_info_send_retry_interval_ms=200,
        stat_enabled=False,
    )
    pinpoint.autoload.autoload()

    import requests
    from flask import Flask

    app = Flask(__name__)


    @app.route("/gunicorn/<name>")
    def call_downstream(name):
        resp = requests.get({downstream_url!r} + "/" + name, timeout=5)
        return "ok " + resp.text
''')


class _Downstream(http.server.ThreadingHTTPServer):
    """Answers after a delay and records the Pinpoint headers, by path."""

    daemon_threads = True

    def __init__(self):
        self.headers_by_path: dict[str, dict[str, str]] = {}
        self.lock = threading.Lock()
        super().__init__(("127.0.0.1", 0), _DownstreamHandler)


class _DownstreamHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 -- BaseHTTPRequestHandler's naming
        with self.server.lock:
            self.server.headers_by_path[self.path] = {
                key: value for key, value in self.headers.items()
                if key.lower().startswith("pinpoint-")}
        time.sleep(_DOWNSTREAM_DELAY_SECONDS)
        body = b"downstream"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):  # noqa: A002 -- stdlib signature
        pass


@pytest.fixture(scope="module")
def collector():
    c = MockCollector().start()
    yield c
    c.stop()


@pytest.fixture(scope="module")
def downstream():
    server = _Downstream()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_listening(proc: subprocess.Popen, port: int) -> None:
    deadline = time.monotonic() + _BOOT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise AssertionError(f"gunicorn exited during boot ({proc.returncode})")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise AssertionError(f"gunicorn did not listen on {port} in time")


def _stop(proc: subprocess.Popen) -> str:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
    try:
        out, _ = proc.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate()
    return out


def _get(url: str) -> tuple[int, bytes, float]:
    started = time.monotonic()
    with urllib.request.urlopen(url, timeout=_REQUEST_TIMEOUT_SECONDS) as resp:
        return resp.status, resp.read(), time.monotonic() - started


def _trace_id(span) -> str:
    tx = span.transactionId
    return f"{tx.agentId}^{tx.agentStartTime}^{tx.sequence}"


@pytest.mark.parametrize(("worker", "extra_args"), [
    pytest.param("sync", [], id="sync"),
    pytest.param("gthread", ["--threads", "4"], id="gthread"),
    pytest.param("gevent", [], id="gevent"),
    pytest.param("eventlet", [], id="eventlet"),
])
def test_worker_answers_requests_and_sends_spans(
    collector, downstream, tmp_path, worker, extra_args,
):
    if worker in ("gevent", "eventlet"):
        pytest.importorskip(worker)
    (tmp_path / "gunicorn_it_app.py").write_text(_APP_TEMPLATE.format(
        app_name=_APP_NAME,
        collector_port=collector.port,
        downstream_url=f"http://127.0.0.1:{downstream.server_port}",
    ))
    port = _free_port()
    # The dev shell may point PINPOINT_PY_* at a live collector, and native
    # reads them over the YAML, so none may leak into the workers.
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("PINPOINT_PY_")}
    # --chdir puts tmp_path first on the workers' path, so the checkout's
    # pinpoint/ must be put there too.
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (_REPO_ROOT, env.get("PYTHONPATH")) if p)
    proc = subprocess.Popen(
        [sys.executable, "-m", "gunicorn",
         "--chdir", str(tmp_path),
         "-b", f"127.0.0.1:{port}",
         "-w", "2", "-k", worker, *extra_args,
         "--graceful-timeout", "5",
         "gunicorn_it_app:app"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    names = [f"{worker}-{i}" for i in range(_CONCURRENT_REQUESTS)]
    try:
        _wait_listening(proc, port)
        with concurrent.futures.ThreadPoolExecutor(len(names)) as pool:
            results = list(pool.map(
                lambda name: _get(f"http://127.0.0.1:{port}/gunicorn/{name}"),
                names))
        for name, (status, body, elapsed) in zip(names, results):
            assert status == 200, name
            assert body == b"ok downstream", name
            assert elapsed < _REQUEST_TIMEOUT_SECONDS, name

        for name in names:
            with downstream.lock:
                sent = downstream.headers_by_path.get(f"/{name}", {})
            trace_id = sent.get(propagator.HEADER_TRACE_ID)
            assert trace_id, f"{name}: the outbound call carried no trace id"

            span = collector.wait_span(f"/gunicorn/{name}")
            assert _trace_id(span) == trace_id, name
            next_span_ids = [
                ev.nextEvent.messageEvent.nextSpanId for ev in span.spanEvent]
            assert int(sent[propagator.HEADER_SPAN_ID]) in next_span_ids, name
            assert sent[propagator.HEADER_PARENT_SPAN_ID] == str(span.spanId)
            collector.wait_agent_info(span.transactionId.agentId)

        trace_ids = {
            downstream.headers_by_path[f"/{name}"][propagator.HEADER_TRACE_ID]
            for name in names}
        assert len(trace_ids) == len(names), "transactions were mixed up"
    except BaseException:
        print(_stop(proc))
        raise
    output = _stop(proc)
    assert "Traceback" not in output, output
