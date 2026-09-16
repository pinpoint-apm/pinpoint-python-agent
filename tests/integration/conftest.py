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

"""Fixtures for the testcontainers-backed instrumentation integration suite.

Layout contract (keeps ``pytest -n auto --dist loadfile`` efficient):

- **one backing service per test module** — each module declares a
  module-scoped container fixture below, so an xdist worker running that file
  starts exactly the containers it needs and modules parallelize cleanly;
- containers are module-scoped, tests inside a file share them;
- every module ``pytest.importorskip``s its client library, so a missing
  driver (no wheel for this Python, etc.) skips instead of failing;
- no Docker daemon → the whole directory skips at collection time.

Span capture: real instrumentation wrappers + real client libraries + real
servers, with the native span sink replaced by ``_recorder`` (see its
docstring for why). ``traced`` pushes a recording span into the context —
enough for every client-side wrapper. ``consumer_agent`` additionally
installs a recording agent for the kafka/rabbitmq consumer wrappers that
open root spans via ``get_agent().new_span``.
"""

from __future__ import annotations

import os
import sys
import time
import urllib.request
from contextlib import contextmanager

import pytest

_here = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(os.path.dirname(_here))
if _root not in sys.path:
    sys.path.insert(0, _root)

# Ryuk (the testcontainers reaper) can't be reached on runtimes whose
# dynamic port forwarding comes up asynchronously (Rancher Desktop: the
# mapped port answers only seconds after the container is Up, and the
# reaper's connect loop gives up). Every container here is scoped by a
# ``with``-managed fixture, so cleanup doesn't depend on the reaper —
# disable it by default, overridable via the env var.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

from tests.integration._recorder import (  # noqa: E402
    IT_INJECT_BASE,
    Recorder,
    RecordingAgent,
    RecordingNativeSpan,
)

# ---------------------------------------------------------------------------
# Docker gate — skip the container-backed modules when no daemon is
# reachable. Modules marked ``no_docker`` (test_core.py: in-process mock
# collector) run regardless.
# ---------------------------------------------------------------------------

_DOCKER_ERR = None


def _docker_available() -> bool:
    global _DOCKER_ERR
    try:
        import docker

        docker.from_env().ping()
        return True
    except Exception as exc:  # noqa: BLE001
        _DOCKER_ERR = exc
        return False


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "no_docker: integration test that needs no container backend")


def pytest_collection_modifyitems(config, items):
    gated = [
        item for item in items
        if item.fspath and str(item.fspath).startswith(_here)
        and not item.get_closest_marker("no_docker")
    ]
    if gated and not _docker_available():
        skip = pytest.mark.skip(
            reason=f"docker daemon not available: {_DOCKER_ERR}")
        for item in gated:
            item.add_marker(skip)


# ---------------------------------------------------------------------------
# Span-recording fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def _uninstrument_all():
    """Every module here instruments real client libraries process-wide via
    its autouse ``_instrument`` fixture and never restores them. Uninstall
    everything left in BaseInstrumentor's global registry when the module
    ends, so patches don't leak into other modules — or into tests/unit,
    which run *after* this directory when the whole tree runs in one process
    (``pytest tests``) and assert on the unpatched classes."""
    yield
    from pinpoint.instrumentor import _global_installed

    for inst in list(_global_installed.values()):
        inst.uninstrument()


@pytest.fixture
def recorder():
    return Recorder()


@pytest.fixture
def traced(recorder):
    """Push a recording root span into the context; yields the recorder.

    Everything the instrumented client libraries do inside the test lands in
    ``recorder.events`` (finished span events, creation order).
    """
    from pinpoint import context as ppctx

    span = RecordingNativeSpan(recorder).span(inject_base=IT_INJECT_BASE)
    token = ppctx.set_current_span(span)
    try:
        yield recorder
    finally:
        ppctx.reset_current_span(token)


@pytest.fixture
def using_span():
    """Yields ``using_span(span=None)`` — make ``span`` current for the block;
    ``None`` detaches whatever is current.

    The held consumer paths (kafka ``__next__`` / single-message ``poll`` /
    ``getone``, the aio_pika consume callback) deliberately step aside when a
    span is already current: a delivery arriving inside someone else's trace
    must not re-parent the rest of it. ``traced`` keeps its span current for
    the whole test, so a consume that should open its own root span has to run
    detached — the way a real consumer loop does.
    """
    from pinpoint import context as ppctx

    @contextmanager
    def _using(span=None):
        token = ppctx.set_current_span(span)
        try:
            yield
        finally:
            ppctx.reset_current_span(token)

    return _using


@pytest.fixture
def consumer_agent(recorder, monkeypatch):
    """Install a recording agent so consumer-side wrappers (kafka poll /
    rabbitmq deliveries), which open *root* spans via ``get_agent()``, record
    into the same recorder. Yields the recorder; consumer root spans appear
    in ``recorder.spans``."""
    import pinpoint.agent as agent_mod

    monkeypatch.setattr(agent_mod, "_instance", RecordingAgent(recorder))
    yield recorder


# ---------------------------------------------------------------------------
# Container helpers
# ---------------------------------------------------------------------------

def _host(container) -> str:
    """Container host, with ``localhost`` pinned to ``127.0.0.1``.

    Some runtimes (Rancher Desktop) publish dynamic ports on IPv4 only while
    ``localhost`` resolves to ``::1`` first — clients that try only the first
    addrinfo entry (pymemcache, some drivers) would hang on connection
    refused. IPv4-literal endpoints also keep the recorded span endpoints
    deterministic for assertions."""
    h = container.get_container_host_ip()
    return "127.0.0.1" if h == "localhost" else h


def _retry(fn, timeout: float = 60.0, interval: float = 0.3):
    """Retry ``fn`` until it stops raising. Container runtimes with async
    port forwarding (Rancher Desktop, some remote daemons) expose the mapped
    port seconds after the container reports Up — and may even accept and
    drop connections while warming up — so readiness must be probed at the
    protocol level, not just TCP connect."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            return fn()
        except Exception:  # noqa: BLE001
            if time.monotonic() >= deadline:
                raise
            time.sleep(interval)


# Images. Env-overridable so CI can pin mirrors.
_MYSQL_IMAGE = os.environ.get("PINPOINT_IT_MYSQL_IMAGE", "mysql:8")
_POSTGRES_IMAGE = os.environ.get("PINPOINT_IT_POSTGRES_IMAGE", "postgres:16-alpine")
_REDIS_IMAGE = os.environ.get("PINPOINT_IT_REDIS_IMAGE", "redis:7-alpine")
_MONGO_IMAGE = os.environ.get("PINPOINT_IT_MONGO_IMAGE", "mongo:6.0")
_MEMCACHED_IMAGE = os.environ.get("PINPOINT_IT_MEMCACHED_IMAGE", "memcached:1.6-alpine")
_RABBITMQ_IMAGE = os.environ.get("PINPOINT_IT_RABBITMQ_IMAGE", "rabbitmq:3.13-alpine")
_KAFKA_IMAGE = os.environ.get("PINPOINT_IT_KAFKA_IMAGE", "confluentinc/cp-kafka:7.5.0")
_ELASTICSEARCH_IMAGE = os.environ.get(
    "PINPOINT_IT_ELASTICSEARCH_IMAGE",
    "docker.elastic.co/elasticsearch/elasticsearch:8.15.0",
)
# Echo server (returns request method/path/headers as JSON) — lets the HTTP
# client tests assert the Pinpoint-* propagation headers actually went out on
# the wire, not just that the wrapper tried to inject them.
_HTTP_ECHO_IMAGE = os.environ.get(
    "PINPOINT_IT_HTTP_ECHO_IMAGE", "mendhak/http-https-echo:37")


@pytest.fixture(scope="module")
def mysql_container():
    from testcontainers.mysql import MySqlContainer

    with MySqlContainer(_MYSQL_IMAGE) as c:
        params = {
            "host": _host(c),
            "port": int(c.get_exposed_port(3306)),
            "user": c.username,
            "password": c.password,
            "database": c.dbname,
        }

        def _probe():
            import pymysql

            pymysql.connect(
                host=params["host"], port=params["port"],
                user=params["user"], password=params["password"],
                database=params["database"], connect_timeout=2,
            ).close()

        _retry(_probe)
        yield params


@pytest.fixture(scope="module")
def postgres_container():
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer(_POSTGRES_IMAGE, driver=None) as c:
        params = {
            "host": _host(c),
            "port": int(c.get_exposed_port(5432)),
            "user": c.username,
            "password": c.password,
            "database": c.dbname,
        }

        def _probe():
            import psycopg

            psycopg.connect(
                host=params["host"], port=params["port"],
                user=params["user"], password=params["password"],
                dbname=params["database"], connect_timeout=2,
            ).close()

        _retry(_probe)
        yield params


@pytest.fixture(scope="module")
def redis_container():
    from testcontainers.redis import RedisContainer

    with RedisContainer(_REDIS_IMAGE) as c:
        yield {
            "host": _host(c),
            "port": int(c.get_exposed_port(6379)),
        }


@pytest.fixture(scope="module")
def mongo_container():
    from testcontainers.mongodb import MongoDbContainer

    with MongoDbContainer(_MONGO_IMAGE) as c:
        yield {
            "url": c.get_connection_url().replace("localhost", "127.0.0.1"),
            "host": _host(c),
            "port": int(c.get_exposed_port(27017)),
        }


@pytest.fixture(scope="module")
def memcached_container():
    from testcontainers.core.container import DockerContainer

    with DockerContainer(_MEMCACHED_IMAGE).with_exposed_ports(11211) as c:
        host = _host(c)
        port = int(c.get_exposed_port(11211))

        def _probe():
            from pymemcache.client.base import Client

            probe = Client((host, port), connect_timeout=2, timeout=2)
            try:
                probe.version()
            finally:
                probe.close()

        _retry(_probe)
        yield {"host": host, "port": port}


@pytest.fixture(scope="module")
def rabbitmq_container():
    from testcontainers.rabbitmq import RabbitMqContainer

    with RabbitMqContainer(_RABBITMQ_IMAGE) as c:
        params = {
            "host": _host(c),
            "port": int(c.get_exposed_port(5672)),
            "user": c.username,
            "password": c.password,
        }

        def _probe():
            import pika

            pika.BlockingConnection(pika.ConnectionParameters(
                host=params["host"], port=params["port"],
                credentials=pika.PlainCredentials(
                    params["user"], params["password"]),
            )).close()

        _retry(_probe)
        yield params


@pytest.fixture(scope="module")
def kafka_container():
    from testcontainers.kafka import KafkaContainer

    with KafkaContainer(_KAFKA_IMAGE) as c:
        yield {"bootstrap": c.get_bootstrap_server()}


@pytest.fixture(scope="module")
def elasticsearch_container():
    from testcontainers.elasticsearch import ElasticSearchContainer

    c = ElasticSearchContainer(_ELASTICSEARCH_IMAGE, mem_limit="2g")
    c.with_env("xpack.security.enabled", "false")
    with c:
        url = f"http://{_host(c)}:{c.get_exposed_port(9200)}"
        _retry(lambda: urllib.request.urlopen(url, timeout=2).close(),
               timeout=120)
        yield {"url": url}


@pytest.fixture(scope="module")
def http_echo_container():
    from testcontainers.core.container import DockerContainer

    with DockerContainer(_HTTP_ECHO_IMAGE).with_exposed_ports(8080) as c:
        host = _host(c)
        port = int(c.get_exposed_port(8080))
        url = f"http://{host}:{port}"
        _retry(lambda: urllib.request.urlopen(url + "/", timeout=2).close())
        yield {"host": host, "port": port, "url": url}
