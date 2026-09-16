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

"""In-process mock Pinpoint collector for the core-module integration tests.

Where the instrumentation modules replace the native span *sink* with
``_recorder``, the core suite keeps the whole native pipeline intact and
replaces the *collector* instead: a real ``grpc.server`` implementing the
Pinpoint collector services (Agent / Metadata / Span / Stat /
ProfilerCommandService) on an ephemeral localhost port. The real C++ agent
registers, pings, and drains spans/metadata/stats into it over the real gRPC
wire protocol, and every received protobuf stays inspectable here.

Python stubs are generated at first use from the vendored gRPC IDL
(``third_party/pinpoint-cpp-agent/3rd_party/pinpoint-grpc-idl``) via
``grpcio-tools``, so the mock always speaks exactly the proto revision the
native agent was built against.
"""

from __future__ import annotations

import contextlib
import glob
import os
import sys
import tempfile
import threading
import time
from concurrent import futures
from typing import Callable, Dict, List, Optional, Tuple

_here = os.path.dirname(os.path.abspath(__file__))
_repo_root = os.path.dirname(os.path.dirname(_here))
PROTO_ROOT = os.path.join(
    _repo_root, "third_party", "pinpoint-cpp-agent",
    "3rd_party", "pinpoint-grpc-idl", "proto",
)

_stub_lock = threading.Lock()
_stub_dir: Optional[str] = None


def ensure_stubs() -> str:
    """Generate ``v1/*_pb2*.py`` from the vendored protos once per process.

    Returns the directory that was inserted into ``sys.path`` (the generated
    modules import each other as ``v1.X_pb2``, so the *parent* of the ``v1``
    package dir goes on the path).
    """
    global _stub_dir
    with _stub_lock:
        if _stub_dir is not None:
            return _stub_dir

        import importlib.resources
        import subprocess

        # Bundled well-known types (google/protobuf/*.proto) ship with
        # grpc_tools; Service.proto imports empty.proto from there.
        well_known = str(importlib.resources.files("grpc_tools") / "_proto")

        out = tempfile.mkdtemp(prefix="pinpoint-grpc-idl-")
        protos = sorted(
            os.path.relpath(p, PROTO_ROOT)
            for p in glob.glob(os.path.join(PROTO_ROOT, "v1", "*.proto"))
        )
        if not protos:
            raise RuntimeError(f"no protos found under {PROTO_ROOT}")
        args = [
            f"-I{PROTO_ROOT}",
            f"-I{well_known}",
            f"--python_out={out}",
            f"--grpc_python_out={out}",
            *protos,
        ]
        # protoc runs in a subprocess, never in-process: importing
        # grpc_tools._protoc_compiler loads a second C++ protobuf/absl
        # runtime, and on Linux that cannot co-tenant a process where
        # pinpoint._native already holds its own shared libprotobuf/libabsl
        # (SIGABRT on import, or munmap heap corruption in the other order).
        # macOS survives only thanks to two-level namespace linking.
        proc = subprocess.run(
            [sys.executable, "-m", "grpc_tools.protoc", *args],
            capture_output=True, text=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"grpc_tools.protoc failed (rc={proc.returncode}): {args}\n"
                f"{proc.stdout}\n{proc.stderr}")

        if out not in sys.path:
            sys.path.insert(0, out)
        _stub_dir = out
        return out


class MockCollector:
    """Records everything the native agent sends over gRPC.

    All buckets are appended under one ``Condition`` (its default RLock also
    guards the snapshot accessors), and ``wait_for`` blocks on that condition
    so tests never poll-sleep.

    Buckets:

    - ``agent_infos`` — ``(invocation_metadata_dict, PAgentInfo)`` per
      ``RequestAgentInfo`` call;
    - ``ping_count`` — pings received on the ``PingSession`` stream;
    - ``api_metas`` / ``string_metas`` / ``sql_metas`` / ``sql_uid_metas`` /
      ``exception_metas`` — metadata registrations;
    - ``span_messages`` — every ``PSpanMessage`` from ``SendSpanBatch`` (and
      the deprecated ``SendSpan`` stream), i.e. spans *and* span chunks;
    - ``stat_messages`` — every ``PStatMessage`` from the agent-stat stream.
    """

    def __init__(self) -> None:
        ensure_stubs()
        self._cond = threading.Condition()
        self.agent_infos: List[Tuple[Dict[str, str], object]] = []
        self.ping_count = 0
        self.api_metas: List[object] = []
        self.string_metas: List[object] = []
        self.sql_metas: List[object] = []
        self.sql_uid_metas: List[object] = []
        self.exception_metas: List[object] = []
        self.span_messages: List[object] = []
        self.stat_messages: List[object] = []
        self._server = None
        self.port: Optional[int] = None

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(self) -> "MockCollector":
        import grpc

        from v1 import Service_pb2_grpc as svc

        # Parked handlers hold a worker each: one ping stream, one command
        # stream and one stat stream per live agent, and re-init tests briefly
        # overlap two agents. 16 leaves ample headroom for the unary traffic.
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
        agent_svc, meta_svc, span_svc, stat_svc, cmd_svc = _build_servicers(self)
        svc.add_AgentServicer_to_server(agent_svc, self._server)
        svc.add_MetadataServicer_to_server(meta_svc, self._server)
        svc.add_SpanServicer_to_server(span_svc, self._server)
        svc.add_StatServicer_to_server(stat_svc, self._server)
        svc.add_ProfilerCommandServiceServicer_to_server(cmd_svc, self._server)
        self.port = self._server.add_insecure_port("127.0.0.1:0")
        if not self.port:
            raise RuntimeError("failed to bind mock collector port")
        self._server.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            # grace=1 gives in-flight unary calls a moment, then cancels the
            # parked streaming handlers (their request iterators raise and the
            # handlers unwind).
            self._server.stop(grace=1).wait()
            self._server = None

    # ------------------------------------------------------------------
    # recording (called from server handler threads)
    # ------------------------------------------------------------------
    def _record(self, mutate: Callable[[], None]) -> None:
        with self._cond:
            mutate()
            self._cond.notify_all()

    # ------------------------------------------------------------------
    # snapshots + waiting
    # ------------------------------------------------------------------
    def wait_for(self, fn: Callable[[], object], timeout: float = 20.0,
                 what: str = "condition"):
        """Block until ``fn()`` returns something truthy; returns that value.

        ``fn`` runs under the collector lock, so it can read the buckets
        directly (the Condition's RLock makes the snapshot accessors safe to
        call from inside it too).
        """
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                got = fn()
                if got:
                    return got
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(f"timed out waiting for {what}")
                self._cond.wait(remaining)

    def spans(self) -> List[object]:
        """All received root/continued spans (PSpan), in arrival order."""
        with self._cond:
            return [m.span for m in self.span_messages if m.HasField("span")]

    def span_chunks(self) -> List[object]:
        """All received async-span chunks (PSpanChunk), in arrival order."""
        with self._cond:
            return [m.spanChunk for m in self.span_messages
                    if m.HasField("spanChunk")]

    def spans_with_rpc(self, rpc: str) -> List[object]:
        return [s for s in self.spans() if s.acceptEvent.rpc == rpc]

    def wait_span(self, rpc: str, timeout: float = 20.0):
        """Wait for (and return) the first PSpan whose acceptEvent.rpc matches."""
        return self.wait_for(
            lambda: next(iter(self.spans_with_rpc(rpc)), None),
            timeout=timeout, what=f"span with rpc={rpc!r}")

    def wait_chunk(self, span_id: int, timeout: float = 20.0):
        """Wait for the first PSpanChunk belonging to ``span_id``."""
        return self.wait_for(
            lambda: next((c for c in self.span_chunks()
                          if c.spanId == span_id), None),
            timeout=timeout, what=f"span chunk for spanId={span_id}")

    def api_meta_for(self, api_info: str):
        with self._cond:
            return next((m for m in self.api_metas if m.apiInfo == api_info),
                        None)

    def wait_api_meta(self, api_info: str, timeout: float = 20.0):
        return self.wait_for(lambda: self.api_meta_for(api_info),
                             timeout=timeout,
                             what=f"api metadata {api_info!r}")

    def string_meta_for(self, value: str):
        with self._cond:
            return next(
                (m for m in self.string_metas if m.stringValue == value), None)

    def agent_info_for(self, agent_id: str):
        """Latest (metadata, PAgentInfo) registered under ``agent_id``."""
        with self._cond:
            for md, info in reversed(self.agent_infos):
                if md.get("agentid") == agent_id:
                    return md, info
            return None

    def wait_agent_info(self, agent_id: str, timeout: float = 20.0):
        return self.wait_for(lambda: self.agent_info_for(agent_id),
                             timeout=timeout,
                             what=f"agent registration for {agent_id!r}")

    def agent_info_for_application(self, application_name: str):
        """Latest (metadata, PAgentInfo) for an application name."""
        with self._cond:
            for md, info in reversed(self.agent_infos):
                if md.get("applicationname") == application_name:
                    return md, info
            return None

    def wait_agent_info_for_application(
        self,
        application_name: str,
        timeout: float = 20.0,
    ):
        return self.wait_for(
            lambda: self.agent_info_for_application(application_name),
            timeout=timeout,
            what=f"agent registration for application {application_name!r}",
        )


def _build_servicers(collector: MockCollector):
    """Servicer classes close over ``collector``; defined lazily because the
    generated base classes only exist after ``ensure_stubs()``."""
    from google.protobuf import empty_pb2

    from v1 import Service_pb2_grpc as svc
    from v1 import Span_pb2

    class AgentServicer(svc.AgentServicer):
        def RequestAgentInfo(self, request, context):
            metadata = {k: v for k, v in context.invocation_metadata()}
            collector._record(
                lambda: collector.agent_infos.append((metadata, request)))
            return Span_pb2.PResult(success=True)

        def PingSession(self, request_iterator, context):
            for ping in request_iterator:
                def _bump():
                    collector.ping_count += 1
                collector._record(_bump)
                yield ping

    class MetadataServicer(svc.MetadataServicer):
        def RequestApiMetaData(self, request, context):
            collector._record(lambda: collector.api_metas.append(request))
            return Span_pb2.PResult(success=True)

        def RequestStringMetaData(self, request, context):
            collector._record(lambda: collector.string_metas.append(request))
            return Span_pb2.PResult(success=True)

        def RequestSqlMetaData(self, request, context):
            collector._record(lambda: collector.sql_metas.append(request))
            return Span_pb2.PResult(success=True)

        def RequestSqlUidMetaData(self, request, context):
            collector._record(lambda: collector.sql_uid_metas.append(request))
            return Span_pb2.PResult(success=True)

        def RequestExceptionMetaData(self, request, context):
            collector._record(lambda: collector.exception_metas.append(request))
            return Span_pb2.PResult(success=True)

    class SpanServicer(svc.SpanServicer):
        def SendSpan(self, request_iterator, context):  # deprecated stream
            for message in request_iterator:
                collector._record(
                    lambda m=message: collector.span_messages.append(m))
            return empty_pb2.Empty()

        def SendSpanBatch(self, request, context):
            def _extend():
                collector.span_messages.extend(request.span)
            collector._record(_extend)
            # No partial_success set == full success to the agent.
            return Span_pb2.PSpanResultBatch()

    class StatServicer(svc.StatServicer):
        def SendAgentStat(self, request_iterator, context):
            for message in request_iterator:
                collector._record(
                    lambda m=message: collector.stat_messages.append(m))
            return empty_pb2.Empty()

    class CommandServicer(svc.ProfilerCommandServiceServicer):
        # The agent keeps a HandleCommandV2 stream open for its lifetime; the
        # mock never issues commands, it just drains the handshake message(s)
        # until the client (or server stop) closes the stream.
        def HandleCommandV2(self, request_iterator, context):
            try:
                for _message in request_iterator:
                    continue
            except Exception:  # noqa: BLE001 — cancelled at server stop
                pass
            yield from ()

        def HandleCommand(self, request_iterator, context):  # deprecated
            try:
                for _message in request_iterator:
                    continue
            except Exception:  # noqa: BLE001
                pass
            yield from ()

        def CommandEcho(self, request, context):
            return empty_pb2.Empty()

    return (AgentServicer(), MetadataServicer(), SpanServicer(),
            StatServicer(), CommandServicer())


@contextlib.contextmanager
def start_native_agent(app_name: str, extra_yaml: str = ""):
    """Start the real native agent against a fresh ``MockCollector``, wait
    until it is registered and enabled, yield it, and tear both down."""
    from pinpoint import _native
    collector = MockCollector().start()
    config = f"""
Enable: true
ApplicationName: "{app_name}"
Collector:
  Host: "127.0.0.1"
  AgentPort: {collector.port}
  SpanPort: {collector.port}
  StatPort: {collector.port}
  AgentInfo:
    SendRetryIntervalMs: 100
Sampling:
  Type: "PERCENT"
  PercentRate: 100.0
Stat:
  Enable: false
{extra_yaml}"""
    agent = _native.start_agent("", config, "", 1700, "unit-test", [], [])
    try:
        collector.wait_agent_info_for_application(app_name)
        deadline = time.monotonic() + 5
        while not agent.enable():
            if time.monotonic() >= deadline:
                raise AssertionError("native agent did not become enabled")
            time.sleep(0.01)
        yield agent
    finally:
        agent.shutdown()
        collector.stop()
