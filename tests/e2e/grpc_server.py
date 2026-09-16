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

"""Standalone gRPC server for the python integration test.

Direct port of ``test/it_test/grpc_server.cpp`` from pinpoint-cpp-agent.
Pair with ``test_server.py`` to drive every RPC pattern across two
processes — the HTTP server's ``/grpc-*`` endpoints fan out to this server,
so propagator inject/extract is exercised end-to-end against a real collector.

The service itself is the ``examples/grpc`` demo servicer; this wrapper adds
the annotated child event per RPC that the cpp / python integration tests
expect, so both emit structurally identical traces.

Run:
    .venv/bin/python tests/e2e/grpc_server.py
"""

from __future__ import annotations

import os
import sys

# Reuse the demo module: servicer, serve(), and the generated testapp stubs.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(_REPO_ROOT, "examples", "grpc"))

# Resolves to examples/grpc/grpc_server.py via the sys.path insert above —
# this script itself runs as __main__, so the names don't collide.
import grpc_server as _demo  # noqa: E402

import pinpoint  # noqa: E402
from pinpoint.annotation import ANNOTATION_API  # noqa: E402
from pinpoint.autoload import autoload  # noqa: E402
from pinpoint.service_type import SERVICE_TYPE_GRPC_SERVER  # noqa: E402


def _annotate_current(method: str) -> None:
    """Mirror cpp ``ScopedSpanEvent + ANNOTATION_API`` on the current span."""
    with pinpoint.trace(method, service_type=SERVICE_TYPE_GRPC_SERVER) as ev:
        if ev is not None:
            ev.annotate_string(ANNOTATION_API, method)


class HelloServicer(_demo.HelloServicer):
    """Demo servicer plus the annotated child event the cpp it_test emits."""

    def UnaryCallUnaryReturn(self, request, context):
        _annotate_current("grpcdemo.Hello/UnaryCallUnaryReturn")
        return super().UnaryCallUnaryReturn(request, context)

    def UnaryCallStreamReturn(self, request, context):
        _annotate_current("grpcdemo.Hello/UnaryCallStreamReturn")
        yield from super().UnaryCallStreamReturn(request, context)

    def StreamCallUnaryReturn(self, request_iterator, context):
        _annotate_current("grpcdemo.Hello/StreamCallUnaryReturn")
        return super().StreamCallUnaryReturn(request_iterator, context)

    def StreamCallStreamReturn(self, request_iterator, context):
        _annotate_current("grpcdemo.Hello/StreamCallStreamReturn")
        yield from super().StreamCallStreamReturn(request_iterator, context)


def _init_agent_from_env() -> None:
    """Initialize the agent. PINPOINT_PY_* env vars steer config.

    Set ``PINPOINT_DISABLE=true`` to skip init and autoload entirely, matching
    ``test_server.py`` — an untraced-baseline run has to turn off both halves
    of the e2e pair, or the /grpc-* endpoints keep paying the agent's cost."""

    if os.environ.get("PINPOINT_DISABLE", "").strip().lower() in (
        "1", "true", "yes", "on",
    ):
        print("pinpoint instrumentation disabled (PINPOINT_DISABLE set)", flush=True)
        return

    os.environ.setdefault(
        "PINPOINT_PY_APPLICATION_NAME", "py-e2e-grpc-server")
    os.environ.setdefault("PINPOINT_PY_AGENT_NAME", "py-e2e-grpc-agent")
    pinpoint.init(
        application_name=os.environ["PINPOINT_PY_APPLICATION_NAME"],
        agent_name=os.environ["PINPOINT_PY_AGENT_NAME"],
        server_info=os.environ.get("PINPOINT_PY_SERVER_INFO", "gRPC IT Test Server"),
    )
    autoload()


def serve() -> None:
    _init_agent_from_env()
    _demo.serve(HelloServicer())


if __name__ == "__main__":
    serve()
