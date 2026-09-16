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

"""Tornado front-end that fans out to the grpcio demo server.

Pair with grpc_server.py to see distributed tracing across two services.
Start the server first so the handlers have something to call:

    # terminal 1: start the gRPC server
    pinpoint-run --app-name demo-grpc-server --agent-name demo-grpc-server \
        --collector localhost -- python examples/grpc/grpc_server.py
    # terminal 2: start this Tornado client app
    pinpoint-run --app-name demo-grpc-client --agent-name demo-grpc-client \
        --collector localhost -- python examples/grpc/grpc_client.py
    # terminal 3: hit it
    curl http://localhost:8888/unary
    curl http://localhost:8888/server-stream
    curl http://localhost:8888/client-stream
    curl http://localhost:8888/bidi

Or manually:
    import pinpoint
    pinpoint.init(
        application_name="demo-grpc-client",
        agent_name="demo-grpc-client",
        server_info="gRPC Client",
    )
    pinpoint.autoload.autoload()

The gRPC target and HTTP port are overridable:
    GRPC_TARGET=other-host:50051 PORT=9000 python examples/grpc/grpc_client.py

What this shows
---------------
- The ``tornado`` instrumentation wraps ``RequestHandler._execute`` so
  each HTTP request opens a Pinpoint root span — no manual
  ``@pinpoint.span`` is needed on the handlers (cf. the earlier CLI
  version, where each call site had to declare its own root span).
- ``grpc.insecure_channel(...)`` is auto-wrapped so the
  ``PinpointClientInterceptor`` is attached to the channel; outbound
  gRPC calls nest as child span events under the active Tornado span
  and propagate Pinpoint-* headers as gRPC metadata to the server.
- All four RPC kinds are traced (unary-unary, unary-stream,
  stream-unary, stream-stream). For streaming responses the child
  event's lifetime tracks the iteration, so the span event reflects
  the actual on-the-wire duration rather than just the call setup.
"""

from __future__ import annotations

import os
import sys

import grpc
import tornado.ioloop
import tornado.web

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import testapp_pb2
import testapp_pb2_grpc

TARGET = os.environ.get("GRPC_TARGET", "localhost:50051")


class _StubMixin:
    """Resolve the shared HelloStub stashed on the Tornado application."""

    def stub(self) -> testapp_pb2_grpc.HelloStub:
        return self.application.settings["grpc_stub"]


class UnaryHandler(_StubMixin, tornado.web.RequestHandler):
    def get(self):
        response = self.stub().UnaryCallUnaryReturn(
            testapp_pb2.Greeting(msg="Hello from unary client")
        )
        self.write({"response": response.msg})


class ServerStreamHandler(_StubMixin, tornado.web.RequestHandler):
    def get(self):
        responses = [
            r.msg
            for r in self.stub().UnaryCallStreamReturn(
                testapp_pb2.Greeting(msg="Stream greetings")
            )
        ]
        self.write({"responses": responses})


class ClientStreamHandler(_StubMixin, tornado.web.RequestHandler):
    def get(self):
        def _requests():
            for i in range(3):
                yield testapp_pb2.Greeting(msg=f"Message {i}")

        response = self.stub().StreamCallUnaryReturn(_requests())
        self.write({"response": response.msg})


class BidiHandler(_StubMixin, tornado.web.RequestHandler):
    def get(self):
        def _requests():
            for i in range(3):
                yield testapp_pb2.Greeting(msg=f"Message {i}")

        responses = [
            r.msg for r in self.stub().StreamCallStreamReturn(_requests())
        ]
        self.write({"responses": responses})


def build_app() -> tornado.web.Application:
    # grpc.insecure_channel() is auto-wrapped by pinpoint's grpc
    # instrumentation so the PinpointClientInterceptor is attached.
    channel = grpc.insecure_channel(TARGET)
    stub = testapp_pb2_grpc.HelloStub(channel)
    return tornado.web.Application(
        [
            (r"/unary", UnaryHandler),
            (r"/server-stream", ServerStreamHandler),
            (r"/client-stream", ClientStreamHandler),
            (r"/bidi", BidiHandler),
        ],
        grpc_stub=stub,
    )


def main() -> None:
    import pinpoint
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="python-demo-grpc-client",
        agent_name="python-demo-grpc-client-1",
        server_info="gRPC Client",
    )
    autoload()

    app = build_app()
    port = int(os.environ.get("PORT", "8888"))
    app.listen(port, address="0.0.0.0")
    print(f"Tornado gRPC client listening on http://0.0.0.0:{port}", flush=True)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
