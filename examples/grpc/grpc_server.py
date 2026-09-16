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

"""Minimal grpcio server demo, ported from the C++ pinpoint grpc example.

Pair with grpc_client.py to see distributed tracing across two services.
Start the server first so the client has something to call:

    # terminal 1
    pinpoint-run --app-name demo-grpc-server --agent-name demo-grpc-server \
        --collector localhost -- python examples/grpc/grpc_server.py
    # terminal 2
    pinpoint-run --app-name demo-grpc-client --agent-name demo-grpc-client \
        --collector localhost -- python examples/grpc/grpc_client.py

Or manually:
    import pinpoint
    pinpoint.init(
        application_name="demo-grpc-server",
        agent_name="demo-grpc-server",
        server_info="gRPC Server",
    )
    pinpoint.autoload.autoload()

The bind address is overridable for split-host deployments:
    GRPC_BIND=0.0.0.0:50051 python examples/grpc/grpc_server.py

The service mirrors the C++ pinpoint-cpp-examples/grpc demo: one
`grpcdemo.Hello` service exposing the four canonical RPC patterns
(unary-unary, unary-stream, stream-unary, stream-stream). The Pinpoint
grpc instrumentation auto-wraps `grpc.server(...)` so the server
interceptor is installed without any user code. All four handler kinds
are wrapped — for streaming handlers the span lifetime tracks the full
response generator (or request iterator), so the span ends when the
last yield completes rather than when the handler function returns.
"""

import os
import sys
import time
from concurrent import futures

import grpc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import testapp_pb2
import testapp_pb2_grpc


class HelloServicer(testapp_pb2_grpc.HelloServicer):
    """Mirror of the C++ HelloServiceImpl: one method per RPC pattern."""

    def UnaryCallUnaryReturn(self, request, context):
        return testapp_pb2.Greeting(msg=f"Unary response: {request.msg}")

    def UnaryCallStreamReturn(self, request, context):
        for i in range(3):
            yield testapp_pb2.Greeting(msg=f"Stream #{i}: {request.msg}")

    def StreamCallUnaryReturn(self, request_iterator, context):
        parts = [req.msg for req in request_iterator]
        return testapp_pb2.Greeting(msg="Combined: " + ", ".join(parts))

    def StreamCallStreamReturn(self, request_iterator, context):
        for req in request_iterator:
            yield testapp_pb2.Greeting(msg=f"Echo: {req.msg}")


def serve(servicer=None) -> None:
    bind = os.environ.get("GRPC_BIND", "[::]:50051")
    # grpc.server() is auto-wrapped by pinpoint's grpc instrumentation so the
    # PinpointServerInterceptor is prepended before any user-supplied ones.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    testapp_pb2_grpc.add_HelloServicer_to_server(servicer or HelloServicer(), server)
    server.add_insecure_port(bind)
    server.start()
    print(f"gRPC server listening on {bind}", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.stop(grace=2).wait()


if __name__ == "__main__":
    # Manual init path (remove this block if launching via pinpoint-run).
    import pinpoint
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="python-demo-grpc-server",
        agent_name="python-demo-grpc-server-1",
        server_info="gRPC Server",
    )
    autoload()

    serve()
