# gRPC

Instruments [gRPC](https://grpc.io/) on both sides through interceptors —
Pinpoint's own transport to the collector is unaffected, this is for your
application's RPCs.

| | |
|---|---|
| Target module | `grpc` |
| Hook points | `grpc.server`, `grpc.insecure_channel`, `grpc.secure_channel` |
| Span role | Root span (server), span event (client) |
| Service type | `GRPC_SERVER` (server), `GRPC` (client) |
| Opt-out alias | `grpc` |

## What gets traced

**Server** — `grpc.server(...)` is wrapped so a `PinpointServerInterceptor` is
prepended to any user-supplied interceptors. All four handler kinds
(unary-unary, unary-stream, stream-unary, stream-stream) are wrapped, so the
server span covers the full RPC including streaming I/O. Inbound Pinpoint
metadata stitches the RPC into the caller's trace.

**Client** — `insecure_channel` / `secure_channel` are wrapped the same way and
intercept all four client RPC kinds. Outbound metadata is augmented with
Pinpoint headers and a child span event opens on the active span. The gRPC
status code is annotated on the event.

Non-blocking and streaming calls end their event **at dispatch**, so a response
consumed on another thread never carries the parent's event with it — the event
measures the call, not the consumer's iteration.

## Usage

No code changes on either side:

```bash
pinpoint-run --app-name my-service --collector localhost -- python server.py
```

```python
# server.py — the interceptor is inserted by the wrapper on grpc.server()
import grpc
from concurrent import futures

server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
add_GreeterServicer_to_server(Greeter(), server)
server.add_insecure_port("[::]:50051")
server.start()
# Each RPC -> root span: /helloworld.Greeter/SayHello
```

```python
# client.py — metadata is injected by the wrapper on the channel
import grpc

with grpc.insecure_channel("localhost:50051") as channel:
    stub = GreeterStub(channel)
    stub.SayHello(HelloRequest(name="pinpoint"))
    # └─ span event: /helloworld.Greeter/SayHello   (Pinpoint metadata injected)
```

Channels and servers created *before* `autoload()` runs are not intercepted —
the hooks apply at construction time. Under `pinpoint-run` this never happens;
with an in-code bootstrap, call `autoload()` before building them.

## See also

- Example: [`examples/grpc/`](../../../examples/grpc) — `grpc_server.py`,
  `grpc_client.py`, and the `.proto` they share
- Unit tests: [`test_grpc_instrumentation.py`](../../../tests/unit/instrumentations/test_grpc_instrumentation.py)
- [Custom Instrumentation Guide](../../../docs/custom_instrumentation.md)
