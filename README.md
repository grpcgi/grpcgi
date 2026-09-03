# grpcgi

> **Design status (2026-09).** The protocol is being rebuilt from first
> principles as `grpcgi.v1alpha1`. The normative specification lives in
> [`docs/spec/`](docs/spec/README.md) and the schema in
> [`proto/grpcgi/v1alpha1/`](proto/grpcgi/v1alpha1/). The Go implementation at
> [github.com/grpcgi/go](https://github.com/grpcgi/go) speaks the new design: any `net/http` handler served
> from a `grpc.Server`, plus an `http.RoundTripper` that calls it back with no
> proxy. Everything below this line describes the earlier `v1` prototype and
> its Python implementation, which do not yet implement the new design.

**gRPC transport for Python ASGI applications via Envoy.**

grpcgi lets you run any ASGI web application (Django, FastAPI, Starlette, …)
over a gRPC transport instead of plain HTTP, making Python web servers
first-class citizens in a gRPC/xDS service mesh.

---

## Motivation

Python web frameworks sit on a mature, battle-tested serving stack — Django's
ORM, DRF, middleware, template engine, authentication; FastAPI's dependency
injection; Starlette's WebSocket support. But that stack speaks HTTP/1.1 or
HTTP/2 to the outside world and has no native integration with the service-mesh
primitives that modern infrastructure runs on (Envoy, xDS, gRPC health
checking, rich load-balancing policies).

The common workaround is to put Envoy in front of a uWSGI/Gunicorn/Uvicorn
process and do HTTP-to-HTTP proxying. This works, but it means your Python
service is just another opaque HTTP backend with none of the performance,
observability, or routing richness that gRPC upstreams get for free.

grpcgi flips the transport. Instead of HTTP, Envoy speaks **our own
protocol** — `grpcgi.v1.HttpBridge` for request/response and
`grpcgi.v1.WebSocketBridge` for WebSocket — to a small in-process gRPC server.
That server translates the gRPC calls into ASGI events and calls your app.
The app sees a completely standard ASGI interface and needs no changes.

On the Envoy side you get:
- **xDS routing** — route by path, header, cluster, weight — without touching
  application code.
- **gRPC keepalive and health checking** — configured independently of the app.
- **Envoy's load-balancing policies** — round-robin, least-request, ring-hash —
  applied to gRPC upstreams, not plain HTTP backends.
- **WebSocket over gRPC bidi-streaming** — frames are translated by an Envoy
  WASM or native C++ filter; no changes to the app or the browser.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Browser / gRPC client                       │
└────────────────────┬────────────────────────────┬───────────────────┘
                     │  HTTP/1.1  or  HTTP/2      │  WebSocket (WS/WSS)
                     ▼                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                             Envoy proxy                             │
│                                                                     │
│  ┌─────────────────────────────────┐  ┌──────────────────────────┐  │
│  │  HTTP filter chain              │  │  WS connection           │  │
│  │  ┌──────────────────────────┐   │  │  ┌────────────────────┐  │  │
│  │  │  grpcgi WASM / native    │   │  │  │  grpcgi WASM /     │  │  │
│  │  │  HTTP filter             │   │  │  │  native WS filter  │  │  │
│  │  │  (translates HTTP to     │   │  │  │  (translates WS    │  │  │
│  │  │   HttpBridge.Process     │   │  │  │   frames to        │  │  │
│  │  │   gRPC call)             │   │  │  │   WebSocketBridge  │  │  │
│  │  └──────────────────────────┘   │  │  │   .Connect)        │  │  │
│  └─────────────────────────────────┘  └──────────────────────────┘  │
└────────────────────┬────────────────────────────┬───────────────────┘
                     │                            │
        gRPC  grpcgi.v1.HttpBridge.Process        │  gRPC  grpcgi.v1.WebSocketBridge.Connect
        (bidi-streaming, HTTP wrapped in proto)   │  (bidi-streaming, one Frame per WS frame)
                     │                            │
                     ▼                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        grpcgi server  :50051                        │
│                                                                     │
│  HttpBridgeServicer                  WebSocketBridgeServicer        │
│  ┌──────────────────────────────┐   ┌────────────────────────────┐  │
│  │ RequestHeaders               │   │ initial gRPC metadata:     │  │
│  │  :method, :path, :authority  │   │  grpcgi-path, headers …    │  │
│  │  :scheme, headers …          │   │                            │  │
│  │        ↓                     │   │ Frame { payload, binary }  │  │
│  │ ASGI http scope              │   │        ↓                   │  │
│  │        ↓                     │   │ ASGI websocket scope       │  │
│  │ BodyChunk stream  →  receive │   │        ↓                   │  │
│  │ ResponseHeaders   ←  send    │   │ receive / send events      │  │
│  │ BodyChunk stream  ←  send    │   │                            │  │
│  └──────────────────────────────┘   └────────────────────────────┘  │
└────────────────────┬────────────────────────────┬───────────────────┘
                     │          ASGI              │
                     ▼                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Python ASGI app  (Django, FastAPI, Starlette, …)       │
│                                                                     │
│   async def app(scope, receive, send): …                            │
└─────────────────────────────────────────────────────────────────────┘
```

### Wire protocols

The normative message ordering, HTTP mapping, failure semantics, flow-control
rules, and compatibility policy are in [the grpcgi v1 protocol
specification](docs/protocol.md). The examples below are only an overview.

**HTTP bridge** — `grpcgi.v1.HttpBridge.Process`

```
Client (Envoy filter)                    Server (grpcgi)
──────────────────────────────────────────────────────
ProcessingRequest { request_headers }  →
ProcessingRequest { request_body }     →   (zero or more body chunks)
                                       ←  ProcessingResponse { response_headers }
                                       ←  ProcessingResponse { response_body }  (one or more)
```

**WebSocket bridge** — `grpcgi.v1.WebSocketBridge.Connect`

```
Client (Envoy WASM/native filter)        Server (grpcgi)
──────────────────────────────────────────────────────
[gRPC initial metadata: path, headers]
Frame { payload, binary=false }        →   (text frame)
Frame { payload, binary=true  }        →   (binary frame)
                                       ←  Frame { payload, binary }
… bidirectional until stream close …
```

Ping/pong frames are handled by the Envoy filter and never forwarded to grpcgi.
gRPC keepalive between Envoy and grpcgi is configured independently.

---

## Repository layout

```
proto/
  grpcgi/v1/
    http.proto          — HttpBridge service (HTTP request/response)
    websocket.proto     — WebSocketBridge service (WebSocket frames)

python/                 — Python prototype (grpcio + grpc.aio)
  grpcgi/
    server.py           — serve(app) — registers both servicers, starts gRPC server
    http.py             — HttpBridgeServicer → ASGI http scope
    ws.py               — WebSocketBridgeServicer → ASGI websocket scope
  examples/
    echo.py             — minimal ASGI echo app (HTTP + WebSocket)
  generate_proto.sh     — regenerate _proto/ from proto/

wasm/                   — Rust proxy-wasm network filter (WebSocket → gRPC)
  src/lib.rs            — WsBridgeStream state machine + frame codec

envoy-filter/           — Native C++ Envoy filter (for upstreaming)
  ws_grpc_bridge_filter.h/.cc  — envoy.filters.http.grpc_websocket_bridge
  config.h/.cc                 — xDS factory + REGISTER_FACTORY
  BUILD.bazel                  — Bazel targets for Envoy tree
  proto/envoy/extensions/…     — filter config proto

demo/
  server.py             — grpcgi server wrapping a Django + WebSocket echo app
  test_client.py        — HTTP test client (simulates the WASM filter)
  ws_test_client.py     — WebSocket test client
  django_app/           — minimal Django hello-world ASGI project
  setup.sh / run_*.sh   — one-step setup and run scripts
```

---

## Running the demo

```bash
cd demo
bash setup.sh          # create venv, install grpcio + django + grpcgi prototype
```

```
# Terminal 1 — grpcgi server (Django + WebSocket echo) on :50051
./run_server.sh

# Terminal 2 — Envoy on :8080 loading the WASM filter  (requires docker or podman)
./run_envoy.sh

# Terminal 3 — HTTP through the full stack: curl → Envoy → WASM → grpcgi.v1 → Django
curl http://localhost:8080/
curl http://localhost:8080/hello/

# WebSocket test (direct to grpcgi)
./run_ws_test.sh
```

Or all at once (Envoy + server + tests):

```bash
./run_demo.sh
```

Expected output:

```
=== GET / ===
→ GET /
← HTTP 200
   Content-Type: text/html; charset=utf-8
<h1>Hello from Django via grpcgi!</h1>…

=== GET /hello/ ===
→ GET /hello/
← HTTP 200   Content-Type: text/plain
Hello, World!

=== WebSocket echo ===
→ text: 'Hello grpcgi!'
→ text: 'WebSocket over gRPC'
← text: 'echo: Hello grpcgi!'
← text: 'echo: WebSocket over gRPC'
done.
```

### With Envoy (once the WASM filter is deployed)

When the WASM filter is built and loaded, the test clients are replaced by
Envoy. The grpcgi server stays exactly the same. A minimal Envoy config:

```yaml
# envoy.yaml (illustrative — requires the grpcgi WASM filter loaded)
static_resources:
  listeners:
  - address: { socket_address: { address: 0.0.0.0, port_value: 8080 } }
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: grpcgi
          http_filters:
          # HTTP → grpcgi.v1.HttpBridge translation
          - name: envoy.filters.http.wasm
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.wasm.v3.Wasm
              config:
                vm_config:
                  runtime: envoy.wasm.runtime.v8
                  code: { local: { filename: /etc/envoy/grpcgi_http.wasm } }
                configuration:
                  "@type": type.googleapis.com/google.protobuf.StringValue
                  value: '{"cluster":"grpcgi","authority":"grpcgi:50051"}'
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
          route_config:
            virtual_hosts:
            - domains: ["*"]
              routes:
              - match: { prefix: "/" }
                route: { cluster: grpcgi }

  clusters:
  - name: grpcgi
    type: STATIC
    typed_extension_protocol_options:
      envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
        "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
        explicit_http_config: { http2_protocol_options: {} }
    load_assignment:
      cluster_name: grpcgi
      endpoints:
      - lb_endpoints:
        - endpoint: { address: { socket_address: { address: 127.0.0.1, port_value: 50051 } } }
```

WebSocket connections are handled by a separate filter instance (or the
native C++ filter `envoy.filters.http.grpc_websocket_bridge`) configured on
the same or a separate listener.

---

## Components

### Proto (`proto/`)

The two service definitions are the canonical contract between Envoy and grpcgi.
The HTTP bridge proto mirrors the semantics of Envoy's `ext_proc` protocol but
is deliberately simpler and owns the field numbers so we can evolve it
independently. The WebSocket bridge is minimal by design: control frames
(ping/pong) are absorbed by the Envoy filter; only data frames (text/binary)
cross the gRPC boundary.

### Python prototype (`python/`)

A reference implementation using `grpcio` + `grpc.aio`. Suitable for
development and testing; production throughput should use
[grpyc](https://github.com/…) (Rust-based Python gRPC, zero-copy) once the
protocol is stable.

Key design points:
- Two `asyncio.Queue` instances per request decouple the gRPC stream reader from
  the ASGI coroutine, so body chunks stream through without buffering.
- WebSocket initial metadata carries the HTTP upgrade headers (`grpcgi-path`,
  `grpcgi-authority`, `grpcgi-scheme`), because gRPC forbids metadata keys
  starting with `:`.
- WSGI apps (Flask, older Django) work via `asgiref.WsgiToAsgi` with no
  changes to the app.

### WASM filter (`wasm/`)

A Rust `proxy-wasm` **network-level** filter that runs inside Envoy without
requiring a custom Envoy build. State machine:

```
AwaitingUpgradeRequest
  → (HTTP upgrade request parsed, gRPC stream opened)
AwaitingUpgradeResponse
  → (101 Switching Protocols seen)
Active
  → parse WS frames → forward as Frame proto messages
  ← receive Frame proto messages → encode as WS frames
```

Proto encoding is hand-rolled (no prost) to keep the WASM binary small.
Compiles to ~170 KB.

### Native C++ filter (`envoy-filter/`)

Production-quality Envoy HTTP filter implementing the same WebSocket-to-gRPC
translation. Registered as `envoy.filters.http.grpc_websocket_bridge` and
structured to be upstreamed to Envoy mainline. Uses Envoy's
`Grpc::AsyncBidirectionalStreamCallbacks` and follows the same conventions as
`envoy.filters.http.grpc_web`.

---

## Status

| Component | Status |
|---|---|
| Proto definitions | Stable for prototyping |
| Python prototype (HTTP) | Working |
| Python prototype (WebSocket) | Working |
| WASM filter (WebSocket) | Compiles, logic complete, integration testing needed |
| Native C++ filter | Skeleton complete, needs Envoy tree integration test |
| Envoy HTTP filter for HttpBridge | Not started |
| grpyc integration | Not started |

The demo runs end-to-end on Python: Django HTTP and WebSocket echo both work
through the grpcgi ASGI bridge. The Envoy integration (WASM filter → grpcgi)
is the next milestone.
