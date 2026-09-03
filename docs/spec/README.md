# grpcgi specification (v1alpha1)

grpcgi carries HTTP exchanges from an HTTP edge (a proxy, gateway, or web
server) to an application server over a structured, streamed, transport-neutral
schema. It is the FastCGI idea rebuilt on protobuf and gRPC: the application
keeps its native serving interface and speaks no HTTP on the wire, and the
edge speaks one protocol to every application regardless of language.

This directory is the normative specification. It is split the way HTTP itself
is split, into semantics and mappings, so that the schema outlives any one
transport.

| Document | Role |
|---|---|
| [exchange-model.md](exchange-model.md) | Abstract semantics: event ordering, state machines, header rules, tunnels, errors, security considerations, conformance list. Transport-independent. |
| [schema.md](schema.md) | Encoding rules for the protobuf schema in `proto/grpcgi/v1alpha1/`: field ranges, the frozen header table policy, typed fields, extensibility, versioning. |
| [header-table.md](header-table.md) | The frozen registry of well-known header names and their field numbers. Generated; never edited by hand. |
| [grpc-binding.md](grpc-binding.md) | The primary binding: one bidirectional RPC per exchange, metadata, deadlines, status translation, passthrough of native gRPC, limits, Envoy integration. |
| [local-binding.md](local-binding.md) | A zero-dependency process binding over a Unix socket or stdio. The CGI and FastCGI replacement. Sketch, not yet normative. |
| [mappings.md](mappings.md) | Informative mappings to in-process interfaces: ASGI, WSGI, WASI HTTP, the Fetch handler, Rack, and servlets. |
| [vectors/vectors.md](vectors/vectors.md) | Byte-exact golden vectors for every event kind. Generated. |

## Status

`v1alpha1`. Assigned field numbers are retained, but the package may change
incompatibly until two independent implementations pass the conformance suite
described in exchange-model.md §10. The previous `grpcgi.v1` prototype
schema in `proto/grpcgi/v1/` is superseded and will be removed.

Implementations of this revision:

| Where | What |
|---|---|
| [github.com/grpcgi/go](https://github.com/grpcgi/go) | Reference implementation in Go (`import "github.com/grpcgi/go"`): serve any `net/http` handler from a `grpc.Server`, an `http.RoundTripper` that speaks the bridge (the proxyless edge in library form), message-oriented WebSocket, and `grpcgi-curl`. Its tests encode and decode every golden vector. |
| `python/` | Still speaks the superseded `grpcgi.v1` prototype. Migration pending. |

## Design decisions

These were made deliberately and are recorded so they are not relitigated by
accident. Rationale is in the linked documents.

- **Head, body stream, trailers, per direction.** The same ladder as HTTP/2,
  ext_proc, and http-over-capnp. No unary RPC; a buffered client sends one
  head with the body inline. (exchange-model §3)
- **Structured control data.** Method, scheme, authority, and path are typed
  fields, as in BHTTP (RFC 9292) and Google's `google.rpc.HttpRequest`. There
  are no pseudo-headers in header lists. (schema §2)
- **Frozen table of well-known headers as dedicated fields.** Recovers the
  static-table half of HPACK without connection state, parses as a switch on
  field number, and is frozen forever because it cannot be grown safely.
  Dynamic-table compression is explicitly not a goal. (schema §3)
- **Header values are bytes.** HTTP field values are octets. (schema §2)
- **Tunnels are a response variant, not a second service.** WebSocket is
  extended CONNECT (RFC 8441); an HTTP/1.1 upgrade is normalized to it at the
  edge; a 2xx to CONNECT switches the stream. (exchange-model §5)
- **The outer status is the server's channel; the head is the application's.**
  A complete HTTP response always ends with `OK`. A non-OK status before the
  head maps to an HTTP status by a fixed table. (grpc-binding §5)
- **Native gRPC is not encapsulated.** The schema can carry it losslessly, but
  the gRPC binding forwards `application/grpc` on HTTP/2+ to the application's
  own services and uses the bridge only for other HTTP. (grpc-binding §6)
- **Transport flow control only.** No application-level windows; bounded
  chunk sizes. (exchange-model §7)
- **No compression of head messages.** Ever. (schema §6)

## Prior art

Read before proposing changes; most questions have been answered before.

- CGI (RFC 3875), FastCGI, SCGI, AJP, the uwsgi protocol: the lineage.
- Google's HTTP-over-Stubby, visible publicly as `google/rpc/http.proto` and
  the App Engine runtime protocol (`java.apphosting.HttpRequest`, `UPRequest`).
- RFC 9292 Binary HTTP: the IETF's serialization of HTTP messages.
- Envoy `ext_proc`: the message ladder this borrows, built for mutation rather
  than serving.
- Cap'n Proto `http-over-capnp`: the closest full design, including WebSocket
  and common-header interning.
- WASI HTTP and the WinterTC Fetch handler: the in-process interfaces this
  must map onto losslessly.
- RFC 8441 (WebSocket over HTTP/2), RFC 9297/9298 (HTTP datagrams, capsules).

## Non-goals

Server push. HTTP/1 connection semantics. Process lifecycle management.
Application-level flow control. Cross-request header compression. Being the
application's API: a gRPC client calling `HttpBridge/Exchange` is opening an
HTTP exchange, not calling the application's own service.
