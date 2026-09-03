> **Superseded.** This document describes the `grpcgi.v1` prototype
> protocol. The design has been rebuilt from first principles as `v1alpha1`;
> see [`docs/spec/README.md`](spec/README.md). This file is retained only until
> the Python bridge and filters move to the new schema.

# grpcgi v1 protocol specification

Status: candidate specification. The package is named `grpcgi.v1`, but v1 is
not frozen until the conformance suite named below exists. Field numbers already
published in the prototype are retained. New implementations must implement
`HttpBridge.Process` and `WebSocketBridge.Session`; `Handle` and `Connect` are
compatibility RPCs.

grpcgi carries downstream HTTP application semantics over an upstream gRPC
transport. The outer gRPC status describes the bridge operation. It never
stands in for the application's HTTP status or, when proxying an HTTP/gRPC
application, the inner `grpc-status` trailer.

## Transport and service discovery

The transport is HTTP/2 gRPC. The canonical services are:

- `/grpcgi.v1.HttpBridge/Process`
- `/grpcgi.v1.WebSocketBridge/Session`

A deployment may use h2c on a protected loopback or pod network. Any traffic
crossing a workload boundary should use TLS, normally mutual TLS. Authority,
SNI, trust roots, and client identity are deployment configuration, not fields
in the application protocol. A server may additionally expose standard gRPC
health checking for service `grpcgi.v1.HttpBridge` and/or
`grpcgi.v1.WebSocketBridge`.

Proxies must not retry a call after any request body byte has been delivered
unless the route explicitly opts into a replay policy and the complete request
is safely buffered. A WebSocket session is never retried after `accept`.

## HTTP bridge

### Request sequence

The client sends exactly this sequence on `Process`:

1. One `request_headers` message.
2. Zero or more `request_body` messages. Exactly the final chunk has
   `end_of_stream=true`, unless trailers follow.
3. Zero or one `request_trailers` message, which implicitly ends the request.

`RequestHeaders.end_of_stream=true` ends a bodyless, trailerless request. For
compatibility, a client may instead half-close immediately after headers. Empty
non-final body chunks are legal but discouraged. Any message after the request
has ended, duplicate headers, or duplicate trailers is a protocol error.

Required pseudo-headers are `:method`, `:path`, `:scheme`, and `:authority`.
They precede regular fields and occur once. `CONNECT` follows RFC 9110 extended
CONNECT rules. Regular names are lower-case. Hop-by-hop fields (`connection`,
`keep-alive`, `proxy-connection`, `transfer-encoding`, and `upgrade`) are
removed by the edge. `te` is allowed only with value `trailers`.

Duplicate headers, their order, and raw values are preserved. Senders use
`Header.raw_value`; `Header.value` exists for prototype compatibility. Exactly
one value field is populated. Receivers prefer `raw_value` and otherwise UTF-8
encode `value`. NUL, CR, or LF in a name or value is a protocol error. The
protocol does not comma-join fields, particularly `set-cookie`.

`:path` contains the raw path plus optional query. `http_version` records the
downstream version rather than the h2 bridge transport. `remote_address` and
`remote_port` are the post-trusted-proxy peer identity. Servers must treat them
as trustworthy only when the bridge peer itself is authenticated and
authorized to assert client identity. `root_path` is an optional application
mount prefix.

Request trailers contain regular fields only. In an ASGI bridge they are
exposed using the ASGI HTTP trailers extension when the application supports
it; otherwise the bridge drains and discards them rather than folding them into
headers.

### Response sequence

The server sends:

1. Zero or more `response_headers` messages with a 100..199 status.
2. Exactly one final `response_headers` with a 200..599 status.
3. Zero or more `response_body` messages.
4. Zero or one `response_trailers`, which implicitly ends the response.

`ResponseHeaders.end_of_stream=true` ends a bodyless, trailerless final
response. Otherwise the final body chunk sets `end_of_stream=true` when no
trailers follow. `HEAD`, 1xx, 204, and 304 responses carry no body regardless
of application output. A response must not contain hop-by-hop fields.

The outer RPC finishes with gRPC `OK` after a complete HTTP response, including
an application 4xx or 5xx. An inner HTTP/gRPC response carries `grpc-status`,
`grpc-message`, and application trailers in `response_trailers`; those fields
must not be mistaken for the outer RPC status.

### Unary compatibility RPC

`Handle` buffers the complete request and response. It supports trailers but
not informational responses, incremental delivery, cancellation after partial
delivery, or bounded-memory streaming. A client must enforce configured body
limits before calling it. Xin does not use this RPC.

## WebSocket bridge

`Session` maps one downstream WebSocket connection to one bidi RPC. Its event
state machine is:

```
client: open -------------------------------- data* ---- close?
server:       accept ---- data* ---- close?
              \ reject
```

The first client event is exactly one `open`. The first server event is one
`accept` or `reject`. Data before acceptance is a protocol error. `reject`
ends the stream and represents an HTTP response before upgrade. `accept`
selects at most one subprotocol offered by the client. Extension negotiation
is not forwarded in v1; the edge owns WebSocket compression.

Each `data` is one complete message, not one wire frame. The edge reassembles
fragments, unmasks client frames, enforces the configured message limit, and
validates text as UTF-8. It frames server events for the downstream peer. Ping
and pong remain edge-local and are not events.

Either peer may send `close`. Codes and reasons follow RFC 6455; the reason is
UTF-8 and code plus encoded reason must fit the 125-byte control-frame limit.
Code zero means the transport ended without a close frame and maps to ASGI
disconnect code 1006. After sending close, a peer sends no more data. A clean
close finishes the outer RPC with `OK`; an abnormal bridge failure uses a
non-OK outer status in addition to closing/resetting downstream as possible.

`Connect` is the legacy data-only RPC. It obtains handshake input from initial
metadata, treats stream opening as implicit acceptance, and cannot preserve a
close code. It remains available during migration but is not conformant for a
new v1 implementation.

## Cancellation, deadlines, and flow control

Downstream disconnect immediately cancels the bridge RPC. Upstream cancellation
before response headers produces no response when the downstream is already
gone. Cancellation after response start resets/closes the downstream stream;
the proxy must not manufacture a second HTTP response.

Bridge implementations rely on gRPC/HTTP2 flow control and add no unbounded
queues. Reading from either side stops when the opposite side cannot accept
data. Implementations expose finite limits for request bytes, response bytes
when buffering is required, WebSocket message bytes, header count, and total
header bytes. Limit checks happen before allocating the declared size.

The route deadline covers connect, send, application work, and receive. The
smaller of the downstream deadline and configured route deadline is propagated
to the outer RPC. Timeout after downstream response start resets the stream.

## Failure mapping at the edge

Before downstream response headers, Xin and other HTTP edges use this mapping:

| Outer gRPC outcome | Downstream HTTP | Stable failure class |
|---|---:|---|
| `INVALID_ARGUMENT`, bad event ordering | 502 | `protocol_error` |
| `UNAVAILABLE`, connect/TLS failure | 502 | `upstream_unavailable` |
| `DEADLINE_EXCEEDED` | 504 | `upstream_timeout` |
| `RESOURCE_EXHAUSTED` while sending request | 413 | `request_too_large` |
| `RESOURCE_EXHAUSTED` while receiving response | 502 | `response_too_large` |
| `UNAUTHENTICATED` or `PERMISSION_DENIED` on bridge hop | 502 | `bridge_identity` |
| other non-OK status | 502 | `bridge_error` |

An application-generated HTTP status is passed through unchanged. After
response start, failures reset HTTP/2 or HTTP/3 streams and close HTTP/1.x
connections. WebSocket failures before `accept` use an HTTP response; after
accept they use close code 1011 when a close frame can still be sent, then end
the connection.

## Observability

The edge propagates W3C `traceparent`, `tracestate`, `baggage`, and the normal
request identifier as HTTP headers inside the request, not as private gRPC
metadata. The bridge creates a child span named
`grpcgi.v1.HttpBridge/Process` or `grpcgi.v1.WebSocketBridge/Session` and must
not duplicate user cookies or authorization values into span attributes.

At minimum implementations expose counters by route, service, and bounded
outcome class; active HTTP and WebSocket streams; request/response bytes;
time-to-response-headers; total duration; upstream connect/TLS duration; and
flow-control stall time. Logs include request ID, route, chosen endpoint,
outer gRPC code, downstream HTTP status or WebSocket close code, byte counts,
and duration. Endpoint addresses and error text are never metric labels.

## Compatibility and conformance

Unknown protobuf fields are ignored as required by protobuf. Unknown oneof
events are a protocol error because continuing could desynchronize stream
state. Senders do not reuse field numbers. Additive optional fields and new
RPCs are permitted within v1; a change to event order or existing field meaning
requires `grpcgi.v2`.

The conformance suite must cover bodyless requests, chunk boundaries, request
and response trailers, duplicate headers, byte-valued headers, informational
responses, early application response while upload continues, cancellation in
both directions, every failure mapping, flow-control under a slow peer, message
and header limits, HTTP/gRPC nested trailers, WebSocket accept/reject,
subprotocol selection, fragmented input, invalid UTF-8, ping/pong locality,
close handshake, abrupt disconnect, and graceful drain.
