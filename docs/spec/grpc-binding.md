# grpcgi gRPC binding

Status: v1alpha1. This is the primary binding of the exchange model to a
transport. It defines how events are carried on gRPC, what goes in metadata,
how deadlines and cancellation map, how the RPC status is translated to HTTP,
which streams bypass the bridge, and how an Envoy integration should be
shaped. The service definition is `proto/grpcgi/v1alpha1/bridge.proto`.

## 1. Service

```
service grpcgi.v1alpha1.HttpBridge {
  rpc Exchange(stream RequestEvent) returns (stream ResponseEvent);
}
```

One RPC carries exactly one exchange. The edge is the gRPC client. The
application process is a gRPC server that typically registers `HttpBridge`,
its own native gRPC services, and `grpc.health.v1.Health` on one listener.

The transport is HTTP/2. Deployments MAY use h2c on loopback or within a pod;
any hop crossing a workload boundary uses TLS, normally mutual TLS. Authority,
SNI, trust roots, and client identity are deployment configuration, not
protocol fields.

## 2. Events and messages

Each `RequestEvent` and `ResponseEvent` is one gRPC message. Event order is as
in exchange-model.md §3. The edge half-closes the request stream when the
request has ended; a server MUST also accept half-close as end of request per
exchange-model §3.1. The server ends the call after the response has ended.

Per-message compression MUST be disabled on the call (`grpc-encoding:
identity`). A server MUST reject a compressed head message with
`INVALID_ARGUMENT`.

## 3. Metadata

Request metadata is for the bridge hop, never for HTTP content. The binding
defines no required keys. Deployments MAY add keys for route identity or
tracing of the bridge call itself, and MUST NOT copy HTTP headers, cookies, or
authorization values into metadata. Trace context for the HTTP request travels
inside the head as `traceparent`, `tracestate`, and `baggage`; a bridge span
named `grpcgi.v1alpha1.HttpBridge/Exchange` is a child of it.

Response initial metadata carries nothing required. Response trailing
metadata carries the RPC status and, on failure, MAY carry
`grpc-status-details-bin` with a `google.rpc.Status` (§5.3).

## 4. Deadlines, cancellation, retries

The edge sets the RPC deadline to the smaller of the downstream deadline and
its route timeout. Expiry maps per §5. Downstream disconnect cancels the RPC.
Server-side cancellation is a non-OK status and maps per §5.

Retries are the edge's decision. The edge MUST NOT retry after any request
body byte has been sent unless the route has explicitly opted into a replay
policy and the whole request is buffered. A head with `end_of_stream` true or
an inline body is retriable under the usual idempotency rules. An accepted
tunnel is never retried.

A session exchange (exchange-model.md §5.1) that the server ends with
`Detach` and `OK` is resumed by the edge on an endpoint of its choosing; that
is not a retry, the request head is re-sent by design with `resume_session`.
An edge that hashes `resume_session` to choose the endpoint gets affinity
without connection stickiness. `UNAVAILABLE` on a session exchange permits
one resume attempt; anything else is an error as usual.

## 5. Status translation

The RPC status is the server's channel; the HTTP status is the application's.
A complete HTTP response ends with `OK` regardless of its status code. When
the RPC ends non-OK, the edge acts by when and where the status arose.

### 5.1 Status from the server before the first ResponseHead

Synthesize an HTTP response using this table.

| gRPC status | HTTP | Failure class | Notes |
|---|---:|---|---|
| `CANCELLED` | 499 | `bridge_error` | Log only; the client is gone. |
| `UNKNOWN` | 500 | `bridge_error` | |
| `INTERNAL` | 500 | `bridge_error` | Also the code the server uses for a protocol error it detected in the application's output. |
| `DATA_LOSS` | 500 | `bridge_error` | |
| `DEADLINE_EXCEEDED` | 504 | `upstream_timeout` | |
| `UNAVAILABLE` | 503 | `upstream_unavailable` | The server's draining or overload signal. Retriable subject to §4. |
| `RESOURCE_EXHAUSTED` | 429 | `request_too_large` if raised while the edge was sending, else `bridge_error` | `Retry-After` from `RetryInfo` when present. |
| `UNIMPLEMENTED` | 502 | `bridge_error` | The peer does not serve the bridge. |
| `INVALID_ARGUMENT` | 502 | `protocol_error` | The edge sent a malformed exchange. The client's request was not at fault. |
| `FAILED_PRECONDITION`, `OUT_OF_RANGE`, `ABORTED`, `ALREADY_EXISTS`, `NOT_FOUND` | 502 | `bridge_error` | These describe the bridge call, not the user's request. |
| `PERMISSION_DENIED`, `UNAUTHENTICATED` | 502 | `bridge_identity` | The peer rejected the edge's identity. 401 or 403 would tell the user to re-authenticate for a fault they cannot fix. |

The upper rows are the canonical `google.rpc.Code` mapping used by every
gRPC-to-HTTP gateway. The lower rows diverge because on this hop the caller is
the edge, so codes about the request's arguments, identity, or preconditions
describe a broken hop and map to 502.

The synthesized response has a fixed body per status, `content-type:
text/plain; charset=utf-8`, `cache-control: no-store`, and no text from
`grpc-message`, which goes to the access log.

### 5.2 Status synthesized by the edge's gRPC client

Connection failure, TLS failure, stream reset, or deadline expiry with nothing
received from the peer use the edge's ordinary upstream-failure codes: 503 for
connect and TLS failure, 504 for timeout, 502 for reset or malformed response.
The server never spoke, so §5.1 does not apply.

### 5.3 Status details

`google.rpc.RetryInfo.retry_delay` becomes a `Retry-After` header on 429 and
503, rounded up to whole seconds. No other detail type affects the response.

### 5.4 Status after the first ResponseHead

The HTTP status is committed. The edge resets the downstream stream:
`CANCEL` for `CANCELLED` and `DEADLINE_EXCEEDED`, `INTERNAL_ERROR` for all
other codes, on HTTP/2 and the HTTP/3 equivalents; it closes the connection on
HTTP/1.1. For an accepted WebSocket it sends close code 1011 first when a
close frame can still be written.

### 5.5 What the server must not do

The server MUST NOT use the RPC status as a shortcut for application
responses, for example `NOT_FOUND` in place of a 404 head. The table maps
those codes to 502 precisely so that doing so is visibly wrong.

## 6. Passthrough of native gRPC

The bridge is for HTTP that is not gRPC. An application's own gRPC services
are served natively on the same listener, and the edge forwards native gRPC
streams to them without encapsulation.

The edge applies a passthrough predicate to each downstream stream before
routing. The default predicate is: the downstream protocol is HTTP/2 or
HTTP/3, the method is `POST`, and `content-type` is `application/grpc` or
begins with `application/grpc+`. `application/grpc-web` and Connect requests
do not match and are bridged as HTTP. An HTTP/1.1 request carrying
`application/grpc` does not match, because the client cannot receive trailers;
it is bridged as HTTP, or handled by the edge's existing HTTP/1.1 gRPC shim.

The predicate is configuration. An operator MAY force all traffic through the
bridge or narrow passthrough to specific routes. Both kinds of stream share
one connection pool, one health view, and one load-balancing decision per
host, which is why the predicate belongs on the upstream protocol settings and
not on a second cluster.

Transcoding filters that rewrite a client dialect into gRPC before routing
(JSON transcoding, gRPC-Web, Connect-to-gRPC) therefore compose unchanged: their
output matches the predicate and reaches the application's native service.

## 7. Limits

| Limit | Default | Notes |
|---|---:|---|
| body chunk size | 64 KiB | Hard maximum 1 MiB. Senders MUST split larger bodies. |
| head size | 64 KiB | Sum of all header names and values plus control data. |
| header count | 256 | Per head, dedicated and overflow together. |
| WebSocket message size | 1 MiB | Enforced at the edge before producing the event. |
| session key | 256 B | Longer keys are a protocol error. |
| session TTL | 60 s default, 1 h maximum | The edge clamps `session_ttl_ms`. |
| detached buffer | 64 KiB | Downstream bytes held while a resume is opening. |
| gRPC max message size | 4 MiB | Must exceed head size plus chunk size. |

A server that exceeds a limit while receiving replies `RESOURCE_EXHAUSTED`;
the edge maps it to 413 if it was still sending the request and to 502 with
class `response_too_large` if it was receiving.

## 8. Health, drain, observability

The server exposes `grpc.health.v1.Health`. The empty service name reports the
process; `grpcgi.v1alpha1.HttpBridge` reports the bridge and SHOULD reflect
the application's lifespan state so that mesh ejection is application-aware.

To drain, the server marks health `NOT_SERVING`, sends GOAWAY on its
connections, answers new `Exchange` calls with `UNAVAILABLE`, and lets
in-flight exchanges complete within a grace period. Session exchanges need
not wait for the grace period: the server sends `Detach` and ends them, and
the edge resumes them elsewhere, so a drain does not disconnect long-lived
clients.

Implementations expose at minimum: exchanges by route and failure class;
active HTTP and tunnel streams; request and response bytes; time to first
response head; total duration; flow-control stall time; and a response flag or
filter-state entry marking bridged versus passthrough streams so that operators
can slice by transport. Logs include request id, route, chosen endpoint, RPC
code, HTTP status or close code, byte counts, and duration. Endpoint addresses
and error text are never metric labels.

## 9. Envoy integration

The correct shape in Envoy is an upstream protocol, not an HTTP filter. The
router selects the host, applies retries, timeouts, hedging, shadowing, and
outlier detection, then hands the stream to a grpcgi upstream that
encapsulates it over the cluster's HTTP/2 connection pool and returns a real
response header map, body, and trailers. Every downstream filter composes
unchanged; `%RESPONSE_CODE%`, `retry_on: 5xx`, and `upstream_rq_5xx` see the
application's status; and the passthrough predicate is a property of the
upstream protocol options, so both stream kinds share one pool. The extension
point is the router's generic upstream, the same one that tunnels CONNECT to
TCP.

A proxy-wasm HTTP filter that generates the response itself is an acceptable
stopgap where that extension point is unavailable, with the understood cost
that router policies do not see the exchange. Such a filter MUST still apply
§5 and MUST set the downstream response code from the head.
