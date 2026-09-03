# grpcgi exchange model

Status: v1alpha1. This document is transport-independent. It defines what an
exchange is, the order of events in each direction, the rules for heads,
bodies, trailers, tunnels, and sessions, how errors are reported, and what a conforming
implementation must demonstrate. Bindings (grpc-binding.md, local-binding.md)
say how events are framed and delivered; schema.md says how they are encoded.

The key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are to be
interpreted as described in RFC 2119 and RFC 8174.

## 1. Terminology

- **Edge.** The party that terminates HTTP from clients and originates
  exchanges. A proxy, gateway, or web server.
- **Application.** The party that serves exchanges. It exposes an in-process
  interface (ASGI, WASI HTTP, a Fetch handler, and so on) and never parses
  HTTP framing.
- **Exchange.** One HTTP request and its response, or one tunnel established
  by CONNECT. An exchange has exactly two event streams: the request stream
  from edge to application, and the response stream from application to edge.
- **Event.** One message on a stream: a head, a body chunk, a trailer section,
  a WebSocket message, or a WebSocket close.
- **Head.** Control data plus the header section. `RequestHead` carries
  method, scheme, authority, path, and headers. `ResponseHead` carries status
  and headers.
- **Protocol error.** A violation of this document detected by a receiver.
  §8 says how each side reports one.

## 2. Design goals and non-goals

The model carries any HTTP/2 or HTTP/3 exchange without loss, so that an
application behind an edge observes exactly the semantics of RFC 9110. It
normalizes what HTTP/2 already normalized: lower-case field names, structured
control data, length-delimited bodies, no hop-by-hop fields, no reason
phrases. It adds the small set of metavariables that CGI established and every
in-process interface still needs: peer address, mount prefix, downstream
protocol facts.

It does not define connection management between edge and application, does
not define how applications are started or scaled, does not compress headers
across exchanges, and does not carry server push.

## 3. Exchange structure

### 3.1 Request stream

```
request-stream  = RequestHead
                  ( *BodyChunk [Trailers] )      ; when not a tunnel
                / ( *WebSocketMessage [WebSocketClose] ) ; accepted websocket
                / ( *BodyChunk )                  ; accepted raw tunnel
```

- The first event MUST be exactly one `RequestHead`. A second head is a
  protocol error.
- A request ends in exactly one of three ways: the head has `end_of_stream`
  true; the last `BodyChunk` has `end_of_stream` true; or a `Trailers` event
  is sent, which itself ends the stream. A head with a non-empty inline `body`
  ends the request as if `end_of_stream` were true.
- Every `BodyChunk` except possibly the last MUST have non-empty `data`.
  An empty chunk with `end_of_stream` false is a protocol error.
- Any event after the request has ended is a protocol error.
- For compatibility with transports that lack a separate end signal, a
  receiver MUST also treat transport half-close after a chunk with
  `end_of_stream` false as end of request without trailers. A sender SHOULD
  NOT rely on this.

### 3.2 Response stream

```
response-stream = *InformationalHead FinalHead
                  ( *BodyChunk [Trailers] )
                / ( *WebSocketMessage [WebSocketClose] ) ; after 2xx to websocket CONNECT
                / ( *BodyChunk )                          ; after 2xx to raw CONNECT
InformationalHead = ResponseHead with status 100..199
FinalHead         = ResponseHead with status 200..599
```

- Zero or more informational heads MAY precede the final head. An
  informational head MUST NOT set `end_of_stream` or carry `body`.
- Exactly one final head follows. A second final head is a protocol error.
- The response ends by the same three rules as the request.
- A response to a `HEAD` request, and any response with status 204 or 304,
  MUST NOT carry body events regardless of what the application produced.
  The application-side bridge discards such bodies. `content_length` MAY still
  be present on a `HEAD` response and describes the representation.
- A response MUST NOT carry `1xx` heads when the downstream is HTTP/1.0. The
  edge drops them.

### 3.3 Early response

An application MAY send its final head, and complete its response, before the
request stream has ended. This is normal for rejections and for `Expect:
100-continue` handling. When the response completes with the request still
open:

- the application-side bridge stops delivering request events to the
  application and discards further request data;
- the edge, on observing response completion, stops sending request events
  and resets the downstream request body per RFC 9113 §8.1 (HTTP/2 or HTTP/3
  `RST_STREAM` with `NO_ERROR`, or reading and discarding the remainder on
  HTTP/1.1 when it is small enough to do so; otherwise closing the
  connection).

### 3.4 Buffered mode

A head MAY carry the complete body inline in `body`. This exists so that a
simple client can send one message and stop, and so that unary transports can
carry the protocol. A head with a non-empty `body` and `end_of_stream` false
is a protocol error. A receiver treats an inline body exactly as one
`BodyChunk` with `end_of_stream` true. Inline bodies are subject to the same
limits as streamed bodies plus the binding's maximum message size, and
senders MUST NOT use them for bodies whose size is unknown.

## 4. Heads

### 4.1 Control data

`method` is an enum with `custom_method` for unlisted tokens. `custom_method`
MUST be an upper-case token and MUST be empty unless `method` is
`METHOD_OTHER`.

`scheme` is `http` or `https`. The edge rejects any other scheme with 400.
WebSocket exchanges use `https` or `http`, never `ws` or `wss`.

`authority` is the host and optional port. The edge reconciles `:authority`
and `Host` per RFC 9113 §8.3.1: when both are present and differ, the edge
either rejects with 400 or chooses one by policy; only the result crosses the
wire, and `host` never appears in `headers`. The application-side bridge
synthesizes a `Host` header for interfaces that need one.

`path` is the request target in origin form: path and optional query, starting
with `/`. The exceptions are:

| Request | `path` | `authority` |
|---|---|---|
| `OPTIONS *` | `*` | as sent |
| plain `CONNECT host:port` | empty | `host:port` |
| extended CONNECT (`protocol` set) | as sent, starts with `/` | as sent |
| absolute-form target from an HTTP/1.1 client | origin part only | from the target's authority |

`path` MUST be ASCII. The edge rejects other targets with 400. The edge MUST
NOT normalize percent-encoding, dot segments, or case; that is the
application's decision.

`http_version` records the version spoken by the downstream client, not the
version of any bridge transport.

### 4.2 Header fields

Names are lower-case ASCII tokens (RFC 9110 §5.1). Values are octet strings
with leading and trailing whitespace removed and MUST NOT contain NUL, CR, or
LF. A sender MUST reject a message it cannot represent under these rules
rather than modify it.

The schema gives some names dedicated fields (header-table.md). The rule that
makes this correct is:

> Every value of a given field name travels in exactly one place. If the name
> has a dedicated field, all of its values go there, in order. Otherwise all of
> its values go in `headers`, in order, interleaved with other names in the
> order received.

Order among values of one name is significant and MUST be preserved. Order
among different names is not significant (RFC 9110 §5.3) and MAY change.
Receivers MUST NOT comma-join values; in particular `set-cookie` is never
joined. `cookie` values split into multiple fields by an HTTP/2 or HTTP/3
client are preserved as separate values and MAY be joined with `; ` by the
application-side bridge when the in-process interface requires a single value.

The following names MUST NOT appear in `headers` or in trailers; a receiver
treats them as a protocol error: any name beginning with `:`; `host`;
`connection`; `keep-alive`; `proxy-connection`; `transfer-encoding`;
`upgrade`; `http2-settings`; and any name that has a dedicated field.

`te` MAY appear only with the value `trailers`.

### 4.3 Typed fields

`content_length` is a typed field. The edge parses `Content-Length` and
rejects the message with 400 (request) or treats it as a protocol error
(response) when the header is absent from the wire but required, present more
than once with differing values, or not a valid non-negative integer. When the
body length is unknown, the field is absent. A sender that supplies
`content_length` MUST send exactly that many body bytes; a receiver MUST treat
a mismatch as a protocol error. The typed form is lossy by design: an
encapsulation that validates is a feature, as it was for HTTP/2.

### 4.4 Metavariables

`remote_address` and `remote_port` are the downstream peer after the edge's
trusted-proxy policy has been applied, which is to say the value the edge
would place in its own logs as the client. They are assertions by the edge.
An application MUST treat them as trustworthy only when the bridge peer is
authenticated and authorized to assert client identity. `local_address` and
`local_port` are where the edge accepted the connection. `tls` and
`tls_server_name` describe the downstream connection. `root_path` is the mount
prefix and is included in `path`.

Client certificate identity is not a metavariable. It travels in headers such
as `x-forwarded-client-cert`, because that is where existing applications
already look for it.

### 4.5 Edge-internal headers

The edge MUST remove its own internal headers before encapsulation, and MUST
document which names it removes. The application sees only what a client could
legitimately have sent plus what the edge intends to assert. This is the
"safe headers" rule from Google's App Engine protocol and it is what makes
metavariable trust tractable.

## 5. Tunnels and WebSocket

A tunnel is requested with `method` `METHOD_CONNECT`. `protocol` empty means a
plain CONNECT to `authority`; `protocol` non-empty means extended CONNECT
(RFC 8441) for that protocol, with `path`, `scheme`, and `authority` set as
for a normal request.

The edge normalizes an HTTP/1.1 `GET` with `Upgrade: websocket` into a
`METHOD_CONNECT` head with `protocol` `websocket`, removing `connection`,
`upgrade`, `sec-websocket-key`, and computing nothing on the application's
behalf. `sec-websocket-version`, `sec-websocket-protocol`,
`sec-websocket-extensions`, and `origin` are forwarded. On the way back the
edge synthesizes the 101 response and `sec-websocket-accept` itself.

The application answers with a `ResponseHead`:

- A status of 200..299 accepts the tunnel. The head MUST NOT carry `body`,
  `content_length`, or `end_of_stream`. For `websocket`, the head MAY carry
  one `sec-websocket-protocol` value, which MUST be one the client offered.
  Extension negotiation is not forwarded in this version; the edge owns
  WebSocket compression.
- Any other status rejects the tunnel. The response then proceeds as a normal
  response, with body and trailers, and the exchange ends.

After acceptance:

- For `protocol` `websocket`, both streams carry `WebSocketMessage` events and
  at most one `WebSocketClose`. Each message is one complete WebSocket message,
  not one frame: the edge reassembles fragments, unmasks client frames,
  enforces the message size limit, and validates text as UTF-8. Ping and pong
  never cross the bridge. Either side MAY send `WebSocketClose`; after sending
  it, that side sends no more messages. A close with code 0 means the transport
  ended without a close frame. The peer SHOULD answer a close with its own
  close. A clean close completes the exchange normally.
- For any other `protocol`, and for plain CONNECT, both streams carry
  `BodyChunk` events as raw tunnel bytes. `end_of_stream` true half-closes
  that direction. Datagram-capable protocols are out of scope for this version;
  event kinds 6..99 are reserved for them.

### 5.1 Resumable sessions

A long-lived exchange pins a downstream connection to one application
instance for as long as it lives. Sessions decouple the two: the application
names the exchange, and the edge may then end the exchange and re-attach the
same downstream connection to a new one, on any instance, without the
downstream noticing. Deploys, drains, and rebalancing stop being visible to a
WebSocket or an event stream.

**Opt in.** The application puts `session` (an opaque key, 1..256 bytes) on
the final head of an accepted tunnel, or on a final head whose response body
has not ended. `session_ttl_ms` says how long after the exchange ends the
application will honor a resume for that key; 0 means the edge default of
60 000. `session` on an informational head is a protocol error. `session` on
a head with `end_of_stream`, or on an exchange whose request body is still
arriving, is ignored: only exchanges whose request side is complete are
resumable. Without `session` nothing below applies and the exchange lives
exactly as long as the downstream connection.

**Detach.** Only the application detaches. After a head with `session`, when
it wants to let go of the exchange without ending the session (it is draining,
the session is idle, it is scaling down), it sends `Detach` and then ends the
RPC normally. `Detach` is the last event it sends. `Detach` on an exchange
whose final head did not carry `session` is a protocol error. The edge records
how much of the application's output the downstream has received:
`WebSocketMessage` events for a websocket tunnel, body bytes for a response
body. The downstream connection stays open and sees nothing. The edge resumes
as soon as downstream data arrives, and otherwise no later than
`Detach.retry_after_ms` after the detach (0 means now), always before the
session TTL expires. Downstream data arriving while detached MAY be buffered
up to a limit while the new exchange opens; beyond the limit the edge closes
the downstream (WebSocket code 1013).

Edge cancellation is not a detach. It means the downstream went away, and the
application treats the session like any other abandoned state. An exchange
that ends with the binding's unavailable status is not a detach either, but
because the application arbitrates every resume, the edge MAY attempt one
resume before failing the downstream: an application that holds no state for
the key answers 410 and nothing is lost.

**Resume.** The edge opens a new exchange whose `RequestHead` is the original
head with `end_of_stream` true, `resume_session` set to the key, and
`resume_after` set to the recorded count. There is no request body. The
application answers with a final head:

- 2xx: the session continues. The application resumes its output after
  `resume_after`: for a websocket, the first `WebSocketMessage` it sends is the
  one the downstream has not seen; for a body, the first `BodyChunk` starts at
  byte `resume_after`. Headers on a resumed head are not forwarded, the
  downstream already has them. The head MAY carry a new `session` to rotate
  the key and a new `session_ttl_ms`; without `session` the exchange is no
  longer resumable.
- 410 Gone: the key is unknown or expired. Any other status is treated the
  same way. The edge ends the downstream: close code 1012 for a websocket, a
  reset or connection close for a body stream.

Delivery from the downstream to the application is at most once across a
detach: a message the edge sent on the old exchange may not have been
processed if the instance failed. Applications that need more use their own
acknowledgements, as they do today. Where session state lives is the
application's concern: a shared store, or endpoint affinity on the session
key, which the edge MAY implement by hashing `resume_session` when choosing
an instance.

## 6. Cancellation and deadlines

Downstream disconnect cancels the exchange immediately in both directions.
Application-side cancellation before the final head produces no downstream
response if the downstream is already gone, and otherwise a synthesized error
response per the binding. Cancellation after the final head resets the
downstream stream; the edge MUST NOT manufacture a second response.

A deadline covers the whole exchange: connect, request delivery, application
work, and response delivery. The edge propagates the smaller of the downstream
deadline and its route deadline. Expiry before the final head yields 504;
expiry after it resets the stream.

## 7. Flow control and limits

Implementations rely on the transport's flow control and MUST NOT add
unbounded queues. Reading from one side stops when the other cannot accept
data. Concretely, an application-side bridge MUST NOT read a request chunk
from the transport until the application has asked for it, and MUST NOT
accept a response chunk from the application until the transport can take it.

Every implementation exposes finite, configurable limits for: head size in
bytes; header count; body chunk size (default 64 KiB, hard maximum set by the
binding); WebSocket message size; total request body size when buffering is
required; and concurrent exchanges. Limit checks happen before allocating the
declared size. A sender MUST split bodies into chunks no larger than the
receiver's advertised or configured maximum.

## 8. Errors

There are three classes of failure and each has one channel.

1. **Application response.** Anything the application decides, including its
   own 4xx and 5xx. Travels in the response head. Never affects the bridge's
   status.
2. **Bridge or server failure before the final head.** Protocol errors
   detected by the application side, overload, draining, unsupported
   features, internal errors in the bridge. Reported by the binding's status
   channel (the gRPC status, or the local binding's END frame). The edge
   translates it to an HTTP status by the binding's fixed table.
3. **Failure after the final head.** Nothing can change the HTTP status.
   The binding's status channel still carries the code for logging and the
   edge resets the downstream stream.

The edge reports a protocol error made by the application by treating it as
class 2 with code `INTERNAL`, logging the specific violation, and never
forwarding application error text to the client. The application side reports
a protocol error made by the edge with `INVALID_ARGUMENT`, which the edge
maps to 502, because the client's request was not at fault.

Failure classes for metrics and logs are stable strings, not status codes:
`protocol_error`, `upstream_unavailable`, `upstream_timeout`,
`request_too_large`, `response_too_large`, `bridge_identity`,
`bridge_error`, `application_error`.

## 9. Security considerations

**Request smuggling is structurally excluded.** The edge fully parses HTTP and
the body is length-delimited on the wire; there is no framing ambiguity for an
application to interpret differently. This is the same argument HTTP/2 made
against HTTP/1.1 and it applies here in full.

**Header injection.** NUL, CR, and LF are protocol errors in names and values
in both directions. The edge validates response heads for pseudo-header names,
hop-by-hop names, and dedicated names appearing in `headers`, so an
application cannot inject them into the downstream response.

**Metavariable trust.** `remote_address` and friends are assertions. An
application MUST accept them only from an authenticated and authorized bridge
peer. Deployments MUST use mutual authentication whenever the bridge hop
crosses a workload boundary; loopback and same-pod hops MAY be plaintext.

**Resource exhaustion.** Declared sizes are checked before allocation. Chunk
size, head size, header count, and message size are bounded. WebSocket
fragments are reassembled at the edge under a limit, never at the application.

**Compression oracles.** There is no cross-exchange dictionary and head
messages are never compressed. Body `content-encoding` passes through
untouched.

**Information leakage.** Bridge and server error text is logged, not sent to
clients. Endpoint addresses and error strings are never metric labels.

**Path confusion.** `root_path` is informational; `path` is always the full
path. The edge does not normalize the path, so an application that makes
authorization decisions on it must normalize first, as it must today.

## 10. Conformance

An implementation is conforming when it passes the conformance suite, which
MUST cover at least:

- bodyless request via `end_of_stream`, via inline empty body, and via
  transport half-close;
- streamed request with chunk boundaries at 1 byte, at the maximum chunk
  size, and across the `content_length` boundary (rejected);
- request and response trailers, including a chunk with `end_of_stream` false
  followed by trailers;
- `HEAD`, 204, and 304 body suppression;
- informational responses, including 103 before 200 and 100 before reading
  the body;
- early response with the request body still arriving, in both HTTP/1.1 and
  HTTP/2 downstream forms;
- duplicate headers in both dedicated fields and the overflow list, with
  order preserved; `set-cookie` never joined; `cookie` crumbs preserved;
- byte-valued header values, including 0x80..0xFF and horizontal tab;
- rejected inputs: pseudo-header in `headers`, `host` in `headers`, dedicated
  name in `headers`, CR in a value, non-ASCII path, unknown scheme,
  `content_length` mismatch, second head, event after end;
- every row of the binding's status translation table, before and after the
  final head;
- cancellation from each side before and after the final head;
- flow control under a slow reader on each side, with bounded memory;
- limits: head size, header count, chunk size, WebSocket message size;
- WebSocket: accept with and without subprotocol, reject with a body,
  fragmented input reassembled, invalid UTF-8 text rejected at the edge,
  ping and pong locality, close handshake from each side, abrupt disconnect
  mapped to code 0, HTTP/1.1 upgrade normalized to extended CONNECT;
- plain CONNECT tunnel bytes in both directions with half-close;
- sessions: `session` on an accepted websocket and on a streaming body;
  `Detach` with and without `retry_after_ms`, followed by a resume with the
  right `resume_after` for messages and for bytes; downstream data during a
  detach triggers the resume; `Detach` without a session rejected;
  cancellation is not a detach; one resume attempt after server unavailable;
  410 on an unknown key; key rotation on resume; `session` ignored on a head
  with `end_of_stream` and rejected on an informational head;
- graceful drain: the application signals draining, in-flight exchanges
  complete, new exchanges are refused with the binding's unavailable code;
- every golden vector in vectors/vectors.md, encode and decode.
