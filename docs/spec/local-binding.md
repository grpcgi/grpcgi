# grpcgi local process binding (sketch)

Status: design sketch, not yet normative. This binding carries the exchange
model between an edge and an application process on the same host with no
RPC stack: length-prefixed protobuf frames over a Unix domain socket or over
the process's standard input and output. It is the replacement for CGI,
FastCGI, SCGI, and the uwsgi protocol, and it exists so that a small binary in
any language can serve HTTP behind an edge by linking only a protobuf decoder.

The messages are exactly those of `exchange.proto`. Nothing about heads,
bodies, trailers, or tunnels changes. This document only defines framing,
multiplexing, and the status channel.

## 1. Connection

Either a `SOCK_STREAM` Unix domain socket accepted by the application, or a
single connection formed by the application's stdin (edge to application) and
stdout (application to edge). Stderr is the application's log and is not part
of the protocol. How the process is started, how many there are, and who
restarts them is out of scope; FastCGI's experience is that specifying this
fragments implementations.

## 2. Frames

All integers are big-endian.

```
+--------+--------+----------+----------+------------------+
| type   | flags  | stream   | length   | payload          |
| 1 byte | 1 byte | 4 bytes  | 4 bytes  | `length` bytes   |
+--------+--------+----------+----------+------------------+
```

| type | name | payload | direction |
|---:|---|---|---|
| 0 | `SETTINGS` | `Settings` message (below) | both, stream 0 only |
| 1 | `EVENT` | one `RequestEvent` (edge to app) or `ResponseEvent` (app to edge) | both |
| 2 | `END` | `Status` message: code and message | app to edge |
| 3 | `RESET` | `Status` message | both |
| 4 | `GOAWAY` | last accepted stream id, `Status` | both, stream 0 only |
| 5 | `PING` | 8 opaque bytes | both, stream 0 only |

`Status.code` uses the `google.rpc.Code` numbering so that §5 of
grpc-binding.md applies unchanged. `END` is the equivalent of the gRPC status
trailer: the application sends it after its last `ResponseEvent`, with code 0
after a complete response and a non-zero code otherwise. `RESET` cancels one
stream from either side.

## 3. Streams

Stream ids are odd, start at 1, and are assigned by the edge in increasing
order. Stream 0 is the connection. Multiplexing is mandatory: an application
MUST accept concurrent streams up to the limit it advertises in `SETTINGS`,
and an edge MUST NOT open more than that. Making multiplexing optional is the
mistake FastCGI made; nobody implemented it and everyone assumed one stream
per connection.

A stream begins with the edge's `EVENT` carrying a `RequestHead` and ends
when the application has sent `END` and both directions have delivered their
final event, or when either side sends `RESET`.

## 4. Settings

`Settings` is a small protobuf message exchanged first on stream 0 by both
sides: maximum concurrent streams, maximum chunk size, maximum head size,
maximum WebSocket message size, and the binding version. A side MUST NOT
exceed the peer's advertised limits. Defaults are those in grpc-binding.md §7.

## 5. Flow control

There is no window mechanism in the frame layer. Backpressure is the socket's
send buffer, which is adequate on a local host only if both sides honor the
rule in exchange-model.md §7: never read an event you are not ready to hand
onward, and never accept an event you cannot write. An implementation MUST
NOT buffer more than one chunk per stream per direction beyond what the
application has requested. If experience shows this is insufficient, a
per-stream window frame will be added in the reserved type range before v1.

## 6. Stdio mode

Over stdin and stdout there is exactly one connection and the application
exits when stdin closes. This is the direct CGI replacement: the edge spawns
the process, writes `SETTINGS` and the first `EVENT`, and reads until `END`.
Streams still carry ids so that the same code serves both modes.

## 7. Open questions

- Whether `SETTINGS` should be a fixed binary struct instead of a protobuf, so
  that the first bytes of a connection can be parsed without a decoder.
- Whether a per-stream window frame is needed from the start.
- Socket activation and readiness signaling conventions for supervisors.
