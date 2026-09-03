# grpcgi schema encoding rules

Status: v1alpha1. This document explains the protobuf schema in
`proto/grpcgi/v1alpha1/exchange.proto` and states the rules a sender or
receiver must follow that the `.proto` file cannot express. Semantics are in
exchange-model.md.

## 1. Message set

| Message | Role |
|---|---|
| `RequestEvent`, `ResponseEvent` | One event on a stream. A `oneof` over the kinds below. |
| `RequestHead`, `ResponseHead` | Control data plus header section. |
| `BodyChunk` | A bounded piece of body or raw tunnel bytes, with an end flag. |
| `Trailers` | Trailer section; always the last event in its direction. |
| `WebSocketMessage`, `WebSocketClose` | Tunnel events after an accepted `websocket` CONNECT. |
| `Detach` | Application lets go of a session exchange; the edge resumes it later. |
| `Header` | A name and value for fields not in the frozen table. |

The same message types are used by every binding. They are named for what they
are, events on the request or response stream, rather than for any RPC.

## 2. Control data and values

Method, scheme, authority, path, status, and `:protocol` are typed fields.
There are no pseudo-headers in any header list, which removes the ordering and
uniqueness rules HTTP/2 needs for them and avoids the `:`-prefixed key problem
in every RPC system's metadata.

Header values are `bytes`. Protobuf `string` fields must be valid UTF-8 and
HTTP field values are octets. Names are `string` because they are ASCII tokens
by definition. `path` and `authority` are `string` because URIs are ASCII by
RFC 3986; the edge rejects non-ASCII targets rather than carrying them.

`content_length` is `optional uint64` so that absence is distinguishable from
zero. It is the only header with a numeric type in this version.

Sessions use four control fields: `ResponseHead.session` (203) and
`session_ttl_ms` (204) opt an exchange in; `RequestHead.resume_session` (213)
and `resume_after` (214) re-attach to it; the `Detach` event (kind 6) is how
the application lets go. `session` is `bytes` because it is opaque to the
edge; `resume_after` counts messages or bytes depending on the
exchange, see exchange-model.md §5.1.

## 3. The frozen header table

### 3.1 What it is

Well-known header names have dedicated fields in the heads. A header whose
name is in the table travels only in its field; all other headers travel in
`headers`. The table is in header-table.md and is generated, together with the
proto field blocks, by `docs/spec/tools/header_table.py`. That script is the
only place the table is edited, and `header_table.py check` is run in CI.

### 3.2 Why

It recovers the static-table half of HPACK, roughly 150 to 250 bytes per
request, with no connection state and no security surface. It also makes the
receiver's dispatch a switch on a small integer instead of string compares,
and gives generated code typed accessors for the fields applications actually
look at. Dynamic-table compression across requests is deliberately not a
goal; its value is byte savings on the cheapest hop in the system and its
cost is per-connection state and a compression oracle.

### 3.3 Field layout

Identical for both heads:

| Range | Use |
|---|---|
| 1..15 | One-byte tags. Control data, `end_of_stream`, and the hottest headers for that direction. |
| 16..199 | Two-byte tags. The rest of the table, alphabetical. Unassigned numbers in this range are permanently reserved. |
| 200..299 | Control fields: metavariables, inline `body`, the overflow `headers` list, `extensions`. |

### 3.4 Every dedicated field is `repeated bytes`

HTTP permits any field to appear more than once, and same-name order is
significant. A dedicated field must therefore be able to hold every value for
its name, which makes it `repeated`. A `repeated bytes` field with one element
encodes in exactly the same bytes as a singular one, so there is no cost for
the common case.

### 3.5 The freeze

The table cannot grow after v1. A dedicated field carries no name, so a
receiver that does not know a field number cannot recover the header and
would drop it silently, which is a correctness failure, not a degradation.
HPACK and QPACK froze their static tables for the same reason. Consequently:

- Names not in the table go in `headers`, forever, for this major version.
- The table was sized generously before freezing so that pressure to grow it
  is low. It covers the QPACK static table, the HPACK static table, Fetch
  metadata, client hints, WebSocket, Connect, gRPC-Web, trace propagation,
  message signatures, and the de facto proxy headers.
- A future major version may publish a new table. It is a different package.

### 3.6 Receiver rules

A name that has a dedicated field but appears in `headers` is a protocol
error. So is any pseudo-header name, `host`, or a hop-by-hop name. A receiver
reconstructs a header section by emitting each dedicated field's values under
its name and each `headers` entry as given. The relative order between names
is unspecified.

## 4. Events, unknown fields, and unknown kinds

Unknown fields in any message are ignored, as protobuf requires. An unknown
`oneof` kind in an event is a protocol error, because continuing could
desynchronize stream state. Kind numbers 6..99 are reserved for future
standard kinds (datagrams and capsules are the expected first use); a receiver
that does not implement a kind treats it as unknown.

## 5. Extensibility

Vendor or experimental data goes in the `extensions` field of a head or of
`Trailers`, as `google.protobuf.Any`. A receiver ignores types it does not
know. An extension MUST NOT be required for correct processing of the
exchange; anything that is required belongs in the standard.

## 6. Compression

Head messages MUST NOT be compressed by the transport. In gRPC that means no
`grpc-encoding` on the call. Body bytes pass through with whatever
`content-encoding` the application chose; the bridge never transcodes.

## 7. Size expectations

For a browser-shaped request with about 18 headers and no large cookie, the
head is roughly 550 bytes with the table and 750 without it. HPACK on a warm
connection would send about 120. With a 2 KB cookie the head is about 2.5 KB
either way. These are estimates for design purposes; the conformance suite
carries measured numbers.

## 8. Versioning

- Package `grpcgi.v1alpha1` until two independent implementations pass the
  conformance suite; then `grpcgi.v1` with the same field numbers.
- Within a major version: adding optional fields, control fields, extension
  types, and event kinds in the reserved range is permitted. Changing the
  meaning of an existing field, the event order, or the header table is not.
- Field numbers are never reused.
- `buf breaking` with the `WIRE_JSON` rule set guards the schema in CI.

## 9. Lint

`proto/buf.yaml` excepts three `buf` style rules on purpose: the service is
not suffixed `Service`, and the RPC's request and response types are not
named after the RPC, because the same types serve the local binding.
