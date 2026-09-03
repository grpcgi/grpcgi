# Mappings to in-process interfaces (informative)

These mappings are not normative, but a bridge for one of these interfaces
that cannot round-trip the exchange model without loss indicates a defect in
either the mapping or the model. Report it.

## ASGI (HTTP)

| Exchange model | ASGI `http` scope or event |
|---|---|
| `RequestHead.method` / `custom_method` | `scope["method"]` |
| `path` before `?` | `scope["path"]` (percent-decoded per ASGI) and `scope["raw_path"]` (as sent) |
| `path` after `?` | `scope["query_string"]` (bytes) |
| `scheme` | `scope["scheme"]` |
| `authority` | synthesized `host` header, first in `scope["headers"]` |
| `http_version` | `scope["http_version"]` (`"1.0"`, `"1.1"`, `"2"`, `"3"`) |
| `remote_address`, `remote_port` | `scope["client"]` |
| `local_address`, `local_port` | `scope["server"]` |
| `root_path` | `scope["root_path"]` |
| dedicated fields and `headers` | `scope["headers"]` as `(bytes, bytes)` pairs; `cookie` values may be joined with `; ` |
| `BodyChunk` | `http.request` with `more_body = not end_of_stream` |
| request `Trailers` | ASGI HTTP trailers extension when supported; otherwise drained and discarded |
| response `ResponseHead` 1xx | `http.response.start` is final-only in ASGI; 103 requires the early hints extension, otherwise the bridge drops it |
| final `ResponseHead` | `http.response.start` with `status` and `headers`; `content-length` becomes the typed field |
| `http.response.body` | `BodyChunk` with `end_of_stream = not more_body` |
| response `Trailers` | ASGI HTTP response trailers extension |
| early response | bridge stops calling `receive`; remaining request data discarded |
| downstream disconnect | `http.disconnect` |

`server` in the scope is the edge's local address, not the authority. The
prototype got this wrong.

## ASGI (WebSocket)

| Exchange model | ASGI `websocket` scope or event |
|---|---|
| `RequestHead` with `METHOD_CONNECT`, `protocol = "websocket"` | scope with `scheme` `ws`/`wss` derived from `scheme`, `subprotocols` parsed from `sec-websocket-protocol` |
| `websocket.accept` | `ResponseHead` status 200, `sec_websocket_protocol` from `subprotocol`, extra headers copied |
| `websocket.close` before accept, or `websocket.http.response.*` | `ResponseHead` with the given status (default 403) and body |
| `WebSocketMessage` | `websocket.receive` with `text` or `bytes` |
| `websocket.send` | `WebSocketMessage` |
| `WebSocketClose` from the edge | `websocket.disconnect` with `code` (0 maps to 1006) |
| `websocket.close` after accept | `WebSocketClose` with `code` and `reason` |

## WSGI

Wrap with an ASGI adapter, or map directly: `REQUEST_METHOD`, `SCRIPT_NAME`
from `root_path`, `PATH_INFO` and `QUERY_STRING` from `path`, `SERVER_NAME` and
`SERVER_PORT` from `authority`, `REMOTE_ADDR` from `remote_address`,
`wsgi.url_scheme` from `scheme`, `CONTENT_TYPE` and `CONTENT_LENGTH` from the
typed and dedicated fields, `HTTP_*` from the rest. WSGI has no trailers, no
1xx, and no WebSocket; the bridge discards or rejects accordingly.

## WASI HTTP (`wasi:http/incoming-handler`)

`incoming-request` fields map one to one: `method` (with `other` for
`METHOD_OTHER`), `scheme`, `authority`, `path-with-query`, `headers`.
`consume` yields the body stream from `BodyChunk` events; `finish` yields
trailers. `response-outparam` receives `outgoing-response` with status and
headers; the outgoing body stream produces `BodyChunk` events and
`outgoing-body.finish` produces `Trailers`. Informational responses are not
representable in WASI HTTP 0.2 and are dropped. WebSocket is not representable
and a CONNECT is rejected with 501 by the bridge.

## Fetch handler (WinterTC)

`Request` has `method`, `url` built from `scheme`, `authority`, and `path`,
`headers` (lower-cased, `set-cookie` kept separate via `getSetCookie`), and a
`ReadableStream` body from `BodyChunk` events. `Response` maps back the same
way. `WebSocketPair` style upgrades map to the tunnel accept. Fetch has no
trailers and no 1xx.

## Rack

`env` is built as for WSGI. `rack.hijack` maps to a raw CONNECT tunnel after a
200 head. WebSocket libraries that use hijack therefore work unchanged, with
the edge doing the framing.

## Servlet and reactive JVM

`HttpServletRequest` getters map to the head; `getInputStream` to chunks;
trailers via `getTrailerFields`; `sendError` and normal responses to the
final head; 103 via `sendEarlyHints` where the container supports it. Jakarta
WebSocket endpoints map to the tunnel events.
