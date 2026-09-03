#!/usr/bin/env python3
"""Single source of truth for the frozen grpcgi header table.

This script owns the assignment of protobuf field numbers to well-known HTTP
header names. It renders two artifacts and must be the only way either is
edited:

  * the generated field blocks inside proto/grpcgi/v1alpha1/exchange.proto
    (between the BEGIN/END GENERATED markers), and
  * docs/spec/header-table.md, the human-readable registry.

The table is FROZEN once v1 ships. Adding a dedicated field later is a wire
break: an older receiver cannot recover the header name from an unknown
field number and would silently drop the header. New names go to the
overflow `headers` list forever. See docs/spec/schema.md.

Usage:
  header_table.py check   # verify proto + registry match this file
  header_table.py write   # rewrite generated blocks and the registry
"""
from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[3]
PROTO = ROOT / "proto/grpcgi/v1alpha1/exchange.proto"
REGISTRY = ROOT / "docs/spec/header-table.md"

# ---------------------------------------------------------------------------
# Field number layout (identical for RequestHead and ResponseHead)
#
#   1..15     one-byte tags: pseudo-fields, end_of_stream, and the hottest
#             headers for that direction.
#   16..199   two-byte tags: the remainder of the frozen header table, in
#             alphabetical order. Unassigned numbers in this range are
#             permanently reserved.
#   200..299  control fields (metavariables, inline body, overflow, extensions).
# ---------------------------------------------------------------------------

# Hot request headers, in field-number order starting at 6.
# (1 method, 2 path, 3 authority, 4 scheme, 5 end_of_stream)
REQUEST_HOT = [
    "cookie",
    "user-agent",
    "accept",
    "accept-encoding",
    "content-type",
    # 11 is content_length (typed) -- see TYPED below
    "authorization",
    "x-request-id",
    "traceparent",
    "x-forwarded-for",
]

# Hot response headers, in field-number order starting at 3.
# (1 status, 2 end_of_stream)
RESPONSE_HOT = [
    "content-type",
    # 4 is content_length (typed)
    "set-cookie",
    "cache-control",
    "etag",
    "date",
    "vary",
    "content-encoding",
    "location",
    "server",
    "last-modified",
    "x-request-id",
    "access-control-allow-origin",
]

# Typed fields occupy a slot inside the hot range.
TYPED = {
    "request": {11: ("content-length", "optional uint64 content_length")},
    "response": {4: ("content-length", "optional uint64 content_length")},
}

# Remaining request headers. Sources: QPACK static table (RFC 9204), HPACK
# static table (RFC 7541), Fetch metadata and client hints, WebSocket
# (RFC 6455/8441), Connect and gRPC-Web, W3C/B3 trace propagation,
# de facto proxy headers (x-forwarded-*, x-envoy-*), HTTP message signatures
# (RFC 9421), and extensible priorities (RFC 9218).
REQUEST_REST = sorted({
    "accept-charset", "accept-language", "access-control-request-headers",
    "access-control-request-method", "alt-used", "baggage", "cache-control",
    "connect-accept-encoding", "connect-content-encoding",
    "connect-protocol-version", "connect-timeout-ms", "content-disposition",
    "content-encoding", "content-language", "date", "dnt", "early-data",
    "expect", "forwarded", "from", "idempotency-key", "if-match",
    "if-modified-since", "if-none-match", "if-range", "if-unmodified-since",
    "max-forwards", "origin", "pragma", "prefer", "priority",
    "proxy-authorization", "purpose", "range", "referer", "save-data",
    "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform", "sec-fetch-dest",
    "sec-fetch-mode", "sec-fetch-site", "sec-fetch-user", "sec-gpc",
    "sec-websocket-extensions", "sec-websocket-key", "sec-websocket-protocol",
    "sec-websocket-version", "signature", "signature-input", "te", "trailer",
    "tracestate", "upgrade-insecure-requests", "via", "x-api-key",
    "x-b3-flags", "x-b3-parentspanid", "x-b3-sampled", "x-b3-spanid",
    "x-b3-traceid", "x-csrf-token", "x-envoy-attempt-count",
    "x-envoy-decorator-operation", "x-envoy-downstream-service-cluster",
    "x-envoy-downstream-service-node", "x-envoy-expected-rq-timeout-ms",
    "x-envoy-external-address", "x-envoy-force-trace", "x-envoy-internal",
    "x-envoy-original-path", "x-forwarded-client-cert", "x-forwarded-host",
    "x-forwarded-port", "x-forwarded-proto", "x-grpc-web",
    "x-http-method-override", "x-real-ip", "x-requested-with", "x-user-agent",
})

RESPONSE_REST = sorted({
    "accept-ch", "accept-ranges", "access-control-allow-credentials",
    "access-control-allow-headers", "access-control-allow-methods",
    "access-control-expose-headers", "access-control-max-age", "age",
    "allow", "alt-svc", "cache-status", "clear-site-data",
    "connect-content-encoding", "content-disposition", "content-language",
    "content-location", "content-range", "content-security-policy",
    "content-security-policy-report-only", "cross-origin-embedder-policy",
    "cross-origin-opener-policy", "cross-origin-resource-policy",
    "expect-ct", "expires", "link", "nel", "permissions-policy", "pragma",
    "preference-applied", "priority", "proxy-authenticate", "referrer-policy",
    "refresh", "report-to", "reporting-endpoints", "repr-digest",
    "retry-after", "sec-websocket-accept", "sec-websocket-extensions",
    "sec-websocket-protocol", "server-timing", "signature", "signature-input",
    "strict-transport-security", "timing-allow-origin", "trailer", "via",
    "www-authenticate", "x-content-type-options", "x-frame-options",
    "x-powered-by", "x-robots-tag", "x-xss-protection",
})

# Names that MUST NOT appear anywhere in a head: pseudo-headers are carried by
# typed fields, `host` is folded into authority, and the rest are hop-by-hop.
FORBIDDEN = [
    ":authority", ":method", ":path", ":protocol", ":scheme", ":status",
    "host", "connection", "keep-alive", "proxy-connection",
    "transfer-encoding", "upgrade", "http2-settings",
]

HOT_START = {"request": 6, "response": 3}
REST_START = 16
REST_END = 199


def field_name(header: str) -> str:
    return header.replace("-", "_")


def layout(direction: str) -> list[tuple[int, str, str]]:
    """Return [(number, header_name, proto_field_decl)] for one head."""
    hot = REQUEST_HOT if direction == "request" else RESPONSE_HOT
    typed = TYPED[direction]
    out: list[tuple[int, str, str]] = []
    n = HOT_START[direction]
    hot_iter = iter(hot)
    while n <= 15:
        if n in typed:
            header, decl = typed[n]
            out.append((n, header, f"{decl} = {n};"))
        else:
            try:
                header = next(hot_iter)
            except StopIteration:
                break
            out.append((n, header, f"repeated bytes {field_name(header)} = {n};"))
        n += 1
    assert next(hot_iter, None) is None, "too many hot headers"
    rest = REQUEST_REST if direction == "request" else RESPONSE_REST
    n = REST_START
    for header in rest:
        assert n <= REST_END, "header table overflow; the range is fixed"
        out.append((n, header, f"repeated bytes {field_name(header)} = {n};"))
        n += 1
    return out


def render_block(direction: str) -> str:
    rows = layout(direction)
    lines = [f"  // Frozen header table ({direction}). Generated by docs/spec/tools/header_table.py."]
    lines.append("  // Every value of a header named here MUST be carried in its field, never in")
    lines.append("  // `headers`. Order among values of one name is significant and preserved.")
    last_hot = max(n for n, _, _ in rows if n <= 15)
    for n, header, decl in rows:
        if n == REST_START:
            lines.append("")
            lines.append("  // Remainder of the table, alphabetical. Two-byte tags.")
        lines.append(f"  {decl}  // {header}")
    last = rows[-1][0]
    if last < REST_END:
        lines.append("")
        lines.append(f"  // Permanently reserved. The table is frozen; new names use `headers`.")
        lines.append(f"  reserved {last + 1} to {REST_END};")
    return "\n".join(lines)


def render_registry() -> str:
    parts = ["# grpcgi frozen header table", ""]
    parts.append("Generated by `docs/spec/tools/header_table.py`. Do not edit by hand.")
    parts.append("")
    parts.append("This registry assigns a protobuf field number to each well-known HTTP field")
    parts.append("name in `RequestHead` and `ResponseHead`. It is frozen for the life of the")
    parts.append("major version: numbers are never added, removed, or renumbered, because an")
    parts.append("older receiver cannot recover a header name from an unknown field number and")
    parts.append("would silently drop the header. Any name not listed here travels in the")
    parts.append("overflow `headers` list. See `schema.md` for the encoding rules.")
    parts.append("")
    parts.append("Every entry is `repeated bytes` unless marked typed. Values for a listed name")
    parts.append("MUST all be carried in the dedicated field; a receiver treats the same name")
    parts.append("appearing in `headers` as a protocol error.")
    parts.append("")
    for direction in ("request", "response"):
        rows = layout(direction)
        parts.append(f"## {direction.capitalize()} head")
        parts.append("")
        parts.append("| Field | Header | Notes |")
        parts.append("|---:|---|---|")
        typed = TYPED[direction]
        for n, header, decl in rows:
            note = ""
            if n in typed:
                note = "typed, validated at the edge"
            elif n <= 15:
                note = "one-byte tag"
            parts.append(f"| {n} | `{header}` | {note} |")
        last = rows[-1][0]
        parts.append(f"| {last + 1}..{REST_END} | reserved | permanently unassigned |")
        parts.append("")
    parts.append("## Names that never appear in a head")
    parts.append("")
    parts.append("Pseudo-headers are carried by typed fields, `host` is folded into `authority`,")
    parts.append("and hop-by-hop fields are removed by the edge. A sender MUST NOT emit these in")
    parts.append("`headers`; a receiver MUST treat them as a protocol error.")
    parts.append("")
    for name in FORBIDDEN:
        parts.append(f"- `{name}`")
    parts.append("")
    parts.append("## Sources")
    parts.append("")
    parts.append("QPACK static table (RFC 9204 Appendix A), HPACK static table (RFC 7541")
    parts.append("Appendix A), Fetch metadata and client hints, WebSocket (RFC 6455, RFC 8441),")
    parts.append("Connect and gRPC-Web, W3C Trace Context and B3 propagation, HTTP message")
    parts.append("signatures (RFC 9421), extensible priorities (RFC 9218), and de facto proxy")
    parts.append("headers (`x-forwarded-*`, `x-envoy-*`).")
    parts.append("")
    return "\n".join(parts)


BEGIN = "  // BEGIN GENERATED {direction} HEADER TABLE"
END = "  // END GENERATED {direction} HEADER TABLE"


def splice(text: str, direction: str) -> str:
    b = BEGIN.format(direction=direction.upper())
    e = END.format(direction=direction.upper())
    pattern = re.compile(re.escape(b) + r".*?" + re.escape(e), re.S)
    if not pattern.search(text):
        sys.exit(f"markers for {direction} not found in {PROTO}")
    return pattern.sub(lambda _: f"{b}\n{render_block(direction)}\n{e}", text)


def main(argv: list[str]) -> int:
    mode = argv[1] if len(argv) > 1 else "check"
    proto = PROTO.read_text()
    new_proto = splice(splice(proto, "request"), "response")
    new_registry = render_registry()
    if mode == "write":
        PROTO.write_text(new_proto)
        REGISTRY.write_text(new_registry)
        print(f"wrote {PROTO.relative_to(ROOT)} and {REGISTRY.relative_to(ROOT)}")
        return 0
    ok = True
    if new_proto != proto:
        print("exchange.proto generated blocks are stale; run `header_table.py write`")
        ok = False
    if not REGISTRY.exists() or REGISTRY.read_text() != new_registry:
        print("header-table.md is stale; run `header_table.py write`")
        ok = False
    for direction in ("request", "response"):
        rows = layout(direction)
        print(f"{direction}: {len(rows)} dedicated fields, last number {rows[-1][0]}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
