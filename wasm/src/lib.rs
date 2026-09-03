// Proxy-wasm network filter: bridges WebSocket connections to the grpcgi
// gRPC service (grpcgi.v1.WebSocketBridge/Connect).
//
// Data flow:
//   downstream client  <-->  [this filter]  <-->  grpcgi via gRPC bidi stream
//
// The filter operates as a StreamContext (L4) so it sees raw TCP bytes.
// WebSocket upgrade is performed by the dummy upstream behind Envoy; the
// filter watches for the 101 response and then takes over both directions.
// All WS control frames are absorbed here; only data frames (opcode 0x01/0x02)
// are forwarded to grpcgi as Frame proto messages.

use log::{debug, error, info, warn};
use proxy_wasm::traits::{Context, RootContext, StreamContext};
use proxy_wasm::types::{Action, ContextType, LogLevel};

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Debug);
    proxy_wasm::set_root_context(|_context_id| -> Box<dyn RootContext> {
        Box::new(WsBridgeRoot {
            grpc_cluster: "grpcgi_cluster".to_string(),
            grpc_service: "grpcgi.v1.WebSocketBridge".to_string(),
        })
    });
}}

// ---------------------------------------------------------------------------
// Root context
// ---------------------------------------------------------------------------

struct WsBridgeRoot {
    grpc_cluster: String,
    grpc_service: String,
}

impl Context for WsBridgeRoot {}

impl RootContext for WsBridgeRoot {
    fn on_configure(&mut self, _plugin_configuration_size: usize) -> bool {
        if let Some(config_bytes) = self.get_plugin_configuration() {
            if let Ok(config_str) = std::str::from_utf8(&config_bytes) {
                // Minimal JSON parsing: look for "grpc_cluster" and
                // "grpc_service" string values without pulling in serde.
                // Expected format: {"grpc_cluster":"...", "grpc_service":"..."}
                if let Some(v) = extract_json_string(config_str, "grpc_cluster") {
                    self.grpc_cluster = v;
                }
                if let Some(v) = extract_json_string(config_str, "grpc_service") {
                    self.grpc_service = v;
                }
            }
        }
        info!(
            "ws-bridge configured: cluster={} service={}",
            self.grpc_cluster, self.grpc_service
        );
        true
    }

    fn get_type(&self) -> Option<ContextType> {
        Some(ContextType::StreamContext)
    }

    fn create_stream_context(&self, _context_id: u32) -> Option<Box<dyn StreamContext>> {
        Some(Box::new(WsBridgeStream {
            state: State::AwaitingUpgradeRequest,
            downstream_buf: Vec::new(),
            grpc_stream_id: None,
            upgrade_headers: Vec::new(),
            grpc_cluster: self.grpc_cluster.clone(),
            grpc_service: self.grpc_service.clone(),
        }))
    }
}

// ---------------------------------------------------------------------------
// Stream context
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
enum State {
    AwaitingUpgradeRequest,
    AwaitingUpgradeResponse,
    Active,
}

struct WsBridgeStream {
    state: State,
    /// Accumulates raw bytes from downstream while we parse WS frames.
    downstream_buf: Vec<u8>,
    /// Token returned by open_grpc_stream; doubles as the stream id used by
    /// send_grpc_stream_message / close_grpc_stream.
    grpc_stream_id: Option<u32>,
    /// Headers extracted from the HTTP upgrade request, forwarded to grpcgi
    /// as gRPC initial metadata so it can reconstruct the ASGI WS scope.
    upgrade_headers: Vec<(String, String)>,
    /// Copied from root context so each stream has its own owned strings.
    grpc_cluster: String,
    grpc_service: String,
}

impl Context for WsBridgeStream {
    fn on_grpc_stream_initial_metadata(&mut self, token_id: u32, _num_elements: u32) {
        // Envoy fires this callback once the gRPC stream is established and
        // the server has sent its initial metadata. We use the token_id
        // (which is the same value returned by open_grpc_stream) as our
        // handle for all subsequent operations on this stream.
        debug!("grpc stream initial metadata: token_id={}", token_id);
        self.grpc_stream_id = Some(token_id);
    }

    fn on_grpc_stream_message(&mut self, _token_id: u32, message_size: usize) {
        // A complete gRPC message (5-byte header + proto body) is waiting in
        // the GrpcReceiveBuffer. Read it, decode the Frame proto, re-encode
        // as a WS frame, and inject into the downstream buffer.
        let data = match self.get_grpc_stream_message(0, message_size) {
            Some(d) => d,
            None => {
                warn!("on_grpc_stream_message: failed to read buffer");
                return;
            }
        };

        let (payload, binary) = match decode_frame_proto(&data) {
            Some(v) => v,
            None => {
                warn!("on_grpc_stream_message: failed to decode Frame proto");
                return;
            }
        };

        let opcode = if binary { 0x02u8 } else { 0x01u8 };
        let ws_frame = encode_ws_frame(opcode, &payload);

        // Replace the entire downstream buffer with this WS frame so Envoy
        // delivers it to the client.  Using offset 0, size 0 appends to
        // whatever Envoy currently has queued.
        self.set_downstream_data(0, 0, &ws_frame);
    }

    fn on_grpc_stream_close(&mut self, token_id: u32, status_code: u32) {
        info!(
            "grpc stream closed: token_id={} status={}",
            token_id, status_code
        );
        // RFC 6455 §7.4.1 – close code 1000 = normal closure.
        let close_frame = encode_ws_frame(0x08, &[0x03, 0xe8]);
        self.set_downstream_data(0, 0, &close_frame);
        self.grpc_stream_id = None;
    }
}

impl StreamContext for WsBridgeStream {
    fn on_downstream_data(&mut self, data_size: usize, _end_of_stream: bool) -> Action {
        match self.state {
            State::AwaitingUpgradeRequest => {
                // Accumulate bytes until we have the full HTTP request headers.
                let chunk = match self.get_downstream_data(0, data_size) {
                    Some(d) => d,
                    None => return Action::Continue,
                };
                self.downstream_buf.extend_from_slice(&chunk);

                // HTTP headers end with CRLF CRLF.
                if let Some(end) = find_subsequence(&self.downstream_buf, b"\r\n\r\n") {
                    let header_block = self.downstream_buf[..end + 4].to_vec();
                    self.upgrade_headers = parse_http_request_headers(&header_block);
                    debug!(
                        "ws upgrade request captured: {} headers",
                        self.upgrade_headers.len()
                    );
                    self.downstream_buf.clear();
                    self.state = State::AwaitingUpgradeResponse;
                }
                // Pass the upgrade request through so Envoy forwards it upstream.
                Action::Continue
            }

            State::AwaitingUpgradeResponse => {
                // Nothing useful from downstream while waiting for 101; let it
                // pass in case the client sends pipelined data (unlikely but
                // harmless to forward).
                Action::Continue
            }

            State::Active => {
                // Accumulate raw WS frame bytes and parse/forward them.
                let chunk = match self.get_downstream_data(0, data_size) {
                    Some(d) => d,
                    None => return Action::Pause,
                };
                self.downstream_buf.extend_from_slice(&chunk);
                self.process_ws_frames();
                // Never let raw WS bytes reach the dummy upstream.
                Action::Pause
            }
        }
    }

    fn on_upstream_data(&mut self, data_size: usize, _end_of_stream: bool) -> Action {
        match self.state {
            State::AwaitingUpgradeResponse => {
                // Look for the 101 Switching Protocols response.
                let data = match self.get_upstream_data(0, data_size) {
                    Some(d) => d,
                    None => return Action::Continue,
                };

                if contains_subsequence(&data, b"HTTP/1.1 101") {
                    debug!("received 101, opening grpc stream");
                    self.state = State::Active;

                    // Build initial metadata from the captured upgrade headers.
                    // We pass them as gRPC metadata so grpcgi can reconstruct
                    // the ASGI websocket scope.
                    let meta_refs: Vec<(&str, &[u8])> = self
                        .upgrade_headers
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_bytes()))
                        .collect();

                    match self.open_grpc_stream(
                        &self.grpc_cluster.clone(),
                        &self.grpc_service.clone(),
                        "Connect",
                        meta_refs,
                    ) {
                        Ok(token_id) => {
                            info!("grpc stream opened: token_id={}", token_id);
                            // grpc_stream_id is set in on_grpc_stream_initial_metadata;
                            // store eagerly too so we can send before metadata arrives.
                            self.grpc_stream_id = Some(token_id);
                        }
                        Err(e) => {
                            error!("open_grpc_stream failed: {:?}", e);
                        }
                    }
                    // Forward the 101 to the client.
                    Action::Continue
                } else {
                    Action::Continue
                }
            }

            State::Active => {
                // In Active state all server→client data comes via gRPC
                // callbacks (on_grpc_stream_message).  Any raw bytes from
                // the dummy upstream should not be forwarded.
                Action::Pause
            }

            State::AwaitingUpgradeRequest => Action::Continue,
        }
    }
}

impl WsBridgeStream {
    /// Drain `self.downstream_buf` by parsing complete WS frames.
    fn process_ws_frames(&mut self) {
        loop {
            let result = parse_ws_frame(&self.downstream_buf);
            match result {
                None => {
                    // Incomplete frame; wait for more data.
                    break;
                }
                Some((opcode, payload, consumed)) => {
                    // Remove the consumed bytes from the front of the buffer.
                    self.downstream_buf.drain(..consumed);

                    match opcode {
                        // Text (0x01) or binary (0x02) data frame.
                        0x01 | 0x02 => {
                            let binary = opcode == 0x02;
                            let grpc_msg = encode_frame_proto(&payload, binary);
                            if let Some(stream_id) = self.grpc_stream_id {
                                self.send_grpc_stream_message(
                                    stream_id,
                                    Some(&grpc_msg),
                                    false,
                                );
                            } else {
                                warn!(
                                    "data frame received but grpc stream not yet open, \
                                     dropping {} bytes",
                                    payload.len()
                                );
                            }
                        }

                        // Close (0x08): half-close the gRPC stream.
                        0x08 => {
                            debug!("ws close frame received, closing grpc stream");
                            if let Some(stream_id) = self.grpc_stream_id {
                                self.close_grpc_stream(stream_id);
                                self.grpc_stream_id = None;
                            }
                            break;
                        }

                        // Ping (0x09): send pong with same payload, do not forward.
                        0x09 => {
                            debug!("ws ping received, sending pong");
                            let pong = encode_ws_frame(0x0A, &payload);
                            self.set_downstream_data(0, 0, &pong);
                        }

                        // Pong (0x0A): discard.
                        0x0A => {
                            debug!("ws pong received, discarding");
                        }

                        other => {
                            warn!("unknown ws opcode 0x{:02x}, discarding frame", other);
                        }
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// WebSocket frame codec
// ---------------------------------------------------------------------------

/// Parse one WebSocket frame from `buf`.
///
/// Returns `Some((opcode, unmasked_payload, total_frame_bytes))` when a
/// complete frame is present, or `None` if more data is needed.
fn parse_ws_frame(buf: &[u8]) -> Option<(u8, Vec<u8>, usize)> {
    if buf.len() < 2 {
        return None;
    }

    let _fin = (buf[0] & 0x80) != 0;
    let opcode = buf[0] & 0x0F;
    let masked = (buf[1] & 0x80) != 0;
    let len_indicator = (buf[1] & 0x7F) as usize;

    // Determine extended payload length and offset of masking key / payload.
    let (payload_len, after_len_offset) = match len_indicator {
        126 => {
            if buf.len() < 4 {
                return None;
            }
            let len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
            (len, 4)
        }
        127 => {
            if buf.len() < 10 {
                return None;
            }
            let len = u64::from_be_bytes([
                buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9],
            ]) as usize;
            (len, 10)
        }
        n => (n, 2),
    };

    // Masking key is 4 bytes immediately after the length fields.
    let mask_offset = after_len_offset;
    let payload_offset = if masked {
        mask_offset + 4
    } else {
        mask_offset
    };

    let total = payload_offset + payload_len;
    if buf.len() < total {
        return None;
    }

    // Unmask payload.
    let raw_payload = &buf[payload_offset..payload_offset + payload_len];
    let payload: Vec<u8> = if masked {
        let key = [
            buf[mask_offset],
            buf[mask_offset + 1],
            buf[mask_offset + 2],
            buf[mask_offset + 3],
        ];
        raw_payload
            .iter()
            .enumerate()
            .map(|(i, &b)| b ^ key[i % 4])
            .collect()
    } else {
        raw_payload.to_vec()
    };

    Some((opcode, payload, total))
}

/// Encode a WebSocket frame for server→client direction (never masked).
fn encode_ws_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(10 + payload.len());

    // Byte 0: FIN=1, RSV=0, opcode.
    frame.push(0x80 | (opcode & 0x0F));

    // Byte(s) 1+: MASK=0 (server→client), payload length.
    let len = payload.len();
    if len < 126 {
        frame.push(len as u8);
    } else if len <= 0xFFFF {
        frame.push(126);
        frame.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        frame.push(127);
        frame.extend_from_slice(&(len as u64).to_be_bytes());
    }

    frame.extend_from_slice(payload);
    frame
}

// ---------------------------------------------------------------------------
// gRPC / proto codec
// ---------------------------------------------------------------------------

/// Encode a `Frame` proto message.
///
/// Raw proto bytes — proxy-wasm adds gRPC framing transparently.
///
/// Proto encoding:
///   field 1 (payload, length-delimited): tag=0x0a, varint(len), bytes
///   field 2 (binary, varint):            tag=0x10, 0x01  (omitted if false)
fn encode_frame_proto(payload: &[u8], binary: bool) -> Vec<u8> {
    let mut proto = Vec::with_capacity(2 + payload.len() + 2);

    if !payload.is_empty() {
        proto.push(0x0a);
        write_varint(&mut proto, payload.len() as u64);
        proto.extend_from_slice(payload);
    }

    if binary {
        proto.push(0x10);
        proto.push(0x01);
    }

    proto
}

/// Decode a `Frame` proto message.
///
/// proxy-wasm strips gRPC framing before delivery — data is pure proto bytes.
/// Returns `(payload, binary)` or `None` on parse error.
fn decode_frame_proto(data: &[u8]) -> Option<(Vec<u8>, bool)> {
    let proto = data;

    let mut payload = Vec::new();
    let mut binary = false;
    let mut i = 0;

    while i < proto.len() {
        // Read tag byte.
        let (tag_and_wire, n) = read_varint(proto, i)?;
        i += n;
        let wire_type = (tag_and_wire & 0x07) as u8;
        let field_number = (tag_and_wire >> 3) as u32;

        match (field_number, wire_type) {
            // field 1, length-delimited: payload bytes
            (1, 2) => {
                let (len, n) = read_varint(proto, i)?;
                i += n;
                let len = len as usize;
                if i + len > proto.len() {
                    return None;
                }
                payload = proto[i..i + len].to_vec();
                i += len;
            }
            // field 2, varint: binary flag
            (2, 0) => {
                let (v, n) = read_varint(proto, i)?;
                i += n;
                binary = v != 0;
            }
            // Unknown field: skip according to wire type.
            (_, wt) => {
                i = skip_field(proto, i, wt)?;
            }
        }
    }

    Some((payload, binary))
}

// ---------------------------------------------------------------------------
// Varint helpers
// ---------------------------------------------------------------------------

/// Write a protobuf unsigned varint into `buf`.
fn write_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            break;
        } else {
            buf.push(byte | 0x80);
        }
    }
}

/// Read a protobuf unsigned varint from `buf` starting at `offset`.
/// Returns `(value, bytes_consumed)` or `None` on truncation.
fn read_varint(buf: &[u8], offset: usize) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    let mut i = offset;
    loop {
        if i >= buf.len() {
            return None;
        }
        let byte = buf[i] as u64;
        i += 1;
        result |= (byte & 0x7F) << shift;
        if (byte & 0x80) == 0 {
            return Some((result, i - offset));
        }
        shift += 7;
        if shift >= 64 {
            return None; // overflow
        }
    }
}

/// Skip over an unknown proto field given its wire type.
fn skip_field(buf: &[u8], mut i: usize, wire_type: u8) -> Option<usize> {
    match wire_type {
        0 => {
            // varint
            let (_, n) = read_varint(buf, i)?;
            Some(i + n)
        }
        1 => {
            // 64-bit
            if i + 8 > buf.len() {
                None
            } else {
                Some(i + 8)
            }
        }
        2 => {
            // length-delimited
            let (len, n) = read_varint(buf, i)?;
            i += n;
            let len = len as usize;
            if i + len > buf.len() {
                None
            } else {
                Some(i + len)
            }
        }
        5 => {
            // 32-bit
            if i + 4 > buf.len() {
                None
            } else {
                Some(i + 4)
            }
        }
        _ => None, // unknown / unsupported wire type
    }
}

// ---------------------------------------------------------------------------
// HTTP upgrade request parsing
// ---------------------------------------------------------------------------

/// Extract all HTTP headers (and the request line) from a raw header block.
/// Returns a flat list of `(name, value)` pairs.
fn parse_http_request_headers(header_block: &[u8]) -> Vec<(String, String)> {
    let text = match std::str::from_utf8(header_block) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let mut headers = Vec::new();
    let mut lines = text.split("\r\n");

    // First line is the request line, e.g. "GET /path HTTP/1.1".
    if let Some(request_line) = lines.next() {
        let mut parts = request_line.splitn(3, ' ');
        let method = parts.next().unwrap_or("").to_string();
        let path = parts.next().unwrap_or("").to_string();
        // Store as pseudo-headers matching HTTP/2 / gRPC metadata conventions.
        headers.push((":method".to_string(), method));
        headers.push((":path".to_string(), path));
    }

    // Remaining lines are "Name: Value" header fields.
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some(colon) = line.find(':') {
            let name = line[..colon].trim().to_lowercase();
            let value = line[colon + 1..].trim().to_string();
            headers.push((name, value));
        }
    }

    headers
}

// ---------------------------------------------------------------------------
// Utility: JSON string extraction (no-alloc JSON parser for two fields)
// ---------------------------------------------------------------------------

/// Extract the string value of a JSON key from a simple flat JSON object.
/// Only handles `"key":"value"` patterns; good enough for plugin config.
fn extract_json_string(json: &str, key: &str) -> Option<String> {
    let needle = format!("\"{}\"", key);
    let key_pos = json.find(&needle)?;
    let after_key = &json[key_pos + needle.len()..];
    let colon_pos = after_key.find(':')?;
    let after_colon = after_key[colon_pos + 1..].trim_start();
    if !after_colon.starts_with('"') {
        return None;
    }
    let value_start = &after_colon[1..];
    let end = value_start.find('"')?;
    Some(value_start[..end].to_string())
}

// ---------------------------------------------------------------------------
// Utility: byte slice search
// ---------------------------------------------------------------------------

/// Find the first occurrence of `needle` in `haystack`, returns the start index.
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Return true if `haystack` contains `needle`.
fn contains_subsequence(haystack: &[u8], needle: &[u8]) -> bool {
    find_subsequence(haystack, needle).is_some()
}

// ---------------------------------------------------------------------------
// Tests (host-independent: pure encoding/decoding logic)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- varint ---

    #[test]
    fn varint_roundtrip_small() {
        let mut buf = Vec::new();
        write_varint(&mut buf, 42);
        assert_eq!(buf, &[42]);
        let (v, n) = read_varint(&buf, 0).unwrap();
        assert_eq!(v, 42);
        assert_eq!(n, 1);
    }

    #[test]
    fn varint_roundtrip_multibyte() {
        let mut buf = Vec::new();
        write_varint(&mut buf, 300);
        let (v, _) = read_varint(&buf, 0).unwrap();
        assert_eq!(v, 300);
    }

    #[test]
    fn varint_roundtrip_large() {
        let mut buf = Vec::new();
        write_varint(&mut buf, 1 << 35);
        let (v, _) = read_varint(&buf, 0).unwrap();
        assert_eq!(v, 1 << 35);
    }

    // --- proto frame encoding / decoding ---

    #[test]
    fn frame_proto_text_roundtrip() {
        let payload = b"hello world";
        let encoded = encode_frame_proto(payload, false);
        let (decoded_payload, decoded_binary) = decode_frame_proto(&encoded).unwrap();
        assert_eq!(decoded_payload, payload);
        assert!(!decoded_binary);
    }

    #[test]
    fn frame_proto_binary_roundtrip() {
        let payload = vec![0x00, 0xFF, 0xAB, 0xCD];
        let encoded = encode_frame_proto(&payload, true);
        let (decoded_payload, decoded_binary) = decode_frame_proto(&encoded).unwrap();
        assert_eq!(decoded_payload, payload);
        assert!(decoded_binary);
    }

    #[test]
    fn frame_proto_empty_payload() {
        let encoded = encode_frame_proto(&[], false);
        let (decoded_payload, decoded_binary) = decode_frame_proto(&encoded).unwrap();
        assert!(decoded_payload.is_empty());
        assert!(!decoded_binary);
    }

    #[test]
    fn frame_proto_grpc_header() {
        let encoded = encode_frame_proto(b"x", false);
        // Compression flag must be 0.
        assert_eq!(encoded[0], 0x00);
        // Length must match.
        let proto_len =
            u32::from_be_bytes([encoded[1], encoded[2], encoded[3], encoded[4]]) as usize;
        assert_eq!(encoded.len(), 5 + proto_len);
    }

    #[test]
    fn frame_proto_known_encoding() {
        // Manually verify the wire bytes for Frame{payload: b"hi", binary: false}.
        // Expected proto body: 0x0a 0x02 b'h' b'i'
        // gRPC header:        0x00 0x00 0x00 0x00 0x04
        let encoded = encode_frame_proto(b"hi", false);
        assert_eq!(encoded, &[0x00, 0x00, 0x00, 0x00, 0x04, 0x0a, 0x02, b'h', b'i']);
    }

    #[test]
    fn frame_proto_known_encoding_binary() {
        // Frame{payload: b"hi", binary: true}
        // proto body: 0x0a 0x02 b'h' b'i' 0x10 0x01
        let encoded = encode_frame_proto(b"hi", true);
        assert_eq!(
            encoded,
            &[0x00, 0x00, 0x00, 0x00, 0x06, 0x0a, 0x02, b'h', b'i', 0x10, 0x01]
        );
    }

    // --- WebSocket frame encoding / decoding ---

    #[test]
    fn ws_frame_small_text_unmasked() {
        let frame = encode_ws_frame(0x01, b"Hi");
        // FIN=1, opcode=1
        assert_eq!(frame[0], 0x81);
        // MASK=0, len=2
        assert_eq!(frame[1], 0x02);
        assert_eq!(&frame[2..], b"Hi");
    }

    #[test]
    fn ws_frame_126_boundary() {
        let payload = vec![0xAA; 126];
        let frame = encode_ws_frame(0x02, &payload);
        assert_eq!(frame[0], 0x82); // FIN + binary
        assert_eq!(frame[1], 126);
        let ext_len = u16::from_be_bytes([frame[2], frame[3]]) as usize;
        assert_eq!(ext_len, 126);
        assert_eq!(frame.len(), 4 + 126);
    }

    #[test]
    fn ws_frame_roundtrip_masked() {
        // Build a masked client→server frame manually.
        let payload = b"test";
        let mask = [0x37, 0xfa, 0x21, 0x3d];
        let mut frame = vec![0x81u8, 0x84]; // FIN+text, MASK+len=4
        frame.extend_from_slice(&mask);
        for (i, &b) in payload.iter().enumerate() {
            frame.push(b ^ mask[i % 4]);
        }

        let (opcode, decoded, consumed) = parse_ws_frame(&frame).unwrap();
        assert_eq!(opcode, 0x01);
        assert_eq!(decoded, payload);
        assert_eq!(consumed, frame.len());
    }

    #[test]
    fn ws_frame_parse_incomplete_returns_none() {
        let frame = encode_ws_frame(0x01, b"hello");
        // Provide only part of the frame.
        assert!(parse_ws_frame(&frame[..frame.len() - 1]).is_none());
    }

    #[test]
    fn ws_frame_parse_unmasked() {
        let frame = encode_ws_frame(0x02, b"binary");
        let (opcode, payload, consumed) = parse_ws_frame(&frame).unwrap();
        assert_eq!(opcode, 0x02);
        assert_eq!(payload, b"binary");
        assert_eq!(consumed, frame.len());
    }

    #[test]
    fn ws_close_frame() {
        let frame = encode_ws_frame(0x08, &[0x03, 0xe8]);
        assert_eq!(frame[0], 0x88); // FIN + close
        assert_eq!(&frame[2..], &[0x03, 0xe8]);
    }

    #[test]
    fn ws_frame_empty_payload() {
        let frame = encode_ws_frame(0x09, &[]);
        let (opcode, payload, consumed) = parse_ws_frame(&frame).unwrap();
        assert_eq!(opcode, 0x09);
        assert!(payload.is_empty());
        assert_eq!(consumed, 2);
    }

    // --- HTTP header parsing ---

    #[test]
    fn parse_request_headers_basic() {
        let raw = b"GET /chat HTTP/1.1\r\nHost: example.com\r\nUpgrade: websocket\r\n\r\n";
        let headers = parse_http_request_headers(raw);
        assert!(headers.contains(&(":method".to_string(), "GET".to_string())));
        assert!(headers.contains(&(":path".to_string(), "/chat".to_string())));
        assert!(headers.contains(&("host".to_string(), "example.com".to_string())));
        assert!(headers.contains(&("upgrade".to_string(), "websocket".to_string())));
    }

    #[test]
    fn parse_request_headers_multi_header() {
        let raw =
            b"GET /ws HTTP/1.1\r\nSec-WebSocket-Key: dGhlIHNhbXBsZQ==\r\nConnection: Upgrade\r\n\r\n";
        let headers = parse_http_request_headers(raw);
        assert!(headers
            .contains(&("sec-websocket-key".to_string(), "dGhlIHNhbXBsZQ==".to_string())));
        assert!(headers.contains(&("connection".to_string(), "Upgrade".to_string())));
    }

    // --- JSON extraction ---

    #[test]
    fn json_extract_present() {
        let json = r#"{"grpc_cluster":"my_cluster","grpc_service":"svc"}"#;
        assert_eq!(
            extract_json_string(json, "grpc_cluster"),
            Some("my_cluster".to_string())
        );
        assert_eq!(
            extract_json_string(json, "grpc_service"),
            Some("svc".to_string())
        );
    }

    #[test]
    fn json_extract_missing() {
        let json = r#"{"other":"value"}"#;
        assert!(extract_json_string(json, "grpc_cluster").is_none());
    }

    #[test]
    fn json_extract_with_spaces() {
        let json = r#"{ "grpc_cluster" : "c1" , "grpc_service" : "s1" }"#;
        assert_eq!(
            extract_json_string(json, "grpc_cluster"),
            Some("c1".to_string())
        );
    }

    // --- find_subsequence ---

    #[test]
    fn find_crlf_crlf() {
        let data = b"GET / HTTP/1.1\r\nHost: x\r\n\r\n";
        assert!(find_subsequence(data, b"\r\n\r\n").is_some());
    }

    #[test]
    fn find_subsequence_not_present() {
        assert!(find_subsequence(b"hello world", b"\r\n\r\n").is_none());
    }

    #[test]
    fn find_subsequence_at_start() {
        assert_eq!(find_subsequence(b"\r\n\r\ndata", b"\r\n\r\n"), Some(0));
    }
}
