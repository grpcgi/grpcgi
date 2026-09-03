/// grpcgi HTTP bridge — proxy-wasm HTTP-level filter.
///
/// Uses dispatch_grpc_call (unary) to call grpcgi.v1.HttpBridge.Handle.
/// This is the correct proxy-wasm pattern for HTTP filters that need
/// async gRPC calls: open_grpc_stream callbacks do not fire reliably from
/// HTTP contexts in proxy-wasm 0.2.
///
/// Flow:
///   on_http_request_headers  → encode headers, dispatch_grpc_call, Pause
///   on_http_request_body     → accumulate body (for POST/PUT)
///   on_grpc_call_response    → decode HttpResponse, send_http_response
///
/// Proto field numbers (must match proto/grpcgi/v1/http.proto):
///   HttpRequest  { headers=1 (repeated Header), body=2 (bytes) }
///   HttpResponse { status=1 (uint32), headers=2 (repeated Header), body=3 (bytes) }
///   Header       { key=1 (string), value=2 (string) }
use proxy_wasm::traits::*;
use proxy_wasm::types::*;
use std::time::Duration;

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Info);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> {
        Box::new(HttpBridgeRoot {
            cluster: "grpcgi".to_string(),
            authority: String::new(),
        })
    });
}}

// ── Root context ──────────────────────────────────────────────────────────────

struct HttpBridgeRoot {
    cluster: String,
    authority: String,
}

impl Context for HttpBridgeRoot {}

impl RootContext for HttpBridgeRoot {
    fn on_configure(&mut self, _: usize) -> bool {
        if let Some(bytes) = self.get_plugin_configuration() {
            let s = String::from_utf8_lossy(&bytes);
            if let Some(c) = json_str_field(&s, "cluster") { self.cluster = c; }
            if let Some(a) = json_str_field(&s, "authority") { self.authority = a; }
        }
        true
    }

    fn get_type(&self) -> Option<ContextType> { Some(ContextType::HttpContext) }

    fn create_http_context(&self, context_id: u32) -> Option<Box<dyn HttpContext>> {
        Some(Box::new(HttpBridgeFilter {
            context_id,
            cluster: self.cluster.clone(),
            authority: self.authority.clone(),
            call_token: None,
            req_headers: Vec::new(),
            req_body: Vec::new(),
            passthrough: false,
            body_pending: false,
        }))
    }
}

// ── Per-request filter ────────────────────────────────────────────────────────

struct HttpBridgeFilter {
    context_id: u32,
    cluster: String,
    authority: String,
    call_token: Option<u32>,
    req_headers: Vec<(String, String)>,
    req_body: Vec<u8>,
    passthrough: bool,
    body_pending: bool,  // true when we need to wait for body before dispatching
}

impl Context for HttpBridgeFilter {
    fn on_grpc_call_response(&mut self, token_id: u32, status_code: u32, response_size: usize) {
        if Some(token_id) != self.call_token {
            return;
        }

        if status_code != 0 {
            log::warn!("[{}] gRPC call failed status={}", self.context_id, status_code);
            self.send_http_response(502, vec![], Some(b"grpcgi: gRPC error"));
            return;
        }

        let raw = match self.get_grpc_call_response_body(0, response_size) {
            Some(b) => b,
            None => {
                self.send_http_response(502, vec![], Some(b"grpcgi: empty response"));
                return;
            }
        };

        match decode_http_response(&raw) {
            Some((status, headers, body)) => {
                let h: Vec<(&str, &str)> = headers.iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                self.send_http_response(status, h, Some(&body));
            }
            None => {
                log::warn!("[{}] failed to decode HttpResponse", self.context_id);
                self.send_http_response(502, vec![], Some(b"grpcgi: decode error"));
            }
        }
    }
}

impl HttpBridgeFilter {
    fn dispatch_handle(&mut self) {
        let msg = encode_http_request(&self.req_headers, &self.req_body);

        let initial_meta: Vec<(&str, &[u8])> = if self.authority.is_empty() {
            vec![]
        } else {
            vec![(":authority", self.authority.as_bytes())]
        };

        match self.dispatch_grpc_call(
            &self.cluster,
            "grpcgi.v1.HttpBridge",
            "Handle",
            initial_meta,
            Some(&msg),
            Duration::from_secs(30),
        ) {
            Ok(token) => { self.call_token = Some(token); }
            Err(e) => {
                log::error!("[{}] dispatch_grpc_call failed: {:?}", self.context_id, e);
                self.passthrough = true;
            }
        }
    }
}

impl HttpContext for HttpBridgeFilter {
    fn on_http_request_headers(&mut self, _: usize, end_of_stream: bool) -> Action {
        let headers = self.get_http_request_headers();

        // Let WebSocket upgrades pass through to the WS network filter.
        for (k, v) in &headers {
            if k.eq_ignore_ascii_case("upgrade") && v.eq_ignore_ascii_case("websocket") {
                self.passthrough = true;
                return Action::Continue;
            }
        }

        self.req_headers = headers;

        if end_of_stream {
            // GET / DELETE / HEAD — no body, dispatch immediately.
            self.dispatch_handle();
            if self.passthrough { Action::Continue } else { Action::Pause }
        } else {
            // Body expected — wait for it before dispatching.
            self.body_pending = true;
            Action::Continue
        }
    }

    fn on_http_request_body(&mut self, body_size: usize, end_of_stream: bool) -> Action {
        if self.passthrough { return Action::Continue; }
        if !self.body_pending { return Action::Continue; }

        if let Some(chunk) = self.get_http_request_body(0, body_size) {
            self.req_body.extend_from_slice(&chunk);
        }

        if end_of_stream {
            self.body_pending = false;
            self.dispatch_handle();
            if self.passthrough { Action::Continue } else { Action::Pause }
        } else {
            Action::Continue
        }
    }

    fn on_http_response_headers(&mut self, _: usize, _: bool) -> Action {
        Action::Continue
    }
}

// ── Proto encoding: HttpRequest ───────────────────────────────────────────────
//
// HttpRequest { headers=1 (repeated Header), body=2 (bytes) }
// Header      { key=1, value=2 }

fn encode_http_request(headers: &[(String, String)], body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();

    for (k, v) in headers {
        // Encode Header { key, value }
        let mut h = Vec::new();
        push_len_field(1, k.as_bytes(), &mut h);
        push_len_field(2, v.as_bytes(), &mut h);
        // HttpRequest field 1: repeated Header
        push_len_field(1, &h, &mut out);
    }

    if !body.is_empty() {
        // HttpRequest field 2: bytes body
        push_len_field(2, body, &mut out);
    }

    out
}

// ── Proto decoding: HttpResponse ─────────────────────────────────────────────
//
// HttpResponse { status=1 (uint32), headers=2 (repeated Header), body=3 (bytes) }

fn decode_http_response(raw: &[u8]) -> Option<(u32, Vec<(String, String)>, Vec<u8>)> {
    let mut status = 200u32;
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut body = Vec::new();
    let mut pos = 0;

    while pos < raw.len() {
        let (tag, n) = read_varint(&raw[pos..])?;
        pos += n;
        let field = (tag >> 3) as u32;
        let wire  = (tag & 0x7) as u32;

        match (field, wire) {
            (1, 0) => {
                // status: uint32
                let (v, n) = read_varint(&raw[pos..])?;
                pos += n;
                status = v as u32;
            }
            (2, 2) => {
                // headers: repeated Header
                let (len, n) = read_varint(&raw[pos..])?;
                pos += n;
                if let Some((k, v)) = decode_header(&raw[pos..pos + len as usize]) {
                    headers.push((k, v));
                }
                pos += len as usize;
            }
            (3, 2) => {
                // body: bytes
                let (len, n) = read_varint(&raw[pos..])?;
                pos += n;
                body.extend_from_slice(&raw[pos..pos + len as usize]);
                pos += len as usize;
            }
            (_, 0) => { let (_, n) = read_varint(&raw[pos..])?; pos += n; }
            (_, 2) => { let (l, n) = read_varint(&raw[pos..])?; pos += n; pos += l as usize; }
            (_, 5) => { pos += 4; }
            (_, 1) => { pos += 8; }
            _ => break,
        }
    }

    Some((status, headers, body))
}

fn decode_header(msg: &[u8]) -> Option<(String, String)> {
    let mut key = String::new();
    let mut val = String::new();
    let mut pos = 0;

    while pos < msg.len() {
        let (tag, n) = read_varint(&msg[pos..])?;
        pos += n;
        let field = (tag >> 3) as u32;
        let wire  = (tag & 0x7) as u32;

        match (field, wire) {
            (1, 2) => {
                let (len, n) = read_varint(&msg[pos..])?;
                pos += n;
                key = String::from_utf8_lossy(&msg[pos..pos + len as usize]).into_owned();
                pos += len as usize;
            }
            (2, 2) => {
                let (len, n) = read_varint(&msg[pos..])?;
                pos += n;
                val = String::from_utf8_lossy(&msg[pos..pos + len as usize]).into_owned();
                pos += len as usize;
            }
            (_, 0) => { let (_, n) = read_varint(&msg[pos..])?; pos += n; }
            (_, 2) => { let (l, n) = read_varint(&msg[pos..])?; pos += n; pos += l as usize; }
            _ => break,
        }
    }
    if key.is_empty() { None } else { Some((key, val)) }
}

// ── Proto helpers ─────────────────────────────────────────────────────────────

fn push_len_field(field_num: u64, data: &[u8], buf: &mut Vec<u8>) {
    let tag = (field_num << 3) | 2;
    write_varint(tag, buf);
    write_varint(data.len() as u64, buf);
    buf.extend_from_slice(data);
}

fn write_varint(mut v: u64, buf: &mut Vec<u8>) {
    loop {
        let b = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 { buf.push(b); break; }
        buf.push(b | 0x80);
    }
}

fn read_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut val = 0u64;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate() {
        val |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 { return Some((val, i + 1)); }
        shift += 7;
        if shift >= 64 { return None; }
    }
    None
}

// ── Minimal JSON field extractor ─────────────────────────────────────────────

fn json_str_field(json: &str, key: &str) -> Option<String> {
    let needle = format!("\"{}\"", key);
    let pos = json.find(&needle)?;
    let rest = &json[pos + needle.len()..];
    let colon = rest.find(':')? + 1;
    let rest = rest[colon..].trim_start();
    if !rest.starts_with('"') { return None; }
    let inner = &rest[1..];
    let end = inner.find('"')?;
    Some(inner[..end].to_string())
}
