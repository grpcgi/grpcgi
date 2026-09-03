#include "ws_grpc_bridge_filter.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "envoy/config/core/v3/grpc_service.pb.h"
#include "envoy/grpc/async_client_manager.h"
#include "envoy/http/codes.h"
#include "envoy/http/header_map.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/common/assert.h"
#include "source/common/grpc/common.h"
#include "source/common/http/header_map_impl.h"
#include "source/common/http/headers.h"

// Generated proto: grpcgi.v1.Frame
#include "grpcgi/v1/websocket.pb.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace GrpcWebSocketBridge {

// ============================================================================
// File-scope constants
// ============================================================================
namespace {

// RFC 6455 §5.2 opcodes.
constexpr uint8_t kOpcodeText = 0x01;
constexpr uint8_t kOpcodeBinary = 0x02;
constexpr uint8_t kOpcodeClose = 0x08;
constexpr uint8_t kOpcodePing = 0x09;
constexpr uint8_t kOpcodePong = 0x0A;

// RFC 6455 §7.4.1: Normal Closure.
constexpr uint16_t kCloseNormalClosure = 1000;

// Every WebSocket frame header is at least 2 bytes.
constexpr size_t kWsMinHeaderBytes = 2;

// The gRPC method descriptor resolved once at stream open.
// Protobuf's generated pool is consulted at runtime; the symbol must be
// present because the grpcgi proto is linked in via cc_proto_library.
const Protobuf::MethodDescriptor* grpcMethodDescriptor() {
  const auto* desc = Protobuf::DescriptorPool::generated_pool()->FindMethodByName(
      "grpcgi.v1.WebSocketBridge.Connect");
  RELEASE_ASSERT(desc != nullptr,
                 "grpcgi.v1.WebSocketBridge.Connect not found in generated proto pool; "
                 "ensure grpcgi/v1/websocket.proto is linked");
  return desc;
}

} // namespace

// ============================================================================
// Constructor / destructor
// ============================================================================

WsGrpcBridgeFilter::WsGrpcBridgeFilter(const WsGrpcBridgeConfig& config,
                                        Grpc::AsyncClientManager& client_manager,
                                        TimeSource& time_source)
    : config_(config), client_manager_(client_manager), time_source_(time_source) {}

WsGrpcBridgeFilter::~WsGrpcBridgeFilter() = default;

// ============================================================================
// Http::StreamFilterBase
// ============================================================================

void WsGrpcBridgeFilter::onDestroy() {
  if (grpc_stream_ != nullptr) {
    grpc_stream_->resetStream();
    grpc_stream_ = nullptr;
  }
  state_ = State::Closed;
}

// ============================================================================
// Http::StreamDecoderFilter
// ============================================================================

void WsGrpcBridgeFilter::setDecoderFilterCallbacks(
    Http::StreamDecoderFilterCallbacks& callbacks) {
  decoder_callbacks_ = &callbacks;
}

Http::FilterHeadersStatus
WsGrpcBridgeFilter::decodeHeaders(Http::RequestHeaderMap& headers, bool end_stream) {
  // Only intercept WebSocket upgrade requests.
  // The Connection header must contain "Upgrade" and the Upgrade header must
  // be exactly "websocket" (case-insensitive per RFC 7230 §6.7).
  const auto upgrade_values = headers.get(Http::Headers::get().Upgrade);
  if (upgrade_values.empty() ||
      !absl::EqualsIgnoreCase(upgrade_values[0]->value().getStringView(), "websocket")) {
    return Http::FilterHeadersStatus::Continue;
  }

  (void)end_stream; // Upgrade GET has no body; end_stream is irrelevant here.

  state_ = State::Connecting;
  startGrpcStream(headers);

  if (state_ == State::Closed) {
    // startGrpcStream sent a local reply; signal end of filter processing.
    return Http::FilterHeadersStatus::StopIteration;
  }

  // Hold the upgrade headers (and any subsequent data) until the gRPC stream
  // signals readiness via onReceiveInitialMetadata.
  return Http::FilterHeadersStatus::StopIteration;
}

Http::FilterDataStatus WsGrpcBridgeFilter::decodeData(Buffer::Instance& data, bool end_stream) {
  switch (state_) {
  case State::Passthrough:
    return Http::FilterDataStatus::Continue;

  case State::Connecting:
    // Buffer everything until the gRPC stream is ready.
    pending_upstream_data_.move(data);
    return Http::FilterDataStatus::StopIterationNoBuffer;

  case State::Closing:
  case State::Closed:
    // Drop data; we have already half-closed or fully closed upstream.
    return Http::FilterDataStatus::StopIterationNoBuffer;

  case State::Active:
    processWebSocketData(data, end_stream);
    return Http::FilterDataStatus::StopIterationNoBuffer;
  }
  PANIC("unreachable");
}

// ============================================================================
// Http::StreamEncoderFilter
// ============================================================================

void WsGrpcBridgeFilter::setEncoderFilterCallbacks(
    Http::StreamEncoderFilterCallbacks& callbacks) {
  encoder_callbacks_ = &callbacks;
}

Http::FilterHeadersStatus
WsGrpcBridgeFilter::encodeHeaders(Http::ResponseHeaderMap& /*headers*/, bool /*end_stream*/) {
  // Let the 101 Switching Protocols response pass unchanged.  WebSocket data
  // from grpcgi is injected via encoder_callbacks_->addEncodedData(), not here.
  return Http::FilterHeadersStatus::Continue;
}

Http::FilterDataStatus WsGrpcBridgeFilter::encodeData(Buffer::Instance& /*data*/,
                                                        bool /*end_stream*/) {
  // We don't intercept response data in-band; all downstream writes come from
  // GrpcStreamCallbacks::onReceiveMessage via encoder_callbacks_->addEncodedData.
  return Http::FilterDataStatus::Continue;
}

// ============================================================================
// GrpcStreamCallbacks
// ============================================================================

// onReceiveInitialMetadata: the gRPC stream is established, grpcgi is ready.
void WsGrpcBridgeFilter::GrpcStreamCallbacks::onReceiveInitialMetadata(
    Grpc::MetadataPtr&& /*metadata*/) {
  // Transition to Active only once (idempotent guard).
  if (parent_.state_ != State::Connecting) {
    return;
  }
  parent_.state_ = State::Active;
  ENVOY_LOG(debug, "grpc_websocket_bridge: gRPC stream ready, flushing buffered data");

  // Drain data that arrived while we were still Connecting.
  if (parent_.pending_upstream_data_.length() > 0) {
    parent_.processWebSocketData(parent_.pending_upstream_data_, false);
  }
}

// onReceiveMessage: decode a Frame and push it as a WebSocket data frame to
// the downstream client.
void WsGrpcBridgeFilter::GrpcStreamCallbacks::onReceiveMessage(
    std::unique_ptr<grpcgi::v1::Frame>&& message) {
  if (parent_.state_ != State::Active && parent_.state_ != State::Closing) {
    return;
  }

  const uint8_t opcode = message->binary() ? kOpcodeBinary : kOpcodeText;
  const std::string& payload = message->payload();

  Buffer::OwnedImpl ws_frame = WsGrpcBridgeFilter::encodeFrame(
      opcode, reinterpret_cast<const uint8_t*>(payload.data()), payload.size());

  parent_.encoder_callbacks_->addEncodedData(ws_frame, false);
}

// onRemoteClose: grpcgi closed the stream (graceful or error).  Send a
// WebSocket close frame and tear down the downstream connection.
void WsGrpcBridgeFilter::GrpcStreamCallbacks::onRemoteClose(Grpc::Status::GrpcStatus status,
                                                               const std::string& message) {
  ENVOY_LOG(debug,
            "grpc_websocket_bridge: upstream gRPC stream closed "
            "(status={}, message='{}')",
            static_cast<int>(status), message);

  if (parent_.state_ == State::Closed) {
    return;
  }
  parent_.state_ = State::Closed;
  parent_.grpc_stream_ = nullptr;

  // Send a close frame so the client can cleanly tear down its WebSocket.
  Buffer::OwnedImpl close_frame = WsGrpcBridgeFilter::buildCloseFrame();
  // end_stream=true: this is the last data we'll ever write downstream.
  parent_.encoder_callbacks_->addEncodedData(close_frame, true);

  // Reset the downstream HTTP/1.1 connection after the close frame drains.
  parent_.decoder_callbacks_->resetStream();
}

// ============================================================================
// Private helpers
// ============================================================================

// ----------------------------------------------------------------------------
// startGrpcStream: open the upstream gRPC bidi stream, forwarding the
// WebSocket upgrade headers as gRPC initial metadata.
// ----------------------------------------------------------------------------
void WsGrpcBridgeFilter::startGrpcStream(const Http::RequestHeaderMap& upgrade_headers) {
  ASSERT(grpc_callbacks_ == nullptr);
  grpc_callbacks_ = std::make_unique<GrpcStreamCallbacks>(*this);

  // Build an envoy GrpcService pointing at the configured cluster.
  envoy::config::core::v3::GrpcService grpc_service;
  grpc_service.mutable_envoy_grpc()->set_cluster_name(config_.cluster_name);
  if (!config_.authority.empty()) {
    grpc_service.mutable_envoy_grpc()->set_authority(config_.authority);
  }

  // Obtain a raw async gRPC client for the configured cluster.
  // getOrCreateRawAsyncClient returns a shared_ptr; we keep it alive alongside
  // the typed wrapper so the underlying transport isn't destroyed mid-stream.
  raw_grpc_client_ = client_manager_.getOrCreateRawAsyncClient(
      grpc_service, decoder_callbacks_->scope(),
      true /* skip_cluster_check — caller ensures the cluster exists */);
  // Wrap the raw client in the typed template helper used throughout Envoy.
  grpc_client_ = Grpc::AsyncClient<grpcgi::v1::Frame, grpcgi::v1::Frame>(raw_grpc_client_);

  // Assemble the initial metadata: copy every upgrade header so that grpcgi
  // can reconstruct the ASGI websocket scope (:path, :authority, :scheme, and
  // all Sec-WebSocket-* extension headers).
  Http::RequestHeaderMapPtr initial_metadata = Http::RequestHeaderMapImpl::create();
  upgrade_headers.iterate(
      [&initial_metadata](const Http::HeaderEntry& entry) -> Http::HeaderMap::Iterate {
        initial_metadata->addCopy(
            Http::LowerCaseString(std::string(entry.key().getStringView())),
            std::string(entry.value().getStringView()));
        return Http::HeaderMap::Iterate::Continue;
      });

  // The method descriptor is looked up once from the generated proto pool.
  static const Protobuf::MethodDescriptor* const kMethodDesc = grpcMethodDescriptor();

  // No per-stream deadline; rely on cluster-level connect/idle timeouts.
  Http::AsyncClient::StreamOptions stream_options;

  // Start the bidi stream.  The typed start() overload that accepts initial
  // metadata headers forwards them as custom gRPC request metadata so that
  // grpcgi can reconstruct the ASGI websocket scope from :path, :authority,
  // :scheme, and all Sec-WebSocket-* headers.
  grpc_stream_ =
      grpc_client_.start(*kMethodDesc, *grpc_callbacks_, stream_options,
                         std::move(initial_metadata));

  if (grpc_stream_ == nullptr) {
    ENVOY_LOG(warn,
              "grpc_websocket_bridge: failed to open gRPC stream to cluster '{}'; "
              "returning 503",
              config_.cluster_name);
    state_ = State::Closed;
    decoder_callbacks_->sendLocalReply(Http::Code::ServiceUnavailable,
                                       "WebSocket bridge unavailable", nullptr, absl::nullopt,
                                       "grpc_websocket_bridge_upstream_error");
  }
}

// ----------------------------------------------------------------------------
// processWebSocketData: parse WebSocket frames from a Buffer::Instance and
// dispatch them by opcode.  Incomplete frames are saved in frame_remainder_.
// ----------------------------------------------------------------------------
void WsGrpcBridgeFilter::processWebSocketData(Buffer::Instance& buf, bool end_stream) {
  ASSERT(state_ == State::Active);

  // Prepend any saved tail bytes from the previous call.
  if (!frame_remainder_.empty()) {
    Buffer::OwnedImpl prefix;
    prefix.add(frame_remainder_.data(), frame_remainder_.size());
    prefix.move(buf);
    buf.move(prefix);
    frame_remainder_.clear();
  }

  const uint64_t total = buf.length();
  if (total == 0) {
    if (end_stream) {
      closeGrpcStream();
    }
    return;
  }

  // linearize() returns a void*; we need at most UINT32_MAX bytes at a time.
  // For WebSocket, frames are bounded to 2^63−1 bytes by the spec, but in
  // practice we only ever see a few megabytes at most per decodeData() call.
  // Guard: if the buffer is absurdly large don't crash; truncate to uint32 max.
  const uint32_t linearize_len =
      (total > std::numeric_limits<uint32_t>::max())
          ? std::numeric_limits<uint32_t>::max()
          : static_cast<uint32_t>(total);

  const uint8_t* raw =
      reinterpret_cast<const uint8_t*>(buf.linearize(linearize_len));

  uint64_t offset = 0;
  while (offset < linearize_len) {
    const WsFrame frame = parseFrame(raw + offset, linearize_len - offset);
    if (!frame.complete) {
      // Stash the remainder for the next decodeData call.
      const size_t tail = static_cast<size_t>(linearize_len - offset);
      frame_remainder_.resize(tail);
      std::memcpy(frame_remainder_.data(), raw + offset, tail);
      break;
    }
    offset += frame.consumed;

    switch (frame.opcode) {
    case kOpcodeText:
    case kOpcodeBinary:
      forwardDataFrame(frame);
      break;

    case kOpcodePing: {
      // Echo back a Pong with the same application data (RFC 6455 §5.5.2).
      Buffer::OwnedImpl pong =
          encodeFrame(kOpcodePong, frame.payload.data(), frame.payload.size());
      encoder_callbacks_->addEncodedData(pong, false);
      break;
    }

    case kOpcodePong:
      // Unsolicited Pong — discard (RFC 6455 §5.5.3).
      break;

    case kOpcodeClose:
      // Client-initiated close: half-close the gRPC stream and stop sending.
      closeGrpcStream();
      // No more processing after a close.
      return;

    default:
      // Reserved or unknown opcode — silently ignore per RFC 6455 §5.2.
      break;
    }

    // If a close opcode was processed and we transitioned out of Active, stop.
    if (state_ != State::Active) {
      return;
    }
  }

  if (end_stream && state_ == State::Active) {
    closeGrpcStream();
  }
}

// ----------------------------------------------------------------------------
// forwardDataFrame: wrap the parsed WS payload in a proto Frame and send it
// over the gRPC stream.
// ----------------------------------------------------------------------------
void WsGrpcBridgeFilter::forwardDataFrame(const WsFrame& frame) {
  ASSERT(state_ == State::Active);
  ASSERT(grpc_stream_ != nullptr);

  grpcgi::v1::Frame proto_frame;
  proto_frame.set_payload(reinterpret_cast<const char*>(frame.payload.data()),
                          frame.payload.size());
  proto_frame.set_binary(frame.opcode == kOpcodeBinary);

  // end_stream=false: we keep the stream open until the WS connection closes.
  grpc_stream_->sendMessage(proto_frame, false);
}

// ----------------------------------------------------------------------------
// closeGrpcStream: half-close the request side of the gRPC stream.
// ----------------------------------------------------------------------------
void WsGrpcBridgeFilter::closeGrpcStream() {
  if (state_ != State::Active) {
    return;
  }
  state_ = State::Closing;
  if (grpc_stream_ != nullptr) {
    grpc_stream_->closeStream();
  }
}

// ============================================================================
// WebSocket frame parsing  (RFC 6455 §5.2)
// ============================================================================

WsGrpcBridgeFilter::WsFrame WsGrpcBridgeFilter::parseFrame(const uint8_t* data, size_t len) {
  WsFrame result;

  if (len < kWsMinHeaderBytes) {
    return result; // truncated — need more data
  }

  const uint8_t byte0 = data[0];
  const uint8_t byte1 = data[1];

  result.fin = (byte0 & 0x80u) != 0;
  result.opcode = byte0 & 0x0Fu;

  const bool masked = (byte1 & 0x80u) != 0;
  const uint8_t len7 = byte1 & 0x7Fu;

  size_t header_len = kWsMinHeaderBytes;
  uint64_t payload_len = 0;

  if (len7 < 126u) {
    payload_len = len7;
  } else if (len7 == 126u) {
    if (len < header_len + 2u) {
      return result; // need more data
    }
    payload_len = (static_cast<uint64_t>(data[2]) << 8u) | static_cast<uint64_t>(data[3]);
    header_len += 2u;
  } else { // len7 == 127
    if (len < header_len + 8u) {
      return result; // need more data
    }
    payload_len = 0;
    for (int i = 0; i < 8; ++i) {
      payload_len = (payload_len << 8u) | static_cast<uint64_t>(data[header_len + i]);
    }
    header_len += 8u;
  }

  // Masking key: 4 bytes, present iff MASK bit is set.
  // Per RFC 6455 §5.3, all client→server frames MUST be masked.
  std::array<uint8_t, 4> mask_key{0, 0, 0, 0};
  if (masked) {
    if (len < header_len + 4u) {
      return result; // need more data
    }
    mask_key[0] = data[header_len + 0];
    mask_key[1] = data[header_len + 1];
    mask_key[2] = data[header_len + 2];
    mask_key[3] = data[header_len + 3];
    header_len += 4u;
  }

  // Guard: payload_len must fit in size_t on 64-bit targets (always true) and
  // the combined frame must fit in the buffer we were given.
  const uint64_t frame_total = static_cast<uint64_t>(header_len) + payload_len;
  if (static_cast<uint64_t>(len) < frame_total) {
    return result; // truncated payload
  }

  // Unmask (or copy) the payload.
  result.payload.resize(static_cast<size_t>(payload_len));
  const uint8_t* src = data + header_len;
  for (size_t i = 0; i < static_cast<size_t>(payload_len); ++i) {
    result.payload[i] = src[i] ^ mask_key[i & 3u];
    // When not masked mask_key is all-zero, so XOR is a no-op.
  }

  result.consumed = static_cast<size_t>(frame_total);
  result.complete = true;
  return result;
}

// ============================================================================
// WebSocket frame encoding  (RFC 6455 §5.2, server→client, never masked)
// ============================================================================

Buffer::OwnedImpl WsGrpcBridgeFilter::encodeFrame(uint8_t opcode, const uint8_t* payload,
                                                    size_t payload_len) {
  Buffer::OwnedImpl buf;

  // Byte 0: FIN=1, RSV1-3=0, opcode[3:0].
  const uint8_t byte0 = static_cast<uint8_t>(0x80u | (opcode & 0x0Fu));
  buf.add(&byte0, 1);

  // Byte(s) 1+: MASK=0, 7-bit / 16-bit / 64-bit payload length.
  if (payload_len < 126u) {
    const uint8_t byte1 = static_cast<uint8_t>(payload_len);
    buf.add(&byte1, 1);
  } else if (payload_len < 65536u) {
    const uint8_t len_bytes[3] = {
        0x7Eu,
        static_cast<uint8_t>((payload_len >> 8u) & 0xFFu),
        static_cast<uint8_t>(payload_len & 0xFFu),
    };
    buf.add(len_bytes, sizeof(len_bytes));
  } else {
    // 64-bit extended payload length (len7 == 127).
    uint8_t len_bytes[9];
    len_bytes[0] = 0x7Fu;
    for (int i = 0; i < 8; ++i) {
      len_bytes[1 + i] =
          static_cast<uint8_t>((payload_len >> (56u - 8u * static_cast<unsigned>(i))) & 0xFFu);
    }
    buf.add(len_bytes, sizeof(len_bytes));
  }

  if (payload_len > 0) {
    buf.add(payload, payload_len);
  }
  return buf;
}

Buffer::OwnedImpl WsGrpcBridgeFilter::buildCloseFrame() {
  const uint8_t payload[2] = {
      static_cast<uint8_t>((kCloseNormalClosure >> 8u) & 0xFFu),
      static_cast<uint8_t>(kCloseNormalClosure & 0xFFu),
  };
  return encodeFrame(kOpcodeClose, payload, sizeof(payload));
}

} // namespace GrpcWebSocketBridge
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
