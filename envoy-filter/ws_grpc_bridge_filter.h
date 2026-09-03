#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "envoy/grpc/async_client.h"
#include "envoy/grpc/async_client_manager.h"
#include "envoy/http/filter.h"
#include "envoy/http/header_map.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/common/assert.h"
#include "source/common/common/minimal_logger.h"
#include "source/common/grpc/typed_async_client.h"

// Generated proto type for the grpcgi.v1.Frame message.
// In the Envoy build tree this comes from the cc_proto_library target for
// grpcgi/v1/websocket.proto.
#include "grpcgi/v1/websocket.pb.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace GrpcWebSocketBridge {

// ---------------------------------------------------------------------------
// WsGrpcBridgeConfig — populated from the xDS typed extension config.
// Held by value inside the per-filter-chain FilterConfig object and
// referenced from individual filter instances.
// ---------------------------------------------------------------------------
struct WsGrpcBridgeConfig {
  // Envoy cluster name that routes to the grpcgi upstream.
  std::string cluster_name;
  // The :authority header sent on the outbound gRPC connection. When empty
  // the cluster name is used.
  std::string authority;
  // Fully-qualified gRPC service name.
  std::string service_full_name{"grpcgi.v1.WebSocketBridge"};
  // gRPC method name within the service.
  std::string method_name{"Connect"};
};

// ---------------------------------------------------------------------------
// WsGrpcBridgeFilter — one instance per HTTP stream.
//
// Implements Http::StreamFilter (both decoder and encoder sides) so it can:
//   - intercept the upstream WebSocket upgrade request (decoder),
//   - inject gRPC-sourced WebSocket frames into the downstream response path
//     (encoder, via addEncodedData).
// ---------------------------------------------------------------------------
class WsGrpcBridgeFilter : public Http::StreamFilter,
                            public Logger::Loggable<Logger::Id::filter> {
public:
  explicit WsGrpcBridgeFilter(const WsGrpcBridgeConfig& config,
                               Grpc::AsyncClientManager& client_manager,
                               TimeSource& time_source);
  ~WsGrpcBridgeFilter() override;

  // ---- Http::StreamDecoderFilter ------------------------------------------
  void setDecoderFilterCallbacks(Http::StreamDecoderFilterCallbacks& callbacks) override;
  Http::FilterHeadersStatus decodeHeaders(Http::RequestHeaderMap& headers,
                                          bool end_stream) override;
  Http::FilterDataStatus decodeData(Buffer::Instance& data, bool end_stream) override;
  Http::FilterTrailersStatus decodeTrailers(Http::RequestTrailerMap&) override {
    return Http::FilterTrailersStatus::Continue;
  }

  // ---- Http::StreamEncoderFilter ------------------------------------------
  void setEncoderFilterCallbacks(Http::StreamEncoderFilterCallbacks& callbacks) override;
  Http::FilterHeadersStatus encode1xxHeaders(Http::ResponseHeaderMap&) override {
    return Http::FilterHeadersStatus::Continue;
  }
  Http::FilterHeadersStatus encodeHeaders(Http::ResponseHeaderMap& headers,
                                          bool end_stream) override;
  Http::FilterDataStatus encodeData(Buffer::Instance& data, bool end_stream) override;
  Http::FilterTrailersStatus encodeTrailers(Http::ResponseTrailerMap&) override {
    return Http::FilterTrailersStatus::Continue;
  }
  Http::FilterMetadataStatus encodeMetadata(Http::MetadataMap&) override {
    return Http::FilterMetadataStatus::Continue;
  }

  // ---- Http::StreamFilterBase ---------------------------------------------
  void onDestroy() override;

private:
  // -------------------------------------------------------------------------
  // GrpcStreamCallbacks
  //
  // Bridges the bidi gRPC stream events back into the parent filter.
  // The lifetime of this object is tied to the filter instance.
  // -------------------------------------------------------------------------
  class GrpcStreamCallbacks
      : public Grpc::AsyncBidirectionalStreamCallbacks<grpcgi::v1::Frame, grpcgi::v1::Frame> {
  public:
    explicit GrpcStreamCallbacks(WsGrpcBridgeFilter& parent) : parent_(parent) {}

    // Initial metadata signals that the stream is open and grpcgi accepted
    // the WebSocket scope.
    void onReceiveInitialMetadata(Grpc::MetadataPtr&&) override;

    // A complete Frame message decoded from the gRPC response stream.
    void onReceiveMessage(std::unique_ptr<grpcgi::v1::Frame>&& message) override;

    // Trailing metadata: grpcgi has half-closed its send side.
    void onReceiveTrailingMetadata(Grpc::MetadataPtr&&) override {}

    // The stream was reset or the server closed it.
    void onRemoteClose(Grpc::Status::GrpcStatus status, const std::string& message) override;

  private:
    WsGrpcBridgeFilter& parent_;
  };

  // -------------------------------------------------------------------------
  // WebSocket wire format helpers
  // -------------------------------------------------------------------------

  // The result of attempting to parse one WebSocket frame from a raw byte
  // buffer.  complete==false means the buffer was truncated mid-frame.
  struct WsFrame {
    bool fin{false};
    uint8_t opcode{0};
    std::vector<uint8_t> payload;
    size_t consumed{0}; // bytes consumed from the input buffer
    bool complete{false};
  };

  // Parse one frame from [data, data+len).  Client→server frames are always
  // masked per RFC 6455 §5.3.
  static WsFrame parseFrame(const uint8_t* data, size_t len);

  // Encode one server→client WebSocket frame (FIN=1, unmasked).
  static Buffer::OwnedImpl encodeFrame(uint8_t opcode, const uint8_t* payload,
                                       size_t payload_len);

  // Build a WebSocket close frame with Normal Closure (1000) status.
  static Buffer::OwnedImpl buildCloseFrame();

  // -------------------------------------------------------------------------
  // gRPC stream management
  // -------------------------------------------------------------------------

  // Open the gRPC bidi stream, attaching upgrade headers as initial metadata.
  void startGrpcStream(const Http::RequestHeaderMap& upgrade_headers);

  // Encode a parsed WS data frame into a proto Frame and send it upstream.
  void forwardDataFrame(const WsFrame& frame);

  // Half-close the gRPC stream (no more client→server messages).
  void closeGrpcStream();

  // Parse and dispatch WebSocket frames from buf.  Handles frame_remainder_.
  void processWebSocketData(Buffer::Instance& buf, bool end_stream);

  // -------------------------------------------------------------------------
  // Filter state machine
  // -------------------------------------------------------------------------
  enum class State {
    Passthrough, // Not a WebSocket; filter is a transparent no-op.
    Connecting,  // Upgrade seen; waiting for gRPC stream initial metadata.
    Active,      // gRPC stream open; frames flowing.
    Closing,     // Client sent close; gRPC stream half-closed.
    Closed,      // Everything torn down.
  };

  // -------------------------------------------------------------------------
  // Member variables
  // -------------------------------------------------------------------------
  const WsGrpcBridgeConfig& config_;
  Grpc::AsyncClientManager& client_manager_;
  TimeSource& time_source_;

  Http::StreamDecoderFilterCallbacks* decoder_callbacks_{nullptr};
  Http::StreamEncoderFilterCallbacks* encoder_callbacks_{nullptr};

  State state_{State::Passthrough};

  // Callbacks object — must outlive grpc_stream_.
  std::unique_ptr<GrpcStreamCallbacks> grpc_callbacks_;

  // Non-owning pointer to the bidi gRPC stream.  Owned by the AsyncClient.
  Grpc::AsyncBidirectionalStream<grpcgi::v1::Frame, grpcgi::v1::Frame>* grpc_stream_{nullptr};

  // Async gRPC client — must outlive grpc_stream_.
  // Holds a shared_ptr to the underlying raw client; initialized lazily in
  // startGrpcStream() and kept alive for the duration of the stream.
  Grpc::AsyncClient<grpcgi::v1::Frame, grpcgi::v1::Frame> grpc_client_;
  // Keeps the raw client shared_ptr alive so that grpc_client_ remains valid.
  Grpc::RawAsyncClientSharedPtr raw_grpc_client_;

  // Upstream data that arrived before the gRPC stream was ready.
  Buffer::OwnedImpl pending_upstream_data_;

  // Tail bytes of an incomplete WebSocket frame that span a decodeData() call.
  std::vector<uint8_t> frame_remainder_;
};

} // namespace GrpcWebSocketBridge
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
