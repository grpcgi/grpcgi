#include "config.h"

#include <memory>
#include <string>

#include "envoy/registry/registry.h"
#include "envoy/server/filter_config.h"

#include "source/common/common/assert.h"

#include "ws_grpc_bridge_filter.h"

// xDS config proto for this filter.
// In the Envoy source tree this is generated from:
//   proto/envoy/extensions/filters/http/grpc_websocket_bridge/v3/config.proto
#include "envoy/extensions/filters/http/grpc_websocket_bridge/v3/config.pb.h"
#include "envoy/extensions/filters/http/grpc_websocket_bridge/v3/config.pb.validate.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace GrpcWebSocketBridge {

// ---------------------------------------------------------------------------
// FilterConfig — per-listener config object that holds the runtime-stable
// WsGrpcBridgeConfig and manufactures per-stream WsGrpcBridgeFilter instances.
// ---------------------------------------------------------------------------
class FilterConfig {
public:
  FilterConfig(
      const envoy::extensions::filters::http::grpc_websocket_bridge::v3::Config& proto_config,
      Grpc::AsyncClientManager& client_manager, TimeSource& time_source)
      : client_manager_(client_manager), time_source_(time_source) {
    config_.cluster_name = proto_config.cluster_name();
    config_.authority = proto_config.authority();
    if (!proto_config.service_full_name().empty()) {
      config_.service_full_name = proto_config.service_full_name();
    }
    if (!proto_config.method_name().empty()) {
      config_.method_name = proto_config.method_name();
    }
  }

  Http::FilterFactoryCb createFilterFactory() {
    return [this](Http::FilterChainFactoryCallbacks& callbacks) {
      callbacks.addStreamFilter(
          std::make_shared<WsGrpcBridgeFilter>(config_, client_manager_, time_source_));
    };
  }

private:
  WsGrpcBridgeConfig config_;
  Grpc::AsyncClientManager& client_manager_;
  TimeSource& time_source_;
};

// ---------------------------------------------------------------------------
// WsGrpcBridgeFilterFactory — registered with REGISTER_FACTORY so that Envoy
// can instantiate this filter from xDS configuration by name.
// ---------------------------------------------------------------------------
class WsGrpcBridgeFilterFactory
    : public Server::Configuration::NamedHttpFilterConfigFactory {
public:
  std::string name() const override {
    return "envoy.filters.http.grpc_websocket_bridge";
  }

  absl::StatusOr<Http::FilterFactoryCb> createFilterFactoryFromProto(
      const Protobuf::Message& proto_config, const std::string& /*stats_prefix*/,
      Server::Configuration::FactoryContext& context) override {
    const auto& typed_config = MessageUtil::downcastAndValidate<
        const envoy::extensions::filters::http::grpc_websocket_bridge::v3::Config&>(
        proto_config, context.messageValidationVisitor());

    auto filter_config = std::make_shared<FilterConfig>(
        typed_config,
        context.clusterManager().grpcAsyncClientManager(),
        context.serverFactoryContext().mainThreadDispatcher().timeSource());

    return filter_config->createFilterFactory();
  }

  ProtobufTypes::MessagePtr createEmptyConfigProto() override {
    return std::make_unique<
        envoy::extensions::filters::http::grpc_websocket_bridge::v3::Config>();
  }
};

// Register the factory under the canonical filter name.
REGISTER_FACTORY(WsGrpcBridgeFilterFactory,
                 Server::Configuration::NamedHttpFilterConfigFactory);

} // namespace GrpcWebSocketBridge
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
