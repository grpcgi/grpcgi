#pragma once

// This header is intentionally minimal: the factory class is entirely defined
// in config.cc and is not referenced from outside this translation unit.
// It exists so that including "config.h" compiles cleanly in test harnesses
// that want to bring the factory into scope without linking config.cc directly.

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace GrpcWebSocketBridge {

// Forward declaration only.  See config.cc for the full factory definition.
class WsGrpcBridgeFilterFactory;

} // namespace GrpcWebSocketBridge
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
