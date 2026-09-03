cmake_minimum_required(VERSION 3.20)
project(envoy_grpc_websocket_bridge CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---------------------------------------------------------------------------
# Where to find Envoy headers and libraries.
#
# When building inside the Envoy source tree via Bazel this CMakeLists.txt is
# not used at all; Bazel drives everything.  This file exists for:
#   1. IDE integration / clangd (via compile_commands.json).
#   2. Out-of-tree builds against a pre-installed Envoy SDK (e.g. from an
#      envoy-dev package or a manually built Envoy tree).
#
# Set ENVOY_INCLUDE_DIR and ENVOY_LIB_DIR on the cmake command line when the
# Envoy headers and libraries are not on the default search paths:
#
#   cmake -B build \
#     -DENVOY_INCLUDE_DIR=/path/to/envoy/source \
#     -DENVOY_LIB_DIR=/path/to/envoy/build/lib
# ---------------------------------------------------------------------------

if(DEFINED ENVOY_INCLUDE_DIR)
  include_directories(SYSTEM "${ENVOY_INCLUDE_DIR}")
endif()

if(DEFINED ENVOY_LIB_DIR)
  link_directories("${ENVOY_LIB_DIR}")
endif()

# ---------------------------------------------------------------------------
# Protobuf — required for both the grpcgi.v1 messages and the filter config.
# ---------------------------------------------------------------------------
find_package(Protobuf REQUIRED)
include_directories(SYSTEM ${Protobuf_INCLUDE_DIRS})

# ---------------------------------------------------------------------------
# grpcgi proto — generates grpcgi/v1/websocket.pb.{h,cc}
# ---------------------------------------------------------------------------
set(GRPCGI_PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../proto")

protobuf_generate_cpp(
  GRPCGI_PROTO_SRCS
  GRPCGI_PROTO_HDRS
  "${GRPCGI_PROTO_DIR}/grpcgi/v1/websocket.proto"
)

# ---------------------------------------------------------------------------
# Filter config proto — generates the typed xDS config message.
# ---------------------------------------------------------------------------
set(FILTER_PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto")

protobuf_generate_cpp(
  FILTER_CFG_PROTO_SRCS
  FILTER_CFG_PROTO_HDRS
  "${FILTER_PROTO_DIR}/envoy/extensions/filters/http/grpc_websocket_bridge/v3/config.proto"
)

# Make generated headers visible to filter sources.
include_directories("${CMAKE_CURRENT_BINARY_DIR}")
# Also expose the proto source tree so #include paths match Envoy conventions.
include_directories("${GRPCGI_PROTO_DIR}")
include_directories("${FILTER_PROTO_DIR}")

# ---------------------------------------------------------------------------
# The filter shared library.
#
# In a Bazel / Envoy mainline build this would be a cc_library() linked
# directly into the Envoy binary.  For out-of-tree builds we produce a
# shared library that can be loaded via Envoy's dynamic filter loading
# support.
# ---------------------------------------------------------------------------
add_library(envoy_grpc_websocket_bridge SHARED
  ws_grpc_bridge_filter.cc
  config.cc
  ${GRPCGI_PROTO_SRCS}
  ${FILTER_CFG_PROTO_SRCS}
)

target_include_directories(envoy_grpc_websocket_bridge
  PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
)

target_link_libraries(envoy_grpc_websocket_bridge
  PRIVATE
    ${Protobuf_LIBRARIES}
    # When linking against a pre-built Envoy SDK, add the relevant libs here.
    # e.g.:  envoy_common  envoy_http_filter  envoy_grpc
    # For now we leave these as comments; the exact lib names depend on how
    # Envoy was built and packaged.
)

# ---------------------------------------------------------------------------
# Compiler warnings — match Envoy's own warning set.
# ---------------------------------------------------------------------------
target_compile_options(envoy_grpc_websocket_bridge
  PRIVATE
    -Wall
    -Wextra
    -Wno-unused-parameter
    -Wshadow
    -Wformat=2
    $<$<CONFIG:Debug>:-g3 -O0>
    $<$<CONFIG:Release>:-O3 -DNDEBUG>
)

# ---------------------------------------------------------------------------
# Bazel BUILD file reference
#
# When contributing to Envoy mainline, the canonical build description is the
# Bazel BUILD file at:
#   source/extensions/filters/http/grpc_websocket_bridge/BUILD
#
# A minimal skeleton is provided in BUILD.bazel next to this file.
# ---------------------------------------------------------------------------
