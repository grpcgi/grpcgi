#!/usr/bin/env bash
# Start Envoy with the grpcgi HTTP-bridge WASM filter.
# Requires: docker or podman, and demo/grpcgi_http_bridge.wasm (run build_wasm.sh first).
set -e
cd "$(dirname "$0")"

WASM="$(pwd)/grpcgi_http_bridge.wasm"
YAML="$(pwd)/envoy.yaml"
ENVOY_IMAGE="docker.io/envoyproxy/envoy:v1.33.0"

if [ ! -f "$WASM" ]; then
    echo "ERROR: $WASM not found. Run ./build_wasm.sh first." >&2
    exit 1
fi

RUNTIME=""
if command -v docker   &>/dev/null; then RUNTIME=docker;
elif command -v podman &>/dev/null; then RUNTIME="sudo podman";
else
    echo "ERROR: docker or podman required." >&2
    exit 1
fi

echo "Starting Envoy $ENVOY_IMAGE on :8080 (admin :9901)…"
exec $RUNTIME run --rm \
    --network host \
    -v "$YAML:/etc/envoy/envoy.yaml:ro" \
    -v "$WASM:/etc/envoy/grpcgi_http_bridge.wasm:ro" \
    "$ENVOY_IMAGE" \
    envoy -c /etc/envoy/envoy.yaml
