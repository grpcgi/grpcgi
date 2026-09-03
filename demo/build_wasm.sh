#!/usr/bin/env bash
# Compile the grpcgi HTTP-bridge WASM filter.
# Output: wasm/http-bridge/target/wasm32-wasip1/release/http_bridge.wasm
set -e
cd "$(dirname "$0")/.."

WASM_OUT="demo/grpcgi_http_bridge.wasm"

if ! rustup target list --installed | grep -q wasm32-wasip1; then
    echo "Installing wasm32-wasip1 target…"
    rustup target add wasm32-wasip1
fi

echo "Building HTTP bridge WASM filter…"
(cd wasm/http-bridge && cargo build --target wasm32-wasip1 --release 2>&1)

cp wasm/http-bridge/target/wasm32-wasip1/release/http_bridge.wasm "$WASM_OUT"
echo "WASM filter written to $WASM_OUT ($(wc -c < "$WASM_OUT") bytes)"
