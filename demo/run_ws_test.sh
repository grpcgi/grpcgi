#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
source venv/bin/activate
python3 ws_test_client.py /ws/ "Hello grpcgi!" "WebSocket over gRPC"
