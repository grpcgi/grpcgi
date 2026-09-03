#!/usr/bin/env bash
# Full end-to-end demo:
#   grpcgi server (Django) + Envoy (WASM filter) + curl
#
# Prerequisites: setup.sh already run, build_wasm.sh already run,
#                docker or podman available.
set -e
cd "$(dirname "$0")"
source venv/bin/activate

# ── 1. Start grpcgi server ─────────────────────────────────────────────────────
python3 server.py &
SERVER_PID=$!
trap "kill $SERVER_PID $ENVOY_PID 2>/dev/null; exit" INT TERM EXIT

echo "Waiting for grpcgi on :50051…"
until python3 -c "import socket; s=socket.socket(); s.connect(('127.0.0.1',50051)); s.close()" 2>/dev/null; do
    sleep 0.3
done
echo "grpcgi ready."

# ── 2. Start Envoy ─────────────────────────────────────────────────────────────
bash run_envoy.sh &
ENVOY_PID=$!

echo "Waiting for Envoy on :8080…"
until python3 -c "import socket; s=socket.socket(); s.connect(('127.0.0.1',8080)); s.close()" 2>/dev/null; do
    sleep 0.5
done
echo "Envoy ready."
echo ""

# ── 3. Test via Envoy (curl → Envoy → WASM filter → grpcgi.v1 → Django) ───────
echo "=== curl http://localhost:8080/ ==="
curl -s http://localhost:8080/
echo ""
echo "=== curl http://localhost:8080/hello/ ==="
curl -s http://localhost:8080/hello/
echo ""

# ── 4. WebSocket test bypasses Envoy (WS Envoy filter not yet wired) ───────────
echo "=== WebSocket via grpcgi.v1.WebSocketBridge (direct, no Envoy) ==="
python3 ws_test_client.py /ws/ "Hello grpcgi!" "WebSocket over gRPC"

echo ""
echo "All tests passed. Ctrl-C to stop."
wait
