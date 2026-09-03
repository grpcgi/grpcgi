#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

python3 -m venv venv
source venv/bin/activate

pip install --quiet grpcio grpcio-tools django

# Install the grpcgi Python prototype (editable so changes in ../python/ are live)
pip install --quiet -e ../python/

echo "Setup complete."
echo "Run ./run_server.sh in one terminal, then ./run_test.sh or ./run_ws_test.sh."
