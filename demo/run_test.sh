#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
source venv/bin/activate

echo "=== GET / ==="
python3 test_client.py GET /
echo "=== GET /hello/ ==="
python3 test_client.py GET /hello/
