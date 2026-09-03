#!/usr/bin/env bash
# Generate Python gRPC stubs from proto files into grpcgi/_proto/.
# Run from the python/ directory:
#   ./generate_proto.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_ROOT="${SCRIPT_DIR}/../proto"
OUT_DIR="${SCRIPT_DIR}/grpcgi/_proto"

mkdir -p "${OUT_DIR}"

python3 -m grpc_tools.protoc \
    --proto_path="${PROTO_ROOT}" \
    --python_out="${OUT_DIR}" \
    --grpc_python_out="${OUT_DIR}" \
    grpcgi/v1/http.proto \
    grpcgi/v1/websocket.proto

# grpc_tools writes relative imports; patch them to be package-relative so the
# generated files work when installed as part of the grpcgi._proto subpackage.
for f in "${OUT_DIR}"/grpcgi/v1/*_pb2_grpc.py; do
    sed -i 's/^from grpcgi\.v1 import \(.*\)_pb2/from grpcgi._proto.grpcgi.v1 import \1_pb2/' "${f}"
done
# Imports between proto files also use the proto-root package. Rewrite those
# in message modules as well (websocket.proto imports http.proto).
for f in "${OUT_DIR}"/grpcgi/v1/*_pb2.py; do
    sed -i 's/^from grpcgi\.v1 import \(.*\)_pb2/from grpcgi._proto.grpcgi.v1 import \1_pb2/' "${f}"
done

# Create __init__.py files so Python treats the generated directories as packages.
touch "${OUT_DIR}/__init__.py"
touch "${OUT_DIR}/grpcgi/__init__.py"
touch "${OUT_DIR}/grpcgi/v1/__init__.py"

echo "Proto stubs generated in ${OUT_DIR}"
