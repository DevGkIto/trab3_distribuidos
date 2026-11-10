#!/usr/bin/env bash
# Gera os stubs gRPC em ./gen
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p gen
python -m grpc_tools.protoc -I proto --python_out=gen --grpc_python_out=gen proto/cluster.proto
touch gen/__init__.py
echo "Stubs gerados em ./gen"
