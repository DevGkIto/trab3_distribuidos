import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def pytest_configure(config):
    """Gera os stubs gRPC em ./gen se ainda nao existirem."""
    if not (ROOT / "gen" / "cluster_pb2.py").exists():
        (ROOT / "gen").mkdir(exist_ok=True)
        (ROOT / "gen" / "__init__.py").touch()
        subprocess.check_call([sys.executable, "-m", "grpc_tools.protoc", "-I", "proto",
                               "--python_out=gen", "--grpc_python_out=gen",
                               "proto/cluster.proto"], cwd=ROOT)