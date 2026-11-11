import os
import sys

_GEN = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "gen")
if _GEN not in sys.path:
    sys.path.insert(0, _GEN)

try:
    import cluster_pb2 as pb
    import cluster_pb2_grpc as pbg
except ImportError as e:
    raise ImportError("Stubs gRPC nao encontrados. Rode: ./scripts/gen_proto.sh") from e