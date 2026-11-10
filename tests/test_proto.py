"""Gera os stubs a partir do .proto e testa serializacao (round-trip)."""
import subprocess
import sys
import pytest

pytest.importorskip("grpc_tools")


@pytest.fixture(scope="module")
def pb(tmp_path_factory):
    out = tmp_path_factory.mktemp("gen")
    r = subprocess.run(
        [sys.executable, "-m", "grpc_tools.protoc", "-I", "proto",
         f"--python_out={out}", f"--grpc_python_out={out}", "proto/cluster.proto"],
        capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    sys.path.insert(0, str(out))
    import cluster_pb2, cluster_pb2_grpc  # noqa
    return cluster_pb2, cluster_pb2_grpc


def rt(msg):
    return type(msg).FromString(msg.SerializeToString())


def test_fetch_response_com_entradas_e_divergencia(pb):
    m, _ = pb
    r = rt(m.FetchResponse(term=3, success=False, leader_id=2, commit_index=4,
                           diverged=True, diverge_index=5,
                           entries=[m.LogEntry(term=3, index=5, key="x", value="ç")]))
    assert (r.term, r.diverged, r.diverge_index, r.commit_index) == (3, True, 5, 4)
    assert r.entries[0].value == "ç"


def test_requests_roundtrip(pb):
    m, _ = pb
    f = rt(m.FetchRequest(replica_id=2, term=1, last_log_term=1, last_log_index=7))
    assert (f.replica_id, f.last_log_index) == (2, 7)
    v = rt(m.VoteRequest(candidate_id=3, term=4, last_log_term=3, last_log_index=9))
    assert (v.candidate_id, v.term) == (3, 4)
    assert rt(m.VoteResponse(term=4, vote_granted=True)).vote_granted is True


def test_cliente_roundtrip(pb):
    m, _ = pb
    w = rt(m.WriteResponse(success=False, leader_id=1, leader_address="localhost:50051"))
    assert w.leader_address == "localhost:50051" and not w.success
    r = rt(m.ReadResponse(found=True, value="10", node_id=2, role="follower", commit_index=3))
    assert (r.value, r.role) == ("10", "follower")


def test_servicos_existem(pb):
    _, g = pb
    for name in ("ClusterServiceStub", "ClusterServiceServicer",
                 "ClientServiceStub", "ClientServiceServicer"):
        assert hasattr(g, name)
