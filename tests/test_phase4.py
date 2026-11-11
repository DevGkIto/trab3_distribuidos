"""Fase 4 (integracao): eleicao automatica, termo incrementado, lider antigo, voto persistido."""
import os
import signal
import subprocess
import sys
import time

import grpc
import pytest

import client
from raftlite import config
from raftlite.pb import pb, pbg
from test_phase2 import ROOT, Cluster, wait_until


class ElectionCluster(Cluster):
    """Todos os nos iguais (sem --role leader): a eleicao escolhe o lider."""

    def __init__(self, tmp, election=(0.5, 1.0), write_timeout=2.0):
        super().__init__(tmp, write_timeout)
        self.election = election

    def start(self, i, election=None):
        lo, hi = election or self.election
        out = open(self.tmp / f"node{i}.out", "ab")
        self.procs[i] = subprocess.Popen(
            [sys.executable, "run_node.py", "--id", str(i), "--leader-id", "0",
             "--base-port", str(self.base), "--data-root", str(self.tmp / "data"),
             "--write-timeout", str(self.write_timeout), "--status-interval", "0",
             "--election-min", str(lo), "--election-max", str(hi)],
            cwd=ROOT, stdout=out, stderr=out)
        assert wait_until(lambda: client.status_node(i, self.base) is not None, 10), f"no {i} nao subiu"

    def statuses(self, nodes=(1, 2, 3, 4)):
        return {n: client.status_node(n, self.base) for n in nodes}

    def leaders(self, nodes=(1, 2, 3, 4)):
        return [(n, s.term) for n, s in self.statuses(nodes).items() if s and s.role == "leader"]

    def wait_leader(self, nodes=(1, 2, 3, 4), min_term=0, timeout=12):
        found = []
        def ok():
            found[:] = [x for x in self.leaders(nodes) if x[1] > min_term]
            return len(found) >= 1
        assert wait_until(ok, timeout), "nenhum lider eleito"
        return found[0]

    def write_ok(self, k, v, timeout=12):
        return wait_until(lambda: (r := self.write(k, v)[0]) is not None and r.success, timeout)


@pytest.fixture
def cluster(tmp_path):
    c = ElectionCluster(tmp_path)
    for i in (1, 2, 3, 4):
        c.start(i)
    yield c
    c.stop_all()


def test_eleicao_inicial_elege_exatamente_um_lider(cluster):
    lid, term = cluster.wait_leader()
    assert term >= 1
    seen = {}                                   # termo -> lideres vistos nesse termo
    end = time.time() + 3
    while time.time() < end:
        for n, t in cluster.leaders():
            seen.setdefault(t, set()).add(n)
        time.sleep(0.2)
    assert all(len(v) == 1 for v in seen.values()), seen   # nunca 2 lideres no mesmo termo


def test_lider_saudavel_nao_provoca_eleicoes(cluster):
    lid, term = cluster.wait_leader()
    time.sleep(1.5)                             # deixa o cluster estabilizar
    lid, term = cluster.wait_leader()
    end = time.time() + 4
    while time.time() < end:
        assert cluster.leaders() == [(lid, term)]          # mesmo lider, mesmo termo
        time.sleep(0.4)


def test_cenario2_queda_do_lider_elege_novo_com_termo_maior(cluster):
    l1, t1 = cluster.wait_leader()
    assert cluster.write_ok("antes", "1")
    cluster.kill(l1)
    vivos = tuple(n for n in (1, 2, 3, 4) if n != l1)
    l2, t2 = cluster.wait_leader(nodes=vivos, min_term=t1)
    assert l2 != l1 and t2 > t1                           # novo lider, termo incrementado
    assert cluster.write_ok("depois", "2")                # escrita volta a funcionar
    assert wait_until(lambda: cluster.read_all_equal("antes", "1", nodes=vivos))
    assert wait_until(lambda: cluster.read_all_equal("depois", "2", nodes=vivos))


def test_lider_antigo_volta_como_seguidor_e_sincroniza(cluster):
    l1, t1 = cluster.wait_leader()
    assert cluster.write_ok("a", "1")
    cluster.kill(l1)
    vivos = tuple(n for n in (1, 2, 3, 4) if n != l1)
    l2, t2 = cluster.wait_leader(nodes=vivos, min_term=t1)
    assert cluster.write_ok("b", "2")
    cluster.start(l1)                                      # antigo lider volta
    assert wait_until(lambda: (s := client.status_node(l1, cluster.base)) is not None
                      and s.role == "follower" and s.term >= t2)
    assert wait_until(lambda: cluster.read_all_equal("b", "2"))   # alcancou os 4 nos
    assert len(cluster.leaders()) == 1


def test_lider_pausado_e_retomado_vira_seguidor(cluster):
    l1, t1 = cluster.wait_leader()
    pid = cluster.procs[l1].pid
    os.kill(pid, signal.SIGSTOP)                           # "congela" o lider
    try:
        vivos = tuple(n for n in (1, 2, 3, 4) if n != l1)
        l2, t2 = cluster.wait_leader(nodes=vivos, min_term=t1)
    finally:
        os.kill(pid, signal.SIGCONT)                       # retoma o lider antigo
    assert wait_until(lambda: (s := client.status_node(l1, cluster.base)) is not None
                      and s.role == "follower" and s.term >= t2, timeout=10)
    assert [x[0] for x in cluster.leaders()] == [l2]


def test_sem_quorum_nao_ha_eleicao(cluster):
    l1, t1 = cluster.wait_leader()
    outros = [n for n in (1, 2, 3, 4) if n != l1]
    cluster.kill(outros[0]); cluster.kill(outros[1])       # sobram 2 nos (< quorum 3)
    time.sleep(4)
    vivos = (l1, outros[2])
    assert all(n == l1 for n, _ in cluster.leaders(vivos))   # ninguem alem do lider atual
    r, _ = cluster.write("x", "1")
    assert r is None or not r.success                      # nada e comitado sem quorum


# ------------------------------------------------------------ voto unico persistido (rede real)
def rpc_vote(c, node, cand, term):
    with grpc.insecure_channel(config.address(node, c.base), options=config.CHANNEL_OPTIONS) as ch:
        return pbg.ClusterServiceStub(ch).RequestVote(
            pb.VoteRequest(candidate_id=cand, term=term, last_log_term=0, last_log_index=0),
            timeout=2)


def test_voto_unico_sobrevive_a_kill_e_reinicio(tmp_path):
    c = ElectionCluster(tmp_path, election=(60, 61))       # no 2 nunca vira candidato sozinho
    try:
        c.start(2)
        assert rpc_vote(c, 2, cand=3, term=5).vote_granted
        assert not rpc_vote(c, 2, cand=4, term=5).vote_granted   # voto unico no termo 5
        c.kill(2)
        c.start(2)
        assert client.status_node(2, c.base).term == 5           # termo persistido
        assert not rpc_vote(c, 2, cand=4, term=5).vote_granted   # continua NEGADO apos reinicio
        assert rpc_vote(c, 2, cand=3, term=5).vote_granted       # mesmo candidato: ok
        assert rpc_vote(c, 2, cand=4, term=6).vote_granted       # termo novo: ok
    finally:
        c.stop_all()