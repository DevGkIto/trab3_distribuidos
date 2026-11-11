"""Integracao da Fase 2: sobe 4 processos reais (no 1 = lider fixo)."""
import random
import subprocess
import sys
import time
from pathlib import Path

import pytest

import client
from raftlite import config

ROOT = Path(__file__).resolve().parent.parent


def wait_until(cond, timeout=8.0, step=0.1):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(step)
    return False


class Cluster:
    def __init__(self, tmp, write_timeout=2.0):
        self.tmp, self.write_timeout = tmp, write_timeout
        self.base = random.randint(20000, 40000)
        self.procs = {}

    def start(self, i):
        out = open(self.tmp / f"node{i}.out", "ab")
        self.procs[i] = subprocess.Popen(
            [sys.executable, "run_node.py", "--id", str(i),
             "--role", "leader" if i == 1 else "follower",
             "--base-port", str(self.base), "--data-root", str(self.tmp / "data"),
             "--write-timeout", str(self.write_timeout), "--status-interval", "0"],
            cwd=ROOT, stdout=out, stderr=out)
        assert wait_until(lambda: client.status_node(i, self.base) is not None, 10), f"no {i} nao subiu"

    def kill(self, i):
        self.procs[i].kill()
        self.procs[i].wait()

    def stop_all(self):
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()
                p.wait()

    def data(self, i):
        return self.tmp / "data" / f"node{i}"

    def write(self, k, v, node=1):
        return client.write_with_redirect(k, v, node, self.base)

    def read(self, i, k):
        return client.read_node(i, k, self.base)

    def read_all_equal(self, k, v, nodes=(1, 2, 3, 4)):
        return all((r := self.read(n, k)) is not None and r.found and r.value == v for n in nodes)


@pytest.fixture
def cluster(tmp_path):
    c = Cluster(tmp_path)
    for i in (1, 2, 3, 4):
        c.start(i)
    yield c
    c.stop_all()


def test_cenario1_escrita_e_leitura_distribuida(cluster):
    resp, _ = cluster.write("x", "10")
    assert resp.success
    assert wait_until(lambda: cluster.read_all_equal("x", "10"))


def test_sucesso_so_depois_do_commit(cluster):
    resp, _ = cluster.write("a", "1")
    assert resp.success
    assert client.status_node(1, cluster.base).commit_index >= resp.index


def test_escrita_em_seguidor_redireciona(cluster):
    resp, trail = cluster.write("r", "7", node=3)
    assert resp.success
    assert any("redirecionando" in t for t in trail)
    assert wait_until(lambda: cluster.read_all_equal("r", "7"))


def test_cliente_pula_no_fora_do_ar(cluster):
    cluster.kill(2)
    resp, trail = cluster.write("p", "9", node=2)
    assert resp.success and any("indisponivel" in t for t in trail)


def test_logs_identicos_nos_4_nos(cluster):
    for i in range(10):
        assert cluster.write(f"k{i}", str(i))[0].success
    def same():
        txt = [cluster.data(n).joinpath("log.jsonl").read_text() for n in (1, 2, 3, 4)]
        return len(set(txt)) == 1 and txt[0].count("\n") == 10
    assert wait_until(same)


def test_cenario4a_replica_desatualizada_alcanca(cluster):
    cluster.kill(4)
    for k, v in (("x", "1"), ("y", "2"), ("z", "3")):
        assert cluster.write(k, v)[0].success       # 3 nos vivos = quorum
    cluster.start(4)
    assert wait_until(lambda: cluster.read_all_equal("z", "3"))
    assert cluster.data(4).joinpath("log.jsonl").read_text() == \
           cluster.data(1).joinpath("log.jsonl").read_text()


def test_cenario5_dado_uncommitted_fica_invisivel(cluster):
    cluster.data(1).joinpath("HOLD_COMMIT").touch()       # segura o commit
    resp, _ = cluster.write("sec", "42")
    assert not resp.success                                # nao confirma ao cliente
    for n in (2, 3, 4):                                    # dado ESTA no log das replicas...
        assert wait_until(lambda n=n: "sec" in cluster.data(n).joinpath("log.jsonl").read_text())
    for n in (1, 2, 3, 4):                                 # ...mas leitura nao enxerga
        assert not cluster.read(n, "sec").found
    cluster.data(1).joinpath("HOLD_COMMIT").unlink()      # libera
    assert wait_until(lambda: cluster.read_all_equal("sec", "42"))


def test_reinicio_restaura_estado_sem_reaplicar(cluster):
    assert cluster.write("x", "5")[0].success
    assert wait_until(lambda: cluster.read_all_equal("x", "5"))
    cluster.kill(2)
    cluster.start(2)
    r = cluster.read(2, "x")
    assert r.found and r.value == "5"                      # db.json restaurado
    assert cluster.data(2).joinpath("log.jsonl").read_text().count("\n") == 1
    assert cluster.write("y", "6")[0].success
    assert wait_until(lambda: cluster.read_all_equal("y", "6"))