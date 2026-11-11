"""Fase 3 (integracao) - Cenario 3: perda de quorum bloqueia escritas."""
import time

import pytest

import client
from test_phase2 import Cluster, wait_until


@pytest.fixture
def cluster(tmp_path):
    c = Cluster(tmp_path, write_timeout=2.0)
    for i in (1, 2, 3, 4):
        c.start(i)
    yield c
    c.stop_all()


def log_lines(c, n=1):
    return c.data(n).joinpath("log.jsonl").read_text().count("\n")


def test_cenario3_perda_e_retorno_de_quorum(cluster):
    assert cluster.write("antes", "ok")[0].success                    # 4 nos: aceita

    cluster.kill(4)
    time.sleep(0.8)                                                   # > heartbeat timeout
    assert cluster.write("com3", "ok")[0].success                     # 3 nos = quorum: aceita

    cluster.kill(3)
    time.sleep(0.8)                                                   # agora so 2 nos
    linhas = log_lines(cluster)
    t0 = time.time()
    resp, _ = cluster.write("sem_quorum", "x")
    elapsed = time.time() - t0
    assert resp is not None and not resp.success
    assert "quorum" in resp.error_message
    assert elapsed < 1.0                                              # recusa IMEDIATA (timeout e 2 s)
    assert log_lines(cluster) == linhas                               # nem chegou a gravar no log
    assert cluster.read(1, "antes").value == "ok"                     # leitura de comitados segue ok
    assert not cluster.read(1, "sem_quorum").found

    cluster.start(3)                                                  # volta 1 replica -> 3 nos
    assert wait_until(lambda: (r := cluster.write("depois", "ok")[0]) is not None and r.success)
    assert wait_until(lambda: cluster.read_all_equal("depois", "ok", nodes=(1, 2, 3)))


def test_quorum_com_uma_falha_nao_bloqueia(cluster):
    cluster.kill(2)
    time.sleep(0.8)
    assert cluster.write("x", "1")[0].success