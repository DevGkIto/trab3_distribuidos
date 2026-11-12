"""Fase 5: Cenario 4B (truncamento) deterministico + chaos test.

Chaos mais longo:  CHAOS_SECONDS=120 python -m pytest tests/test_phase5.py -k chaos -s
"""
import os
import random
import threading
import time

import pytest

import client
from test_phase2 import wait_until
from test_phase4 import ElectionCluster

CHAOS_SECONDS = float(os.environ.get("CHAOS_SECONDS", "20"))
ALL = (1, 2, 3, 4)


@pytest.fixture
def cluster(tmp_path):
    c = ElectionCluster(tmp_path)
    for i in ALL:
        c.start(i)
    yield c
    c.stop_all()


def logtxt(c, n):
    return c.data(n).joinpath("log.jsonl").read_text()


def logs_iguais(c):
    return len({logtxt(c, n) for n in ALL}) == 1


def test_cenario4b_truncamento_da_entrada_nao_comitada(cluster):
    l1, t1 = cluster.wait_leader()
    assert cluster.write_ok("base", "0")                          # indice 1, comitado

    # 1) segura a replicacao no lider N1 e grava uma entrada SO no log local dele
    cluster.data(l1).joinpath("PAUSE_REPLICATION").touch()
    resp, err = client.write_once(l1, "perdida", "X", cluster.base, timeout=1.0)
    assert resp is None and err == "DEADLINE_EXCEEDED"            # nunca confirmada ao cliente
    assert "perdida" in logtxt(cluster, l1)                       # esta no log do N1...
    for n in ALL:
        if n != l1:
            assert "perdida" not in logtxt(cluster, n)            # ...e em mais nenhum

    # 2) crash imediato do N1
    cluster.kill(l1)
    vivos = tuple(n for n in ALL if n != l1)

    # 3) novo lider N2, termo maior, nova escrita comitada
    l2, t2 = cluster.wait_leader(nodes=vivos, min_term=t1)
    assert t2 > t1
    assert cluster.write_ok("nova", "Y")                          # ocupa o indice 2 no termo t2

    # 4) N1 volta: vira seguidor, TRUNCA a entrada nao comitada e ressincroniza
    cluster.data(l1).joinpath("PAUSE_REPLICATION").unlink()
    cluster.start(l1)
    assert wait_until(lambda: (s := client.status_node(l1, cluster.base)) is not None
                      and s.role == "follower" and s.term >= t2)
    assert wait_until(lambda: cluster.read_all_equal("nova", "Y"))
    assert wait_until(lambda: logs_iguais(cluster))               # log do N1 == log do lider
    assert "perdida" not in logtxt(cluster, l1)
    assert all(not cluster.read(n, "perdida").found for n in ALL)  # valor perdido nunca aparece
    saida = cluster.tmp.joinpath(f"node{l1}.out").read_text()
    assert "truncando log a partir do indice 2" in saida


def test_chaos_nenhuma_escrita_confirmada_se_perde(cluster):
    cluster.wait_leader()
    stop, up, erros = threading.Event(), set(ALL), []

    def caos():
        rng = random.Random()
        try:
            while not stop.is_set():
                time.sleep(rng.uniform(0.6, 1.6))
                if stop.is_set():
                    break
                n = rng.choice(ALL)
                if n in up:
                    if len(up) > 2:                      # mantem ao menos 2 nos de pe
                        cluster.kill(n)                  # kill -9: crash de verdade
                        up.discard(n)
                else:
                    cluster.start(n)
                    up.add(n)
        except Exception as e:                           # pragma: no cover
            erros.append(e)

    th = threading.Thread(target=caos)
    th.start()
    acked, i, fim = {}, 0, time.time() + CHAOS_SECONDS
    while time.time() < fim:
        k, v = f"k{i}", str(i)
        i += 1
        r, _ = client.write_with_redirect(k, v, random.choice(ALL), cluster.base,
                                          retry_for=3, timeout=4)
        if r is not None and r.success:
            acked[k] = v
        time.sleep(0.05)
    stop.set()
    th.join()
    assert not erros, erros
    print(f"\nchaos: {i} tentativas, {len(acked)} confirmadas")
    assert acked, "nenhuma escrita confirmada durante o caos"

    for n in ALL:                                        # religa todos
        if n not in up:
            cluster.start(n)
    cluster.wait_leader(timeout=20)
    # entrada do termo atual comita as anteriores (regra de seguranca do Raft)
    assert cluster.write_ok("final", "ok", timeout=30)
    assert wait_until(lambda: cluster.read_all_equal("final", "ok"), 20)

    # DURABILIDADE: toda escrita confirmada esta em todos os nos, com o valor certo
    assert wait_until(lambda: all(cluster.read_all_equal(k, v) for k, v in acked.items()), 20)
    # CONVERGENCIA: logs byte a byte identicos, um unico lider
    assert wait_until(lambda: logs_iguais(cluster), 20)
    assert len(cluster.leaders()) == 1