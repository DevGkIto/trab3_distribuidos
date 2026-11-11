"""Fase 3 (unitarios): vivacidade das replicas e bloqueio de escrita sem quorum."""
import time

from raftlite.core import active_replicas, has_quorum, quorum_size
from raftlite.node import Node
from raftlite.pb import pb


# ------------------------------------------------ funcoes puras (relogio simulado)
def test_replica_dentro_do_timeout_e_ativa():
    assert active_replicas({2: 9.8, 3: 9.9}, now=10.0, heartbeat_timeout=0.5) == [2, 3]


def test_replica_fora_do_timeout_e_inativa():
    assert active_replicas({2: 9.0, 3: 9.9}, now=10.0, heartbeat_timeout=0.5) == [3]


def test_limite_exato_conta_como_ativa():
    assert active_replicas({2: 9.5}, now=10.0, heartbeat_timeout=0.5) == [2]      # == timeout
    assert active_replicas({2: 9.49}, now=10.0, heartbeat_timeout=0.5) == []      # logo acima


def test_replica_que_nunca_buscou_nao_e_ativa():
    assert active_replicas({}, now=10.0, heartbeat_timeout=0.5) == []


def test_has_quorum_cluster_de_4():
    q = quorum_size(4)  # 3
    assert has_quorum(3, q) and has_quorum(2, q)        # lider + 2 = 3 -> ok
    assert not has_quorum(1, q) and not has_quorum(0, q)  # lider + 1 = 2 -> sem quorum


# ------------------------------------------------ Node sem rede (handlers diretos)
def fetch(node, rid):
    return node.handle_fetch(pb.FetchRequest(replica_id=rid, term=1,
                                             last_log_term=0, last_log_index=0))


def write(node, k="a", v="1"):
    return node.handle_write(pb.WriteRequest(key=k, value=v))


def mk_leader(tmp_path, heartbeat=5.0):
    return Node(1, "leader", tmp_path / "n1", write_timeout=0.2,
                status_interval=0, heartbeat_timeout=heartbeat)


def test_recusa_sem_quorum_e_nao_grava_no_log(tmp_path):
    n = mk_leader(tmp_path)
    r = write(n)
    assert not r.success and "quorum" in r.error_message
    assert n.log.last_index() == 0
    fetch(n, 2)                                  # lider + 1 replica = 2 < 3
    r = write(n)
    assert not r.success and "quorum" in r.error_message
    assert n.log.last_index() == 0


def test_aceita_com_quorum_mas_sem_commit_da_timeout(tmp_path):
    n = mk_leader(tmp_path)
    fetch(n, 2); fetch(n, 3)                     # lider + 2 = 3 -> tem quorum
    r = write(n)
    assert not r.success and "timeout" in r.error_message   # aceitou, mas ninguem confirmou
    assert n.log.last_index() == 1                          # entrada ficou no log


def test_replica_que_para_de_buscar_perde_quorum(tmp_path):
    n = mk_leader(tmp_path, heartbeat=0.2)
    fetch(n, 2); fetch(n, 3)
    assert "timeout" in write(n).error_message              # tem quorum
    time.sleep(0.3)                                         # ninguem busca mais
    assert "quorum" in write(n).error_message               # recusa imediata
    fetch(n, 2); fetch(n, 3)                                # voltaram
    assert "timeout" in write(n, "b", "2").error_message    # quorum restaurado