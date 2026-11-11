"""Fase 4 (unitarios, sem rede): voto, termo, persistencia do voto, timer aleatorio."""
from raftlite.core import Log, LogEntry, NodeState, Role, decide_vote
from raftlite.node import Node
from raftlite.pb import pb
from raftlite.storage import Storage


def mklog(*terms):
    return Log(LogEntry(t, i + 1, f"k{i+1}", f"v{i+1}") for i, t in enumerate(terms))


def vote(state, log, cand, term, lt=0, li=0, storage=None):
    return decide_vote(state, log, cand, term, lt, li, storage)


# ------------------------------------------------------------ decide_vote (puro)
def test_nega_termo_menor_e_nada_muda():
    s = NodeState(1, current_term=5)
    assert vote(s, Log(), 2, 4) is False
    assert (s.current_term, s.voted_for) == (5, None)


def test_concede_voto_e_registra():
    s = NodeState(1, current_term=3)
    assert vote(s, Log(), 2, 4) is True
    assert (s.current_term, s.voted_for) == (4, 2)


def test_voto_unico_por_termo():
    s = NodeState(1)
    assert vote(s, Log(), 2, 1) is True
    assert vote(s, Log(), 3, 1) is False       # outro candidato, mesmo termo
    assert s.voted_for == 2


def test_voto_idempotente_mesmo_candidato():
    s = NodeState(1)
    assert vote(s, Log(), 2, 1) is True
    assert vote(s, Log(), 2, 1) is True


def test_termo_novo_libera_novo_voto():
    s = NodeState(1)
    assert vote(s, Log(), 2, 1) is True
    assert vote(s, Log(), 3, 2) is True
    assert s.voted_for == 3


def test_nega_log_desatualizado_mas_atualiza_termo():
    s = NodeState(1, current_term=1)
    log = mklog(1, 1, 2)                        # meu log: ultimo termo 2, indice 3
    assert vote(s, log, 2, 5, lt=1, li=9) is False   # candidato: termo de log menor
    assert s.current_term == 5 and s.voted_for is None   # termo atualizado, sem voto


def test_concede_log_mais_atualizado_ou_igual():
    log = mklog(1, 1, 2)
    assert vote(NodeState(1), log, 2, 1, lt=2, li=3) is True    # igual
    assert vote(NodeState(1), log, 2, 1, lt=3, li=1) is True    # termo maior
    assert vote(NodeState(1), log, 2, 1, lt=2, li=2) is False   # mesmo termo, indice menor


def test_lider_vira_seguidor_ao_ver_termo_maior():
    s = NodeState(1, role=Role.LEADER, current_term=2, voted_for=1, leader_id=1)
    assert vote(s, Log(), 3, 3) is True
    assert s.role == Role.FOLLOWER and s.leader_id is None


def test_candidato_nega_outro_no_mesmo_termo():
    s = NodeState(1, role=Role.CANDIDATE, current_term=4, voted_for=1)   # votou em si
    assert vote(s, Log(), 2, 4) is False
    assert s.role == Role.CANDIDATE


def test_voto_persistido_antes_de_retornar(tmp_path):
    st = Storage(tmp_path)
    s = NodeState(1)
    assert vote(s, Log(), 3, 7, storage=st) is True
    assert Storage(tmp_path).load_meta() == (7, 3)      # ja esta em disco


# ------------------------------------------------------------ Node (handlers, sem rede)
def mknode(tmp_path, **kw):
    return Node(2, "follower", tmp_path / "n2", status_interval=0, leader_id=0, **kw)


def rv(node, cand, term, lt=0, li=0):
    return node.handle_request_vote(pb.VoteRequest(candidate_id=cand, term=term,
                                                   last_log_term=lt, last_log_index=li))


def test_voto_sobrevive_a_reinicio_do_no(tmp_path):
    n = mknode(tmp_path)
    assert rv(n, 3, 5).vote_granted is True
    n2 = mknode(tmp_path)                                # "reinicio": nova instancia, mesmo disco
    assert (n2.state.current_term, n2.state.voted_for) == (5, 3)
    assert rv(n2, 4, 5).vote_granted is False            # outro candidato, mesmo termo: NEGADO
    assert rv(n2, 3, 5).vote_granted is True             # o mesmo candidato: ok
    assert rv(n2, 4, 6).vote_granted is True             # termo novo: ok


def test_lider_antigo_vira_seguidor_ao_receber_requestvote_maior(tmp_path):
    n = Node(1, "leader", tmp_path / "n1", status_interval=0)
    assert n.state.role == Role.LEADER and n.state.current_term == 1
    r = rv(n, 2, 2)
    assert r.vote_granted and r.term == 2
    assert n.state.role == Role.FOLLOWER


def test_fetch_com_termo_maior_derruba_o_lider(tmp_path):
    n = Node(1, "leader", tmp_path / "n1", status_interval=0)
    resp = n.handle_fetch(pb.FetchRequest(replica_id=2, term=9, last_log_term=0, last_log_index=0))
    assert resp.success is False and resp.term == 9
    assert n.state.role == Role.FOLLOWER and n.state.current_term == 9


def test_resposta_de_lider_com_termo_menor_e_ignorada(tmp_path):
    n = mknode(tmp_path)
    n.state.current_term = 5
    n._handle_fetch_response(pb.FetchResponse(term=3, success=True, leader_id=1, commit_index=0))
    assert n.state.leader_id is None and n.state.current_term == 5


def test_candidato_volta_a_seguidor_ao_achar_lider_do_mesmo_termo(tmp_path):
    n = mknode(tmp_path)
    n.state.role, n.state.current_term, n.state.voted_for = Role.CANDIDATE, 4, 2
    n._handle_fetch_response(pb.FetchResponse(term=4, success=True, leader_id=3, commit_index=0))
    assert n.state.role == Role.FOLLOWER and n.state.leader_id == 3


def test_dica_de_lider_e_seguida(tmp_path):
    n = mknode(tmp_path)
    n._handle_fetch_response(pb.FetchResponse(term=0, success=False, leader_id=4,
                                              error_message="nao lider"))
    assert n.state.leader_id == 4
    n._handle_fetch_response(pb.FetchResponse(term=0, success=False, leader_id=0))
    assert n.state.leader_id is None                    # sem dica: volta a procurar


# ------------------------------------------------------------ timer aleatorio
def test_timeout_dentro_da_faixa_e_aleatorio(tmp_path):
    import time
    n = mknode(tmp_path, election_min=1.0, election_max=2.0)
    vals = set()
    for _ in range(50):
        n._reset_election_timer()
        d = n._election_deadline - time.monotonic()
        assert 0.9 < d <= 2.0
        vals.add(round(d, 3))
    assert len(vals) > 40                               # valores distintos: nao e constante


def test_nos_diferentes_sorteiam_prazos_diferentes(tmp_path):
    prazos = {Node(i, "follower", tmp_path / f"n{i}", status_interval=0)._election_deadline
              for i in (1, 2, 3, 4)}
    assert len(prazos) == 4