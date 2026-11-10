import pytest
from raftlite.core import (
    Log, LogEntry, LogError, NodeState, Role, apply_committed, compute_commit_index,
    find_divergence, follower_commit_index, is_log_up_to_date, quorum_size,
    step_down_if_newer, truncate_safely,
)


def mklog(*terms):
    """mklog(1,1,2) -> log com 3 entradas de termos 1,1,2."""
    return Log(LogEntry(t, i + 1, f"k{i+1}", f"v{i+1}") for i, t in enumerate(terms))


# ------------------------------------------------------------------ Log
def test_log_vazio():
    l = Log()
    assert (l.last_index(), l.last_term(), l.term_at(0)) == (0, 0, 0)
    assert l.get(1) is None and l.term_at(1) is None


def test_log_append_sequencial_e_consultas():
    l = mklog(1, 1, 2)
    assert (l.last_index(), l.last_term()) == (3, 2)
    assert l.term_at(2) == 1 and l.term_at(3) == 2 and l.term_at(4) is None


def test_log_rejeita_indice_nao_sequencial():
    l = mklog(1)
    with pytest.raises(LogError):
        l.append(LogEntry(1, 3, "a", "b"))


def test_log_rejeita_termo_decrescente():
    l = mklog(2)
    with pytest.raises(LogError):
        l.append(LogEntry(1, 2, "a", "b"))


def test_log_truncate_from():
    l = mklog(1, 1, 1, 2, 2)
    removed = l.truncate_from(3)
    assert l.last_index() == 2 and len(removed) == 3


def test_log_entries_from_com_limite():
    l = mklog(1, 1, 1, 1)
    assert [e.index for e in l.entries_from(2)] == [2, 3, 4]
    assert [e.index for e in l.entries_from(2, limit=2)] == [2, 3]
    assert l.entries_from(9) == []


# ------------------------------------------------------------ quorum
def test_quorum_size():
    assert quorum_size(4) == 3 and quorum_size(3) == 2 and quorum_size(5) == 3


# ------------------------------------------------- is_log_up_to_date
@pytest.mark.parametrize("cand,me,expected", [
    ((2, 3), (1, 5), True),    # termo maior vence mesmo com indice menor
    ((1, 3), (1, 5), False),   # mesmo termo, indice menor
    ((1, 5), (1, 5), True),    # empate e aceito
    ((1, 7), (1, 5), True),    # mesmo termo, indice maior
    ((1, 9), (2, 1), False),   # termo menor perde mesmo com indice maior
    ((0, 0), (0, 0), True),    # ambos vazios
])
def test_is_log_up_to_date(cand, me, expected):
    assert is_log_up_to_date(*cand, *me) is expected


# ------------------------------------------------ compute_commit_index
def test_commit_exemplo_do_enunciado_5_5_3():
    l = mklog(1, 1, 1, 1, 1)  # lider em 5
    assert compute_commit_index(l, [5, 5, 3], 3, 1) == 5


def test_commit_3_3_3():
    assert compute_commit_index(mklog(1, 1, 1, 1, 1), [3, 3, 3], 3, 1) == 3


def test_commit_5_2_2():
    assert compute_commit_index(mklog(1, 1, 1, 1, 1), [5, 2, 2], 3, 1) == 2


def test_commit_so_2_nos_nao_avanca():
    # lider + 1 replica = 2 < quorum 3
    assert compute_commit_index(mklog(1, 1, 1), [3], 3, 1, current_commit=1) == 1


def test_commit_replicas_paradas_em_zero():
    assert compute_commit_index(mklog(1, 1), [0, 0, 0], 3, 1) == 0


def test_commit_nunca_retrocede():
    assert compute_commit_index(mklog(1, 1, 1), [0, 0, 0], 3, 1, current_commit=2) == 2


def test_commit_nao_conta_entrada_de_termo_antigo():
    # entradas 1-2 do termo 1 replicadas na maioria, mas lider ja esta no termo 2
    l = mklog(1, 1)
    assert compute_commit_index(l, [2, 2, 0], 3, current_term=2) == 0


def test_commit_entrada_do_termo_atual_comita_as_antigas_junto():
    l = mklog(1, 1, 2)  # idx 3 e do termo 2 (atual)
    assert compute_commit_index(l, [3, 3, 0], 3, current_term=2) == 3


# ---------------------------------------------------- follower_commit_index
def test_follower_commit_limitado_pelo_proprio_log():
    assert follower_commit_index(10, 4, 0) == 4
    assert follower_commit_index(3, 4, 0) == 3
    assert follower_commit_index(1, 4, 3) == 3  # monotonico


# --------------------------------------------------- step_down_if_newer
class FakeStorage:
    def __init__(self): self.saved = []
    def save_meta(self, t, v): self.saved.append((t, v))


def test_step_down_termo_maior():
    s = NodeState(1, role=Role.LEADER, current_term=3, voted_for=1, leader_id=1)
    st = FakeStorage()
    assert step_down_if_newer(s, 5, st) is True
    assert (s.current_term, s.role, s.voted_for, s.leader_id) == (5, Role.FOLLOWER, None, None)
    assert st.saved == [(5, None)]


@pytest.mark.parametrize("incoming", [3, 2, 0])
def test_step_down_termo_igual_ou_menor_nao_muda(incoming):
    s = NodeState(1, role=Role.LEADER, current_term=3, voted_for=1)
    st = FakeStorage()
    assert step_down_if_newer(s, incoming, st) is False
    assert (s.current_term, s.role, s.voted_for) == (3, Role.LEADER, 1)
    assert st.saved == []


# -------------------------------------------------------- apply_committed
def test_apply_committed_aplica_ate_commit():
    l = mklog(1, 1, 1)
    s = NodeState(1, commit_index=2)
    db = {}
    applied = apply_committed(s, l, db)
    assert [e.index for e in applied] == [1, 2]
    assert db == {"k1": "v1", "k2": "v2"}  # entrada 3 (nao comitada) invisivel
    assert s.last_applied == 2


def test_apply_committed_e_idempotente():
    l = mklog(1, 1)
    s = NodeState(1, commit_index=2)
    db = {}
    apply_committed(s, l, db)
    assert apply_committed(s, l, db) == []


def test_apply_committed_sobrescreve_chave():
    l = Log([LogEntry(1, 1, "x", "10"), LogEntry(1, 2, "x", "15")])
    s = NodeState(1, commit_index=2)
    db = {}
    apply_committed(s, l, db)
    assert db == {"x": "15"}


def test_apply_committed_commit_alem_do_log_e_erro():
    with pytest.raises(LogError):
        apply_committed(NodeState(1, commit_index=5), mklog(1), {})


# --------------------------------------------------------- find_divergence
def test_divergencia_nenhuma_log_igual_ou_prefixo():
    leader = mklog(1, 1, 2)
    assert find_divergence(leader, 3, 2) is None
    assert find_divergence(leader, 2, 1) is None   # replica atrasada
    assert find_divergence(leader, 0, 0) is None   # replica vazia


def test_divergencia_termo_diferente_no_mesmo_indice():
    leader = mklog(1, 1, 2)
    assert find_divergence(leader, 3, 1) == 3


def test_divergencia_replica_com_log_mais_longo():
    leader = mklog(1, 1)
    assert find_divergence(leader, 4, 1) == 3  # trunca o excedente


def _sync(local: Log, leader: Log, commit=0):
    """Simula Fetches repetidos ate a replica convergir."""
    for _ in range(50):
        d = find_divergence(leader, local.last_index(), local.last_term())
        if d is None:
            for e in leader.entries_from(local.last_index() + 1):
                local.append(e)
            return
        truncate_safely(local, d, commit)
    raise AssertionError("nao convergiu")


def test_sync_caso_enunciado_4b():
    # N1 gravou idx 3 (termo 1) sem replicar; novo lider tem idx 3 do termo 2
    local = mklog(1, 1, 1)
    leader = mklog(1, 1, 2)
    _sync(local, leader, commit=2)
    assert [(e.index, e.term) for e in local.all()] == [(1, 1), (2, 1), (3, 2)]


def test_sync_divergencia_total_a_partir_do_indice_1():
    local = mklog(1, 1, 1)
    leader = mklog(2, 2)
    _sync(local, leader, commit=0)
    assert local.all() == leader.all()


def test_sync_local_mais_longo():
    local = mklog(1, 1, 1, 1, 1)
    leader = mklog(1, 1)
    _sync(local, leader)
    assert local.all() == leader.all()


def test_sync_local_mais_curto_nao_trunca():
    local = mklog(1)
    leader = mklog(1, 1, 2, 2)
    _sync(local, leader)
    assert local.all() == leader.all()


# --------------------------------------------------------- truncate_safely
def test_nunca_trunca_comitado():
    l = mklog(1, 1, 1)
    with pytest.raises(LogError):
        truncate_safely(l, 2, commit_index=2)
    assert l.last_index() == 3  # log intacto
    assert len(truncate_safely(l, 3, commit_index=2)) == 1
