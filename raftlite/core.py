"""Logica pura do no: log, termo, voto, commit e divergencia.

Nada aqui faz rede ou le/escreve disco diretamente. Isso permite testar
tudo em milissegundos. Convencao: indices do log comecam em 1; indice 0
significa "log vazio" e tem termo 0.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Iterable, List, Optional


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass(frozen=True)
class LogEntry:
    term: int
    index: int
    key: str
    value: str


class LogError(Exception):
    pass


class Log:
    """Log em memoria (lista ordenada de entradas, indices 1..N)."""

    def __init__(self, entries: Optional[Iterable[LogEntry]] = None):
        self._entries: List[LogEntry] = []
        for e in entries or []:
            self.append(e)

    def __len__(self) -> int:
        return len(self._entries)

    def last_index(self) -> int:
        return len(self._entries)

    def last_term(self) -> int:
        return self._entries[-1].term if self._entries else 0

    def get(self, index: int) -> Optional[LogEntry]:
        if 1 <= index <= len(self._entries):
            return self._entries[index - 1]
        return None

    def term_at(self, index: int) -> Optional[int]:
        """Termo da entrada em `index`. 0 -> 0. Fora do log -> None."""
        if index == 0:
            return 0
        e = self.get(index)
        return e.term if e else None

    def append(self, entry: LogEntry) -> None:
        if entry.index != self.last_index() + 1:
            raise LogError(
                f"indice {entry.index} nao e sequencial (esperado {self.last_index() + 1})"
            )
        if entry.term < self.last_term():
            raise LogError("termo da entrada menor que o da ultima entrada")
        self._entries.append(entry)

    def truncate_from(self, index: int) -> List[LogEntry]:
        """Remove entradas com indice >= index. Retorna as removidas."""
        if index < 1:
            raise LogError("indice de truncamento deve ser >= 1")
        removed = self._entries[index - 1:]
        del self._entries[index - 1:]
        return removed

    def entries_from(self, index: int, limit: Optional[int] = None) -> List[LogEntry]:
        start = max(index, 1) - 1
        chunk = self._entries[start:]
        return chunk[:limit] if limit is not None else chunk

    def all(self) -> List[LogEntry]:
        return list(self._entries)


@dataclass
class NodeState:
    node_id: int
    role: Role = Role.FOLLOWER
    current_term: int = 0
    voted_for: Optional[int] = None
    commit_index: int = 0
    last_applied: int = 0
    leader_id: Optional[int] = None


# ---------------------------------------------------------------- quorum
def quorum_size(n_nodes: int) -> int:
    """Maioria absoluta: N=4 -> 3."""
    return n_nodes // 2 + 1


# ---------------------------------------------------------------- termo
def step_down_if_newer(state: NodeState, term: int, storage=None) -> bool:
    """Atualizacao obrigatoria de termo.

    Se `term` > termo local: atualiza termo, limpa voto, vira Seguidor.
    Retorna True se algo mudou. Se `storage` for dado, persiste meta.
    """
    if term > state.current_term:
        state.current_term = term
        state.voted_for = None
        state.role = Role.FOLLOWER
        state.leader_id = None
        if storage is not None:
            storage.save_meta(state.current_term, state.voted_for)
        return True
    return False


# ---------------------------------------------------------------- voto
def is_log_up_to_date(cand_last_term: int, cand_last_index: int,
                      my_last_term: int, my_last_index: int) -> bool:
    """O log do candidato e tao ou mais atualizado que o meu?

    1) termo da ultima entrada maior vence; 2) mesmo termo: maior indice vence.
    """
    if cand_last_term != my_last_term:
        return cand_last_term > my_last_term
    return cand_last_index >= my_last_index


# ---------------------------------------------------------------- commit
def compute_commit_index(leader_log: Log, match_indexes: Iterable[int],
                         quorum: int, current_term: int,
                         current_commit: int = 0) -> int:
    """Novo indice de commit do lider.

    `match_indexes`: ultimo indice informado por cada replica (sem o lider).
    O lider conta a si mesmo com seu ultimo indice. O candidato a commit e o
    maior indice presente em pelo menos `quorum` nos. Regra de seguranca do
    Raft: so avanca se a entrada nesse indice for do termo ATUAL. O commit
    nunca retrocede.
    """
    indices = sorted([leader_log.last_index(), *match_indexes], reverse=True)
    if len(indices) < quorum:
        return current_commit
    candidate = indices[quorum - 1]
    if candidate <= current_commit:
        return current_commit
    if leader_log.term_at(candidate) != current_term:
        return current_commit
    return candidate


def follower_commit_index(leader_commit: int, my_last_index: int,
                          current_commit: int) -> int:
    """Commit da replica = min(commit do lider, meu ultimo indice), monotonico."""
    return max(current_commit, min(leader_commit, my_last_index))


def apply_committed(state: NodeState, log: Log, db: Dict[str, str]) -> List[LogEntry]:
    """Aplica ao banco as entradas de last_applied+1 ate commit_index."""
    if state.commit_index > log.last_index():
        raise LogError("commit_index maior que o ultimo indice do log")
    applied: List[LogEntry] = []
    for i in range(state.last_applied + 1, state.commit_index + 1):
        entry = log.get(i)
        db[entry.key] = entry.value
        state.last_applied = i
        applied.append(entry)
    return applied


# ---------------------------------------------------------------- divergencia
def find_divergence(leader_log: Log, replica_last_index: int,
                    replica_last_term: int) -> Optional[int]:
    """Verificacao de consistencia feita pelo lider no Fetch.

    Retorna None se o log da replica e prefixo do log do lider. Caso
    contrario, retorna o indice a partir do qual a replica deve truncar
    (remove entradas com index >= retorno). Se a divergencia for mais
    profunda, a replica recua nos Fetches seguintes ate concordar.
    """
    if replica_last_index == 0:
        return None
    if replica_last_index > leader_log.last_index():
        return leader_log.last_index() + 1
    if leader_log.term_at(replica_last_index) != replica_last_term:
        return replica_last_index
    return None


def truncate_safely(log: Log, from_index: int, commit_index: int) -> List[LogEntry]:
    """Trunca, mas NUNCA entradas ja comitadas (bug grave se ocorrer)."""
    if from_index <= commit_index:
        raise LogError(
            f"tentativa de truncar entrada comitada (from={from_index}, commit={commit_index})"
        )
    return log.truncate_from(from_index)
