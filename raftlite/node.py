"""No do cluster: Seguidor / Candidato / Lider (Fases 2 a 4)."""
from __future__ import annotations

import random
import threading
import time
from pathlib import Path
from typing import Dict

import grpc

from . import config
from .core import (Log, LogEntry, LogError, NodeState, Role, active_replicas,
                   apply_committed, compute_commit_index, decide_vote,
                   find_divergence, follower_commit_index, has_quorum,
                   quorum_size, step_down_if_newer, truncate_safely)
from .pb import pb, pbg
from .storage import Storage


def _to_pb(e: LogEntry):
    return pb.LogEntry(term=e.term, index=e.index, key=e.key, value=e.value)


class Node:
    def __init__(self, node_id: int, role: str, data_dir, base_port: int = config.DEFAULT_BASE_PORT,
                 leader_id: int = 1, write_timeout: float = config.WRITE_TIMEOUT,
                 status_interval: float = 1.0,
                 heartbeat_timeout: float = config.HEARTBEAT_TIMEOUT,
                 election_min: float = config.ELECTION_TIMEOUT_MIN,
                 election_max: float = config.ELECTION_TIMEOUT_MAX):
        self.id = node_id
        self.base_port = base_port
        self.write_timeout = write_timeout
        self.status_interval = status_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.election_min = election_min
        self.election_max = election_max
        self._rng = random.Random()
        self._rr = 0                        # rodizio de pares quando o lider e desconhecido
        self._active_prev = set()           # para detectar mudancas ativa/inativa

        # --- estado persistido (recarregado apos reinicio) ---
        self.storage = Storage(data_dir)
        term, voted = self.storage.load_meta()
        self.state = NodeState(node_id, current_term=term, voted_for=voted)
        self.log = Log(self.storage.load_log())
        self.db, last_applied = self.storage.load_db()
        self.state.last_applied = last_applied
        self.state.commit_index = last_applied   # limite inferior seguro

        # --- estado volatil do lider ---
        self.match_index: Dict[int, int] = {}
        self.last_fetch_time: Dict[int, float] = {}
        self.hold_file = Path(data_dir) / "HOLD_COMMIT"   # debug: Cenario 5
        self.pause_file = Path(data_dir) / "PAUSE_REPLICATION"

        # Um unico mutex protege todo o estado. NUNCA chamar RPC com ele travado.
        self.cond = threading.Condition()
        self._stop = threading.Event()
        self._stubs: Dict[int, object] = {}
        self._leader_down = False
        self._election_deadline = 0.0
        self._reset_election_timer()

        if role == "leader":                 # bootstrap opcional (modo lider fixo)
            if self.state.current_term < 1:
                self.state.current_term = 1
                self.state.voted_for = node_id
                self.storage.save_meta(1, node_id)
            self.state.role = Role.LEADER
            self.state.leader_id = node_id
        else:
            self.state.role = Role.FOLLOWER
            self.state.leader_id = leader_id if leader_id and leader_id != node_id else None

    # ------------------------------------------------------------ util
    @property
    def peers(self):
        return [n for n in range(1, config.N_NODES + 1) if n != self.id]

    def event(self, msg: str) -> None:
        print(f"[{time.strftime('%H:%M:%S')}][No {self.id}] {msg}", flush=True)

    def start(self) -> None:
        loops = [self._fetch_loop, self._monitor_loop, self._election_loop, self._probe_loop]
        if self.status_interval > 0:
            loops.append(self._status_loop)
        for fn in loops:
            threading.Thread(target=fn, daemon=True).start()

    def stop(self) -> None:
        self._stop.set()

    def _reset_election_timer(self) -> None:
        """Novo prazo ALEATORIO: evita que todos virem candidatos juntos (split vote)."""
        self._election_deadline = time.monotonic() + self._rng.uniform(
            self.election_min, self.election_max)

    def _stub(self, node_id: int):
        if node_id not in self._stubs:
            ch = grpc.insecure_channel(config.address(node_id, self.base_port),
                                       options=config.CHANNEL_OPTIONS)
            self._stubs[node_id] = pbg.ClusterServiceStub(ch)
        return self._stubs[node_id]

    def _observe_term(self, term: int) -> bool:
        """Atualizacao obrigatoria de termo (qualquer mensagem/resposta). Lock travado."""
        st = self.state
        old_term, old_role = st.current_term, st.role
        if step_down_if_newer(st, term, self.storage):
            self.event(f"termo maior observado ({old_term} -> {term}): "
                       f"{old_role.value} -> SEGUIDOR")
            if old_role == Role.LEADER:
                self._reset_election_timer()
            self.cond.notify_all()            # acorda Writes pendentes (lideranca perdida)
            return True
        return False

    def _status_loop(self) -> None:
        while not self._stop.wait(self.status_interval):
            with self.cond:
                s = self.state
                extra = ""
                if s.role == Role.LEADER:
                    extra = (f" | Replicas ativas: {len(self._active_replicas_locked())}"
                             f"/{config.N_NODES - 1}")
                print(f"[No {self.id}] Papel: {s.role.value.upper():9} | Termo: {s.current_term} | "
                      f"Commit: {s.commit_index} | Ultimo indice: {self.log.last_index()}{extra}",
                      flush=True)

    # ------------------------------------------------------------ quorum (Fase 3)
    def _active_replicas_locked(self):
        return active_replicas(self.last_fetch_time, time.monotonic(), self.heartbeat_timeout)

    def _monitor_loop(self) -> None:
        """Lider: registra quando uma replica fica inativa/ativa e se o quorum se perde."""
        while not self._stop.wait(0.1):
            with self.cond:
                if self.state.role != Role.LEADER:
                    self._active_prev = set()
                    continue
                now = set(self._active_replicas_locked())
                for r in sorted(now - self._active_prev):
                    self.event(f"replica {r} ATIVA")
                for r in sorted(self._active_prev - now):
                    self.event(f"replica {r} marcada INATIVA "
                               f"(sem Fetch ha mais de {int(self.heartbeat_timeout * 1000)} ms)")
                if now != self._active_prev:
                    ok = has_quorum(len(now), quorum_size(config.N_NODES))
                    self.event(f"quorum: {1 + len(now)}/{config.N_NODES} nos ativos "
                               f"({'OK' if ok else 'PERDIDO'})")
                self._active_prev = now

    def _probe_loop(self) -> None:
        """Lider sonda pares inativos: se algum responder com termo maior, vira seguidor.
        (Cobre o lider antigo que volta de uma pausa/particao sem ninguem buscar nele.)"""
        while not self._stop.wait(config.PROBE_INTERVAL):
            with self.cond:
                if self.state.role != Role.LEADER:
                    continue
                active = set(self._active_replicas_locked())
                targets = [p for p in self.peers if p not in active]
            for p in targets:
                try:
                    resp = self._stub(p).GetStatus(pb.StatusRequest(),
                                                   timeout=config.PROBE_RPC_TIMEOUT)
                except grpc.RpcError:
                    continue
                with self.cond:
                    self._observe_term(resp.term)

    # ------------------------------------------------------------ eleicao (Fase 4)
    def _election_loop(self) -> None:
        while not self._stop.wait(0.05):
            with self.cond:
                if self.state.role != Role.LEADER and time.monotonic() >= self._election_deadline:
                    self._start_election_locked()

    def _start_election_locked(self) -> None:
        st = self.state
        st.current_term += 1
        st.voted_for = self.id
        st.role = Role.CANDIDATE
        st.leader_id = None
        self.storage.save_meta(st.current_term, st.voted_for)   # persiste ANTES de pedir votos
        self._reset_election_timer()
        term = st.current_term
        self.event(f"sem contato com o lider: virou CANDIDATO (termo {term})")
        req = pb.VoteRequest(candidate_id=self.id, term=term,
                             last_log_term=self.log.last_term(),
                             last_log_index=self.log.last_index())
        threading.Thread(target=self._run_election, args=(term, req), daemon=True).start()

    def _run_election(self, term: int, req) -> None:
        votes = {self.id}                     # vota em si mesmo
        need = quorum_size(config.N_NODES)

        def ask(peer: int) -> None:
            try:   # RPC fora do lock, em paralelo
                resp = self._stub(peer).RequestVote(req, timeout=config.VOTE_RPC_TIMEOUT)
            except grpc.RpcError:
                return
            with self.cond:
                st = self.state
                if self._observe_term(resp.term):
                    return                    # alguem tem termo maior: desisto
                if st.role != Role.CANDIDATE or st.current_term != term:
                    return                    # a eleicao ja foi decidida/expirou: descarta
                if resp.vote_granted:
                    votes.add(peer)
                    self.event(f"voto recebido do no {peer} ({len(votes)}/{need})")
                    if len(votes) >= need:
                        self._become_leader_locked()

        for p in self.peers:
            threading.Thread(target=ask, args=(p,), daemon=True).start()

    def _become_leader_locked(self) -> None:
        st = self.state
        st.role = Role.LEADER
        st.leader_id = self.id
        self.match_index.clear()
        self.last_fetch_time.clear()
        self._active_prev = set()
        self.event(f"*** ELEITO LIDER no termo {st.current_term} ***")
        self.cond.notify_all()

    def handle_request_vote(self, req):
        with self.cond:
            st = self.state
            old_term, old_role = st.current_term, st.role
            granted = decide_vote(st, self.log, req.candidate_id, req.term,
                                  req.last_log_term, req.last_log_index, self.storage)
            if st.current_term != old_term:
                self.event(f"termo maior observado ({old_term} -> {st.current_term}) "
                           f"via RequestVote do no {req.candidate_id}")
            if old_role == Role.LEADER and st.role != Role.LEADER:
                self.event("lider antigo: virando SEGUIDOR")
                self._reset_election_timer()
                self.cond.notify_all()
            if granted:
                self._reset_election_timer()
                self.event(f"voto CONCEDIDO ao no {req.candidate_id} (termo {req.term})")
            else:
                self.event(f"voto NEGADO ao no {req.candidate_id} (termo {req.term}; "
                           f"meu termo {st.current_term}, voted_for={st.voted_for})")
            return pb.VoteResponse(term=st.current_term, vote_granted=granted)

    # ------------------------------------------------------------ commit
    def _apply_locked(self) -> None:
        applied = apply_committed(self.state, self.log, self.db)
        if applied:
            self.storage.save_db(self.db, self.state.last_applied)
            for e in applied:
                self.event(f"aplicado ao banco: indice {e.index} ({e.key}={e.value})")

    def _advance_commit_locked(self) -> None:
        if self.hold_file.exists():          # debug (Cenario 5): segura o commit
            return
        st = self.state
        new = compute_commit_index(self.log, list(self.match_index.values()),
                                   quorum_size(config.N_NODES), st.current_term, st.commit_index)
        if new > st.commit_index:
            st.commit_index = new
            self.event(f"commit avancou para {new}")
            self._apply_locked()
            self.cond.notify_all()           # acorda Writes pendentes

    # ------------------------------------------------------------ lado do LIDER
    def handle_fetch(self, req):
        with self.cond:
            st = self.state
            self._observe_term(req.term)
            if st.role != Role.LEADER:
                return pb.FetchResponse(term=st.current_term, success=False,
                                        leader_id=st.leader_id or 0,
                                        error_message=config.NOT_LEADER)
            rid = req.replica_id
            self.last_fetch_time[rid] = time.monotonic()      # heartbeat

            div = find_divergence(self.log, req.last_log_index, req.last_log_term)
            if div is not None:
                self.match_index[rid] = 0                      # nao conta para quorum
                return pb.FetchResponse(term=st.current_term, success=False, leader_id=self.id,
                                        commit_index=st.commit_index, diverged=True, diverge_index=div)

            self.match_index[rid] = req.last_log_index         # confirmacao de recebimento
            self._advance_commit_locked()
            entries = [] if self.pause_file.exists() else \
                self.log.entries_from(req.last_log_index + 1, config.FETCH_BATCH)
            return pb.FetchResponse(term=st.current_term, success=True, leader_id=self.id,
                                    commit_index=st.commit_index,
                                    entries=[_to_pb(e) for e in entries])

    def handle_write(self, req):
        with self.cond:
            st = self.state
            if st.role != Role.LEADER:
                lid = st.leader_id or 0
                return pb.WriteResponse(success=False, leader_id=lid,
                                        leader_address=config.address(lid, self.base_port) if lid else "",
                                        error_message=config.NOT_LEADER, term=st.current_term)
            # Sem quorum, sem escrita: recusa ANTES de gravar no log.
            active = self._active_replicas_locked()
            if not has_quorum(len(active), quorum_size(config.N_NODES)):
                msg = (f"sem quorum: apenas {1 + len(active)} de {config.N_NODES} nos ativos "
                       f"(minimo {quorum_size(config.N_NODES)})")
                self.event(f"escrita RECUSADA - {msg}")
                return pb.WriteResponse(success=False, leader_id=self.id,
                                        error_message=msg, term=st.current_term)
            term = st.current_term
            entry = LogEntry(term, self.log.last_index() + 1, req.key, req.value)
            self.log.append(entry)
            self.storage.append_entries([entry])
            self.event(f"gravacao recebida: indice {entry.index}, termo {term} "
                       f"({req.key}={req.value}) - aguardando commit")

            def fail(msg):
                return pb.WriteResponse(success=False, error_message=msg, term=term, index=entry.index)

            deadline = time.monotonic() + self.write_timeout
            while st.commit_index < entry.index:
                if st.role != Role.LEADER or st.current_term != term:
                    return fail("lideranca perdida antes do commit")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return fail("nao foi possivel comitar (timeout)")
                self.cond.wait(remaining)
            if self.log.term_at(entry.index) != term:
                return fail("entrada descartada")
            self.event(f"confirmando ao cliente: indice {entry.index} comitado")
            return pb.WriteResponse(success=True, leader_id=self.id, term=term, index=entry.index)

    # ------------------------------------------------------------ leitura
    def handle_read(self, req):
        with self.cond:   # le SOMENTE o banco final (dados comitados)
            v = self.db.get(req.key)
            return pb.ReadResponse(found=v is not None, value=v or "", node_id=self.id,
                                   role=self.state.role.value, term=self.state.current_term,
                                   commit_index=self.state.commit_index)

    def handle_status(self):
        with self.cond:
            s = self.state
            return pb.StatusResponse(node_id=self.id, role=s.role.value, term=s.current_term,
                                     commit_index=s.commit_index,
                                     last_log_index=self.log.last_index(),
                                     leader_id=s.leader_id or 0)

    # ------------------------------------------------------------ lado da REPLICA
    def _fetch_loop(self) -> None:
        while not self._stop.is_set():
            t0 = time.monotonic()
            try:
                self._fetch_once()
            except Exception as e:  # nunca deixar a thread morrer
                self.event(f"erro no fetch: {e!r}")
            self._stop.wait(max(0.0, config.FETCH_INTERVAL - (time.monotonic() - t0)))

    def _fetch_once(self) -> None:
        with self.cond:
            st = self.state
            if st.role == Role.LEADER:
                return
            target = st.leader_id
            known = bool(target)
            if not known:                    # lider desconhecido: procura rodando pelos pares
                peers = self.peers
                target = peers[self._rr % len(peers)]
                self._rr += 1
            req = pb.FetchRequest(replica_id=self.id, term=st.current_term,
                                  last_log_term=self.log.last_term(),
                                  last_log_index=self.log.last_index())
        try:   # RPC fora do lock
            resp = self._stub(target).Fetch(req, timeout=config.FETCH_RPC_TIMEOUT)
        except grpc.RpcError:
            with self.cond:
                if known and self.state.leader_id == target:
                    self.state.leader_id = None      # lider morreu: passa a procurar outro
                    if not self._leader_down:
                        self.event(f"lider (no {target}) inacessivel")
                        self._leader_down = True
            return
        self._leader_down = False
        with self.cond:
            self._handle_fetch_response(resp)

    def _handle_fetch_response(self, resp) -> None:
        st = self.state
        if st.role == Role.LEADER or resp.term < st.current_term:
            return                                   # resposta de lider velho: ignora
        self._observe_term(resp.term)
        if not resp.success and not resp.diverged:   # no que nao e lider: segue a dica
            hint = resp.leader_id
            st.leader_id = hint if hint and hint != self.id else None
            return

        # --- resposta de um LIDER valido (termo >= o meu) ---
        if st.role == Role.CANDIDATE:
            self.event(f"lider {resp.leader_id} encontrado no termo {resp.term}: "
                       f"CANDIDATO -> SEGUIDOR")
        elif st.leader_id != resp.leader_id:
            self.event(f"seguindo o lider {resp.leader_id} (termo {resp.term})")
        st.role = Role.FOLLOWER
        st.leader_id = resp.leader_id
        self._reset_election_timer()                 # Fetch bem-sucedido reinicia o timer

        if resp.diverged:
            try:
                removed = truncate_safely(self.log, resp.diverge_index, st.commit_index)
            except LogError as e:
                self.event(f"ERRO GRAVE: {e}")
                return
            self.storage.truncate_log_from(resp.diverge_index)
            self.event(f"divergencia: truncando log a partir do indice {resp.diverge_index} "
                       f"({len(removed)} entrada(s) removida(s))")
            return

        entries = [LogEntry(e.term, e.index, e.key, e.value) for e in resp.entries]
        if entries and entries[0].index == self.log.last_index() + 1:
            try:
                for e in entries:
                    self.log.append(e)
            except LogError as e:
                self.event(f"entrada invalida do lider: {e}")
                return
            self.storage.append_entries(entries)
            self.event(f"recebidas {len(entries)} entrada(s) (ate o indice {entries[-1].index}) - "
                       f"ainda nao comitadas")

        new = follower_commit_index(resp.commit_index, self.log.last_index(), st.commit_index)
        if new > st.commit_index:
            st.commit_index = new
            self._apply_locked()