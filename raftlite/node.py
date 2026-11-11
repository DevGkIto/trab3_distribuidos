"""No do cluster. Fase 2: lider fixo (--role leader), sem eleicao."""
from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Dict, Optional

import grpc

from . import config
from .core import (Log, LogEntry, LogError, NodeState, Role, active_replicas,
                   apply_committed, compute_commit_index, has_quorum,
                   find_divergence, follower_commit_index,
                   quorum_size, step_down_if_newer, truncate_safely)
from .pb import pb, pbg
from .storage import Storage


def _to_pb(e: LogEntry):
    return pb.LogEntry(term=e.term, index=e.index, key=e.key, value=e.value)


class Node:
    def __init__(self, node_id: int, role: str, data_dir, base_port: int = config.DEFAULT_BASE_PORT,
                 leader_id: int = 1, write_timeout: float = config.WRITE_TIMEOUT,
                 status_interval: float = 1.0, heartbeat_timeout: float = config.HEARTBEAT_TIMEOUT):
        self.id = node_id
        self.base_port = base_port
        self.write_timeout = write_timeout
        self.status_interval = status_interval
        self.heartbeat_timeout = heartbeat_timeout
        self._active_prev = set()

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

        # Um unico mutex protege todo o estado. NUNCA chamar RPC com ele travado.
        self.cond = threading.Condition()
        self._stop = threading.Event()
        self._stubs: Dict[int, object] = {}
        self._leader_down = False

        if role == "leader":
            if self.state.current_term < 1:
                self.state.current_term = 1
                self.storage.save_meta(1, self.state.voted_for)
            self.state.role = Role.LEADER
            self.state.leader_id = node_id
        else:
            self.state.role = Role.FOLLOWER
            self.state.leader_id = leader_id

    # ------------------------------------------------------------ util
    def event(self, msg: str) -> None:
        print(f"[{time.strftime('%H:%M:%S')}][No {self.id}] {msg}", flush=True)

    def start(self) -> None:
        threading.Thread(target=self._fetch_loop, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        if self.status_interval > 0:
            threading.Thread(target=self._status_loop, daemon=True).start()

    def stop(self) -> None:
        self._stop.set()

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
            step_down_if_newer(st, req.term, self.storage)
            if st.role != Role.LEADER:
                return pb.FetchResponse(term=st.current_term, success=False,
                                        leader_id=st.leader_id or 0,
                                        error_message="este no nao e o lider")
            rid = req.replica_id
            self.last_fetch_time[rid] = time.monotonic()      # heartbeat (usado na Fase 3)

            div = find_divergence(self.log, req.last_log_index, req.last_log_term)
            if div is not None:
                self.match_index[rid] = 0                      # nao conta para quorum
                return pb.FetchResponse(term=st.current_term, success=False, leader_id=self.id,
                                        commit_index=st.commit_index, diverged=True, diverge_index=div)

            self.match_index[rid] = req.last_log_index         # confirmacao de recebimento
            self._advance_commit_locked()
            entries = self.log.entries_from(req.last_log_index + 1, config.FETCH_BATCH)
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
                                        error_message="este no nao e o lider", term=st.current_term)
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
    def _stub(self, leader_id: int):
        if leader_id not in self._stubs:
            ch = grpc.insecure_channel(config.address(leader_id, self.base_port),
                                       options=config.CHANNEL_OPTIONS)
            self._stubs[leader_id] = pbg.ClusterServiceStub(ch)
        return self._stubs[leader_id]

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
            if st.role == Role.LEADER or not st.leader_id:
                return
            leader_id = st.leader_id
            req = pb.FetchRequest(replica_id=self.id, term=st.current_term,
                                  last_log_term=self.log.last_term(),
                                  last_log_index=self.log.last_index())
        try:   # RPC fora do lock
            resp = self._stub(leader_id).Fetch(req, timeout=config.FETCH_RPC_TIMEOUT)
        except grpc.RpcError:
            if not self._leader_down:
                self.event(f"lider (no {leader_id}) inacessivel")
                self._leader_down = True
            return
        if self._leader_down:
            self.event(f"lider (no {leader_id}) voltou a responder")
            self._leader_down = False
        with self.cond:
            self._handle_fetch_response(resp)

    def _handle_fetch_response(self, resp) -> None:
        st = self.state
        if st.role == Role.LEADER or resp.term < st.current_term:
            return
        step_down_if_newer(st, resp.term, self.storage)
        if resp.leader_id:
            st.leader_id = resp.leader_id
        if not resp.success and not resp.diverged:
            return

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