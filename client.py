"""Cliente CLI.
  python client.py write x 10 [--node 3]
  python client.py read x --node 2
  python client.py read-all x
  python client.py status
"""
import argparse
import sys
import time

import grpc

from raftlite import config
from raftlite.pb import pb, pbg


def _try_once(key, value, start_node, base_port, max_redirects, timeout):
    """Uma rodada: segue redirecionamentos e pula nos fora do ar.
    Retorna (resposta|None, trilha, vale_tentar_de_novo)."""
    trail, tried, redirects, node = [], set(), 0, start_node
    while True:
        tried.add(node)
        try:
            with grpc.insecure_channel(config.address(node, base_port),
                                       options=config.CHANNEL_OPTIONS) as ch:
                resp = pbg.ClientServiceStub(ch).Write(
                    pb.WriteRequest(key=key, value=value), timeout=timeout)
        except grpc.RpcError:
            trail.append(f"no {node} indisponivel")
            rest = [n for n in range(1, config.N_NODES + 1) if n not in tried]
            if not rest:
                return None, trail, True
            node = rest[0]
            continue
        if resp.success:
            trail.append(f"no {node}: OK (indice {resp.index}, termo {resp.term})")
            return resp, trail, False
        if resp.error_message == config.NOT_LEADER:
            if resp.leader_id and resp.leader_id not in tried and redirects < max_redirects:
                trail.append(f"no {node} nao e lider -> redirecionando para o no {resp.leader_id}")
                redirects += 1
                node = resp.leader_id
                continue
            rest = [n for n in range(1, config.N_NODES + 1) if n not in tried]
            if rest:
                trail.append(f"no {node} nao e lider e nao sabe quem e")
                node = rest[0]
                continue
            return resp, trail, True            # ninguem sabe o lider (eleicao em curso?)
        trail.append(f"no {node}: {resp.error_message}")   # recusa definitiva (quorum, timeout)
        return resp, trail, False


def write_with_redirect(key, value, start_node=1, base_port=config.DEFAULT_BASE_PORT,
                        max_redirects=3, timeout=config.WRITE_TIMEOUT + 2, retry_for=0.0):
    """Retorna (resposta|None, trilha). Se nao houver lider/no acessivel, repete por
    `retry_for` segundos (util durante uma eleicao). Recusas definitivas nao repetem."""
    end = time.monotonic() + retry_for
    trail_all = []
    while True:
        resp, trail, retry = _try_once(key, value, start_node, base_port, max_redirects, timeout)
        trail_all += trail
        if not retry or time.monotonic() >= end:
            return resp, trail_all
        trail_all.append("sem lider no momento; tentando novamente...")
        time.sleep(0.3)


def read_node(node_id, key, base_port=config.DEFAULT_BASE_PORT, timeout=1.0):
    try:
        with grpc.insecure_channel(config.address(node_id, base_port), options=config.CHANNEL_OPTIONS) as ch:
            return pbg.ClientServiceStub(ch).Read(pb.ReadRequest(key=key), timeout=timeout)
    except grpc.RpcError:
        return None


def status_node(node_id, base_port=config.DEFAULT_BASE_PORT, timeout=1.0):
    try:
        with grpc.insecure_channel(config.address(node_id, base_port), options=config.CHANNEL_OPTIONS) as ch:
            return pbg.ClusterServiceStub(ch).GetStatus(pb.StatusRequest(), timeout=timeout)
    except grpc.RpcError:
        return None

def write_once(node_id, key, value, base_port=config.DEFAULT_BASE_PORT, timeout=3.0):
    """UMA tentativa em UM no, sem redirecionar nem tentar outros (debug do Cenario 4B).
    Retorna (resposta|None, codigo_de_erro|None)."""
    try:
        with grpc.insecure_channel(config.address(node_id, base_port),
                                   options=config.CHANNEL_OPTIONS) as ch:
            return pbg.ClientServiceStub(ch).Write(
                pb.WriteRequest(key=key, value=value), timeout=timeout), None
    except grpc.RpcError as e:
        return None, e.code().name


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base-port", type=int, default=config.DEFAULT_BASE_PORT)
    sub = p.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("write"); w.add_argument("key"); w.add_argument("value")
    w.add_argument("--node", type=int, default=1)
    w.add_argument("--retry-for", type=float, default=5.0,
                   help="segundos tentando de novo se nao houver lider (padrao 5)")
    w.add_argument("--once", action="store_true",
                   help="uma unica tentativa no --node, sem redirecionar (Cenario 4B)")
    w.add_argument("--timeout", type=float, default=3.0, help="timeout do --once (s)")
    r = sub.add_parser("read"); r.add_argument("key"); r.add_argument("--node", type=int, required=True)
    ra = sub.add_parser("read-all"); ra.add_argument("key")
    sub.add_parser("status")
    a = p.parse_args()

    if a.cmd == "write" and a.once:
        resp, err = write_once(a.node, a.key, a.value, a.base_port, a.timeout)
        if resp is None:
            print(f"no {a.node}: sem resposta ({err}) - a escrita PODE ter sido gravada no log do no")
        else:
            print(f"no {a.node}: {'OK' if resp.success else resp.error_message}")
        sys.exit(0 if resp is not None and resp.success else 1)
    elif a.cmd == "write":
        resp, trail = write_with_redirect(a.key, a.value, a.node, a.base_port,
                                          retry_for=a.retry_for)
        print("\n".join(trail))
        sys.exit(0 if resp is not None and resp.success else 1)
    elif a.cmd == "read":
        r = read_node(a.node, a.key, a.base_port)
        if r is None:
            print(f"no {a.node}: OFFLINE")
        else:
            print(f"no {r.node_id} ({r.role}, commit={r.commit_index}): "
                  f"{r.value if r.found else '<nao encontrado>'}")
    elif a.cmd == "read-all":
        print(f"{'No':<4}{'Papel':<11}{'Commit':<8}Valor")
        for n in range(1, config.N_NODES + 1):
            r = read_node(n, a.key, a.base_port)
            if r is None:
                print(f"{n:<4}{'OFFLINE':<11}")
            else:
                print(f"{n:<4}{r.role:<11}{r.commit_index:<8}{r.value if r.found else '<nao encontrado>'}")
    elif a.cmd == "status":
        print(f"{'No':<4}{'Papel':<11}{'Termo':<7}{'Commit':<8}Ultimo indice")
        for n in range(1, config.N_NODES + 1):
            s = status_node(n, a.base_port)
            print(f"{n:<4}{'OFFLINE':<11}" if s is None else
                  f"{n:<4}{s.role:<11}{s.term:<7}{s.commit_index:<8}{s.last_log_index}")


if __name__ == "__main__":
    main()