"""Sobe um no.
  python run_node.py --id 1                 # todos os nos iguais: a eleicao escolhe o lider
  python run_node.py --id 1 --role leader   # (opcional) lider fixo, so para testes antigos
"""
import argparse
import time
from concurrent import futures

import grpc

from raftlite import config
from raftlite.node import Node
from raftlite.pb import pbg
from raftlite.server import ClientServicer, ClusterServicer


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--id", type=int, required=True)
    p.add_argument("--role", choices=["leader", "follower"], default="follower")
    p.add_argument("--leader-id", type=int, default=1,
                   help="dica inicial de quem e o lider (0 = desconhecido)")
    p.add_argument("--base-port", type=int, default=config.DEFAULT_BASE_PORT)
    p.add_argument("--data-root", default="data")
    p.add_argument("--write-timeout", type=float, default=config.WRITE_TIMEOUT)
    p.add_argument("--heartbeat-timeout", type=float, default=config.HEARTBEAT_TIMEOUT)
    p.add_argument("--election-min", type=float, default=config.ELECTION_TIMEOUT_MIN)
    p.add_argument("--election-max", type=float, default=config.ELECTION_TIMEOUT_MAX)
    p.add_argument("--status-interval", type=float, default=1.0, help="0 desliga")
    a = p.parse_args()

    node = Node(a.id, a.role, f"{a.data_root}/node{a.id}", a.base_port, a.leader_id,
                a.write_timeout, a.status_interval,
                heartbeat_timeout=a.heartbeat_timeout,
                election_min=a.election_min, election_max=a.election_max)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pbg.add_ClusterServiceServicer_to_server(ClusterServicer(node), server)
    pbg.add_ClientServiceServicer_to_server(ClientServicer(node), server)
    server.add_insecure_port(f"0.0.0.0:{a.base_port + a.id}")
    server.start()
    node.start()
    node.event(f"iniciado como {a.role.upper()} na porta {a.base_port + a.id}")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        node.stop()
        server.stop(0)


if __name__ == "__main__":
    main()