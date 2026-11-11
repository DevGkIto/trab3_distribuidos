"""Parametros do cluster. Ajuste aqui os tempos para a demonstracao."""

N_NODES = 4
HOST = "127.0.0.1"
DEFAULT_BASE_PORT = 50050        

FETCH_INTERVAL = 0.1               
FETCH_RPC_TIMEOUT = 0.5            
HEARTBEAT_TIMEOUT = 0.5            
WRITE_TIMEOUT = 3.0                
FETCH_BATCH = 50                   

ELECTION_TIMEOUT_MIN = 1.5         
ELECTION_TIMEOUT_MAX = 3.0
VOTE_RPC_TIMEOUT = 0.5             
PROBE_INTERVAL = 0.5               
PROBE_RPC_TIMEOUT = 0.3

NOT_LEADER = "este no nao e o lider"

CHANNEL_OPTIONS = [
    ("grpc.enable_http_proxy", 0),
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 1000),
]


def address(node_id: int, base_port: int = DEFAULT_BASE_PORT) -> str:
    return f"{HOST}:{base_port + node_id}"