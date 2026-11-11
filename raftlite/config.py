"""Parametros do cluster"""

N_NODES = 4
HOST = "127.0.0.1"
DEFAULT_BASE_PORT = 50050         

FETCH_INTERVAL = 0.1             
FETCH_RPC_TIMEOUT = 0.5           
HEARTBEAT_TIMEOUT = 0.5           
WRITE_TIMEOUT = 3.0              
FETCH_BATCH = 50  

CHANNEL_OPTIONS = [("grpc.enable_http_proxy", 0)]

def address(node_id: int, base_port: int = DEFAULT_BASE_PORT) -> str:
    return f"{HOST}:{base_port + node_id}"