from web3 import Web3, HTTPProvider
import backoff
import logging
from typing import Dict

logger = logging.getLogger(__name__)

# Cache of Web3 clients per RPC URL
_web3_clients: Dict[str, Web3] = {}

@backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=None)
def _create_web3_client(rpc_url: str) -> Web3:
    logger.info(f"Connecting to RPC: {rpc_url}")
    w3 = Web3(HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))

    if not w3.is_connected():
        raise ConnectionError(f"Failed to connect to RPC: {rpc_url}")

    logger.info(f"Connected to {rpc_url} ✅")
    return w3

def get_web3_client(rpc_url: str) -> Web3:
    """Returns a cached or newly created Web3 client for a given RPC URL."""
    if rpc_url not in _web3_clients:
        _web3_clients[rpc_url] = _create_web3_client(rpc_url)
    return _web3_clients[rpc_url]