# app/utils/sanitize.py
from web3.datastructures import AttributeDict
from hexbytes import HexBytes
from typing import List
import math

def sanitize_log(log):
    """Convert Web3 log to JSON-safe dict."""
    out = {}
    for k, v in dict(log).items():
        if isinstance(v, (bytes, bytearray, HexBytes)):
            out[k] = v.hex()
        elif isinstance(v, AttributeDict):
            out[k] = dict(v)
        else:
            out[k] = v
    return out


def chunk_logs(logs: List[dict], n_chunks: int) -> List[List[dict]]:
    """Split `logs` into ≈even chunks."""
    if n_chunks <= 1 or len(logs) <= n_chunks:
        return [logs]
    size = math.ceil(len(logs) / n_chunks)
    return [logs[i : i + size] for i in range(0, len(logs), size)]