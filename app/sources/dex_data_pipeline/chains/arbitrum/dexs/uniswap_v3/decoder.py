from app.sources.dex_data_pipeline.chains.arbitrum.client import get_client
import math
from typing import List
from app.celery.celery_app import celery_app

w3 = get_client()

@celery_app.task(name="decode_log_chunk")
def decode_log_chunk(logs_chunk, block_cache, abi):
    """
    Decode a chunk of logs.  Runs in its own Celery worker process,
    giving real parallelism without ProcessPoolExecutor.
    """
    # lightweight ABI tools only
    from web3 import Web3
    codec = Web3().codec
    from web3._utils.events import get_event_data

    out = []
    for log in logs_chunk:
        evt = get_event_data(codec, abi, log)
        bn  = log["blockNumber"]
        out.append({
            "block_number": bn,
            "timestamp":    block_cache[str(bn)],
            "tx_hash":      log["transactionHash"],
            "log_index":    log["logIndex"],
            "sender":       evt["args"]["sender"],
            "recipient":    evt["args"]["recipient"],
            "amount0":      evt["args"]["amount0"],
            "amount1":      evt["args"]["amount1"],
            "sqrtPriceX96": evt["args"]["sqrtPriceX96"],
            "liquidity":    evt["args"]["liquidity"],
            "tick":         evt["args"]["tick"],
        })
    return out


def chunk_logs(logs: List[dict], n_chunks: int) -> List[List[dict]]:
    """Split `logs` into ≈even chunks."""
    if n_chunks <= 1 or len(logs) <= n_chunks:
        return [logs]
    size = math.ceil(len(logs) / n_chunks)
    return [logs[i : i + size] for i in range(0, len(logs), size)]
