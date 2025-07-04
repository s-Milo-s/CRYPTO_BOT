from app.sources.dex_data_pipeline.evm.utils.client import get_web3_client
from app.sources.dex_data_pipeline.config.settings import ARBITRUM_RPC_URL
import math
from typing import List
from app.celery.celery_app import celery_app
from decimal import Decimal

w3 = get_web3_client(ARBITRUM_RPC_URL)

def _price_raw(sqrtPriceX96: int) -> Decimal:
        sqrt_price = Decimal(sqrtPriceX96) / (1 << 96)
        return sqrt_price * sqrt_price  # token1 per token0


@celery_app.task(name="decode_log_chunk")
def decode_log_chunk(logs_chunk, block_cache, abi, dec0: int, dec1: int):
    """
    Decode a chunk of logs.  Runs in its own Celery worker process,
    giving real parallelism without ProcessPoolExecutor.
    """
    # lightweight ABI tools only
    from web3 import Web3
    codec = Web3().codec
    from web3._utils.events import get_event_data
    from decimal import Decimal

    exponent = dec0 - dec1
    price_scale = Decimal(10) ** exponent
    dec0 = Decimal(10) ** dec0
    dec1 = Decimal(10) ** dec1

    out = []
    for log in logs_chunk:
        evt = get_event_data(codec, abi, log)
        bn  = log["blockNumber"]
        price = _price_raw(evt["args"]["sqrtPriceX96"]) * price_scale
        base_vol = abs(Decimal(evt["args"]["amount0"])) / dec0  # token0
        quote_vol  = abs(Decimal(evt["args"]["amount1"])) / dec1  # token1
        out.append({
            "block_number": bn,
            "timestamp":    block_cache[str(bn)],
            "tx_hash":      log["transactionHash"],
            "log_index":    log["logIndex"],
            "sender":       evt["args"]["sender"],
            "recipient":    evt["args"]["recipient"],
            "base_vol":     base_vol,
            "quote_vol":    quote_vol,
            "price":        price,
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
