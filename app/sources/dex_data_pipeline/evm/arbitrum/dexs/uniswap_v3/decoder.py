from app.sources.dex_data_pipeline.evm.utils.client import get_web3_client
from app.sources.dex_data_pipeline.config.settings import ARBITRUM_RPC_URL
from app.celery.celery_app import celery_app
from decimal import Decimal

w3 = get_web3_client(ARBITRUM_RPC_URL)

def _price_raw(sqrtPriceX96: int) -> Decimal:
        sqrt_price = Decimal(sqrtPriceX96) / (1 << 96)
        return sqrt_price * sqrt_price  # token1 per token0


@celery_app.task(name="uniswap_decode_log_chunk")
def decode_log_chunk(logs_chunk, block_cache, abi, dec0: int, dec1: int, base_is_token1):
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
        args = evt["args"]
        bn  = log["blockNumber"]
        price = _price_raw(evt["args"]["sqrtPriceX96"]) * price_scale
        if base_is_token1:
            # token1 is base, token0 is quote → flip volumes
            base_vol = abs(Decimal(args["amount1"])) / dec1
            quote_vol = abs(Decimal(args["amount0"])) / dec0
        else:
            base_vol = abs(Decimal(args["amount0"])) / dec0
            quote_vol = abs(Decimal(args["amount1"])) / dec1

        out.append({
            "block_number": bn,
            "timestamp":    block_cache[str(bn)],
            "tx_hash":      log["transactionHash"],
            "log_index":    log["logIndex"],
            "sender":       args["sender"],
            "recipient":    args["recipient"],
            "base_vol":     base_vol,
            "quote_vol":    quote_vol,
            "price":        price,
            "liquidity":    args["liquidity"],
            "tick":         args["tick"],
        })
    return out

