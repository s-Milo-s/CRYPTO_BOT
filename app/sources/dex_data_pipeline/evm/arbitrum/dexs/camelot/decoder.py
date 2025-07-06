from app.sources.dex_data_pipeline.evm.utils.client import get_web3_client
from app.sources.dex_data_pipeline.config.settings import ARBITRUM_RPC_URL
from app.celery.celery_app import celery_app
from decimal import Decimal

# Web3 client for direct Arbitrum RPC access
w3 = get_web3_client(ARBITRUM_RPC_URL)


@celery_app.task(name="camelot_v3_decode_log_chunk")
def decode_log_chunk(logs_chunk, block_cache, abi, dec0: int, dec1: int, base_is_token1: bool):
    """Decode a chunk of Camelot V3 (Uniswap‑V3‑style) **Swap** logs.

    Camelot V3 leverages Algebra (v1.9), whose events mirror Uniswap V3.  
    Swap event signature:
        event Swap(
            address indexed sender,
            address indexed recipient,
            int256  amount0,
            int256  amount1,
            uint160 sqrtPriceX96,
            uint128 liquidity,
            int24  tick
        );

    Notes
    -----
    * ``amount0`` / ``amount1`` are **signed**.  A negative value means the token
      *left* the pool (user received it).  Positive means *entered* the pool.
    * We take ``abs()`` to express traded volume.
    * ``base_is_token1`` tells us which token is the price *denominator* (base).
    """
    from web3 import Web3
    from web3._utils.events import get_event_data

    codec = Web3().codec

    # Pre‑compute decimal scaling factors
    dec0 = Decimal(10) ** dec0
    dec1 = Decimal(10) ** dec1

    decoded = []

    for log in logs_chunk:
        evt = get_event_data(codec, abi, log)
        args = evt["args"]
        bn = log["blockNumber"]
        ts = block_cache[str(bn)]

        # Raw signed deltas
        amount0 = Decimal(args["amount0"])
        amount1 = Decimal(args["amount1"])

        # Absolute trade volumes per token
        token0_vol = abs(amount0) / dec0
        token1_vol = abs(amount1) / dec1

        if token0_vol == 0 or token1_vol == 0:
            # Ignore dust / zero‑volume swaps
            continue

        # Map to base / quote
        if base_is_token1:
            base_vol = token1_vol
            quote_vol = token0_vol
        else:
            base_vol = token0_vol
            quote_vol = token1_vol

        price = quote_vol / base_vol

        decoded.append({
            "block_number": bn,
            "timestamp": ts,
            "tx_hash": log["transactionHash"],
            "log_index": log["logIndex"],
            "sender": args["sender"],
            "recipient": args["recipient"],
            "base_vol": base_vol,
            "quote_vol": quote_vol,
            "price": price,
            "liquidity": args["liquidity"],  # pool liquidity after swap
            "tick": args["tick"],
            "sqrt_price_x96": args["sqrtPriceX96"],
        })

    return decoded
