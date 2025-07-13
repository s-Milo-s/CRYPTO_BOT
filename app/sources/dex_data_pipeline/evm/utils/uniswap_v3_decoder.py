from decimal import Decimal
from app.celery.celery_app import celery_app
import logging

logger = logging.getLogger(__name__)


def _price_raw(sqrt_price_x96: int) -> Decimal:
    """Convert Uniswap V3 sqrtPriceX96 → token1 per token0 as Decimal."""
    sqrt_price = Decimal(sqrt_price_x96) / (1 << 96)
    return sqrt_price * sqrt_price  # token1 per token0


@celery_app.task(name="uniswap_decode_log_chunk")
def decode_log_chunk(
    logs_chunk,
    block_cache,
    abi,
    dec0: int,
    dec1: int,
    base_is_token1: bool,
):
    """Decode raw Swap events into per‑swap dicts suitable for the *raw_swaps* table.

    ‑‑ No USD‑specific fields are produced.  
    ‑‑ Signed flows are kept so later analytics can tell buys from sells.
    """
    from web3 import Web3
    from web3._utils.events import get_event_data

    codec = Web3().codec

    # Decimal scaling helpers
    exponent = dec0 - dec1
    price_scale = Decimal(10) ** exponent
    d0 = Decimal(10) ** dec0
    d1 = Decimal(10) ** dec1

    out: list[dict] = []
    for log in logs_chunk:
        evt = get_event_data(codec, abi, log)
        args = evt["args"]

        bn = log["blockNumber"]
        price = _price_raw(args["sqrtPriceX96"]) * price_scale

        # ------------------------------------------------------------------
        # Signed token flows (pool perspective → opposite sign of wallet)
        # ------------------------------------------------------------------
        if base_is_token1:
            base_delta_raw = -Decimal(args["amount1"]) / d1  # token1 is base
            quote_delta_raw = -Decimal(args["amount0"]) / d0  # token0 is quote
        else:
            base_delta_raw = -Decimal(args["amount0"]) / d0  # token0 is base
            quote_delta_raw = -Decimal(args["amount1"]) / d1  # token1 is quote

        # Wallet bought base if it *spent* quote (negative quote_delta)
        is_buy = quote_delta_raw < 0

        out.append({
            # ─── identity & metadata ───────────────────────────────────────
            "block_number": bn,
            "timestamp": block_cache[str(bn)],
            "tx_hash": log["transactionHash"],
            "log_index": log["logIndex"],
            "sender": args["sender"],
            "recipient": args["recipient"],
            # ─── numeric flows (abs + signed) ──────────────────────────────
            "base_delta": base_delta_raw,            # signed
            "quote_delta": quote_delta_raw,          # signed
            "base_vol": abs(base_delta_raw),        # unsigned (for volume)
            "quote_vol": abs(quote_delta_raw),
            # ─── price / pool context ─────────────────────────────────────
            "price": price,
            "liquidity": args.get("liquidity"),
            "tick": args.get("tick"),
            # ─── convenience flag ─────────────────────────────────────────
            "is_buy": is_buy,
        })

    logger.debug("decoded %d swaps", len(out))
    return out
