# uniswap_v2_decoder.py
# --------------------------------------------------------------
# Decode Uni V2‑style Swap events (Aerodrome vAMM, Camelot V2, etc.)
# --------------------------------------------------------------
from decimal import Decimal
from app.celery.celery_app import celery_app
import logging

logger = logging.getLogger(__name__)


@celery_app.task(name="uniswap_v2_decode_log_chunk")
def decode_log_chunk(
    logs_chunk: list,
    block_cache: dict,
    abi: dict,
    dec0: int,
    dec1: int,
    base_is_token1: bool,
):
    """
    Decode V2 Swap events → per‑swap dicts for *raw_swaps*.

    ── No USD conversion here (done downstream).
    ── Signed flows (pool perspective) preserved so wallet
       perspective can be inferred later.
    """
    from web3 import Web3
    from web3._utils.events import get_event_data
    codec = Web3().codec

    # Decimal scaling helpers
    d0 = Decimal(10) ** dec0  # token0 divisor
    d1 = Decimal(10) ** dec1  # token1 divisor

    out: list[dict] = []
    for log in logs_chunk:
        evt  = get_event_data(codec, abi, log)
        args = evt["args"]
        bn   = log["blockNumber"]

        # ------------------------------------------------------------------
        # Extract raw unsigned amounts
        # ------------------------------------------------------------------
        amt0_in  = Decimal(args["amount0In"])
        amt1_in  = Decimal(args["amount1In"])
        amt0_out = Decimal(args["amount0Out"])
        amt1_out = Decimal(args["amount1Out"])

        # ------------------------------------------------------------------
        # Map to base / quote, scale by decimals, compute signed deltas
        # ------------------------------------------------------------------
        if base_is_token1:                       # token1 = base
            base_in   = amt1_in / d1
            base_out  = amt1_out / d1
            quote_in  = amt0_in / d0             # token0 = quote
            quote_out = amt0_out / d0
        else:                                    # token0 = base
            base_in   = amt0_in / d0
            base_out  = amt0_out / d0
            quote_in  = amt1_in / d1             # token1 = quote
            quote_out = amt1_out / d1

        base_delta_raw  = base_in  - base_out    # pool perspective
        quote_delta_raw = quote_in - quote_out

        # Wallet bought base if it *spent* quote (negative quote_delta_raw)
        is_buy = quote_delta_raw < 0

        # Execution price (quote per base, using abs to ignore sign)
        price = abs(quote_in + quote_out) / abs(base_in + base_out)

        out.append({
            # ─── identity & metadata ─────────────────────────────────────
            "block_number": bn,
            "timestamp": block_cache[str(bn)],
            "tx_hash": log["transactionHash"],
            "log_index": log["logIndex"],
            "sender": args["sender"],
            "recipient": args["to"],
            # ─── numeric flows ───────────────────────────────────────────
            "base_delta":  base_delta_raw,
            "quote_delta": quote_delta_raw,
            "base_vol":    abs(base_delta_raw),
            "quote_vol":   abs(quote_delta_raw),
            # ─── price / pool context ───────────────────────────────────
            "price":       price,
            "liquidity":   None,   # not available in V2 event
            "tick":        None,   # not applicable
            # ─── convenience flag ───────────────────────────────────────
            "is_buy":      is_buy,
        })

    logger.debug("decoded %d V2 swaps", len(out))
    return out
