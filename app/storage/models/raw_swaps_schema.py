from sqlalchemy import (
    Column, Integer, Numeric, Text, Boolean,
    TIMESTAMP as TIMESTAMPTZ, Index
)
from app.storage.base import Base  # your declarative_base
from typing import Dict

# ---------------------------------------------------------------------------
# Dynamic model factory & cache
# ---------------------------------------------------------------------------
_raw_swap_model_cache: Dict[str, type] = {}


def get_raw_swaps_model_by_name(table_name: str):
    """Return & cache a SQLAlchemy model for a pool-specific *raw_swaps* table.

    Columns match the decoder output:
      • block / tx / log identity
      • wallet addresses
      • **signed** base & quote deltas  (base_delta, quote_delta)
      • absolute volumes (base_vol, quote_vol)
      • pool-context (price, liquidity, tick)
      • convenience flag `is_buy`
    """
    if table_name in _raw_swap_model_cache:
        return _raw_swap_model_cache[table_name]

    class_attrs = {
        "__tablename__": table_name,
        # surrogate PK for easy partition / housekeeping
        "id": Column(Integer, primary_key=True, autoincrement=True),
        # ─── on-chain identity ────────────────────────────────────────────
        "block_number": Column(Integer, nullable=False),
        "timestamp": Column(TIMESTAMPTZ, nullable=False),
        "tx_hash": Column(Text, nullable=False),
        "log_index": Column(Integer, nullable=False),
        # ─── wallet addresses ────────────────────────────────────────────
        "sender": Column(Text, nullable=False),
        "recipient": Column(Text, nullable=False),
        #─── who really called the tx & how we tagged it --------------
        "caller":     Column(Text),             # NULL if trace failed / dropped
        "router_tag": Column(Text),             # e.g. 'EOA', 'uniswap_router'
        # ─── signed token deltas (pool perspective) ──────────────────────
        "base_delta": Column(Numeric(38, 18), nullable=False),   # ± base token
        "quote_delta": Column(Numeric(38, 18), nullable=False),  # ± quote token
        # ─── unsigned volumes for quick aggregations ────────────────────
        "base_vol": Column(Numeric(38, 18), nullable=False),   # |base_delta|
        "quote_vol": Column(Numeric(38, 18), nullable=False),  # |quote_delta|
        # ─── pool context ────────────────────────────────────────────────
        "price": Column(Numeric(38, 18), nullable=False),  # token1 per token0
        "liquidity": Column(Numeric(38, 0)),
        "tick": Column(Integer),
        # ─── convenience flag ─────────────────────────────────────────────
        "is_buy": Column(Boolean, nullable=False),  # True → wallet bought base
        "__mapper_args__": {"eager_defaults": True},
    }

    cls_name = f"{table_name.title().replace('_', '')}RawSwaps"
    model_cls = type(cls_name, (Base,), class_attrs)

    # UNIQUE constraint to deduplicate (block, tx_hash, log_index)
    Index(
        f"uq_{table_name}_block_tx_log",
        model_cls.block_number,
        model_cls.tx_hash,
        model_cls.log_index,
        unique=True,
    )

    _raw_swap_model_cache[table_name] = model_cls
    return model_cls