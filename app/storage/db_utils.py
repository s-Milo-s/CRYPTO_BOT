from sqlalchemy import inspect
from sqlalchemy import text
from app.storage.db import get_db
import logging
log = logging.getLogger(__name__)

DEFAULT_CHAIN = "arbitrum"
DEFAULT_DEX   = "uniswap_v3"
def table_exists(session, symbol: str, interval: str) -> bool:
    """Check if a kline table for the given symbol exists in the database.
    Converts symbol to lowercase and inspects current schema."""
    ...
    tablename = f"{symbol.replace('/', '').lower()}_{interval}_klines"
    inspector = inspect(session.bind)
    return tablename in inspector.get_table_names()



def table_exists_agg(chain, dex, symbol0: str, symbol1: str,interval) -> bool:
    table_name = f"{chain}_{dex}_{symbol0.lower()}{symbol1.lower()}_{interval}_klines"
    log.info(f"Checking if table {table_name} exists")
    db = next(get_db())
    inspector = inspect(db.bind)
    return table_name if table_name in inspector.get_table_names() else None

def resolve_table_name(
    chain: str,
    dex: str,
    token0: str,
    token1: str,
    pair: str,
    interval: str = "1m"
) -> tuple[str | None, str | None]:
    """
    Resolve table name based on user-defined pair (e.g., 'ARB/USDC'),
    even if token0/token1 are in reverse order.

    Returns
    -------
    (kl_table_name, wallet_stats_table_name) or (None, None)
    """
    desired_base, _ = pair.upper().split("/")

    name1 = table_exists_agg(chain, dex, token0, token1, interval)
    name2 = table_exists_agg(chain, dex, token1, token0, interval)

    if name1 and token0.upper() == desired_base:
        base_token = token0
        quote_token = token1
        return (
            name1,
            f"{chain}_{dex}_{base_token.lower()}{quote_token.lower()}_raw_swaps"
        )
    elif name2 and token1.upper() == desired_base:
        base_token = token1
        quote_token = token0
        return (
            name2,
            f"{chain}_{dex}_{base_token.lower()}{quote_token.lower()}_raw_swaps"
        )
    else:
        log.error(f"Table for {pair} not found with tokens {token0}, {token1}")
        return None, None
    
def create_table_if_not_exists(session, chain, dex, token1, token0, base_is_token1):
    base_token = token1 if base_is_token1 else token0
    quote_token = token0 if base_is_token1 else token1

    kl_table_name = f"{chain}_{dex}_{base_token.lower()}{quote_token.lower()}_1m_klines"
    swap_table_name = f"{chain}_{dex}_{base_token.lower()}{quote_token.lower()}_raw_swaps"

    create_klines_stmt = f"""
    CREATE TABLE IF NOT EXISTS {kl_table_name} (
        minute_start TIMESTAMPTZ PRIMARY KEY,
        open_price NUMERIC,
        open_ts TIMESTAMPTZ,
        close_price NUMERIC,
        close_ts TIMESTAMPTZ,
        high_price NUMERIC,
        low_price NUMERIC,
        avg_price NUMERIC,
        swap_count INTEGER,
        total_base_volume NUMERIC,
        total_quote_volume NUMERIC,
        trade_imbalance NUMERIC,
        price_volatility NUMERIC,
        price_momentum NUMERIC
    );
    """
    create_swaps_stmt = f"""
    CREATE TABLE IF NOT EXISTS {swap_table_name} (
        id              BIGSERIAL PRIMARY KEY,              -- surrogate key

        -- on-chain identity (uniqueness enforced below)
        block_number    INTEGER      NOT NULL,
        "timestamp"     TIMESTAMPTZ  NOT NULL,
        tx_hash         TEXT         NOT NULL,
        log_index       INTEGER      NOT NULL,

        -- wallet addresses
        sender          TEXT         NOT NULL,
        recipient       TEXT         NOT NULL,

        -- enrichment (who really called the tx & how we tagged it)
        caller          TEXT,                           
        router_tag      TEXT,                         
        
        -- signed deltas (pool perspective)
        base_delta      NUMERIC(38, 18) NOT NULL,
        quote_delta     NUMERIC(38, 18) NOT NULL,

        -- absolute volumes (fast volume queries)
        base_vol        NUMERIC(38, 18) NOT NULL,
        quote_vol       NUMERIC(38, 18) NOT NULL,

        -- pool context
        price           NUMERIC(38, 18) NOT NULL,
        liquidity       NUMERIC(38, 0),
        tick            INTEGER,

        -- convenience flag
        is_buy          BOOLEAN      NOT NULL,

        -- make (block, tx, log_index) globally unique for dedup
        UNIQUE (block_number, tx_hash, log_index)
    );
    """

    session.execute(text(create_swaps_stmt))
    session.execute(text(create_klines_stmt))
    session.commit()
    return kl_table_name,swap_table_name