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

def resolve_table_name(chain: str, dex: str, token0: str, token1: str, pair: str, interval: str = "1m") -> str | None:
    """
    Resolve table name based on user-defined pair (e.g., 'ARB/USDC'), 
    even if token0/token1 are in reverse order.
    """
    desired_base, _ = pair.upper().split("/")
    
    # Try both combinations to find match
    name1 = table_exists_agg(chain, dex, token0, token1, interval)
    name2 = table_exists_agg(chain, dex, token1, token0, interval)

    if name1 and token0.upper() == desired_base:
        return name1
    elif name2 and token1.upper() == desired_base:
        return name2
    else:
        log.error(f"Table for {pair} not found with tokens {token0}, {token1}")
        return None