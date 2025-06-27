from sqlalchemy import inspect
from sqlalchemy import text
from app.storage.db import get_db

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
    db = next(get_db())
    inspector = inspect(db.bind)
    return table_name in inspector.get_table_names()