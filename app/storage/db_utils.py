from sqlalchemy import inspect

def table_exists(session, symbol: str, interval: str) -> bool:
    """Check if a kline table for the given symbol exists in the database.
    Converts symbol to lowercase and inspects current schema."""
    ...
    tablename = f"{symbol.replace('/', '').lower()}_{interval}_klines"
    inspector = inspect(session.bind)
    return tablename in inspector.get_table_names()