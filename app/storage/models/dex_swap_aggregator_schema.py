from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Numeric, Integer, TIMESTAMP as TIMESTAMPTZ
from decimal import Decimal

Base = declarative_base()
_kline_model_cache = {}

def get_aggregate_model_by_name(table_name: str):
    """
    Dynamically generate (and cache) a SQLAlchemy model for an aggregate table.
    Table name must be passed as-is (e.g., 'eth_uniswap_solusdt_1m_klines').
    """
    if table_name in _kline_model_cache:
        return _kline_model_cache[table_name]

    class_attrs = {
        '__tablename__': table_name,
        'minute_start': Column(TIMESTAMPTZ, primary_key=True),
        'open_price': Column(Numeric(38, 18)),
        'open_ts': Column(TIMESTAMPTZ),
        'close_price': Column(Numeric(38, 18)),
        'close_ts': Column(TIMESTAMPTZ),
        'high_price': Column(Numeric(38, 18)),
        'low_price': Column(Numeric(38, 18)),
        'avg_price': Column(Numeric(38, 18)),
        'swap_count': Column(Integer),
        'total_base_volume': Column(Numeric(38, 18)),
        'total_quote_volume': Column(Numeric(38, 18)),

        # New metrics
        'trade_imbalance': Column(Numeric(38, 18)),
        'price_volatility': Column(Numeric(38, 18)),
        'price_momentum': Column(Numeric(38, 18)),

        '__mapper_args__': {'eager_defaults': True},
    }

    cls_name = f"{table_name.title().replace('_', '')}Agg"
    model_class = type(cls_name, (Base,), class_attrs)
    _kline_model_cache[table_name] = model_class
    return model_class