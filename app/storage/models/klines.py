from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, Numeric, Integer

Base = declarative_base()
# Global cache for generated models
_kline_model_cache = {}


def get_kline_class(symbol: str,interval: str):
    """
    Dynamically generate (and cache) a SQLAlchemy model class for a given symbol + interval.
    Reuses previously created models to avoid re-registration errors.
    """
    ...
    key = f"{symbol.replace('/', '').lower()}_{interval}"

    if key in _kline_model_cache:
        return _kline_model_cache[key]
    
    tablename = f"{key}_klines"

    class_attrs = {
        '__tablename__': tablename,
        'open_time': Column(BigInteger, primary_key=True),
        'open_price': Column(Numeric(18, 8), nullable=False),
        'high_price': Column(Numeric(18, 8), nullable=False),
        'low_price': Column(Numeric(18, 8), nullable=False),
        'close_price': Column(Numeric(18, 8), nullable=False),
        'volume': Column(Numeric(18, 8), nullable=False),
        'close_time': Column(BigInteger, nullable=False),
        'quote_volume': Column(Numeric(18, 8)),
        'number_of_trades': Column(Integer),
        'taker_buy_base_volume': Column(Numeric(18, 8)),
        'taker_buy_quote_volume': Column(Numeric(18, 8)),
        '__mapper_args__': {'eager_defaults': True}
    }
    cls_name = f"{symbol.replace('/', '')}{interval.upper()}Kline"
    cls = type(cls_name, (Base,), class_attrs)
    _kline_model_cache[key] = cls
    return cls