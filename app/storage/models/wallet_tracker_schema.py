from sqlalchemy import Column, Numeric, Integer, Text, TIMESTAMP as TIMESTAMPTZ
from app.storage.base import Base  # your declarative_base
from typing import Dict

_wallet_model_cache: Dict[str, type] = {}

def get_wallet_stats_model_by_name(table_name: str):
    if table_name in _wallet_model_cache:
        return _wallet_model_cache[table_name]

    class_attrs = {
        '__tablename__': table_name,
        'wallet_address': Column(Text, primary_key=True),
        'volume_usd': Column(Numeric(38, 18)),
        'pnl_usd': Column(Numeric(38, 18)),
        'return_sum': Column(Numeric(38, 18)),
        'return_squared_sum': Column(Numeric(38, 18)),
        'num_returns': Column(Integer),
        'open_position_usd': Column(Numeric(38, 18)),
        'cost_basis_usd': Column(Numeric(38, 18)),
        'last_updated': Column(TIMESTAMPTZ, nullable=True),
        '__mapper_args__': {'eager_defaults': True},
    }

    class_name = f"{table_name.title().replace('_', '')}WalletStats"
    model_class = type(class_name, (Base,), class_attrs)
    _wallet_model_cache[table_name] = model_class
    return model_class