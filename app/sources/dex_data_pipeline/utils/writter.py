from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
import sqlalchemy as sa
from sqlalchemy.orm import Session
import re
from typing import Dict 
from decimal import Decimal
from app.storage.models.dex_swap_aggregator_schema import get_aggregate_model_by_name
from sqlalchemy import func
from app.storage.models.trade_size_distribution import TradeSizeDistributionTable
import time
import sqlalchemy.exc
from app.storage.db import SessionLocal
import logging


log = logging.getLogger(__name__)


# Example schema structure for one aggregated row (per minute)
# {
#     'minute_start': datetime,
#     'open_price': Decimal,
#     'high_price': Decimal,
#     'low_price': Decimal,
#     'close_price': Decimal,
#     'avg_price': Decimal,
#     'swap_count': int,
#     'total_base_volume': Decimal,
#     'total_quote_volume': Decimal
# }

def upsert_aggregated_klines(
    db: Session,
    table_name,
    new_rows: Dict
):
    """
    Upsert minute-aggregated kline data via one SQL statement.
    
    Args:
        db (Session): SQLAlchemy session.
        table_name (str): Name of the table model.
        new_rows (Dict): Mapping of minute_start → row dict.
    """
    model = get_aggregate_model_by_name(table_name)
    table = model.__table__
    
    log.info(f"--------Upserting aggregated klines {len(new_rows)}")

    # Convert dict-of-dicts into list-of-dicts
    to_upsert = [
        {'minute_start': minute, **row_dict}
        for minute, row_dict in new_rows.items()
    ]   

    stmt = pg_insert(table).values(to_upsert)

    stmt = stmt.on_conflict_do_update(
        index_elements=['minute_start'],
        set_={
            'open_price': sa.case(
                (table.c.open_ts <= stmt.excluded.open_ts, table.c.open_price),
                    else_=stmt.excluded.open_price
            ),
            'open_ts': sa.func.LEAST(table.c.open_ts, stmt.excluded.open_ts),
            'close_price': sa.case(
                (table.c.close_ts >= stmt.excluded.close_ts, table.c.close_price),
                else_=stmt.excluded.close_price
            ),
            'close_ts': sa.func.GREATEST(table.c.close_ts, stmt.excluded.close_ts),
            'high_price': sa.func.GREATEST(table.c.high_price, stmt.excluded.high_price),
            'low_price': sa.func.LEAST(table.c.low_price, stmt.excluded.low_price),
            'swap_count': table.c.swap_count + stmt.excluded.swap_count,
            'total_base_volume': table.c.total_base_volume + stmt.excluded.total_base_volume,
            'total_quote_volume': table.c.total_quote_volume + stmt.excluded.total_quote_volume,
            'avg_price': func.coalesce(
                (table.c.total_base_volume + stmt.excluded.total_base_volume) /
                func.nullif((table.c.total_quote_volume + stmt.excluded.total_quote_volume), 0),
                0
            )
        }
    )

    db.execute(stmt)
    log.info(f"----------Finished upserting {len(to_upsert)} rows into {table_name}")


BUCKET_COLUMN_MAP = {
    -2: "bucket_neg2",
    -1: "bucket_neg1",
     0: "bucket_0",
     1: "bucket_1",
     2: "bucket_2",
     3: "bucket_3",
     4: "bucket_4",
     5: "bucket_5",
     6: "bucket_6",
}

def insert_or_update_trade_size_distribution(
    db: Session, 
    pool_name: str, 
    buckets: dict[int, int]
):
    log.info(f"Inserting/updating trade size distribution for pool: {pool_name}")

    # Build insert values
    row_data = {"pool_name": pool_name}
    for key, column in BUCKET_COLUMN_MAP.items():
        row_data[column] = buckets.get(key, 0)

    table = TradeSizeDistributionTable.__table__
    stmt = pg_insert(table).values(row_data)

    # Build upsert logic
    update_dict = {
        column: table.c[column] + stmt.excluded[column]
        for column in BUCKET_COLUMN_MAP.values()
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=["pool_name"],
        set_=update_dict
    )

    db.execute(stmt)