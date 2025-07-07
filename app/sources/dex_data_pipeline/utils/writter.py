from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
import re
from typing import Dict 
from decimal import Decimal
from app.storage.models.dex_swap_aggregator_schema import get_aggregate_model_by_name
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
    Upsert minute-aggregated kline data.
    
    Args:
        db (Session): SQLAlchemy session.
        table (Base): SQLAlchemy table model to insert into.
        new_rows (Dict): Mapping of minute_start → row dict.
    """
    model = get_aggregate_model_by_name(table_name)
    table   = model.__table__
    log.info(f"--------Upserting aggregated klines {len(new_rows.keys())}")
    minute_keys = list(new_rows.keys())
    # Step 1: Fetch existing records in one query
    existing_rows = {
        row.minute_start: row
        for row in db.execute(
            select(table).where(table.c.minute_start.in_(minute_keys))
        ).fetchall()
    }
    log.info(f"--------Fetched {len(existing_rows)} existing rows for {len(minute_keys)} minutes")
    to_upsert = []

    for minute, incoming in new_rows.items():
        existing = existing_rows.get(minute)

        if existing:
            # Combine values
            combined = {
                'minute_start': minute,
                'open_price': existing.open_price if existing.open_ts <= incoming['open_ts'] else incoming['open_price'],
                'open_ts': min(existing.open_ts, incoming['open_ts']),
                'close_price': existing.close_price if existing.close_ts >= incoming['close_ts'] else incoming['close_price'],
                'close_ts': max(existing.close_ts, incoming['close_ts']),
                'high_price': max(existing.high_price, incoming['high_price']),
                'low_price': min(existing.low_price, incoming['low_price']),
                'swap_count': existing.swap_count + incoming['swap_count'],
                'total_base_volume': existing.total_base_volume + incoming['total_base_volume'],
                'total_quote_volume': existing.total_quote_volume + incoming['total_quote_volume'],
            }

            # VWAP recompute
            if combined['total_quote_volume'] > 0:
                combined['avg_price'] = combined['total_base_volume'] / combined['total_quote_volume']
            else:
                combined['avg_price'] = Decimal(0)

        else:
            combined = incoming
        to_upsert.append(combined)

    # Step 2: Upsert in bulk
    stmt = pg_insert(table).values(to_upsert)
    update_cols = {
        col: stmt.excluded[col]
        for col in to_upsert[0].keys()
        if col != 'minute_start'
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=['minute_start'],
        set_=update_cols
    )
    log.info(f"----------Upserting {len(to_upsert)} rows into {table_name}")

    db.execute(stmt)
    db.commit()

    log.info(f"----------Finished upserting {len(to_upsert)} rows into {table_name}")
