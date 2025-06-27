from sqlalchemy import select, insert, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from typing import Dict, List
from decimal import Decimal
from app.storage.models.dex_swap_aggregator_schema import get_aggregate_model_by_name

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
    print(f"--------Upserting aggregated klines {len(new_rows.keys())}")
    minute_keys = list(new_rows.keys())
    # Step 1: Fetch existing records in one query
    existing_rows = {
        row.minute_start: row
        for row in db.execute(
            select(table).where(table.c.minute_start.in_(minute_keys))
        ).fetchall()
    }
    print(f"--------Fetched {len(existing_rows)} existing rows for {len(minute_keys)} minutes")
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
    print(f"----------Upserting {len(to_upsert)} rows into {table_name}")

    db.execute(stmt)
    db.commit()

def delete_pct_outliers_sql(
    engine,
    table: str,
    ts_col: str = "minute_start",
    price_col: str = "avg_price",
    pct_thresh: float = 0.30
):
    """
    Delete rows whose price deviates more than `pct_thresh`
    from the previous row (ordered by timestamp).

    Args
    ----
    engine      : SQLAlchemy engine
    table       : table name  (schema.table if needed)
    ts_col      : timestamp column used for ordering (default 'minute_start')
    price_col   : price column (default 'avg_price')
    pct_thresh  : fractional threshold, e.g. 0.30 = 30 %
    """

    sql = f"""
    WITH flagged AS (
        SELECT
            {ts_col},
            {price_col},
            LAG({price_col}) OVER (ORDER BY {ts_col}) AS prev_price
        FROM {table}
    ),
    to_remove AS (
        SELECT {ts_col}
        FROM flagged
        WHERE prev_price IS NOT NULL
          AND ABS({price_col} - prev_price) / prev_price > :pct
    )
    DELETE FROM {table}
    USING to_remove
    WHERE {table}.{ts_col} = to_remove.{ts_col};
    """

    with engine.begin() as conn:          # automatic commit
        rows = conn.execute(text(sql), {"pct": pct_thresh}).rowcount
        print(f"Deleted {rows} outlier rows (>|{pct_thresh*100:.0f}%| jump).")
