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

    print(f"----------Finished upserting {len(to_upsert)} rows into {table_name}")


def delete_price_anomalies(
    db: Session,
    table_name: str,
    pct_threshold: float = 0.05,
    volume_floor: float | None = None,
) -> int:
    """
    Delete rows from the given table where the average price changes
    more than pct_threshold (e.g., 0.05 = 5%) compared to the previous minute.
    
    Args:
        db: SQLAlchemy Session
        table_name: Name of the target table
        pct_threshold: Threshold for percentage change (0.05 = 5%)
        volume_floor: Optional minimum volume condition to filter deletes

    Returns:
        Number of rows deleted
    """

    # Buffer let db settle after previous operations
    time.sleep(2)

    volume_clause = ""
    if volume_floor is not None:
        volume_clause = f"AND total_base_volume < {volume_floor}"

    sql = sql = f"""
        WITH cleaned AS (
            DELETE FROM {table_name}
            WHERE avg_price = 0
            RETURNING minute_start
        ),
        price_changes AS (
            SELECT
                minute_start,
                avg_price,
                LAG(avg_price) OVER (ORDER BY minute_start) AS prev_avg,
                ABS(avg_price - LAG(avg_price) OVER (ORDER BY minute_start)) 
                    / NULLIF(LAG(avg_price) OVER (ORDER BY minute_start), 0) AS pct_change
            FROM {table_name}
        ),
        to_delete AS (
            SELECT minute_start
            FROM price_changes
            WHERE prev_avg IS NOT NULL
            AND pct_change > :threshold
            {volume_clause}
        )
        DELETE FROM {table_name}
        WHERE minute_start IN (SELECT minute_start FROM to_delete)
        RETURNING minute_start;
        """


    result = db.execute(text(sql), {"threshold": pct_threshold})
    rows = result.fetchall()
    db.commit()
    return len(rows)

def delete_price_anomalies_with_retry(
    table_name: str,
    retries: int = 3,
    delay: float = 2.0
) -> int:
    """
    Deletes price anomalies from the given table, with retry logic.
    """
    attempt = 0
    while attempt < retries:
        try:
            with SessionLocal() as db:
                return delete_price_anomalies(db, table_name)
        except sqlalchemy.exc.InterfaceError as e:
            print(f"[delete_price_anomalies] InterfaceError on attempt {attempt + 1}: {e}")
            attempt += 1
            time.sleep(delay)
        except Exception as e:
            print(f"[delete_price_anomalies] Unhandled error: {e}")
            break
    print(f"[delete_price_anomalies] Failed after {retries} attempts.")
    return 0  # or raise if you'd prefer to escalate