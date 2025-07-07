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
            log.error(f"[delete_price_anomalies] InterfaceError on attempt {attempt + 1}: {e}")
            attempt += 1
            time.sleep(delay)
        except Exception as e:
            log.error(f"[delete_price_anomalies] Unhandled error: {e}")
            break
    log.info(f"[delete_price_anomalies] Failed after {retries} attempts.")
    return 0  # or raise if you'd prefer to escalate