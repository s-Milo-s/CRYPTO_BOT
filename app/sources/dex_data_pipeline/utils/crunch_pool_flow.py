from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Literal
import time
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.utils.clean_util import normalize_symbol
from app.utils.constants  import STABLECOINS
from app.utils.query_bank import STABLE_SQL, VOLATILE_SQL

log = logging.getLogger(__name__)

def full_hour_window(days_back: int = 30) -> tuple[datetime, datetime]:
    """
    Return (start, end) so that:

    • end  == the top of the *previous* UTC hour  (e.g. called at 13:07 → end = 13:00)
    • start == end − <days_back> days            (rounded to the same minute/second)

    This guarantees every bucket you aggregate is a full 60‑minute slice.
    """
    now      = datetime.now(tz=timezone.utc)
    end_hour = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    start    = end_hour - timedelta(days=days_back)
    return start, end_hour


def crunch_pool_flow(
    db: Session,
    raw_table: str,
    quote_token: str,
    days_back: int = 30,
) -> None:
    """
    Aggregate swap flow into hourly buckets and upsert into `pool_flow_hourly`.

    Parameters
    ----------
    db          : SQLAlchemy session
    raw_table   : e.g. 'arbitrum_uniswap_v3_wethusdc_raw_swaps'
    pool_slug   : canonical slug ('arbitrum_uniswap_v3_wethusdc')
    quote_token : quoted token symbol (e.g. 'USDC' or 'WETH')
    hours_back  : look‑back window (default 30 d)
    """
    pool_slug = raw_table.replace("_raw_swaps", "")
    start_job = datetime.now(tz=timezone.utc)
    is_stable = normalize_symbol(quote_token) in STABLECOINS
    log.info(
        "→ Flow‑agg job for %s (%s, %d  day look‑back)",
        raw_table, "stable" if is_stable else "volatile", days_back,
    )

    start, end = full_hour_window(days_back=days_back)
    sql   = STABLE_SQL if is_stable else VOLATILE_SQL.format(
        raw_table=raw_table, symbol=normalize_symbol(quote_token)
    )

    df = pd.read_sql(
        text(sql.format(raw_table=raw_table)),
        db.bind,
        params={"start": start, "end": end, "pool_slug": pool_slug},
        parse_dates=["bucket_start"],
    )
    if df.empty:
        log.warning("No swap rows found for %s → skipping", pool_slug)
        return
    log.info("  • Aggregated %s hourly buckets", f"{len(df):,}")


    # ── load via temp table then UPSERT
    slug_safe = pool_slug.replace("/", "_")[:40]          # keep it short & safe
    tmp = f"_pool_flow_tmp_{slug_safe}_{int(time.time())}"
    
    try:
        with db.begin():
            conn = db.connection()  # ← use the transactional connection

            # 1. TEMP table (drops on COMMIT)
            conn.execute(text(f"""
                CREATE TEMP TABLE {tmp} (
                pool_slug    TEXT,
                bucket_start TIMESTAMPTZ,
                buys_usd     NUMERIC(38,2),
                sells_usd    NUMERIC(38,2),
                volume_usd   NUMERIC(38,2),
                pressure     NUMERIC(8,5)
                ) ON COMMIT DROP;
            """))

            # 2. COPY rows into the temp table
            df.to_sql(tmp, conn, index=False,
                    if_exists="append", method="multi", chunksize=10_000)

            temp_rows = conn.execute(text(f"SELECT COUNT(*) FROM {tmp}")).scalar()
            log.info("  • Temp‑table rows = %s", temp_rows)
            if temp_rows == 0:
                raise RuntimeError("Temp table is empty after to_sql()")

            # 3. UPSERT overwrite into destination
            upsert = conn.execute(text(f"""
                INSERT INTO pool_flow_hourly
                (pool_slug, bucket_start, buys_usd, sells_usd, volume_usd, pressure)
                SELECT pool_slug, bucket_start, buys_usd, sells_usd, volume_usd, pressure
                FROM   {tmp}
                ON CONFLICT (pool_slug, bucket_start) DO UPDATE
                SET buys_usd   = EXCLUDED.buys_usd,
                    sells_usd  = EXCLUDED.sells_usd,
                    volume_usd = EXCLUDED.volume_usd,
                    pressure   = EXCLUDED.pressure;
            """))
            log.info("  • Rows inserted/updated = %s", upsert.rowcount)

            if upsert.rowcount == 0:
                raise RuntimeError("UPSERT affected 0 rows – check query & schema")

    except (SQLAlchemyError, RuntimeError) as e:
        log.exception("Pool‑flow aggregation failed: %s", e)
        db.rollback()
        raise

    elapsed = (datetime.now(tz=timezone.utc) - start_job).total_seconds()
    log.info("✅ pool_flow_hourly up‑to‑date for %s in %.1f s", pool_slug, elapsed)