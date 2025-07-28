"""
crunch_wallet_metrics.py (stream‑friendly)
-----------------------------------------
Fast, low‑memory wallet aggregation that scales to **millions of swaps** and
hundreds‑of‑thousands of wallets.  Heavy math is pushed to Postgres; Python
only handles the  ~100 k aggregated rows.

Key improvements
================
* **Single GROUP BY in SQL** → no giant DataFrame in RAM.
* **turnover_24h** and **last_trade** computed in‑DB.
* **UPSERT** batched via a temporary table written with `to_sql(chunksize=5 000)`.
* Verbose logging for each phase.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import List

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.utils.clean_util import normalize_symbol
from app.utils.constants import STABLECOINS

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------

def metrics_table_name(raw_table: str) -> str:
    if not raw_table.endswith("_raw_swaps"):
        raise ValueError("Expected raw table ending in '_raw_swaps'")
    return raw_table.replace("_raw_swaps", "_wallet_metrics")

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def crunch_wallet_metrics(
    db: Session,
    raw_table: str,
    metrics_table: str | None = None,
    days_back: int = 90,
    quote_token: str = "USDC"
) -> None:
    metrics_table = metrics_table or metrics_table_name(raw_table)

    start_ts = datetime.now(tz=timezone.utc)  # bench log
    log.info("→ Wallet‑metrics job for %s (look‑back %d days)", raw_table, days_back)

    end = datetime.now(tz=timezone.utc)
    start = end - timedelta(days=days_back)
    symbol      = normalize_symbol(quote_token)
    is_stable   = symbol in STABLECOINS

    # --------------------------------------------------------------
    # 1. Aggregate in SQL (fast)
    # --------------------------------------------------------------
    stablecoin_sql = """
        WITH base AS (
        SELECT
            s.caller AS wallet,
            s.quote_vol,
            s.is_buy,
            s."timestamp"
        FROM {raw_table} s
        WHERE s.caller IS NOT NULL
            AND s."timestamp" >= :start
            AND s."timestamp" <  :end
        ),

        usd_swaps AS (
        SELECT
            wallet,
            "timestamp",
            quote_vol::float8 * (CASE WHEN is_buy THEN 1 ELSE -1 END) AS signed_vol_usd
        FROM base
        )

        SELECT
        wallet,
        SUM(ABS(signed_vol_usd))                                    AS turnover,
        SUM(CASE WHEN signed_vol_usd > 0 THEN  signed_vol_usd END)  AS buy_volume,
        SUM(CASE WHEN signed_vol_usd < 0 THEN -signed_vol_usd END)  AS sell_volume,
        COUNT(*)                                                    AS trades,
        MAX("timestamp")                                            AS last_trade,
        SUM(
            CASE
            WHEN "timestamp" >= (:end - interval '24 hours')
            THEN ABS(signed_vol_usd)
            END
        ) AS turnover_24h
        FROM usd_swaps
        GROUP BY wallet;
        """.format(raw_table=raw_table)
    
    volatile_sql = """
        WITH base AS (
        SELECT
            s.caller AS wallet,
            date_trunc('hour', s."timestamp")
            - (EXTRACT(hour FROM s."timestamp")::int % 8) * interval '1 hour'
            AS bucket_start,
            s.quote_vol,
            s.is_buy,
            s."timestamp"
        FROM {raw_table} s
        WHERE s.caller IS NOT NULL
            AND s."timestamp" >= :start
            AND s."timestamp" <  :end
        ),

        priced AS (
        SELECT
            b.wallet,
            b."timestamp",
            b.quote_vol,
            b.is_buy,
            COALESCE(p.{symbol}, 0)::float8 AS bucket_price
        FROM base b
        LEFT JOIN price_8h_usd p ON p.bucket_start = b.bucket_start
        ),

        usd_swaps AS (
        SELECT
            wallet,
            "timestamp",
            quote_vol * bucket_price * (CASE WHEN is_buy THEN 1 ELSE -1 END) AS signed_vol_usd
        FROM priced
        WHERE bucket_price IS NOT NULL
        )

        SELECT
        wallet,
        SUM(ABS(signed_vol_usd))                                    AS turnover,
        SUM(CASE WHEN signed_vol_usd > 0 THEN  signed_vol_usd END)  AS buy_volume,
        SUM(CASE WHEN signed_vol_usd < 0 THEN -signed_vol_usd END)  AS sell_volume,
        COUNT(*)                                                    AS trades,
        MAX("timestamp")                                            AS last_trade,
        SUM(
            CASE
            WHEN "timestamp" >= (:end - interval '24 hours')
            THEN ABS(signed_vol_usd)
            END
        ) AS turnover_24h
        FROM usd_swaps
        GROUP BY wallet;
        """.format(raw_table=raw_table, symbol=symbol.lower())


    sql = stablecoin_sql if is_stable else volatile_sql
    sql = text(sql)

    grouped = pd.read_sql(
    sql,
    db.bind,
    params={
        "start": start,
        "end": end
        }
    )
    log.info("  • Aggregated %s wallets in SQL", f"{len(grouped):,}")

    if grouped.empty:
        log.warning("No wallet rows → aborting")
        return

    # --------------------------------------------------------------
    # 2. Derived columns (Python, cheap)
    # --------------------------------------------------------------
    grouped["net_bias"] = (
        (grouped["buy_volume"] - grouped["sell_volume"]) /
        grouped["turnover"].replace({0: np.nan})
    ).fillna(0)

    grouped["avg_trade_usd"] = grouped["turnover"] / grouped["trades"].clip(lower=1)
    grouped["color_val"]     = np.floor(np.log10(grouped["trades"].clip(lower=1))).astype(int)
    grouped["bubble_size"]   = np.sqrt(grouped["avg_trade_usd"].clip(lower=1))

    cols: List[str] = [
        "wallet", "turnover", "buy_volume", "sell_volume", "trades",
        "net_bias", "avg_trade_usd", "color_val", "bubble_size",
        "turnover_24h", "last_trade",
    ]

    # --------------------------------------------------------------
    # 3. Ensure destination table exists
    # --------------------------------------------------------------
    db.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS {metrics_table} (
              wallet          TEXT PRIMARY KEY,
              turnover        DOUBLE PRECISION,
              buy_volume      DOUBLE PRECISION,
              sell_volume     DOUBLE PRECISION,
              trades          INTEGER,
              net_bias        DOUBLE PRECISION,
              avg_trade_usd   DOUBLE PRECISION,
              color_val       INTEGER,
              bubble_size     DOUBLE PRECISION,
              turnover_24h    DOUBLE PRECISION,
              last_trade      TIMESTAMPTZ,
              updated_at      TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
    )

    # --------------------------------------------------------------
    # 4. Bulk‑copy into temp then UPSERT in batches (5 k)
    # --------------------------------------------------------------
    tmp = "_wallet_metrics_tmp"
    log.info("  • COPY → %s", tmp)
    grouped.to_sql(tmp, db.bind, index=False, if_exists="replace", method="multi", chunksize=5_000)

    log.info("  • Upserting into %s", metrics_table)
    db.execute(
        text(
            f"""
            INSERT INTO {metrics_table} ({', '.join(cols)})
            SELECT {', '.join(cols)} FROM {tmp}
            ON CONFLICT (wallet) DO UPDATE SET
              turnover       = EXCLUDED.turnover,
              buy_volume     = EXCLUDED.buy_volume,
              sell_volume    = EXCLUDED.sell_volume,
              trades         = EXCLUDED.trades,
              net_bias       = EXCLUDED.net_bias,
              avg_trade_usd  = EXCLUDED.avg_trade_usd,
              color_val      = EXCLUDED.color_val,
              bubble_size    = EXCLUDED.bubble_size,
              turnover_24h   = EXCLUDED.turnover_24h,
              last_trade     = EXCLUDED.last_trade,
              updated_at     = NOW();
            """
        )
    )
    db.execute(text(f"DROP TABLE IF EXISTS {tmp}"))

    log.info("✅ Wallet metrics refreshed in %s in %.1f s", metrics_table, (datetime.now(tz=timezone.utc) - start_ts).total_seconds())
