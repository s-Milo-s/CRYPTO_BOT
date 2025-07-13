import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from decimal import Decimal
import logging

log = logging.getLogger(__name__)

def crunch_metrics_for_table(
    db: Session,
    table_name: str,
    roll_window: int = 60,
):
    log.info(f"Crunching metrics for table: {table_name}")

    df = pd.read_sql(f"""
        SELECT minute_start, avg_price, total_base_volume, total_quote_volume
        FROM {table_name}
        ORDER BY minute_start
    """, db.bind)

    # Compute metrics
    vol_denom = (
        df["total_base_volume"].astype(float) +
        df["total_quote_volume"].astype(float) +
        1e-9
    )
    df["trade_imbalance"] = (
        (df["total_base_volume"] - df["total_quote_volume"]) / vol_denom
    )

    df["price_volatility"] = (
        df["avg_price"].astype(float)
        .rolling(window=roll_window, min_periods=1)
        .std()
    )

    df["price_momentum"] = (
        df["avg_price"].astype(float)
        .pct_change(periods=roll_window)
    )

    # Write full metrics to one temp table
    metrics_df = df[[
        "minute_start", "trade_imbalance",
        "price_volatility", "price_momentum"
    ]]
    metrics_df.to_sql("_metrics_tmp", db.bind, index=False, if_exists="replace")
    log.info(f"Wrote {len(metrics_df)} rows to _metrics_tmp")

    # Index it for fast joins
    db.execute(text("CREATE INDEX IF NOT EXISTS idx_metrics_tmp_minute ON _metrics_tmp(minute_start)"))

    # Update in smaller batches to avoid DB timeouts
    batch_size = 5000
    for i in range(0, len(metrics_df), batch_size):
        db.execute(text(f"""
            UPDATE {table_name} t
            SET
              trade_imbalance   = s.trade_imbalance,
              price_volatility  = s.price_volatility,
              price_momentum    = s.price_momentum
            FROM _metrics_tmp s
            WHERE t.minute_start = s.minute_start
              AND t.minute_start >= :start
              AND t.minute_start < :end
        """), {
            "start": metrics_df["minute_start"].iloc[i],
            "end": metrics_df["minute_start"].iloc[min(i + batch_size, len(metrics_df) - 1)]
        })
        log.info(f"✅ Updated rows {i}–{i+batch_size}")

    db.execute(text("DROP TABLE IF EXISTS _metrics_tmp"))
    log.info(f"✅ Done computing metrics for {table_name}")
