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
    chunk_size: int = 10_000
):
    """
    Compute ML metrics for a given pair's minute-level table.
    """
    log.info(f"Crunching metrics for table: {table_name}")

    df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY minute_start", db.bind)

    vol_denom = (
        df["total_base_volume"].astype("float") +
        df["total_quote_volume"].astype("float") +
        1e-9
    )
    df["trade_imbalance"] = (
        (df["total_base_volume"] - df["total_quote_volume"]) / vol_denom
    )

    df["price_volatility"] = (
        df["avg_price"]
        .astype("float")
        .rolling(window=roll_window, min_periods=1)
        .std()
    )

    df["price_momentum"] = (
        df["avg_price"]
        .astype("float")
        .pct_change(periods=roll_window)
    )

    # Write back in chunks
    for start in range(0, len(df), chunk_size):
        sub = df.iloc[start:start+chunk_size][[
            "minute_start", "trade_imbalance",
            "price_volatility", "price_momentum"
        ]]
        sub.to_sql("_metrics_tmp", db.bind, index=False, if_exists="replace")

        db.execute(text(f"""
            UPDATE {table_name} t
            SET
              trade_imbalance   = s.trade_imbalance,
              price_volatility  = s.price_volatility,
              price_momentum    = s.price_momentum
            FROM _metrics_tmp s
            WHERE t.minute_start = s.minute_start
        """))
        db.execute(text("DROP TABLE IF EXISTS _metrics_tmp"))
        db.commit()

        log.info("Updated rows %d–%d", start, start + len(sub))

    log.info(f"Done computing metrics for {table_name}")
