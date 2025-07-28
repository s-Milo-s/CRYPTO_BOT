from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import List, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.utils.constants import STABLECOINS, SUPPORTED_CONVERSIONS
from app.utils.clean_util import normalize_symbol
from app.storage.models.price_8h_usd import Price8hUSD
from app.sources.binance.binance_klines_source import fetch_price_series
from decimal import Decimal
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import insert

log = logging.getLogger(__name__)


class FillQuoteUSDPrices:
    """Fetch and insert 8h quote prices for a given symbol and time range."""
    PRICE_TABLE = "price_8h_usd"
    BUCKET_HOURS = 8

    def __init__(self, session: Session, symbol: str, days_back: int = 30) -> None:
        self.session = session
        self.symbol = normalize_symbol(symbol).lower()
        self.days_back = days_back
        self.end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        self.start_ms = self.end_ms - days_back * 86_400_000

        if self.symbol in STABLECOINS:
            log.info("[%s] Detected stablecoin – skipping price backfill.", self.symbol)
            self.missing_price_intervals: List[Tuple[int, int]] = []
        elif self.symbol not in SUPPORTED_CONVERSIONS:
            raise ValueError(
                f"Unsupported symbol '{self.symbol}'. Only ETH or stablecoins handled."
            )
    async def fill_missing_prices(
        self,
        interval: str = "8h",
    ) -> None:
        """
        Pull an entire `days_back` window of {symbol}‑USD prices and upsert into
        price_8h_usd, overwriting any existing rows.

        • No gap detection – always fetch the full range (≤ 360 rows for 45 d).
        • Adds verbose logs for each step.
        """
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        start_ms = now_ms - self.days_back * 24 * 60 * 60 * 1000

        log.info(
            "[%s] Refreshing %d‑day price window (%s → now)",
            self.symbol.upper(), self.days_back,
            datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat()
        )

        # ── 1. Fetch price series ------------------------------------------------
        try:
            data = await fetch_price_series(
                symbol=f"{self.symbol.upper()}USD",
                interval=interval,
                start_ms=start_ms,
                end_ms=now_ms,
            )
        except Exception as e:
            log.error("[%s] Fetch failed: %s", self.symbol.upper(), e)
            return

        if not data:
            log.warning("[%s] No data returned by price API.", self.symbol.upper())
            return

        # ── 2. Prepare rows ------------------------------------------------------
        rows = [
            {
                "bucket_start": datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                self.symbol:    Decimal(str(price)),
                "created_at":   datetime.now(timezone.utc),
            }
            for ts, price in data
        ]
        log.info("[%s] Fetched %d price points", self.symbol.upper(), len(rows))

        # ── 3. Upsert into DB ----------------------------------------------------
        try:
            stmt = (
                insert(Price8hUSD)
                .values(rows)
                .on_conflict_do_update(
                    index_elements=["bucket_start"],
                    set_={self.symbol: insert(Price8hUSD).excluded[self.symbol]},
                )
            )
            result = self.session.execute(stmt)
            self.session.commit()
            log.info("[%s] Upserted %d rows into %s",
                    self.symbol.upper(), result.rowcount, self.PRICE_TABLE)
        except Exception as e:
            self.session.rollback()
            log.error("[%s] DB insert failed: %s", self.symbol.upper(), e)
