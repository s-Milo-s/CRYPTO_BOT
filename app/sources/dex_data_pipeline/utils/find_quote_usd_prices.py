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
        elif self.symbol in SUPPORTED_CONVERSIONS:
            self.missing_price_intervals = self._detect_missing_price_intervals()
        else:
            raise ValueError(
                f"Unsupported symbol '{self.symbol}'. Only ETH or stablecoins handled."
            )

    def _detect_missing_price_intervals(self) -> List[Tuple[int, int]]:
        symbol_col_name = self.symbol
        if not hasattr(Price8hUSD, symbol_col_name):
            raise ValueError(
                f"Column '{symbol_col_name}' not found on {self.PRICE_TABLE}; schema mismatch."
            )
        price_col = getattr(Price8hUSD, symbol_col_name)

        start_dt = datetime.fromtimestamp(self.start_ms / 1000, tz=timezone.utc)
        start_dt -= timedelta(hours=start_dt.hour % self.BUCKET_HOURS)

        end_dt = datetime.fromtimestamp(self.end_ms / 1000, tz=timezone.utc)
        end_dt -= timedelta(hours=end_dt.hour % self.BUCKET_HOURS)

        expected_buckets = []
        cursor = start_dt
        while cursor <= end_dt:
            expected_buckets.append(cursor)
            cursor += timedelta(hours=self.BUCKET_HOURS)

        existing_rows = (
            self.session.execute(
                select(Price8hUSD.bucket_start).where(
                    Price8hUSD.bucket_start.between(start_dt, end_dt),
                    price_col.isnot(None),
                )
            ).scalars().all()
        )
        existing_set = set(existing_rows)
        missing_buckets = [b for b in expected_buckets if b not in existing_set]
        if not missing_buckets:
            return []

        gaps = []
        step = timedelta(hours=self.BUCKET_HOURS)

        if missing_buckets[0] == expected_buckets[0]:
            gap_end = missing_buckets[-1]
            for b in reversed(missing_buckets):
                if b + step in existing_set:
                    gap_end = b
                    break
            gaps.append((int(missing_buckets[0].timestamp() * 1000), int(gap_end.timestamp() * 1000)))

        elif missing_buckets[-1] == expected_buckets[-1]:
            gap_start = missing_buckets[0]
            for b in missing_buckets:
                if b - step in existing_set:
                    gap_start = b
                    break
            gaps.append((int(gap_start.timestamp() * 1000), int(missing_buckets[-1].timestamp() * 1000)))

        return gaps

    async def fill_missing_prices(self, interval: str = "8h") -> None:
        if not self.missing_price_intervals:
            log.info("[%s] No price gaps detected.", self.symbol.upper())
            return

        all_rows = []
        for gap_start, gap_end in self.missing_price_intervals:
            log.info("[%s] Fetching price data from %s to %s", self.symbol.upper(), gap_start, gap_end)
            try:
                data = await fetch_price_series(
                    symbol=f"{self.symbol.upper()}USD",
                    interval=interval,
                    start_ms=gap_start,
                    end_ms=gap_end,
                )
            except Exception as e:
                log.error("[%s] Fetch failed: %s", self.symbol.upper(), e)
                continue

            for ts, price in data:
                row = {
                    "bucket_start": datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                    self.symbol: Decimal(str(price)),
                    "created_at": datetime.now(timezone.utc),
                }
                all_rows.append(row)

        if not all_rows:
            log.warning("[%s] No data fetched for gaps.", self.symbol.upper())
            return

        try:
            stmt = (
                insert(Price8hUSD)
                .values(all_rows)
                .on_conflict_do_update(
                    index_elements=["bucket_start"],
                    set_={self.symbol: insert(Price8hUSD).excluded[self.symbol]},
                )
            )
            self.session.execute(stmt)
            self.session.commit()
            log.info("[%s] Inserted %d rows into %s", self.symbol.upper(), len(all_rows), self.PRICE_TABLE)
        except Exception as e:
            self.session.rollback()
            log.error("[%s] DB insert failed: %s", self.symbol.upper(), e)
