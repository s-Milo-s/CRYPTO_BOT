from __future__ import annotations

import logging
import time
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Tuple
from app.utils.constants import STABLECOINS, SUPPORTED_CONVERSIONS
from sqlalchemy.orm import Session

from app.utils.clean_util import normalize_symbol
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from app.storage.models.price_8h_usd import Price8hUSD
from decimal import Decimal
from app.sources.binance.binance_klines_source import fetch_price_series

log = logging.getLogger(__name__)


class FillQuoteUSDPrices:  # pylint: disable=too-few-public-methods
    """Incrementally build trade‑size aggregation utilities.

    Parameters
    ----------
    session : sqlalchemy.orm.Session
        Active DB session (won’t be closed by this class).
    symbol : str
        Asset symbol – expects formats like "ETH", "WETH", "USDC".
    days_back : int, default 30
        How far back (in days) the aggregation window should stretch.
    """
    PRICE_TABLE = "price_8h_usd"
    BUCKET_HOURS = 8
    PRICE_TABLE = "price_15m_usd"  # ⬅️ Suggestion: 1 row per 15‑min bucket, cols = coins‑to‑USD
    INTERVAL_MS = BUCKET_HOURS * 60 * 60 * 1_000  # 8 h in ms

    def __init__(self, session: Session, symbol: str, days_back: int = 30) -> None:
        """Initialize the aggregator with a session and symbol."""
        log.info(
            "Initializing trade size aggregator for symbol '%s' with %d days back.",
            symbol.upper(),
            days_back,
        )
        self.session: Session = session
        self.symbol: str = normalize_symbol(symbol).lower()
        self.days_back: int = days_back
        self.end_ms: int = int(time.time() * 1000)
        self.start_ms: int = self.end_ms - days_back * 86_400_000  # days → ms

        # Shortcut: skip any processing for USD‑pegged assets
        if self.symbol in STABLECOINS:
            log.info(
                "[%s] Detected stablecoin – trade‑size aggregation skipped.",
                self.symbol,
            )
            self.missing_price_intervals: List[Tuple[int, int]] = []
            return

        # For ETH/WETH (scope kept small per requirements)
        if self.symbol in SUPPORTED_CONVERSIONS:
            self.missing_price_intervals = self._detect_missing_price_intervals()
            self.fill_missing_prices()
        else:
            raise ValueError(
                f"Unsupported symbol '{self.symbol}'. Only ETH or stablecoins handled at this stage."
            )

    # -------------------------------------------------------------------
    # Private helpers (stubs for now)
    # -------------------------------------------------------------------

    def _detect_missing_price_intervals(self) -> List[Tuple[int, int]]:
        symbol_col_name = self.symbol
        if not hasattr(Price8hUSD, symbol_col_name):
            raise ValueError(
                f"Column '{symbol_col_name}' not found on {self.PRICE_TABLE}; schema mismatch."
            )
        price_col = getattr(Price8hUSD, symbol_col_name)

        start_dt = datetime.utcfromtimestamp(self.start_ms / 1_000).replace(
            minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )
        start_dt -= timedelta(hours=start_dt.hour % self.BUCKET_HOURS)

        end_dt = datetime.utcfromtimestamp(self.end_ms / 1_000).replace(
            minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )
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

        # Assume only one contiguous gap at start and/or end
        gaps = []
        step = timedelta(hours=self.BUCKET_HOURS)

        if missing_buckets[0] == expected_buckets[0]:
            gap_end = missing_buckets[-1]
            for b in reversed(missing_buckets):
                if b + step in existing_set:
                    gap_end = b
                    break
            gaps.append((int(missing_buckets[0].timestamp() * 1_000), int(gap_end.timestamp() * 1_000)))

        elif missing_buckets[-1] == expected_buckets[-1]:
            gap_start = missing_buckets[0]
            for b in missing_buckets:
                if b - step in existing_set:
                    gap_start = b
                    break
            gaps.append((int(gap_start.timestamp() * 1_000), int(missing_buckets[-1].timestamp() * 1_000)))

        return gaps
    def fill_missing_prices(self, interval: str = "8h") -> None:
        if not self.missing_price_intervals:
            log.info("[%s] No price gaps detected.", self.symbol.upper())
            return

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._run_price_backfill(interval))

    async def _run_price_backfill(self, interval: str) -> None:
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
                    "bucket_start": datetime.utcfromtimestamp(ts / 1000).replace(tzinfo=timezone.utc),
                    self.symbol: Decimal(str(price)),
                    "created_at": datetime.now(timezone.utc),
                }
                all_rows.append(row)

        if not all_rows:
            log.warning("[%s] No data fetched for gaps.", self.symbol.upper())
            return

        try:
            self.session.execute(
                insert(Price8hUSD)
                .values(all_rows)
                .on_conflict_do_nothing(index_elements=["bucket_start"])
                )
            self.session.commit()
            log.info("[%s] Inserted %d rows into %s", self.symbol.upper(), len(all_rows), self.PRICE_TABLE)
        except Exception as e:
            self.session.rollback()
            log.error("[%s] DB insert failed: %s", self.symbol.upper(), e)