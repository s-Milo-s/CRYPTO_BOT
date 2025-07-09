import httpx
import asyncio
import backoff
from app.utils.constants import INTERVAL_MS
import logging

log = logging.getLogger(__name__)

BINANCE_URL = "https://api.binance.us/api/v3/klines"
MAX_LIMIT = 1000

@backoff.on_exception(backoff.expo, httpx.RequestError, max_tries=3, jitter=None)
async def fetch_kline_chunk(client, symbol, interval, start_time, end_time):
    params = {
        "symbol": symbol.upper().replace("/", ""),
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": MAX_LIMIT,
    }
    resp = await client.get(BINANCE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

async def fetch_price_series(symbol: str, interval: str, start_ms: int, end_ms: int) -> list[float]:
    """Fetches close prices for a given symbol and time range without saving."""
    if interval not in INTERVAL_MS:
        raise ValueError(f"Unsupported interval: {interval}")

    interval_ms = INTERVAL_MS[interval]
    prices = []

    async with httpx.AsyncClient() as client:
        cursor = end_ms
        while cursor >= start_ms:
            chunk_start = max(start_ms, cursor - MAX_LIMIT * interval_ms)
            try:
                data = await fetch_kline_chunk(client, symbol, interval, chunk_start, cursor)
                # Grab close prices (index 4 of each row)
                for row in data:
                    timestamp = int(row[0])  # open_time in ms
                    close_price = float(row[4])
                    prices.append((timestamp, close_price))
            except Exception as e:
                print(f"❌ Error fetching data: {e}")
                break
            cursor = chunk_start - interval_ms
            await asyncio.sleep(0.2)
    log.info(prices)
    return list(reversed(prices))  # Oldest → newest
