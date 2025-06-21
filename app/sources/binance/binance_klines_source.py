import httpx
import time
from datetime import datetime, timedelta
from httpx import RequestError, HTTPStatusError
import asyncio
from app.utils.constants import INTERVAL_MS
import backoff
from app.storage.db_utils import table_exists
from sqlalchemy.exc import IntegrityError
from app.storage.models.klines import get_kline_class
from decimal import Decimal
from sqlalchemy import func

BINANCE_URL = "https://api.binance.us/api/v3/klines"
MAX_LIMIT = 1000

@backoff.on_exception(backoff.expo, httpx.RequestError, max_tries=3, jitter=None)
async def fetch_kline_chunk(client: httpx.AsyncClient, symbol: str, interval: str, start_time: int, end_time: int):
    """Fetch a kline data chunk from Binance for a given time range.
    Automatically retries on httpx.RequestError using backoff."""
    ...
    params = {
        "symbol": symbol.upper().replace("/", ""),
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1000
    }
    response = await client.get(BINANCE_URL, params=params)
    response.raise_for_status()
    return response.json()

def insert_klines(session, symbol: str, interval: str, rows: list):
    """Bulk insert kline rows into the database, skipping duplicates on conflict."""
    KlineModel = get_kline_class(symbol, interval)
    objects = []

    for row in rows:
        try:
            kline = KlineModel(
                open_time=int(row[0]),
                open_price=Decimal(row[1]),
                high_price=Decimal(row[2]),
                low_price=Decimal(row[3]),
                close_price=Decimal(row[4]),
                volume=Decimal(row[5]),
                close_time=int(row[6]),
                quote_volume=Decimal(row[7]),
                number_of_trades=int(row[8]),
                taker_buy_base_volume=Decimal(row[9]),
                taker_buy_quote_volume=Decimal(row[10])
            )
            objects.append(kline)
        except Exception as e:
            print(f"[!] Failed to construct row: {e}")

    try:
        session.bulk_save_objects(objects, return_defaults=False)
        session.commit()
        return (len(objects), 0)
    except IntegrityError:
        session.rollback()
        print("[!] Bulk insert failed due to duplicates or constraint issue.")
        return (0, len(objects))


def get_existing_time_bounds(session, symbol: str, interval: str):
    """Returns the min and max open_time for existing klines in the DB."""
    ...
    KlineModel = get_kline_class(symbol, interval)
    result = session.query(
        func.min(KlineModel.open_time),
        func.max(KlineModel.open_time)
    ).first()

    return result if result != (None, None) else (None, None)


async def fetch_all_klines(symbol: str, interval: str, end_days_back: int, session=None):
    """Fetch all Binance kline data for a symbol over a time range in chunks.
    Validates inputs, retries on failure, and stores results in the database."""
    ...
    if interval not in INTERVAL_MS:
        return {"status": "error", "reason": f"Invalid interval: {interval}"}
    
    if not table_exists(session, symbol, interval):
        return {"status": "error", "reason": f"Table for {symbol} - {interval} not found in DB."}
    
    interval_ms = INTERVAL_MS[interval]
    end_time_target = int(time.time() * 1000)
    start_time_target = end_time_target - (end_days_back * 86_400_000)

    existing_min, existing_max = get_existing_time_bounds(session, symbol, interval)

    ranges_to_fetch = []

    # Case 1: no data exists at all
    if existing_min is None or existing_max is None:
        ranges_to_fetch.append((start_time_target, end_time_target))

     # Case 2: need older data
    elif start_time_target < existing_min:
        ranges_to_fetch.append((start_time_target, existing_min - interval_ms))

    # Case 3: need newer data
    if existing_max is not None and end_time_target > existing_max:
        ranges_to_fetch.append((existing_max + interval_ms, end_time_target))

    if not ranges_to_fetch:
        return {"status": "success", "message": "Data already up to date."}
    
    total_lines_inserted = 0
    total_lines_skipped = 0
    total_chunks = 0
    async with httpx.AsyncClient() as client:
        for (range_start, range_end) in ranges_to_fetch:
            end_time = range_end
            while end_time > range_start:
                chunk_start = max(range_start, end_time - MAX_LIMIT * interval_ms)
                try:
                    data = await fetch_kline_chunk(client, symbol, interval, chunk_start, end_time)
                    total_chunks += 1
                    print(f"[{symbol} {interval}] Fetched {len(data)} rows from {chunk_start} to {end_time}")
                    inserted, skipped = insert_klines(session, symbol,interval, data)
                    total_lines_inserted += inserted
                    total_lines_skipped += skipped
                except (RequestError, HTTPStatusError) as e:
                    print(f"❌ Failed to fetch chunk: {e}")
                    return {"status": "error", "reason": str(e)}
                end_time = chunk_start - interval_ms
                await asyncio.sleep(0.2)
    return {
        "status": "success",
        "symbol": symbol,
        "interval": interval,
        "total_rows_inserted": total_lines_inserted,
        "total_rows_skipped": total_lines_skipped,
        "total_chunks": total_chunks
    }