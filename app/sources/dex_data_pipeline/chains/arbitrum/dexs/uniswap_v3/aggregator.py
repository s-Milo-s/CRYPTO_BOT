from collections import defaultdict
from datetime import datetime
from decimal import Decimal, getcontext
from celery import shared_task, group
from app.celery.celery_app import celery_app
from app.sources.dex_data_pipeline.ingestion.writter import upsert_aggregated_klines
from app.storage.db import get_db
getcontext().prec = 28  # High precision for price math


class Aggregator:
    def __init__(self, dec0: int, dec1: int):
        self.dec0 = Decimal(10) ** dec0          # e.g. 1e6  for USDT
        self.dec1 = Decimal(10) ** dec1          # e.g. 1e18 for WETH
        self.price_scale = Decimal(10) ** (dec0 - dec1)

        self.buckets = defaultdict(lambda: {
            'open_price': None,
            'open_ts': None,
            'close_price': None,
            'close_ts': None,
            'high_price': Decimal('-Infinity'),
            'low_price': Decimal('Infinity'),
            'swap_count': 0,
            'total_base_volume': Decimal(0),
            'total_quote_volume': Decimal(0),
        })

    @staticmethod
    def _minute_key( timestamp: int) -> datetime:
        return datetime.utcfromtimestamp(timestamp).replace(second=0, microsecond=0)

    def _price_raw(self, sqrtPriceX96: int) -> Decimal:
        sqrt_price = Decimal(sqrtPriceX96) / (1 << 96)
        return sqrt_price * sqrt_price  # token1 per token0

    def add(self, swap: dict):
        ts = swap['timestamp']
        price = self._price_raw(swap['sqrtPriceX96']) * self.price_scale
        base_vol = abs(Decimal(swap['amount0'])) / self.dec0  # token0
        quote_vol  = abs(Decimal(swap['amount1'])) / self.dec1  # token1

        minute = self._minute_key(ts)
        bucket = self.buckets[minute]

        # Open price (earliest timestamp)
        if bucket['open_ts'] is None or ts < bucket['open_ts']:
            bucket['open_price'] = price
            bucket['open_ts'] = ts

        # Close price (latest timestamp)
        if bucket['close_ts'] is None or ts > bucket['close_ts']:
            bucket['close_price'] = price
            bucket['close_ts'] = ts

        bucket['high_price'] = max(bucket['high_price'], price)
        bucket['low_price'] = min(bucket['low_price'], price)
        bucket['total_base_volume'] += base_vol
        bucket['total_quote_volume'] += quote_vol
        bucket['swap_count'] += 1

    def aggregate(self):
        result = {}
        for minute, data in self.buckets.items():
            vwap = (data['total_quote_volume'] / data['total_base_volume']) if data['total_base_volume'] else None
            result[minute] = {
                'minute_start': minute,
                'open_price': data['open_price'],
                'high_price': data['high_price'],
                'low_price': data['low_price'],
                'close_price': data['close_price'],
                'avg_price': vwap,
                'swap_count': data['swap_count'],
                'total_base_volume': data['total_base_volume'],
                'total_quote_volume': data['total_quote_volume'],
            }
        return result

    def reset(self):
        self.buckets.clear()

@celery_app.task(name="aggregate_and_upsert")
def aggregate_and_upsert(decoded_chunks, dec0, dec1, table):
    aggregator = Aggregator(dec0, dec1)
    for chunk in decoded_chunks:       # decoded_chunks is 8 lists
        for log in chunk:
            aggregator.add(log)

    minutes = aggregator.aggregate()
    if minutes:
        db = next(get_db())
        try:
            upsert_aggregated_klines(db, table, minutes)
        except Exception as e:
            print(f"Error during upsert: {e}")
            db.rollback()
            raise

