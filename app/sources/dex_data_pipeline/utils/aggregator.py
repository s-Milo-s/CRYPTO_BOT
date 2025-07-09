from collections import defaultdict
from datetime import datetime
from decimal import Decimal, getcontext
from celery import shared_task, group
from app.celery.celery_app import celery_app
from app.sources.dex_data_pipeline.utils.writter import upsert_aggregated_klines, insert_or_update_trade_size_distribution
from app.storage.db import SessionLocal
import math
from collections import defaultdict
from decimal import Decimal
from app.utils.constants import SUPPORTED_CONVERSIONS
import logging
logger = logging.getLogger(__name__)

getcontext().prec = 28  # High precision for price math


class Aggregator: 
    def __init__(self):

        # Initialize buckets for each minute
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


    def add(self, swap: dict):
        ts = swap['timestamp']
        price = swap['price'] 
        base_vol = swap['base_vol']  # token0
        quote_vol  = swap['quote_vol']  # token1

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

class TradeSizeDistribution:
    def __init__(self):
        # bucket_key → count
        self.buckets = defaultdict(int)

    @staticmethod
    def _bucket_key(quote_vol: Decimal) -> int:
        """Returns log10 floor bucket key. E.g., $151 → 2 (100-999)"""
        if quote_vol <= 0:
            return -999  # handle edge case with special bucket
        return int(math.floor(math.log10(float(quote_vol))))

    def add(self, swap: dict):
        quote_vol = swap["quote_vol"]
        key = self._bucket_key(quote_vol)
        if key > 6 or key < -2:
            return
        self.buckets[key] += 1

@celery_app.task(name="aggregate_and_upsert")
def aggregate_and_upsert(decoded_chunks,table,quote_pair):
    aggregator = Aggregator()
    dist = TradeSizeDistribution()
    for chunk in decoded_chunks:       # decoded_chunks is 8 lists
        for log in chunk:
            aggregator.add(log)
            if quote_pair in SUPPORTED_CONVERSIONS:
                dist.add(log)

    minutes = aggregator.aggregate()
    if minutes:
        with SessionLocal() as db:
            upsert_aggregated_klines(db, table, minutes)
            if quote_pair in SUPPORTED_CONVERSIONS:
                insert_or_update_trade_size_distribution(db, pool_name=table, buckets=dist.buckets)
            db.commit()

