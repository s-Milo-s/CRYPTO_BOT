from collections import defaultdict
from datetime import datetime
from decimal import Decimal, getcontext
from collections import defaultdict
from decimal import Decimal
import logging
logger = logging.getLogger(__name__)

getcontext().prec = 28  # High precision for price math


class SwapAggregator: 
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
