
from collections import defaultdict
from decimal import Decimal, getcontext
from app.storage.db import WorkerSessionLocal as SessionLocal
import math
from collections import defaultdict
from decimal import Decimal

getcontext().prec = 28  # High precision for price math

class TradeSizeAggregator:
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