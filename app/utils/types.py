from typing import NamedTuple
from datetime import datetime
from decimal import Decimal

class PricePoint(NamedTuple):
    bucket_start: datetime
    usd_price: Decimal