from app.sources.dex_data_pipeline.chains.arbitrum.client import get_client
from typing import Tuple
import time
from typing import Dict, List
from web3 import Web3
from typing import List, Dict
from datetime import datetime, timedelta
import time
import re
from sqlalchemy import text
from sqlalchemy.orm import Session

w3 = get_client()

def get_latest_block() -> int:
    return w3.eth.block_number

def get_block_timestamp(block_number: int) -> int:
    block = w3.eth.get_block(block_number)
    return block.timestamp

def find_block_by_timestamp(target_ts: int, start_block: int = 0, end_block: int = None) -> int:
    """Binary search for block closest to given timestamp."""
    if end_block is None:
        end_block = get_latest_block()

    while start_block <= end_block:
        mid = (start_block + end_block) // 2
        mid_ts = get_block_timestamp(mid)

        if mid_ts < target_ts:
            start_block = mid + 1
        elif mid_ts > target_ts:
            end_block = mid - 1
        else:
            return mid

    return start_block  # Closest after the timestamp

def walk_block_ranges(start: int, end: int, step: int = 1000) -> Tuple[int, int]:
    """Yield block ranges in chunks"""
    for i in range(start, end, step):
        yield i, min(i + step - 1, end)

def compute_missing_block_ranges(
    db: Session,
    table_name: str,
    days_back: int,
) -> list[tuple[int, int]]:
    """
    Decide which (start_block, end_block) ranges still need ingestion.

    Returns
    -------
    List[Tuple[start_block, end_block]]
        · empty list  -> nothing new to pull
        · one range   -> either early gap OR late gap
        · two ranges  -> early + late gaps
    """
    if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        raise ValueError(f"Unsafe table name: {table_name}")

    # --- 1️⃣ current min/max minute_start in the table -------------------
    row = db.execute(
        text(f"""
            SELECT
              MIN(EXTRACT(EPOCH FROM minute_start))  AS min_ts,
              MAX(EXTRACT(EPOCH FROM minute_start))  AS max_ts
            FROM {table_name}
        """)
    ).one()
    have_min_ts, have_max_ts = (
        int(row.min_ts) if row.min_ts is not None else None,
        int(row.max_ts) if row.max_ts is not None else None,
    )

    # --- 2️⃣ user-requested backfill window ------------------------------
    want_start_ts = int((datetime.utcnow() - timedelta(days=days_back)).timestamp())
    latest_block  = get_latest_block()

    gaps: list[tuple[int, int]] = []

    if have_min_ts is None:
        # table empty → pull entire span
        gaps.append((find_block_by_timestamp(want_start_ts), latest_block))
        return gaps

    # early gap (history older than we already have)
    if want_start_ts < have_min_ts:
        gaps.append((
            find_block_by_timestamp(want_start_ts),
            find_block_by_timestamp(have_min_ts - 60)   # one minute before first row
        ))

    # late gap (new data since last ingest)
    chain_now_ts = int(time.time())
    if have_max_ts < chain_now_ts - 60:                # 1-minute safety buffer
        gaps.append((
            find_block_by_timestamp(have_max_ts + 60),
            latest_block
        ))

    return gaps

class BlockTimestampResolver:
    def __init__(self):
        self.w3 = get_client()
        self.ranges = []  # Stores (start_block, end_block, start_ts, slope)

    def build_from_logs(self, logs: List[Dict]):
        if not logs:
            return

        block_nums = [log["blockNumber"] for log in logs]
        start_block = min(block_nums)
        end_block = max(block_nums)

        # Avoid rebuilding if already covered
        for start, end, *_ in self.ranges:
            if start <= start_block and end_block <= end:
                return

        header_start = self.w3.eth.get_block(start_block, full_transactions=False)
        header_end = self.w3.eth.get_block(end_block, full_transactions=False)

        ts_start = header_start["timestamp"]
        ts_end = header_end["timestamp"]
        num_blocks = end_block - start_block
        if num_blocks == 0:
            slope = 0.0
        else:
            slope = (ts_end - ts_start) / num_blocks  # seconds per block

        self.ranges.append((start_block, end_block, ts_start, slope))

    def estimate_timestamp(self, block_number: int) -> int:
        for start, end, ts_start, slope in self.ranges:
            if start <= block_number <= end:
                return int(ts_start + (block_number - start) * slope)
        raise ValueError(f"Block {block_number} not in any cached range")

    def assign_timestamps(self, logs: List[Dict]) -> Dict[int, int]:
        self.build_from_logs(logs)  # build range once
        cache = {}
        for log in logs:
            block_num = log["blockNumber"]
            log["timestamp"] = self.estimate_timestamp(block_num)
            cache[str(block_num)] = log["timestamp"]
        return cache



