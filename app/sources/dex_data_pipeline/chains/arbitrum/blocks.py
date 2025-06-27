from app.sources.dex_data_pipeline.chains.arbitrum.client import get_client
from typing import Tuple
import time

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
