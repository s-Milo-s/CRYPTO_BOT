from app.sources.dex_data_pipeline.chains.arbitrum.client import get_client
from typing import Tuple
import time
from typing import Dict, List
from web3 import Web3
from typing import List, Dict

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
