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
import logging
import requests
from app.sources.dex_data_pipeline.config.settings import ARBITRUM_RPC_URL
logger = logging.getLogger(__name__)

class BlockClient:
    def __init__(self, w3: Web3):
        self.w3 = w3

    def get_latest_block(self) -> int:
        return self.w3.eth.block_number

    def get_block_timestamp(self, block_number: int) -> int:
        return self.w3.eth.get_block(block_number).timestamp

    def find_block_by_timestamp(self, target_ts: int, start_block: int = 0, end_block: int = None) -> int:
        if end_block is None:
            end_block = self.get_latest_block()

        while start_block <= end_block:
            mid = (start_block + end_block) // 2
            mid_ts = self.get_block_timestamp(mid)

            if mid_ts < target_ts:
                start_block = mid + 1
            elif mid_ts > target_ts:
                end_block = mid - 1
            else:
                return mid
        return start_block

    def walk_block_ranges(self, start: int, end: int, step: int = 1000):
        for i in range(start, end, step):
            yield i, min(i + step - 1, end)

    def compute_missing_block_ranges(
        self,
        db: Session,
        table_name: str,
        days_back: int
    ) -> list[tuple[int, int]]:
        if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
            raise ValueError(f"Unsafe table name: {table_name}")

        row = db.execute(
            text(f"""
                SELECT
                  MIN(EXTRACT(EPOCH FROM minute_start))  AS min_ts,
                  MAX(EXTRACT(EPOCH FROM minute_start))  AS max_ts
                FROM {table_name}
            """)
        ).one()
        have_min_ts = int(row.min_ts) if row.min_ts is not None else None
        have_max_ts = int(row.max_ts) if row.max_ts is not None else None

        want_start_ts = int((datetime.utcnow() - timedelta(days=days_back)).timestamp())
        latest_block = self.get_latest_block()
        gaps = []

        if have_min_ts is None:
            gaps.append((self.find_block_by_timestamp(want_start_ts), latest_block))
            return gaps

        if want_start_ts < have_min_ts:
            gaps.append((
                self.find_block_by_timestamp(want_start_ts),
                self.find_block_by_timestamp(have_min_ts - 60)
            ))

        if have_max_ts < int(time.time()) - 60:
            gaps.append((
                self.find_block_by_timestamp(have_max_ts + 60),
                latest_block
            ))

        return gaps
    
from web3 import Web3
from typing import List, Dict

class BlockTimestampResolver:
    def __init__(self, w3: Web3, num_chunks: int = 5):
        self.w3 = w3
        self.ranges = []  # Stores (start_block, end_block, start_ts, slope)
        self.num_chunks = num_chunks

    def _get_single_block_ts(self, block: int) -> int | None:
        """
        Fallback for a *single* block using the Web3 client already wired into
        this resolver.  Returns unix timestamp or None if the call fails.
        """
        try:
            blk = self.w3.eth.get_block(block, full_transactions=False)
            return blk["timestamp"]
        except Exception as exc:
            logger.warning(f"Web3 rescue call failed for block {block}: {exc}")
            return None


    def batch_get_block_timestamps(self, block_numbers: list[int]) -> dict[int, int]:
        """
        • Builds an “anchor” set {b-1, b, b+1} for every checkpoint (skip –1).  
        • Fetches them in a single JSON-RPC batch (requests).  
        • If either outer edge is still missing, falls back to one-off Web3 calls.  
        • Raises ValueError when *both* start and end anchors cannot be recovered.
        """
        # Anchor expansion  ────────────────────────────────────────────────
        anchors: set[int] = set()
        for b in block_numbers:
            if b > 0:                 # avoid negative heights
                anchors.add(b - 1)
            anchors.add(b)
            anchors.add(b + 1)

        sorted_anchors = sorted(anchors)

        # Batched JSON-RPC (still via requests – Web3 has no batch helper) ─
        payload = [
            {"jsonrpc": "2.0", "method": "eth_getBlockByNumber",
            "params": [hex(b), False], "id": i}
            for i, b in enumerate(sorted_anchors)
        ]
        try:
            r = requests.post(ARBITRUM_RPC_URL, json=payload, timeout=10)
            r.raise_for_status()
            batch_res = r.json()
        except Exception as exc:
            logger.error(f"batch RPC ({len(sorted_anchors)} blocks) failed: {exc}")
            return {}

        ts_map: dict[int, int] = {
            int(item["result"]["number"], 16): int(item["result"]["timestamp"], 16)
            for item in batch_res
            if item.get("result")
        }

        # 2️⃣  Single-block rescue (Web3) ───────────────────────────────────────
        start_blk, end_blk = min(block_numbers), max(block_numbers)

        # left edge
        if all(b not in ts_map for b in (start_blk - 1, start_blk)):
            if (ts := self._get_single_block_ts(start_blk)) is not None:
                ts_map[start_blk] = ts
            else:
                raise ValueError(f"Cannot resolve timestamp for start block {start_blk}")

        # right edge
        if all(b not in ts_map for b in (end_blk, end_blk + 1)):
            if (ts := self._get_single_block_ts(end_blk)) is not None:
                ts_map[end_blk] = ts
            else:
                raise ValueError(f"Cannot resolve timestamp for end block {end_blk}")

        return ts_map

    def build_from_logs(self, logs: List[Dict]):
        """
        Build linear (start_block, end_block, start_ts, slope) segments.

        • Any checkpoint whose RPC reply is `null` is simply ignored.
        • If < 2 checkpoints have usable timestamps we raise – there is no
        way to interpolate a line with just one point.
        """
        if not logs:
            return

        block_nums  = [log["blockNumber"] for log in logs]
        start_block = min(block_nums)
        end_block   = max(block_nums)

        # Skip work if this whole span is already covered
        for s, e, *_ in self.ranges:
            if s <= start_block and end_block <= e:
                return

        # ------------------------------------------------------------------
        # 1. Pick checkpoint blocks (same logic as before).
        # ------------------------------------------------------------------
        step        = max(1, (end_block - start_block) // self.num_chunks)
        checkpoints = list(range(start_block, end_block + 1, step))
        if checkpoints[-1] != end_block:
            checkpoints.append(end_block)

        # 2. Query the node
        block_ts_map = self.batch_get_block_timestamps(checkpoints)

        # 3. Keep only the blocks that actually answered (≠ None)
        #    – ordered list so we can zip neighbours
        avail = sorted(k for k, v in block_ts_map.items() if v is not None)

        # 4. Must have at least two good checkpoints to draw a line
        if len(avail) < 2:
            raise ValueError(
                f"Only {len(avail)} timestamp(s) returned for "
                f"blocks {start_block}…{end_block} – cannot interpolate."
            )

        # 5. Create segments only between **consecutive** good checkpoints
        for b0, b1 in zip(avail, avail[1:]):
            t0, t1 = block_ts_map[b0], block_ts_map[b1]
            slope  = (t1 - t0) / (b1 - b0) if b1 != b0 else 0.0
            self.ranges.append((b0, b1, t0, slope))

    def estimate_timestamp(self, block_number: int) -> int:
        for start, end, ts_start, slope in self.ranges:
            if start <= block_number <= end:
                return int(ts_start + (block_number - start) * slope)
        logger.info(f"Estimating timestamp for block {self.ranges}")
        raise ValueError(f"Block {block_number} not in any cached range")

    def assign_timestamps(self, logs: List[Dict]) -> Dict[int, int]:
        self.build_from_logs(logs)  # build range once
        cache = {}
        for log in logs:
            block_num = log["blockNumber"]
            log["timestamp"] = self.estimate_timestamp(block_num)
            cache[str(block_num)] = log["timestamp"]
        return cache




