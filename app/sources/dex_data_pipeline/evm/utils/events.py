from typing import List
from web3 import Web3
from web3.types import LogReceipt
import backoff

@backoff.on_exception(backoff.expo, Exception, max_tries=3)
def fetch_logs(
    w3: Web3,
    pool_address: str,
    from_block: int,
    to_block: int,
    topics: List[str]
) -> List[LogReceipt]:
    """Generic log fetcher for a given address and topics over a block range."""
    try:
        logs = w3.eth.get_logs({
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": pool_address,
            "topics": topics
        })
        return logs
    except Exception as e:
        print(f"--[!] Error fetching logs for blocks {from_block}-{to_block}: {e}")
        return []
