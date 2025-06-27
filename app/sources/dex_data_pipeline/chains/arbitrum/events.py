from typing import List
from web3.types import LogReceipt
from app.sources.dex_data_pipeline.chains.arbitrum.client import get_client

w3 = get_client()

def fetch_swap_logs(pool_address: str, from_block: int, to_block: int, swap_topic: str) -> List[LogReceipt]:
    """Fetch Uniswap V3 swap logs from given pool within block range."""
    try:
        logs = w3.eth.get_logs({
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": pool_address,
            "topics": [swap_topic]  # Only swap events
        })
        return logs
    except Exception as e:
        print(f"--[!] Error fetching logs for blocks {from_block}-{to_block}: {e}")
        return []