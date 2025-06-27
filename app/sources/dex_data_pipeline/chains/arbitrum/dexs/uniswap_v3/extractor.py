from app.sources.dex_data_pipeline.chains.arbitrum.blocks import (
    find_block_by_timestamp,
    get_latest_block,
    walk_block_ranges,
)
from datetime import datetime, timedelta
from app.sources.dex_data_pipeline.chains.arbitrum.events import fetch_swap_logs
from app.sources.dex_data_pipeline.chains.arbitrum.dexs.uniswap_v3.decoder import decode_swap_event
from app.sources.dex_data_pipeline.chains.arbitrum.dexs.uniswap_v3.aggregator import Aggregator
from app.sources.dex_data_pipeline.ingestion.writter import upsert_aggregated_klines
from sqlalchemy.orm import Session
from app.sources.dex_data_pipeline.chains.arbitrum.dexs.uniswap_v3.config import SWAP_TOPIC
from app.sources.dex_data_pipeline.chains.arbitrum.token_meta import inspect_pool
from app.storage.db_utils import table_exists_agg

def run_extraction(pool_address: str, days_back: int = 1, step: int = 1000, db: Session = None) -> None:
    """
    Orchestrates extraction and aggregation:
    - Computes block range from `days_back`
    - Iterates over block chunks
    - Fetches and decodes swap logs
    - Aggregates swaps into minute OHLCV
    """
    target_time = datetime.utcnow() - timedelta(days=days_back)
    target_ts = int(target_time.timestamp())

    start_block = find_block_by_timestamp(target_ts)
    end_block = get_latest_block()
    token0, token1, dec0, dec1 = inspect_pool(pool_address)
    table_name = table_exists_agg("arbitrum","uniswap",token0,token1,"1m")
    if not table_name:
        print(f"Table for {token0}/{token1} does not exist, skipping extraction")
        return
    aggregator = Aggregator(dec0, dec1)
    block_cache = {}
    print(f"Extracting data from blocks {start_block} to {end_block} for pool {pool_address}")
    for from_block, to_block in walk_block_ranges(start_block, end_block, step=step):
        print(f"Processing block range: {from_block} to {to_block}")
        logs = fetch_swap_logs(pool_address, from_block, to_block, SWAP_TOPIC)
        print(f"----Fetched {len(logs)} logs from blocks {from_block} to {to_block}")
        for log in logs:
            decoded = decode_swap_event(log,block_cache=block_cache)
            aggregator.add(decoded)
        aggregat_minutes = aggregator.aggregate()
        print(f"------Aggregated number of minutes: {len(aggregat_minutes) if aggregat_minutes else 'No data'}")
        if aggregat_minutes:
            upsert_aggregated_klines(db, table_name, aggregat_minutes)

        
        # Next step is to take these minutes and save wi
        