from celery import chord
from app.utils.sanitize import sanitize_log
import time
from datetime import datetime, timedelta
from app.sources.dex_data_pipeline.evm.utils.events import fetch_logs
from app.sources.dex_data_pipeline.evm.arbitrum.dexs.uniswap_v3.decoder import chunk_logs, decode_log_chunk
from app.sources.dex_data_pipeline.utils.aggregator import aggregate_and_upsert
from sqlalchemy.orm import Session
from app.sources.dex_data_pipeline.evm.arbitrum.dexs.uniswap_v3.config import SWAP_TOPIC,SWAP_ABI
from app.sources.dex_data_pipeline.config.settings import ARBITRUM_RPC_URL
from app.sources.dex_data_pipeline.evm.utils.token_meta import inspect_pool
from app.storage.db_utils import table_exists_agg
from app.sources.dex_data_pipeline.evm.utils.client import get_web3_client
from app.sources.dex_data_pipeline.evm.utils.blocks import BlockTimestampResolver, BlockClient
from app.sources.dex_data_pipeline.utils.writter import delete_price_anomalies_with_retry
from app.storage.db import SessionLocal
import logging

log = logging.getLogger(__name__)




def uniswap_orchestrator(pool_address: str, days_back: int = 1, step: int = 1000) -> None:
    """End‑to‑end swap log extraction → minute‑level OHLCV aggregation.

    Parameters
    ----------
    pool_address : str
        Uniswap‑style pool address (checksum format).
    days_back : int, default 1
        How many days of history to backfill.
    step : int, default 1_000
        Number of blocks per crawl chunk.
    db : sqlalchemy.orm.Session | None, default None
        The DB session if needed downstream (kept for interface parity).
    """

    start_ts = time.time()
    w3 = get_web3_client(ARBITRUM_RPC_URL)
    blockClient = BlockClient(w3)
    # ---------------------------------------------------------------------
    # Resolve the block range we need to crawl.
    # ---------------------------------------------------------------------
    target_time = datetime.utcnow() - timedelta(days=days_back)
    target_ts = int(target_time.timestamp())

    start_block = blockClient.find_block_by_timestamp(target_ts)
    end_block = blockClient.get_latest_block()

    # ---------------------------------------------------------------------
    # Inspect the pool (symbols, decimals) & verify aggregation table exists.
    # ---------------------------------------------------------------------
    token0, token1, dec0, dec1 = inspect_pool(w3, pool_address)
    table_name = table_exists_agg("arbitrum","uniswap",token0,token1,"1m")

    if not table_name:
        log.error(f"Table for {token0}/{token1} does not exist, skipping extraction")
        return

    # ---------------------------------------------------------------------
    # Instantiate a *single* timestamp resolver so we reuse its internal map
    # across all block ranges. This cuts RPC calls dramatically vs creating
    # a new object each loop.
    # ---------------------------------------------------------------------
    ts_resolver = BlockTimestampResolver(w3)


    log.info(f"Extracting data from blocks {start_block} to {end_block} for pool {pool_address}")

    # Check if we have any gaps in the aggregated data for this pool.
    with SessionLocal() as session:
        gaps = blockClient.compute_missing_block_ranges(session, table_name, days_back)
    if not gaps:
        log.info("[run_extraction] Up-to-date ✔")
        return

    total_logs = 0
    # Celery chord chain that we build incrementally so the tasks execute in
    # the same order as the ranges we crawl.
    for gap_start, gap_end in gaps:
        log.info(f"[run_extraction] Processing gap from {gap_start} to {gap_end}")
        for from_block, to_block in blockClient.walk_block_ranges(gap_start, gap_end, step=step):
            log.info(f"Processing block range: {from_block} to {to_block}")
            
            raw_logs = fetch_logs(w3, pool_address, from_block, to_block, [SWAP_TOPIC])
            raw_logs = [sanitize_log(log) for log in raw_logs]
            total_logs += len(raw_logs)
            log.info(f"----Fetched {len(raw_logs)} logs from blocks {from_block} to {to_block}")
            if not raw_logs:
                continue
            
            # Resolve/create timestamps in‑batch (adds to resolver's cache internally).
            block_cache = ts_resolver.assign_timestamps(raw_logs)

            # Split into chunks to leverage CPU cores / asyncio workers.
            chunks = chunk_logs(raw_logs, n_chunks=8)
            log.info(f"----Chunked logs into {len(chunks)} chunks")

            # Build the chord for this range.
            range_chord = chord(
            header=[
                decode_log_chunk.s(chunk,block_cache, SWAP_ABI, dec0, dec1)
                for chunk in chunks
            ],
            body=aggregate_and_upsert.s(table_name)
            )
            
        # ✅ Launch this chord now before moving to next range
            log.info(f"----Launching chord for blocks {from_block} to {to_block}")
            result = range_chord.apply_async()
            result.get()
            log.info(f"----Chord finished for blocks {from_block} to {to_block}")
    # ---------------------------------------------------------------------

    # Cleanup: delete any price anomalies from the aggregated table.
    del_mins = delete_price_anomalies_with_retry(table_name)
    log.info(f"[run_extraction] Deleted {del_mins} price anomalies from {table_name}")

    # ---------------------------------------------------------------------
    # Finalize: print duration and return.
    duration = time.time() - start_ts
    log.info(f"[run_extraction] Completed setup in {duration:.2f}s")
    log.info(f"[run_extraction] Total logs processed: {total_logs}")
        