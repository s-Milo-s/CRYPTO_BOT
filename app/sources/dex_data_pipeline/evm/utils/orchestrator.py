from celery import chord
from celery import group
from app.utils.log_utils import sanitize_log, chunk_logs
import time
from datetime import datetime, timedelta
from app.sources.dex_data_pipeline.evm.utils.events import fetch_logs
from app.sources.dex_data_pipeline.utils.aggregator_and_upsert.aggregator_and_upsert_handler import aggregate_and_upsert
from app.sources.dex_data_pipeline.evm.utils.token_meta import inspect_pool
from app.storage.db_utils import resolve_table_name
from app.sources.dex_data_pipeline.evm.utils.client import get_web3_client
from app.sources.dex_data_pipeline.evm.utils.blocks import BlockTimestampResolver, BlockClient
from app.sources.dex_data_pipeline.utils.cleaner import delete_price_anomalies_with_retry
from app.sources.dex_data_pipeline.utils.feature_generator import crunch_metrics_for_table
from app.sources.dex_data_pipeline.utils.log_extraction_metrics import log_extraction_metrics
from app.storage.db import SessionLocal
from app.utils.clean_util import clean_symbol
import logging
from app.storage.db_utils import create_table_if_not_exists
log = logging.getLogger(__name__)



def run_evm_orchestration(
        rpc_url: str,
        pool_address: str,
        swap_topic: str,
        swap_abi: dict,
        decode_log_chunk_fn,
        chain: str = "arbitrum",
        dex: str = "uniswap",
        pair: str = "ARB/USDC",
        days_back: int = 1, 
        step: int = 1000
        ) -> None:
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
    w3 = get_web3_client(rpc_url)
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
    token0 = clean_symbol(token0)
    token1 = clean_symbol(token1)
    table_name, swap_table = resolve_table_name(chain, dex, token0, token1, pair)
    base_symbol, quote_pair = pair.upper().split("/")  # normalize case
    token0 = token0.upper()
    token1 = token1.upper()

    if base_symbol == token0:
        base_is_token1 = False
    elif base_symbol == token1:
        base_is_token1 = True
    else:
        raise ValueError(f"Base symbol '{base_symbol}' doesn't match token0 '{token0}' or token1 '{token1}'")
    
    # Check if the aggregation table exists, if not create it.
    if not table_name:
        log.info(f"Table for pair does not exist, creating it now")
        with SessionLocal() as session:
            table_name, swap_table = create_table_if_not_exists(
                session,
                chain,
                dex,
                token1,
                token0,
                base_is_token1
            )
        log.info(f"Created table: {table_name} for pair {token0}/{token1}")
        log.info(f"Created table for wallet_stats: {swap_table} ")

    # we will implement this later it is too translate quote token to USD (we have to pull USD proces by 8h time buckets 
    # very useful for trade size distribution)
    
    # log.info(f"Using table: {table_name} for {token0}/{token1} pair")
    # with SessionLocal() as session:
    #     aggTradeSizes = AggregateTradeSizes(session,quote_pair, days_back=days_back)


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
        log.info(f"Processing gap from {gap_start} to {gap_end}")
        for from_block, to_block in blockClient.walk_block_ranges(gap_start, gap_end, step=step):
            range_time = time.time()
            log.info(f"Processing block range: {from_block} to {to_block}")
            
            raw_logs = fetch_logs(w3, pool_address, from_block, to_block, [swap_topic])
            raw_logs = [sanitize_log(log) for log in raw_logs]
            len_of_logs = len(raw_logs)
            total_logs += len_of_logs
            log.info(f"----Fetched {len_of_logs} logs from blocks {from_block} to {to_block}")
            if not raw_logs:
                continue
            
            # Resolve/create timestamps in‑batch (adds to resolver's cache internally).
            block_cache = ts_resolver.assign_timestamps(raw_logs)

            # Split into chunks to leverage CPU cores / asyncio workers.
            max_workers = 8
            log_chunks = min(max_workers, max(1, (len_of_logs + 99) // 200))
            chunks = chunk_logs(raw_logs, n_chunks=log_chunks)
            log.info(f"------Chunked logs into {len(chunks)} chunks")

            # Build the chord for this range.
            range_chord = chord(
                    header=[ decode_log_chunk_fn.s(chunk, block_cache, swap_abi,
                                                dec0, dec1, base_is_token1)
                            for chunk in chunks ],
                    # body gets: (decoded_chunks, table_name, swap_table, quote_pair_lower)
                    body = aggregate_and_upsert.s(table_name, swap_table, quote_pair.lower())
            )
            range_chord.apply_async()
            # decode_jobs = [
            #     decode_log_chunk_fn.s(chunk, block_cache, swap_abi, dec0, dec1, base_is_token1)
            #     for chunk in chunks
            # ]

            # log.info("Launching decode tasks...")
            # decode_results = group(decode_jobs).apply_async()
            # decoded_chunks = decode_results.get(timeout=300)  # wait + fail loudly if stuck

            # log.info(f"Got decoded result with {len(decoded_chunks)} chunks")

            # # Step 2: Run aggregation manually and wait
            # log.info("Launching aggregation task...")
            # agg_result = aggregate_and_upsert.s(decoded_chunks, table_name, swap_table, quote_pair.lower()).apply_async()
            # agg_result_output = agg_result.get(timeout=300)
            # log.info(agg_result_output)
            # log.info("Aggregation task completed.")
            # result.get()
            range_duration = time.time() - range_time
            log.info(f"----------Chord finished for blocks {from_block} to {to_block} duration: {range_duration:.2f}s")
    # ---------------------------------------------------------------------

    # Cleanup: delete any price anomalies from the aggregated table.
    del_mins = delete_price_anomalies_with_retry(table_name)
    log.info(f"[run_extraction] Deleted {del_mins} price anomalies from {table_name}")

    with SessionLocal() as db:
        crunch_metrics_for_table(db,table_name)
        log.info(f"[run_extraction] Metrics crunching completed for {table_name}")
        duration = time.time() - start_ts
        db.commit()
    with SessionLocal() as db:
        log_extraction_metrics(
            db,
            block_range=f"{start_block}-{end_block}",
            log_count=total_logs,
            duration_seconds=duration
        )
        db.commit()

    # ---------------------------------------------------------------------
    # Finalize: print duration and return.
    log.info(f"[run_extraction] Completed setup in {duration:.2f}s")
    log.info(f"[run_extraction] Total logs processed: {total_logs}")
        