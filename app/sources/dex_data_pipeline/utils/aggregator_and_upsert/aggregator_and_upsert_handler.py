from app.celery.celery_app import celery_app
from app.storage.db import WorkerSessionLocal as SessionLocal
from app.utils.constants import SUPPORTED_CONVERSIONS
import logging
from datetime import datetime, timezone
from app.sources.dex_data_pipeline.utils.aggregator_and_upsert.aggreation.swap_aggregator import SwapAggregator
from app.sources.dex_data_pipeline.utils.aggregator_and_upsert.aggreation.trade_size_aggregator import TradeSizeAggregator
from app.sources.dex_data_pipeline.utils.aggregator_and_upsert.upsert.upsert_aggregated_klines import upsert_aggregated_klines
from app.sources.dex_data_pipeline.utils.aggregator_and_upsert.upsert.upsert_aggregated_trade_sizes import upsert_aggregated_trade_sizes
from app.sources.dex_data_pipeline.utils.aggregator_and_upsert.upsert.upsert_raw_swaps import bulk_insert_swaps
logger = logging.getLogger(__name__)


def get_raw_swaps_table_name_from_kline(kl_table_name: str) -> str:
    if kl_table_name.endswith("_1m_klines"):
        return kl_table_name.replace("_1m_klines", "_raw_swaps")
    raise ValueError(f"Unexpected kline table format: {kl_table_name}")

@celery_app.task(
        name="aggregate_and_upsert_handler",
        queue="aggregate",
        )
def aggregate_and_upsert(decoded_chunks,table,swap_table, quote_pair):
    swap_aggregator = SwapAggregator()
    trade_size_aggregator = TradeSizeAggregator()
    range_logs = [d for sub in decoded_chunks for d in sub]
    for log in range_logs:
        swap_aggregator.add(log)
        if quote_pair in SUPPORTED_CONVERSIONS:
            trade_size_aggregator.add(log)
        log['timestamp']  = datetime.fromtimestamp(log['timestamp'] , tz=timezone.utc)
    minutes = swap_aggregator.aggregate()

    #Upsert Aggs 
    if minutes:
        with SessionLocal() as db:
            upsert_aggregated_klines(db, table, minutes)
            bulk_insert_swaps(db,swap_table, range_logs)
            if quote_pair in SUPPORTED_CONVERSIONS:
                upsert_aggregated_trade_sizes(db, pool_name=table, buckets=trade_size_aggregator.buckets)

            db.commit()

