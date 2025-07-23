from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from app.storage.models.extraction_metrics import extraction_metrics_table
import logging


log = logging.getLogger(__name__)
def extract_pool_slug(table_name: str) -> str:
    """
    "arbitrum_uniswap_v3_wethusdc_5m_klines" → "arbitrum_uniswap_v3_wethusdc"
    """
    return table_name.rsplit("_", 2)[0]


def log_extraction_metrics(
    db: Session,
    block_range: str,
    log_count: int,
    duration_seconds: float,
    table_name: str
):
    insert_stmt = pg_insert(extraction_metrics_table).values(
        block_range=block_range,
        log_count=log_count,
        duration_seconds=round(duration_seconds, 2),
        pool_slug=extract_pool_slug(table_name)
    )
    db.execute(insert_stmt)


