from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from app.storage.models.trade_size_distribution import TradeSizeDistributionTable
import logging

log = logging.getLogger(__name__)

BUCKET_COLUMN_MAP = {
    -2: "bucket_neg2",
    -1: "bucket_neg1",
     0: "bucket_0",
     1: "bucket_1",
     2: "bucket_2",
     3: "bucket_3",
     4: "bucket_4",
     5: "bucket_5",
     6: "bucket_6",
}

def upsert_aggregated_trade_sizes(
    db: Session, 
    pool_name: str, 
    buckets: dict[int, int]
):
    log.info(f"Inserting/updating trade size distribution for pool: {pool_name}")

    # Build insert values
    row_data = {"pool_name": pool_name}
    for key, column in BUCKET_COLUMN_MAP.items():
        row_data[column] = buckets.get(key, 0)

    table = TradeSizeDistributionTable.__table__
    stmt = pg_insert(table).values(row_data)

    # Build upsert logic
    update_dict = {
        column: table.c[column] + stmt.excluded[column]
        for column in BUCKET_COLUMN_MAP.values()
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=["pool_name"],
        set_=update_dict
    )

    db.execute(stmt)