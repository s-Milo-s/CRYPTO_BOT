from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from app.storage.models.raw_swaps_schema import get_raw_swaps_model_by_name
from typing import List
# ---------------------------------------------------------------------
# Assume you already have an SQLAlchemy engine / session.
# ---------------------------------------------------------------------

def bulk_insert_swaps(session: Session, table_name: str, swaps: List[dict]):
    """Insert raw‑swap dicts into the per‑pool table.

    Any row that violates the (block_number, tx_hash, log_index) UNIQUE
    constraint is ignored; all others commit successfully.
    """
    if not swaps:
        return

    Model = get_raw_swaps_model_by_name(table_name)

    stmt = (
        pg_insert(Model.__table__).values(swaps)
        .on_conflict_do_nothing(
            index_elements=["block_number", "tx_hash", "log_index"]
        )
    )

    session.execute(stmt)