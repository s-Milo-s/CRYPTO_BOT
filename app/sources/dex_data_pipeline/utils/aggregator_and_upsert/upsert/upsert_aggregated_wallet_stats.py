from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
import logging
from app.storage.models.wallet_tracker_schema import get_wallet_stats_model_by_name
from sqlalchemy import text

log = logging.getLogger(__name__)

def upsert_aggregated_wallet_stats(
    db: Session,
    table_name: str,
    wallet_rows: list[dict]
):
    """
    Upsert wallet-level stats into the given pool-specific wallet stats table.
    Each row is the output of WalletStatsAggregator.results().
    """
    if not wallet_rows:
        return

    log.info(f"Upserting wallet stats into table: {table_name} ({len(wallet_rows)} rows)")

    insert_rows = [
        {
            "wallet_address": row["wallet"],
            "volume_usd": row["volume_usd"],
            "pnl_usd": row["pnl_usd"],
            "return_sum": row["return_sum"],
            "return_squared_sum": row["return_squared_sum"],
            "num_returns": row["num_returns"],
            "open_position_usd": row["open_position_usd"],
            "cost_basis_usd": row["cost_basis_usd"],
        }
        for row in wallet_rows
    ]

    # Dynamically resolve SQLAlchemy model and table
    Model = get_wallet_stats_model_by_name(table_name)
    table = Model.__table__

    stmt = pg_insert(table).values(insert_rows)

    stmt = stmt.on_conflict_do_update(
        index_elements=["wallet_address"],
        set_={
            "volume_usd": stmt.excluded.volume_usd + table.c.volume_usd,
            "pnl_usd": stmt.excluded.pnl_usd + table.c.pnl_usd,
            "return_sum": stmt.excluded.return_sum + table.c.return_sum,
            "return_squared_sum": stmt.excluded.return_squared_sum + table.c.return_squared_sum,
            "num_returns": stmt.excluded.num_returns + table.c.num_returns,
            "open_position_usd": stmt.excluded.open_position_usd + table.c.open_position_usd,
            "cost_basis_usd": stmt.excluded.cost_basis_usd + table.c.cost_basis_usd,
            "last_updated": text("NOW()")
        }
    )

    db.execute(stmt)