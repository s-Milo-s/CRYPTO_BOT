from sqlalchemy.dialects.postgresql import insert as pg_insert
import sqlalchemy as sa
from sqlalchemy.orm import Session
from typing import Dict 
from app.storage.models.dex_swap_aggregator_schema import get_aggregate_model_by_name
from sqlalchemy import func
import logging


log = logging.getLogger(__name__)

def upsert_aggregated_klines(
    db: Session,
    table_name,
    new_rows: Dict
):
    """
    Upsert minute-aggregated kline data via one SQL statement.
    
    Args:
        db (Session): SQLAlchemy session.
        table_name (str): Name of the table model.
        new_rows (Dict): Mapping of minute_start → row dict.
    """
    model = get_aggregate_model_by_name(table_name)
    table = model.__table__
    
    log.info(f"--------Upserting aggregated klines {len(new_rows)}")

    # Convert dict-of-dicts into list-of-dicts
    to_upsert = [
        {'minute_start': minute, **row_dict}
        for minute, row_dict in new_rows.items()
    ]   

    stmt = pg_insert(table).values(to_upsert)

    stmt = stmt.on_conflict_do_update(
        index_elements=['minute_start'],
        set_={
            'open_price': sa.case(
                (table.c.open_ts <= stmt.excluded.open_ts, table.c.open_price),
                    else_=stmt.excluded.open_price
            ),
            'open_ts': sa.func.LEAST(table.c.open_ts, stmt.excluded.open_ts),
            'close_price': sa.case(
                (table.c.close_ts >= stmt.excluded.close_ts, table.c.close_price),
                else_=stmt.excluded.close_price
            ),
            'close_ts': sa.func.GREATEST(table.c.close_ts, stmt.excluded.close_ts),
            'high_price': sa.func.GREATEST(table.c.high_price, stmt.excluded.high_price),
            'low_price': sa.func.LEAST(table.c.low_price, stmt.excluded.low_price),
            'swap_count': table.c.swap_count + stmt.excluded.swap_count,
            'total_base_volume': table.c.total_base_volume + stmt.excluded.total_base_volume,
            'total_quote_volume': table.c.total_quote_volume + stmt.excluded.total_quote_volume,
            'avg_price': func.coalesce(
                (table.c.total_base_volume + stmt.excluded.total_base_volume) /
                func.nullif((table.c.total_quote_volume + stmt.excluded.total_quote_volume), 0),
                0
            )
        }
    )

    db.execute(stmt)
    log.info(f"----------Finished upserting {len(to_upsert)} rows into {table_name}")