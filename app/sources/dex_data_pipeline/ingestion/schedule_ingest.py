# tasks/ingest.py
"""
Celery‑side wrapper that launches a single pool‐ingestion run.

It reuses the exact same Typer `runner()` that your CLI calls, so
there’s zero code duplication.  The dispatcher will queue this task on
the **orchestrate** queue, and we pass all parameters as keyword args.
"""

from celery import shared_task
from app.sources.dex_data_pipeline.ingestion.cli_ingest import runner     # ← your existing Typer command
import logging
log = logging.getLogger(__name__)

@shared_task(
    name="ingest_pool",          # must match task_routes pattern
    queue="orchestrate",                      # routed by celery_app.task_routes
    bind=True
)
def ingest_pool(
    self,
    *,
    chain: str,
    dex: str,
    pair: str,
    pool_addr: str,
    days_back: int = 1,
) -> None:
    """
    Launch a full end‑to‑end ingestion for one pool.

    Parameters
    ----------
    chain      : "arbitrum" | "base" | …
    dex        : "uniswap_v3" | "camelot" | …
    pair       : e.g. "ARB/USDC"
    pool_addr  : 0x‑prefixed checksum address
    days_back  : how many days of history to back‑fill (default 1)

    The heavy lifting (block walking, chords, DB writes) is done inside
    the Typer runner → *exactly* the same path you use when invoking
    `python ingest.py run ...` from the command line.
    """
    # Call the Typer command directly (bypasses CLI parsing)
    log.info(f"🔄  Starting ingestion for {chain}/{dex} {pair} at {pool_addr} for {days_back} days back")
    runner(
        chain=chain,
        dex=dex,
        pair=pair,
        pool_address=pool_addr,
        days_back=days_back,
    )
