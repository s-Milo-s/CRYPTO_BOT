from celery import shared_task
from redis import Redis
from redlock import Redlock
import time
from app.storage.db import SessionLocal
from app.storage.models.pools import Pool
from app.sources.dex_data_pipeline.ingestion.schedule_ingest import ingest_pool           # Celery wrapper task
import logging
log = logging.getLogger(__name__)

# ── global Redis lock (only ONE dispatcher may run at a time) ───────────
# ── globals ────────────────────────────────────────────────────────────
LOCKER = Redlock([Redis.from_url("redis://redis:6379/0")])

GLOBAL_LOCK_MS = 5 * 60 * 1000    # 300 000 ms  (← remove the comma!)
STAGGER_SECS   = 180              # 3‑min gap

@shared_task(name="dispatch_all", queue="dispatch", bind=True)
def dispatch_all(self):
    log.info("🔄  Starting global dispatcher…")

    lock = LOCKER.lock("global_ingest_lock", GLOBAL_LOCK_MS)
    log.info(f"🔒  Acquired global lock: {bool(lock)}")
    if not lock:
        log.info("🔒 Another dispatcher is running; skipping.")
        return

    try:
        with SessionLocal() as db:
            pools = (db.query(Pool)
                       .filter(Pool.active.is_(True))
                       .order_by(Pool.last_started.nullsfirst())
                       .all())

        for pool in pools:
            log.info(f"🚀 Launching {pool.chain}/{pool.dex} {pool.pair}")
            result = ingest_pool.apply_async(
                kwargs={
                    "chain": pool.chain,
                    "dex": pool.dex,
                    "pair": pool.pair,
                    "pool_addr": pool.address,
                    "days_back": 1,
                },
                queue="orchestrate",
            )
            try:
                log.info(f"✅ Done {pool.address}")
            except Exception:
                log.exception(f"❌ Failed {pool.address}")

            with SessionLocal() as db:
                db.query(Pool).filter_by(id=pool.id)\
                              .update({"last_started": time.time()})
                db.commit()

            time.sleep(STAGGER_SECS)
    finally:
        LOCKER.unlock(lock)