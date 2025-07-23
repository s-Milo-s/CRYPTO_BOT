# celery_app.py  ─────────────────────────────────────────────────────────
from celery import Celery
from celery.schedules import crontab               #  ← NEW
from redbeat import RedBeatSchedulerEntry          #  ← NEW
import os
import logging
import logging.config

# ── 1.  Broker / backend  ────────────────────────────────────
CELERY_BROKER_URL    = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

celery_app = Celery(
    "crypto_tasks",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
)

# ── 2.  Core config Beat & routing tweaks ─────────────────────
celery_app.conf.update(
    # serialization (kept)
    task_serializer       ='json',
    result_serializer     ='json',
    accept_content        =['json'],
    timezone              ='UTC',
    enable_utc            =True,

    # --- use RedBeat for persistent schedules
    beat_scheduler        ="redbeat.RedBeatScheduler",

    # --- recycle workers to avoid long‑lived memory creep
    worker_max_tasks_per_child = 20,
)

# ── 3.  Beat schedule – fires the global dispatcher once per hour ───────
celery_app.conf.beat_schedule = {
    "hourly-dispatch": {
        "task": "dispatch_all",
        "schedule": crontab(minute="*/30"),          # top of every hour
        "options": {"queue": "dispatch"},
    }
}

# ── 5.  Logging ────────────────────────────────────────────
LOGGING_CONFIG = {
    "version": 1,
    "formatters": {
        "custom": {
            "format": "[%(asctime)s] [%(levelname)s] %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        }
    },
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "custom"},
    },
    "root": {"level": "INFO", "handlers": ["console"]},
}
celery_app.conf.worker_hijack_root_logger = False
logging.config.dictConfig(LOGGING_CONFIG)

# ── 6.  *Keep* your task modules so Celery registers them ───────────────
import app.sources.dex_data_pipeline.evm.utils.uniswap_v3_decoder
import app.sources.dex_data_pipeline.utils.aggregator_and_upsert.aggregator_and_upsert_handler
import app.sources.dex_data_pipeline.evm.utils.enrich_tx_batch
import app.sources.dex_data_pipeline.ingestion.schedule_ingest
import app.scheduler.dispatcher
