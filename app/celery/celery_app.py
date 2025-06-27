from celery import Celery
import os
import logging
import logging.config

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

celery_app = Celery(
    "crypto_tasks",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
)

LOGGING_CONFIG = {
    "version": 1,
    "formatters": {
        "custom": {
            "format": "[%(asctime)s] [%(levelname)s] %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "custom",
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"]
    },
}

# Tell Celery not to override root logger
celery_app.conf.worker_hijack_root_logger = False
logging.config.dictConfig(LOGGING_CONFIG)

import app.sources.dex_data_pipeline.ingestion.runner
