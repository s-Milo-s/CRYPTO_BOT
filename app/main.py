# app/main.py
from fastapi import FastAPI
from app.api import api
from app.storage.db import engine
from sqlalchemy import text
import logging
from app.utils.shortname import ShortNameFilter

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger().addFilter(ShortNameFilter())
log = logging.getLogger(__name__)

# include your arbitrage routes
app.include_router(api.router, prefix="/api")

@app.on_event("startup")
def check_db_connection():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            log.info("✅ Database connected.")
    except Exception as e:
        log.error(f"❌ DB connection failed: {e}")
