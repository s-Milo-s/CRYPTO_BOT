# app/main.py
from fastapi import FastAPI
from app.api import arbitrage
from app.storage.db import engine
from sqlalchemy import text

app = FastAPI()

# include your arbitrage routes
app.include_router(arbitrage.router, prefix="/arbitrage")

@app.on_event("startup")
def check_db_connection():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            print("✅ Database connected.")
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
