from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.storage.db import get_db
# from app.sources.dexscanner import fetch_and_store_new_dex_pairs
from app.sources.binance.binance_klines_source import fetch_all_klines
from app.sources.dex_data_pipeline.ingestion.runner import runner



router = APIRouter()

@router.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI application!"}

# @router.post("/fetch-new-dex-pairs")
# def trigger_fetch(db: Session = Depends(get_db)):
#     fetch_and_store_new_dex_pairs(db)
#     return {"status": "completed"}

@router.post("/fetch-klines")
async def fetch_binance_klines(
    symbol: str = "INJ/USDT",
    interval: str = "15m",
    days_back: int = 30,
    db: Session = Depends(get_db)
):
    result = await fetch_all_klines(symbol, interval, days_back, session=db)
    return result

@router.post("/trigger/ingestion")
async def trigger(
    chain: str = "arbitrum",
    dex: str = "uniswap_v3",
    pool_address: str = "0xc6962004f452bE9203591991D15f6b388e09E8D0",
    days_back: int = 1,
):
    runner(chain, dex,pool_address,days_back)
    return {"status": "Ingestion started"}