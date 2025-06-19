from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.storage.db import get_db
from app.sources.dexscanner import fetch_and_store_new_dex_pairs

router = APIRouter()

@router.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI application!"}


router = APIRouter()

@router.post("/fetch-new-dex-pairs")
def trigger_fetch(db: Session = Depends(get_db)):
    fetch_and_store_new_dex_pairs(db)
    return {"status": "completed"}

# @router.get("/refresh-all")
# def refresh_all():
#     results = {}
#     for base, quote in token_pairs:
#         key = f"{base}-{quote}"
#         results[key] = fetch_dex_chain_pairs(base, quote)
#         time.sleep(1)  # crude rate-limiting
#     return JSONResponse(content=results)


# @router.get("/refresh/{base}/{quote}")
# def refresh_single_pair(base: str, quote: str):
#     results = fetch_dex_chain_pairs(base.upper(), quote.upper())
#     return JSONResponse(content={f"{base.upper()}-{quote.upper()}": results})