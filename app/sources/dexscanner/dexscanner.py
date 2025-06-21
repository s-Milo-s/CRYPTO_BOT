import time
import requests
import httpx
import backoff
import logging
import asyncio
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError  # <-- Add this
from app.storage.models import TokenPair, DexPair
from typing import Dict, Any
from collections import defaultdict
from sqlalchemy import insert
from sqlalchemy.engine.row import Row
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import SQLAlchemyError

# --- Configuration ---
RATE_LIMIT_SECONDS = 1
API_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"

# --- Logger ---
logger = logging.getLogger("pair_fetcher")
logger.setLevel(logging.INFO)

# def fetch_and_store_new_dex_pairs(db: Session, rate_limit_per_second: int = RATE_LIMIT_SECONDS):
#     token_pairs = db.query(TokenPair).all()
#     delay = 1 / rate_limit_per_second
#     print(f"token_pairs: {len(token_pairs)}")

#     for token_pair in token_pairs:
#         base = token_pair.base_token
#         quote = token_pair.quote_token
#         search_url = f"{API_SEARCH_URL}/?q={base}/{quote}"
#         print(f"🔎 Query: {base}-{quote}")

#         try:
#             response = requests.get(search_url)
#             if not response.ok:
#                 print(f"❌ Failed to fetch {base}/{quote}")
#                 time.sleep(delay)
#                 continue

#             data = response.json()
#             for result in data.get("pairs", []):
#                 result_base = result["baseToken"]["symbol"]
#                 result_quote = result["quoteToken"]["symbol"]
#                 dex = result["dexId"]
#                 chain = result["chainId"]
#                 pair_address = result["pairAddress"]

#                 new_pair = DexPair(
#                     base_token=result_base,
#                     quote_token=result_quote,
#                     dex=dex,
#                     chain=chain,
#                     pair_address=pair_address,
#                 )
#                 db.add(new_pair)
#                 try: 
#                     db.commit()
#                     print(f"➕ Inserted: {result_base}/{result_quote} on {dex} ({chain})")
#                 except IntegrityError:
#                     db.rollback()
#                     print(f"⚠️ Duplicate skipped: {result_base}/{result_quote} on {dex} ({chain})")

#         except Exception as e:
#             print(f"⚠️ Error on {base}/{quote}: {e}")

#         print(f"⏳ Waiting {delay} seconds before next request...")
#         time.sleep(delay)

#     db.commit()

# # --- Fetch search results from DEX Screener with exponential backoff ---
# @backoff.on_exception(backoff.expo, httpx.RequestError, max_tries=3, jitter=None)
# async def fetch_pair_data(session: httpx.AsyncClient, query: str) -> Dict[str, Any]:
#     response = await session.get(API_SEARCH_URL, params={"q": query})
#     response.raise_for_status()
#     data = response.json()
#     if not data.get("pairs"):
#         logger.warning(f"No pairs found for query: {query}")
#         return {}
#     return data


# def batch_insert_pair_data(rows: List[Row], data: List[Dict[str, Any]]) -> None:
    # seen_addresses = set()
    # batch = []
    # current_timestamp = datetime.utcnow()

    # for row in data:
    #     pair_address = row.get("pairAddress")
    #     if not pair_address or pair_address in seen_addresses:
    #         continue

    #     seen_addresses.add(pair_address)

    #     batch.append({
    #         "pair_address": pair_address,
    #         "timestamp": current_timestamp,
    #         "price_usd": row.get("priceUsd"),
    #         "price_native": row.get("priceNative"),
    #         "liquidity_usd": row.get("liquidity", {}).get("usd"),
    #         "volume_1h": row.get("volume", {}).get("h1"),
    #         "price_change_1h": row.get("priceChange", {}).get("h1"),
    #         "fdv": row.get("fdv"),
    #         "market_cap": row.get("marketCap"),
    #     })

    # if not batch:
    #     logger.info("No new unique records to insert.")
    #     return

    # try:
    #     with engine.begin() as conn:
    #         stmt = insert(pair_data_table).values(batch)
    #         conn.execute(stmt)
    #         logger.info(f"Inserted {len(batch)} unique records into DB.")
    # except SQLAlchemyError as e:
    #     logger.error(f"Database batch insert failed: {e}")
    #     raise


# --- Main fetch function ---
# async def fetch_and_store_all_pairs(db: Session,) -> None:
    # token_pairs = db.query(TokenPair).all()

    # async with httpx.AsyncClient(timeout=10.0) as session:
    #     for token_pair in token_pairs:
    #         query = f"{token_pair.base_token}/{token_pair.quote_token}"
    #         try:
    #             data = await fetch_pair_data(session, query)
    #             if data:
    #                 insert_pair_data(row, data)
    #         except Exception as e:
    #             logger.error(f"Unrecoverable error for query '{query}': {e}")
    #             raise
    #         await asyncio.sleep(RATE_LIMIT_SECONDS)
