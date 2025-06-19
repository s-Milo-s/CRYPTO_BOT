import time
import requests
from sqlalchemy.orm import Session
from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError  # <-- Add this
from app.storage.models import TokenPair, DexPair

def fetch_and_store_new_dex_pairs(db: Session, rate_limit_per_second: int = 1):
    token_pairs = db.query(TokenPair).all()
    delay = 1 / rate_limit_per_second
    print(f"token_pairs: {len(token_pairs)}")

    for token_pair in token_pairs:
        base = token_pair.base_token
        quote = token_pair.quote_token
        search_url = f"https://api.dexscreener.com/latest/dex/search/?q={base}/{quote}"
        print(f"🔎 Query: {base}-{quote}")

        try:
            response = requests.get(search_url)
            if not response.ok:
                print(f"❌ Failed to fetch {base}/{quote}")
                time.sleep(delay)
                continue

            data = response.json()
            for result in data.get("pairs", []):
                result_base = result["baseToken"]["symbol"]
                result_quote = result["quoteToken"]["symbol"]
                dex = result["dexId"]
                chain = result["chainId"]
                pair_address = result["pairAddress"]

                new_pair = DexPair(
                    base_token=result_base,
                    quote_token=result_quote,
                    dex=dex,
                    chain=chain,
                    pair_address=pair_address,
                )
                db.add(new_pair)
                try: 
                    db.commit()
                    print(f"➕ Inserted: {result_base}/{result_quote} on {dex} ({chain})")
                except IntegrityError:
                    db.rollback()
                    print(f"⚠️ Duplicate skipped: {result_base}/{result_quote} on {dex} ({chain})")

        except Exception as e:
            print(f"⚠️ Error on {base}/{quote}: {e}")

        print(f"⏳ Waiting {delay} seconds before next request...")
        time.sleep(delay)

    db.commit()
