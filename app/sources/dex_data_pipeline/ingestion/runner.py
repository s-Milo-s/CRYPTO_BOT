from app.sources.dex_data_pipeline.chains.arbitrum.dexs.uniswap_v3.extractor import run_extraction
from web3 import Web3
from app.celery.celery_app import celery_app
from app.storage.db import get_db
import traceback

from multiprocessing import Process
from web3 import Web3
import traceback
from app.sources.dex_data_pipeline.chains.arbitrum.dexs.uniswap_v3.extractor import run_extraction
from app.storage.db_utils import get_db

def background_task(chain: str, dex: str, pool_address: str, days_back = 1):
    try:
        db = next(get_db())
        match chain:
            case "arbitrum":
                match dex:
                    case "uniswap_v3":
                        pool_address = Web3.to_checksum_address(pool_address)
                        print(f"[task] Starting extraction for {pool_address}")
                        run_extraction(pool_address, db=db, step=5000, days_back=days_back)
                        print("Uniswap V3 extraction completed successfully")
                    case _:
                        print(f"[task] Unsupported dex: {dex}")
            case _:
                print(f"[task] Unsupported chain: {chain}")
    except Exception as e:
        print(f"[task] Error during extraction: {e}")
        traceback.print_exc()

def runner(chain: str, dex: str, pool_address: str, days_back: int = 1):
    print(f"[runner] Kicking off extraction for {chain}/{dex} pool {pool_address} in background",flush=True)
    p = Process(target=background_task, args=(chain, dex, pool_address, days_back))
    p.start()

    return "Extraction kicked off in background"
    
