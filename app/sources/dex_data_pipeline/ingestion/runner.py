from app.sources.dex_data_pipeline.chains.arbitrum.dexs.uniswap_v3.extractor import run_extraction
from web3 import Web3
from app.celery.celery_app import celery_app
from app.storage.db import get_db

@celery_app.task
def runner(chain, dex,pool_address):
    try:
        db = next(get_db())
        match chain:
            case "arbitrum":
                match dex:
                    case "uniswap_v3":
                        pool_address = Web3.to_checksum_address(pool_address)
                        run_extraction(pool_address,db=db)
                        print("Uniswap V3 extraction completed successfully")
                        return "Successfully finished Uniswap V3 extraction"
    except Exception as e:
        print(f"Error during extraction: {e}")
        return f"Error: {e}"
    return "Unsupported chain or dex"
    
