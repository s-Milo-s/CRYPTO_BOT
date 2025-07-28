import typer
from web3 import Web3
from app.sources.dex_data_pipeline.evm.arbitrum.dexs.uniswap_v3.runner import run_uniswap_orchestration
from app.sources.dex_data_pipeline.evm.arbitrum.dexs.camelot.runner import run_camelot_orchestration
from app.sources.dex_data_pipeline.evm.base.dexs.uniswap_v3.runner import run_base_uniswap_orchestration
from app.sources.dex_data_pipeline.evm.base.dexs.pancakeswap.runner import run_base_pancakeswap_orchestration
from app.sources.dex_data_pipeline.evm.base.dexs.aerodrome.runner import run_base_aerodrome_orchestration
from app.storage.db import SessionLocal
from app.sources.dex_data_pipeline.config.settings import ARBITRUM_BLOCKS_PER_CALL, BASE_BLOCKS_PER_CALL
import logging

log = logging.getLogger(__name__)

app = typer.Typer(help="Run DEX data extraction from CLI")

@app.command("run")
def runner(
    chain: str = typer.Option(..., help="e.g. arbitrum"),
    dex: str = typer.Option(..., help="e.g. uniswap_v3"),
    pair: str = typer.Option(..., help="e.g. ARB/USDC"),
    pool_address: str = typer.Option(..., help="0x..."),
    days_back: int = typer.Option(1, help="How many days to back-fill"),
):
    """
    Run extractor inline (long running job, run via CLI or subprocess).
    """
    db = None
    try:
        db = SessionLocal()
        chain = chain.lower()
        dex = dex.lower()
        pool_address = Web3.to_checksum_address(pool_address)

        match chain:
            case "arbitrum":
                match dex:
                    case "uniswap_v3":
                        log.info(f"[cli] Starting extraction for uniswap {pool_address}")
                        run_uniswap_orchestration(pool_address, chain, dex, pair, step=ARBITRUM_BLOCKS_PER_CALL, days_back=days_back)
                        log.info("[cli] Extraction completed successfully")
                    case "camelot":
                        log.info(f"[cli] Starting extraction for comelot {pool_address}")
                        run_camelot_orchestration(pool_address, chain, dex, pair, step=ARBITRUM_BLOCKS_PER_CALL, days_back=days_back)
                        log.info("[cli] Extraction completed successfully")

                    case _:
                        log.info(f"[cli] Unsupported DEX: {dex}")
            case "base":
                match dex:
                    case "uniswap_v3": 
                        log.info(f"[cli] Starting extraction for Uniswap_v3 {pool_address}")
                        run_base_uniswap_orchestration(pool_address, chain, dex, pair, step=BASE_BLOCKS_PER_CALL, days_back=days_back)
                        log.info("[cli] Extraction completed successfully")
                    case "pancakeswap":
                        log.info(f"[cli] Starting extraction for PancakeSwap {pool_address}")
                        run_base_pancakeswap_orchestration(pool_address, chain, dex, pair, step=BASE_BLOCKS_PER_CALL, days_back=days_back)
                        log.info("[cli] Extraction completed successfully")
                    case "aerodrome":
                        log.info(f"[cli] Starting extraction for Aerodrome {pool_address}")
                        run_base_aerodrome_orchestration(pool_address, chain, dex, pair, step=BASE_BLOCKS_PER_CALL, days_back=days_back)
                        log.info("[cli] Extraction completed successfully")
                    case _:
                        log.info(f"[cli] Unsupported DEX: {dex}")
      
            case _:
                log.info(f"[cli] Unsupported chain: {chain}")

    except Exception as e:
        log.error("Extraction failed", exc_info=True)
    finally:
        if db:
            db.close()
            log.info("[cli] Database session closed")

def main():
    app()

if __name__ == "__main__":
    main()

    
