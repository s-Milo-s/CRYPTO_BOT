import typer
import traceback
from web3 import Web3
from app.sources.dex_data_pipeline.evm.arbitrum.dexs.uniswap_v3.orchestrator import uniswap_orchestrator
from app.storage.db import SessionLocal
import logging

log = logging.getLogger(__name__)

app = typer.Typer(help="Run DEX data extraction from CLI")

@app.command("run")
def runner(
    chain: str = typer.Option(..., help="e.g. arbitrum"),
    dex: str = typer.Option(..., help="e.g. uniswap_v3"),
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

        match chain:
            case "arbitrum":
                match dex:
                    case "uniswap_v3":
                        pool_address = Web3.to_checksum_address(pool_address)
                        log.info(f"[cli] Starting extraction for {pool_address}")
                        uniswap_orchestrator(pool_address, step=5000, days_back=days_back)
                        log.info("[cli] Extraction completed successfully")
                    case _:
                        log.info(f"[cli] Unsupported DEX: {dex}")
            case _:
                log.info(f"[cli] Unsupported chain: {chain}")

    except Exception as e:
        log.info(f"[cli] Error during extraction: {e}")
    finally:
        if db:
            db.close()
            log.info("[cli] Database session closed")

def main():
    app()

if __name__ == "__main__":
    main()

    
