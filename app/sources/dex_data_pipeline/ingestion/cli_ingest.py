import typer
import traceback
from web3 import Web3
from app.sources.dex_data_pipeline.chains.arbitrum.dexs.uniswap_v3.extractor import run_extraction
from app.storage.db import SessionLocal

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
                        print(f"[cli] Starting extraction for {pool_address}", flush=True)
                        run_extraction(pool_address, step=5000, days_back=days_back)
                        print("[cli] Extraction completed successfully", flush=True)
                    case _:
                        print(f"[cli] Unsupported DEX: {dex}", flush=True)
            case _:
                print(f"[cli] Unsupported chain: {chain}", flush=True)

    except Exception as e:
        print(f"[cli] Error during extraction: {e}", flush=True)
        traceback.print_exc()
    finally:
        if db:
            db.close()
            print("[cli] Database session closed", flush=True)

def main():
    app()

if __name__ == "__main__":
    main()

    
