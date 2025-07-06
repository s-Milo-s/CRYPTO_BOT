from app.sources.dex_data_pipeline.evm.utils.orchestrator import run_evm_orchestration
from app.sources.dex_data_pipeline.config.settings import ARBITRUM_RPC_URL
from app.sources.dex_data_pipeline.evm.arbitrum.dexs.uniswap_v3.decoder import decode_log_chunk
from app.sources.dex_data_pipeline.evm.arbitrum.dexs.uniswap_v3.config import (
    SWAP_TOPIC,
    SWAP_ABI,
)
import logging

log = logging.getLogger(__name__)

def run_uniswap_orchestration(
    pool_address: str,
    chain, 
    dex,
    pair,
    step: int = 5000,
    days_back: int = 1,
):
    """
    Run Uniswap V3 data extraction orchestration.
    """
    log.info(f"Running Uniswap V3 orchestration for pool {pool_address} with step={step} and days_back={days_back}.")
    run_evm_orchestration(
        rpc_url=ARBITRUM_RPC_URL,
        pool_address=pool_address,
        swap_topic=SWAP_TOPIC,
        swap_abi=SWAP_ABI,
        decode_log_chunk_fn=decode_log_chunk,
        chain=chain,
        dex=dex,
        pair=pair,
        days_back=days_back,
        step=step
    )