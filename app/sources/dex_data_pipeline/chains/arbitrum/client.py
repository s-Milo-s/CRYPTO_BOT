from web3 import Web3, HTTPProvider
import backoff
import logging

from app.sources.dex_data_pipeline.config.settings import ARBITRUM_RPC_URL
logger = logging.getLogger(__name__)
web3 = None  # Singleton Web3 instance


@backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=None)
def create_web3_client():
    global web3
    logger.info("Connecting to Arbitrum node...")
    w3 = Web3(HTTPProvider(ARBITRUM_RPC_URL, request_kwargs={"timeout": 10}))

    if not w3.is_connected():
        raise ConnectionError("Failed to connect to Arbitrum RPC")

    logger.info("Connected to Arbitrum ✅")
    return w3


def get_client():
    global web3
    if web3 is None:
        web3 = create_web3_client()
    return web3