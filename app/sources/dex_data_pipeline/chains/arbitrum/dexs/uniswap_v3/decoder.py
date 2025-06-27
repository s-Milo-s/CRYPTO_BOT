from eth_utils import event_abi_to_log_topic
from app.sources.dex_data_pipeline.chains.arbitrum.dexs.uniswap_v3.config import SWAP_ABI
from web3._utils.events import get_event_data
# Calculate the topic to filter logs by
SWAP_TOPIC = event_abi_to_log_topic(SWAP_ABI)

from web3 import Web3
from web3._utils.events import get_event_data
from app.sources.dex_data_pipeline.chains.arbitrum.client import get_client

w3 = get_client()
swap_event_abi = SWAP_ABI

def decode_swap_event(log, block_cache={}):
    event_data = get_event_data(w3.codec, swap_event_abi, log)
    block_number = log['blockNumber']
    if block_number not in block_cache:
        block_cache[block_number] = w3.eth.get_block(block_number)['timestamp']
    return {
        'block_number': log['blockNumber'],
        'timestamp': block_cache[block_number],
        'tx_hash': log['transactionHash'].hex(),
        'log_index': log['logIndex'],
        'sender': event_data['args']['sender'],
        'recipient': event_data['args']['recipient'],
        'amount0': event_data['args']['amount0'],
        'amount1': event_data['args']['amount1'],
        'sqrtPriceX96': event_data['args']['sqrtPriceX96'],
        'liquidity': event_data['args']['liquidity'],
        'tick': event_data['args']['tick'],
    }