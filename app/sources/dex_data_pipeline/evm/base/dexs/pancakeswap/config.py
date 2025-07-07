from eth_utils import event_abi_to_log_topic
from web3 import Web3

SWAP_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "internalType": "address", "name": "sender", "type": "address"},
        {"indexed": True, "internalType": "address", "name": "recipient", "type": "address"},
        {"indexed": False, "internalType": "int256", "name": "amount0", "type": "int256"},
        {"indexed": False, "internalType": "int256", "name": "amount1", "type": "int256"},
        {"indexed": False, "internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
        {"indexed": False, "internalType": "uint128", "name": "liquidity", "type": "uint128"},
        {"indexed": False, "internalType": "int24", "name": "tick", "type": "int24"}
    ],
    "name": "Swap",
    "type": "event"
}

SWAP_TOPIC = event_abi_to_log_topic(SWAP_ABI)