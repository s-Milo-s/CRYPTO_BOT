import os
os.getenv("ALCHEMY_API_KEY")
ARBITRUM_RPC_URL = "https://arb-mainnet.g.alchemy.com/v2/T8RoKm3pR0HXuejgk4OFDRzEf73zMS6u" 
BASE_RPC_URL = f"https://base-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_API_KEY')}"

POOL_ABI = [
    { "name": "token0", "outputs": [ { "type": "address" } ],
      "inputs": [], "stateMutability": "view", "type": "function"},
    { "name": "token1", "outputs": [ { "type": "address" } ],
      "inputs": [], "stateMutability": "view", "type": "function"},
]

ERC20_DEC_ABI = [
    { "name": "decimals", "outputs": [ { "type": "uint8" } ],
      "inputs": [], "stateMutability": "view", "type": "function"},
    { "name": "symbol", "outputs": [ { "type": "string" } ],
      "inputs": [], "stateMutability": "view", "type": "function"},
]

ROUTER_MAP = {
    "0x1111111254fb6c44bac0bed2854e76f90643097d": "1inch router",
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": "Uniswap V3 router",
    "0x000000000022d473030f116ddee9f6b43ac78ba3": "CowSwap GP-v2",
    # …
}

ARBITRUM_BLOCKS_PER_CALL = 10000
BASE_BLOCKS_PER_CALL = 1500