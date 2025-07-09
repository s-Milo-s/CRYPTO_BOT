import os
os.getenv("ALCHEMY_API_KEY")
ARBITRUM_RPC_URL = "https://arb-mainnet.g.alchemy.com/v2/T8RoKm3pR0HXuejgk4OFDRzEf73zMS6u" 
BASE_RPC_URL = f"https://base-mainnet.g.alchemy.com/v2/{os.getenv("ALCHEMY_API_KEY")}"

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

BLOCKS_PER_CALL = 10000