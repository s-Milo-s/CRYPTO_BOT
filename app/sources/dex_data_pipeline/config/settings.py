ARBITRUM_RPC_URL = "https://arb1.arbitrum.io/rpc" 

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

