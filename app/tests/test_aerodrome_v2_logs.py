#!/usr/bin/env python3
"""
Show a couple of recent swap logs on Base via Alchemy.

Usage:
  export ALCHEMY_BASE_URL="https://base-mainnet.g.alchemy.com/v2/<YOUR‑KEY>"
  python scripts/show_base_logs.py
Requires:
  pip install web3 eth-abi
"""

import os
from pprint import pprint
from web3 import Web3
from eth_abi import abi

# -------------------------------------------------------------------------
# 1️⃣  Config
# -------------------------------------------------------------------------

RPC_URL = os.getenv("ALCHEMY_BASE_URL")
if not RPC_URL:
    raise SystemExit("❌  Set ALCHEMY_BASE_URL env‑var first.")

w3 = Web3(Web3.HTTPProvider(RPC_URL))

# Very liquid pool that always has recent swaps:
POOL = Web3.to_checksum_address(
    "0xd0b53d9277642d899df5c87a3966a349a798f224"  # USDC/WETH 0.05 % (Uniswap V3) :contentReference[oaicite:0]{index=0}
)

# Uniswap‑V3 Swap event:
# Swap(address indexed sender,
#      address indexed recipient,
#      int256  amount0,
#      int256  amount1,
#      uint160 sqrtPriceX96,
#      uint128 liquidity,
#      int24   tick)
SWAP_SIG = Web3.keccak(
    text="Swap(address,address,int256,int256,uint160,uint128,int24)"
).hex()

# How many latest blocks to scan:
BLOCK_LOOKBACK = 200

# -------------------------------------------------------------------------
# 2️⃣  Fetch logs (auto‑shrinks range if >10 000 logs cap is hit)
# -------------------------------------------------------------------------

latest = w3.eth.block_number
from_block = latest - BLOCK_LOOKBACK
to_block = latest

print(f"⏳  Fetching swap logs for blocks {from_block} → {to_block} …")

logs = w3.eth.get_logs(
    {
        "address": POOL,
        "topics": [SWAP_SIG],
        "fromBlock": from_block,
        "toBlock": to_block,
    }
)

print(f"✅  Got {len(logs)} logs.\n")

# -------------------------------------------------------------------------
# 3️⃣  Decode & show first 5 swaps
# -------------------------------------------------------------------------

def decode_swap(log):
    data_bytes = bytes(log["data"]) 
    (
        amount0,
        amount1,
        sqrtPriceX96,
        liquidity,
        tick,
    ) = abi.decode(
        ["int256", "int256", "uint160", "uint128", "int24"],
        data_bytes,
    )
    return {
        "block": log["blockNumber"],
        "sender": "0x" + log["topics"][1].hex()[-40:],      # indexed
        "recipient": "0x" + log["topics"][2].hex()[-40:],   # indexed
        "amount0": amount0,
        "amount1": amount1,
        "tick": tick,
    }

print("🔎  First 5 decoded swaps:")
for log in logs[:5]:
    pprint(decode_swap(log))
