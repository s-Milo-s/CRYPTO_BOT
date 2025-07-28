#!/usr/bin/env python3
"""
Grab recent Aerodrome swaps (V2 & V3) and count how many of each.
"""
import os, sys
from collections import Counter
from pprint import pprint

from eth_abi import abi
from web3 import Web3

RPC = os.getenv("ALCHEMY_BASE_URL") or sys.exit("set ALCHEMY_BASE_URL")
w3 = Web3(Web3.HTTPProvider(RPC))

POOL = Web3.to_checksum_address("0xafb62448929664bfccb0aae22f232520e765ba88")

SIG_V3 = Web3.keccak(
    text="Swap(address,address,int256,int256,uint160,uint128,int24)"
).hex()
SIG_V2 = Web3.keccak(
    text="Swap(address,uint256,uint256,uint256,uint256,address)"
).hex()

LATEST, LOOKBACK = w3.eth.block_number, 2_000
logs = w3.eth.get_logs(
    {
        "address": POOL,
        "topics": [[SIG_V3, SIG_V2]],  # either sig
        "fromBlock": LATEST - LOOKBACK,
        "toBlock": LATEST,
    }
)

print(f"Fetched {len(logs)} swap logs from last {LOOKBACK} blocks\n")

counts = Counter()
decoded_examples = []           # stash first few for display

for log in logs:
    sig = log["topics"][0].hex()

    if sig == SIG_V3:
        counts["V3"] += 1
        if len(decoded_examples) < 5:
            amt0, amt1, _, _, tick = abi.decode(
                ["int256", "int256", "uint160", "uint128", "int24"],
                bytes(log["data"]),
            )
            decoded_examples.append(
                {
                    "type": "V3",
                    "block": log["blockNumber"],
                    "sender": "0x" + log["topics"][1].hex()[-40:],
                    "amt0": amt0,
                    "amt1": amt1,
                    "tick": tick,
                }
            )
    else:  # V2
        counts["V2"] += 1
        if len(decoded_examples) < 5:
            a0in, a1in, a0out, a1out = abi.decode(
                ["uint256"] * 4, bytes(log["data"])
            )
            decoded_examples.append(
                {
                    "type": "V2",
                    "block": log["blockNumber"],
                    "sender": "0x" + log["topics"][1].hex()[-40:],
                    "a0_in": a0in,
                    "a1_in": a1in,
                    "a0_out": a0out,
                    "a1_out": a1out,
                }
            )

print("🔎 First few decoded swaps:")
pprint(decoded_examples)

print("\n📊  Totals in this window:")
print(f"   V2 swaps: {counts['V2']}")
print(f"   V3 swaps: {counts['V3']}")

