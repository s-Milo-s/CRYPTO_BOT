INTERVAL_MS = {
    "1s": 1_000,
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
    "1M": 2_592_000_000  # Approx: 30d * 24h * 60m * 60s * 1000ms
}

SYMBOL_REPLACEMENTS = {
    "₮": "t",    # Tether
    "Ξ": "eth",  # ETH symbol
    "Ƀ": "btc",  # Bitcoin symbol
    # Add more as needed
}

STABLECOINS = {"usdc", "usdt", "dai", "busd", "usdp", "tusd"}


WRAPPER_MAP = {
    "weth": "eth",
    "cbeth": "eth",
    "reth": "eth",
    "steth": "eth",
    "wbtc": "btc",
    "tbtc": "btc",
}

SUPPORTED_CONVERSIONS = {
    "usdc", 
    "usdt", 
    "dai", 
    "busd", 
    "usdp", 
    "tusd"
}
