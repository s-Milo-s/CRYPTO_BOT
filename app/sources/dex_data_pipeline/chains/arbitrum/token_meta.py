from web3 import Web3
from functools import lru_cache
from app.sources.dex_data_pipeline.config.settings import POOL_ABI, ERC20_DEC_ABI, ARBITRUM_RPC_URL

w3 = Web3(Web3.HTTPProvider(ARBITRUM_RPC_URL))

@lru_cache(maxsize=None)
def get_token_meta(token_addr: str):
    token = w3.eth.contract(address=token_addr, abi=ERC20_DEC_ABI)
    return {
        "decimals": token.functions.decimals().call(),
        "symbol":   token.functions.symbol().call()
    }

def inspect_pool(pool_addr: str):
    pool = w3.eth.contract(address=pool_addr, abi=POOL_ABI)
    token0 = pool.functions.token0().call()
    token1 = pool.functions.token1().call()

    meta0 = get_token_meta(token0)
    meta1 = get_token_meta(token1)

    print("Pool:", pool_addr)
    print(f" token0: {token0}  symbol={meta0['symbol']}  decimals={meta0['decimals']}")
    print(f" token1: {token1}  symbol={meta1['symbol']}  decimals={meta1['decimals']}")
    return (meta0['symbol'], meta1['symbol'], meta0['decimals'], meta1['decimals'])
