# app/utils/sanitize.py
from web3.datastructures import AttributeDict
from hexbytes import HexBytes

def sanitize_log(log):
    """Convert Web3 log to JSON-safe dict."""
    out = {}
    for k, v in dict(log).items():
        if isinstance(v, (bytes, bytearray, HexBytes)):
            out[k] = v.hex()
        elif isinstance(v, AttributeDict):
            out[k] = dict(v)
        else:
            out[k] = v
    return out