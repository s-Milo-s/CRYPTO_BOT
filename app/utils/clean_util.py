import unicodedata
import re
from app.utils.constants import SYMBOL_REPLACEMENTS

def clean_symbol(symbol: str) -> str:
    for weird_char, replacement in SYMBOL_REPLACEMENTS.items():
        symbol = symbol.replace(weird_char, replacement)

    normalized = unicodedata.normalize("NFKD", symbol)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r'[^a-zA-Z0-9]', '', ascii_only)
    return cleaned.lower()