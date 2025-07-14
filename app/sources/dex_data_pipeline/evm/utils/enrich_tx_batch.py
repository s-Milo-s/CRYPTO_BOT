# enrich.py  (or wherever you keep the Celery tasks)

import os, requests
from celery import shared_task
from app.sources.dex_data_pipeline.config.settings import ROUTER_MAP

# ────────────────────────────────────────────────────────────────────────────
# Constants ─ tune to taste
# ────────────────────────────────────────────────────────────────────────────
BATCH_SIZE = 100                     # Alchemy hard-limit
RATE_LIMIT = "900/s"                 # keep well under the free-tier 1 000 QPS
RETRIES    = 3                       # exponential back-off handled by Celery

# ────────────────────────────────────────────────────────────────────────────
# Task: enrich_tx_batch
# ────────────────────────────────────────────────────────────────────────────
@shared_task(bind=True,
             queue="enrich",          # put it in its own queue if you like
             rate_limit=RATE_LIMIT,
             max_retries=RETRIES,
             default_retry_delay=3)   # seconds → doubles each retry
def enrich_tx_batch(self, decoded_rows: list[dict],rpc_url) -> list[dict]:
    """
    Parameters
    ----------
    decoded_rows : list[dict]
        Output of `decode_log_chunk_fn`.  Each dict **must** include
        at least {"tx_hash": str, "wallet": str, … }.

    Returns
    -------
    list[dict]
        Same rows with two new keys:
            • 'caller'     – the true EOA that paid gas for the tx
            • 'router_tag' – 'EOA' | known-router label | 'router/agg' | 'missing'
    """
    key = lambda h: h.lower().lstrip("0x")               # internal 64-char hash
    rpc_param  = lambda h: (h := h.lower()) if h.startswith("0x") else "0x" + h
    # ── 1. Collect unique tx-hashes in this chunk ───────────────────────────
    hashes = list({row["tx_hash"].lower() for row in decoded_rows})
    # ── 2. Batch-call eth_getTransactionByHash via Alchemy ─────────────────
    from_map: dict[str, str | None] = {}   # tx_hash → from-address (lowercase)
    for i in range(0, len(hashes), BATCH_SIZE):
        batch = hashes[i : i + BATCH_SIZE]
        payload = [
            {"jsonrpc": "2.0", "id": j, "method": "eth_getTransactionByHash", "params": [rpc_param(h)]}
            for j, h in enumerate(batch)
        ]

        try:
            resp = requests.post(rpc_url, json=payload, timeout=10)
            resp.raise_for_status()
            results = resp.json()
        except Exception as exc:           # network glitch, 5xx, etc.
            raise self.retry(exc=exc)
        for item in results:
            if (res := item.get("result")):               # successful lookup
                k = res["hash"].lower().lstrip("0x")
                v = res["from"].lower()
                from_map[k] = v
            else:
                # could be dropped tx, bad hash, etc.
                from_map[item["id"]] = None

    # ── 3. Enrich each swap row ─────────────────────────────────────────────
    enriched = []
    for row in decoded_rows:
        h       = key(row["tx_hash"])
        caller  = from_map.get(h)                        # None if lookup failed
        sender  = row["sender"].lower()

        # Tag logic ----------------------------------------------------------
        if sender in ROUTER_MAP:                     # known router / aggregator
            tag = ROUTER_MAP[sender]                # e.g. "Uniswap V3 router"
        elif caller == sender:                       # user hit pool directly
            tag = "EOA"
        else:                                        # unknown contract path
            tag = "router/agg"        

        row["caller"]     = caller
        row["router_tag"] = tag
        enriched.append(row)
    return enriched
