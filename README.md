# Wallet‑Watchers Ingestion Pipeline
High‑speed pipeline ingesting DEX swaps and computing wallet‑level metrics for quantitative analysis

# 🚀 Quick Start (Docker Compose)

1. Clone & enter repo
git clone https://github.com/YOUR_ORG/wallet‑watchers‑ingest.git
cd wallet‑watchers‑ingest

2. Copy env template and add your secrets
cp .env.example .env
  └─ fill DATABASE_URL, CELERY_BROKER_URL, CELERY_RESULT_BACKEND, ALCHEMY_API_KEY

3. Launch services (Postgres, Redis, FastAPI, Celery workers …)
docker compose up -d

4. Trigger a 90‑day back‑fill of BRETT/WETH on Base
curl -X POST \
  "http://localhost:8000/api/trigger/ingestion?chain=base&dex=aerodrome&pair=BRETT%2FWETH&pool_address=0x4e829f8a5213c42535ab84aa40bd4adcce9cba02&days_back=90"

Note: All valid chain / dex / pair combinations are listed in cli_ingest.py.

# ⚙️ Architecture
<details> 
## ⚙️ Architecture
<details>
<summary>ASCII diagram (click to expand)</summary>

```text
                                              +-----------------+
                                              |   Alchemy Node  |
                                              +-----------------+
                                                    ^     |
                                                    |     |  eth_getLogs
                               refresh quote prices |     |
+-----------------+                                 |     |
|   Binance API   |---------------------------------+     |
+-----------------+                                       |
                                                          |  eth_getTransactionByHash
                                                          |
                   +---------------------------+          |
                   |  FastAPI (CLI launcher)   |          |
                   +---------------------------+          |
                               |                          |
                               v                          |
            +-----------------------------------+         |
            |   Find pool & start‑for‑loop      |          |
            +-----------------------------------+          |
                               |                           |
                               v                           |
            +-----------------------------------+<---------+
            | Log‑fetch / Block‑time cache      |
            +-----------------------------------+
                         |            |\
     fan‑out to decoders |            | \  (parallel Celery workflow)
                         v            v  \
        +--------------------+  +--------------------+  +--------------------+
        |   Celery Decoder   |  |   Celery Decoder   |  |   Celery Decoder   |
        +--------------------+  +--------------------+  +--------------------+
                |                     |                       |
                v                     v                       v
        +--------------------+  +--------------------+  +--------------------+
        | Celery Enrichment  |  | Celery Enrichment  |  | Celery Enrichment  |
        +--------------------+  +--------------------+  +--------------------+
                \______________   _________|___________   __________________/
                               \ /                     \ /
                                v                       v
                      +---------------------------------------+
                      |     Aggregator  ➜  upsert swaps       |
                      +---------------------------------------+
                                        |
                                        v
                              +---------------------+
                              |   Postgres DB       |
                              +---------------------+
                                        ^
                                        |
                              +---------------------+
                              |   Express Server    |
                              +---------------------+
                                        ^
                                        |
                              +---------------------+
                              | React/Vite Frontend |
                              +---------------------+
```

</details>

# ✨ Feature Highlights
High‑throughput ingestion — ~500 logs / s raw with parallel Celery decoding & enrichment

Wallet‑level aggregation — turnover, avg‑trade size, trade‑count, net‑buy (‑1…1)

Multi‑chain / multi‑DEX — currently Arbitrum‑Uniswap v3 & Base‑Aerodrome

Hourly USD quoting — Binance spot API keeps volume & PnL values fresh

One‑shot or rolling mode — back‑fill any N days or run continuously via cron/systemd

# 🔑 Configuration (.env)

| Variable                | Example                             | Description                                |
| ----------------------- | ----------------------------------- | ------------------------------------------ |
| `DATABASE_URL`          | `postgres://user:pass@host:5432/db` | Postgres connection string                 |
| `CELERY_BROKER_URL`     | `redis://redis:6379/0`              | Celery message broker                      |
| `CELERY_RESULT_BACKEND` | `redis://redis:6379/1`              | Celery task results                        |
| `ALCHEMY_API_KEY`       | `abcd1234…`                         | RPC access for `eth_getLogs` / tx look‑ups |


