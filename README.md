# Wallet‑Watchers Ingestion Pipeline
High‑speed pipeline ingesting DEX swaps and computing wallet‑level metrics for quantitative analysis

# 🚀 Quick Start (Docker Compose)

1. Clone & enter repo <br>
git clone https://github.com/s-Milo-s/CRYPTO_BOT.git <br>
cd wallet‑watchers‑ingest 

2. Copy env template and add your secrets <br>
cp .env.example .env <br>
  └─ fill DATABASE_URL, CELERY_BROKER_URL, CELERY_RESULT_BACKEND, ALCHEMY_API_KEY

3. Launch services (Postgres, Redis, FastAPI, Celery workers …) <br>
docker compose up -d

## 🛠️ API Reference

> **Interactive docs:** once the stack is running, visit **`http://localhost:8000/docs`**  
> (FastAPI auto‑generates a Swagger UI where you can trigger jobs without cURL.)

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `POST` | `/api/trigger/ingestion` | Launch a one‑shot ingest / back‑fill task |

### Query parameters

| Name | Type | Required | Example | Description |
|------|------|----------|---------|-------------|
| `chain` | `str` | ✔ | `base` | Target blockchain (`base`, `arbitrum`, …) |
| `dex` | `str` | ✔ | `aerodrome` | Supported DEX (`aerodrome`, `uniswap`) |
| `pair` | `str` | ✔ | `BRETT/WETH` (URL‑encoded) | Token pair label |
| `pool_address` | `str` | ✔ | `0x4e829f8…` | Pool contract address |
| `days_back` | `int` | ❌ (default =`1`) | `90` | How many days of history to ingest |

Note: All valid chain / dex / pair combinations are listed in cli_ingest.py.

# ⚙️ Architecture
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


