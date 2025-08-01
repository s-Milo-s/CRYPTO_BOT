-- Just a file to store random sql updates and schema changes. This is not offically used by the app 
-- but can be used to update the database schema manually if needed with teaks and changes.
-- Some of these changes are wrong or outdated, so use with caution.

CREATE TABLE IF NOT EXISTS token_pairs (
    id SERIAL PRIMARY KEY,
    base_token TEXT NOT NULL,
    quote_token TEXT NOT NULL,
    symbol TEXT GENERATED ALWAYS AS (base_token || '/' || quote_token) STORED,
    UNIQUE(base_token, quote_token)
);

CREATE TABLE IF NOT EXISTS dex_pairs (
    id SERIAL PRIMARY KEY,
    base_token TEXT NOT NULL,
    quote_token TEXT NOT NULL,
    dex TEXT NOT NULL,
    chain TEXT NOT NULL,
    pair_address TEXT NOT NULL,
    UNIQUE (base_token, quote_token, dex, chain),
    UNIQUE (pair_address)
);


CREATE TABLE IF NOT EXISTS pair_data (
    id SERIAL PRIMARY KEY,
    pair_address TEXT NOT NULL REFERENCES dex_pairs(pair_address),
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),

    price_usd NUMERIC,
    price_native NUMERIC,

    liquidity_usd NUMERIC,
    volume_1h NUMERIC,
    price_change_1h NUMERIC,

    fdv NUMERIC,
    market_cap NUMERIC
);


CREATE TABLE IF NOT EXISTS solusdt_15m_klines (
    open_time BIGINT PRIMARY KEY,          -- Unix timestamp in ms
    open_price NUMERIC(18, 8) NOT NULL,
    high_price NUMERIC(18, 8) NOT NULL,
    low_price NUMERIC(18, 8) NOT NULL,
    close_price NUMERIC(18, 8) NOT NULL,
    volume NUMERIC(18, 8) NOT NULL,
    close_time BIGINT NOT NULL,
    quote_volume NUMERIC(18, 8),
    number_of_trades INTEGER,
    taker_buy_base_volume NUMERIC(18, 8),
    taker_buy_quote_volume NUMERIC(18, 8)
);

CREATE TABLE IF NOT EXISTS eth_uniswap_wethusdt_1m_klines (
    minute_start TIMESTAMPTZ PRIMARY KEY,
    open_price NUMERIC,
    open_ts TIMESTAMPTZ,
    close_price NUMERIC,
    close_ts TIMESTAMPTZ,
    high_price NUMERIC,
    low_price NUMERIC,
    avg_price NUMERIC,
    swap_count INTEGER,
    total_base_volume NUMERIC,
    total_quote_volume NUMERIC
);

CREATE TABLE IF NOT EXISTS arbitrum_uniswap_wethusdt_1m_klines (
    minute_start TIMESTAMPTZ PRIMARY KEY,
    open_price NUMERIC,
    open_ts TIMESTAMPTZ,
    close_price NUMERIC,
    close_ts TIMESTAMPTZ,
    high_price NUMERIC,
    low_price NUMERIC,
    avg_price NUMERIC,
    swap_count INTEGER,
    total_base_volume NUMERIC,
    total_quote_volume NUMERIC
);

CREATE TABLE IF NOT EXISTS arbitrum_uniswap_wethusdc_1m_klines (
    minute_start TIMESTAMPTZ PRIMARY KEY,
    open_price NUMERIC,
    open_ts TIMESTAMPTZ,
    close_price NUMERIC,
    close_ts TIMESTAMPTZ,
    high_price NUMERIC,
    low_price NUMERIC,
    avg_price NUMERIC,
    swap_count INTEGER,
    total_base_volume NUMERIC,
    total_quote_volume NUMERIC
);

CREATE TABLE IF NOT EXISTS arbitrum_camelot_arbweth_1m_klines (
    minute_start TIMESTAMPTZ PRIMARY KEY,
    open_price NUMERIC,
    open_ts TIMESTAMPTZ,
    close_price NUMERIC,
    close_ts TIMESTAMPTZ,
    high_price NUMERIC,
    low_price NUMERIC,
    avg_price NUMERIC,
    swap_count INTEGER,
    total_base_volume NUMERIC,   -- ARB
    total_quote_volume NUMERIC   -- WETH
);

CREATE TABLE IF NOT EXISTS arbitrum_sushiswap_arbusdt_1m_klines (
    minute_start TIMESTAMPTZ PRIMARY KEY,
    open_price NUMERIC,
    open_ts TIMESTAMPTZ,
    close_price NUMERIC,
    close_ts TIMESTAMPTZ,
    high_price NUMERIC,
    low_price NUMERIC,
    avg_price NUMERIC,
    swap_count INTEGER,
    total_base_volume NUMERIC,
    total_quote_volume NUMERIC
);

-- Table name suggestion: price_8h_usd
CREATE TABLE price_8h_usd (
    -- Start of the 8-hour bucket in UTC (aligned to 00:00, 08:00, 16:00)
    bucket_start  TIMESTAMPTZ PRIMARY KEY,

    -- USD price of 1 ETH at the end of the bucket
    eth NUMERIC(18, 8) NOT NULL,

    created_at    TIMESTAMPTZ DEFAULT now() NOT NULL
);

-- Optional index if querying by time range
CREATE INDEX idx_price_8h_usd_bucket ON price_8h_usd(bucket_start);


CREATE TABLE trade_size_distribution (
    id SERIAL PRIMARY KEY,
    pool_name TEXT NOT NULL UNIQUE,

    bucket_neg2 INTEGER DEFAULT 0,
    bucket_neg1 INTEGER DEFAULT 0,
    bucket_0 INTEGER DEFAULT 0,
    bucket_1 INTEGER DEFAULT 0,
    bucket_2 INTEGER DEFAULT 0,
    bucket_3 INTEGER DEFAULT 0,
    bucket_4 INTEGER DEFAULT 0,
    bucket_5 INTEGER DEFAULT 0,
    bucket_6 INTEGER DEFAULT 0,

    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

ALTER TABLE trade_size_distribution
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW();


CREATE TABLE IF NOT EXISTS extraction_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    block_range TEXT,                -- e.g., '1000000-1010000'
    log_count INTEGER NOT NULL,      -- number of logs processed in that range
    duration_seconds NUMERIC(10, 2)  -- how long it took to process in seconds
);

CREATE TABLE pools (
    id            SERIAL PRIMARY KEY,
    chain         VARCHAR(32)  NOT NULL,
    dex           VARCHAR(32)  NOT NULL,
    pair          VARCHAR(32)  NOT NULL,
    address       CHAR(42)     NOT NULL UNIQUE,
    active        BOOLEAN      NOT NULL DEFAULT TRUE,
    last_started  DOUBLE PRECISION
);

CREATE INDEX ix_pools_chain_dex ON pools (chain, dex);

INSERT INTO pools (chain, dex, pair, address, active, last_started)
VALUES
  ('arbitrum', 'uniswap_v3', 'WETH/USDT', '0xc31e54c7a869b9fcbecc14363cf510d1c41fa443', TRUE, NULL),
  ('arbitrum', 'uniswap_v3', 'WBTC/USDT', '0x5969EFddE3cF5C0D9a88aE51E47d721096A97203', TRUE, NULL);

INSERT INTO pools (chain, dex, pair, address, active, last_started)
VALUES
  ('arbitrum', 'uniswap_v3', 'ARB/WETH', '0xc6f780497a95e246eb9449f5e4770916dcd6396a', TRUE, NULL),

INSERT INTO pools (chain, dex, pair, address, active, last_started)
VALUES
  ('arbitrum', 'uniswap_v3', 'CRV/WETH', '0xa95b0f5a65a769d82ab4f3e82842e45b8bbaf101', TRUE, NULL),
  ('base', 'aerodrome', 'DEGEN/WETH', '0xafb62448929664bfccb0aae22f232520e765ba88', TRUE, NULL),
  ('base', 'aerodrome', 'BRETT/WETH', '0x4e829f8a5213c42535ab84aa40bd4adcce9cba02', TRUE, NULL);

CREATE TABLE IF NOT EXISTS pool_flow_hourly (
          pool_slug    TEXT         NOT NULL,
          bucket_start TIMESTAMPTZ  NOT NULL,
          buys_usd     NUMERIC(38,2),
          sells_usd    NUMERIC(38,2),
          volume_usd   NUMERIC(38,2),
          pressure     NUMERIC(8,5),
          PRIMARY KEY (pool_slug, bucket_start)
        );