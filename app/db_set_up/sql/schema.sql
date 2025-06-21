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