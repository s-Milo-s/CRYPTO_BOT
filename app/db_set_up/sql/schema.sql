CREATE TABLE IF NOT EXISTS token_pairs (
    id SERIAL PRIMARY KEY,
    base_token TEXT NOT NULL,
    quote_token TEXT NOT NULL,
    symbol TEXT GENERATED ALWAYS AS (base_token || '/' || quote_token) STORED,
    UNIQUE(base_token, quote_token)
);