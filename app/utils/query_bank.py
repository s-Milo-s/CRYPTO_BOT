STABLE_SQL = """
WITH base AS (
  SELECT
    date_trunc('hour', s."timestamp") AS bucket_start,
    s.quote_vol::float8               AS usd,
    s.is_buy
  FROM {raw_table} s
  WHERE s."timestamp" >= :start
    AND s."timestamp" <  :end
),
agg AS (
  SELECT
    bucket_start,
    SUM(CASE WHEN is_buy THEN usd END)        AS buys_usd,
    SUM(CASE WHEN NOT is_buy THEN usd END)    AS sells_usd,
    SUM(usd)                                  AS volume_usd
  FROM base GROUP BY 1
)
SELECT
  :pool_slug                     AS pool_slug,
  agg.bucket_start               AS bucket_start,
  buys_usd,
  sells_usd,
  volume_usd,
  CASE WHEN volume_usd = 0 THEN 0
       ELSE (buys_usd - sells_usd) / volume_usd
  END                            AS pressure
FROM agg
ORDER BY bucket_start;
"""

VOLATILE_SQL = """
WITH base AS (
  SELECT
    date_trunc('hour', s."timestamp")                                AS bucket_start,
    date_trunc('hour', s."timestamp")
      - (EXTRACT(hour FROM s."timestamp")::int % 8) * interval '1h'  AS price_bucket,
    s.quote_vol,
    s.is_buy
  FROM {raw_table} s
  WHERE s."timestamp" >= :start
    AND s."timestamp" <  :end
),
priced AS (
  SELECT
    b.bucket_start              AS bucket_start,
    quote_vol * COALESCE(p.{symbol}, 0)::float8 AS usd,
    is_buy
  FROM base b
  LEFT JOIN price_8h_usd p
    ON p.bucket_start = b.price_bucket
  WHERE p.{symbol} IS NOT NULL
),
agg AS (
  SELECT
    bucket_start,
    SUM(CASE WHEN is_buy THEN usd END)        AS buys_usd,
    SUM(CASE WHEN NOT is_buy THEN usd END)    AS sells_usd,
    SUM(usd)                                  AS volume_usd
  FROM priced GROUP BY 1
)
SELECT
  :pool_slug                     AS pool_slug,
  agg.bucket_start               AS bucket_start,
  buys_usd,
  sells_usd,
  volume_usd,
  CASE WHEN volume_usd = 0 THEN 0
       ELSE (buys_usd - sells_usd) / volume_usd
  END                            AS pressure
FROM agg
ORDER BY bucket_start;
"""
