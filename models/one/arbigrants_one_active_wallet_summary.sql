{{ config
(
    materialized = 'table'
)
}}

WITH actives_24h AS (
SELECT COUNT(DISTINCT FROM_ADDRESS) as day_active_wallets
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN {{ ref('arbigrants_labels_project_contracts') }} c
ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
AND BLOCK_TIMESTAMP < CURRENT_DATE
AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '1 day'
INNER JOIN {{ ref('arbigrants_labels_project_metadata') }} m
ON m.NAME = c.NAME 
AND m.chain = 'Arbitrum One'
),

actives_growth_24h AS (
WITH active_wallet_counts AS (
  SELECT
      COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP >= CURRENT_DATE - interval '1 day' AND BLOCK_TIMESTAMP < CURRENT_DATE THEN FROM_ADDRESS END) as past_day_wallets,
      COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP < CURRENT_DATE - interval '1 day' AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '2 day' THEN FROM_ADDRESS END) as day_before_wallets
  FROM {{ source('arbitrum_raw', 'transactions') }} t
  INNER JOIN {{ ref('arbigrants_labels_project_contracts') }} c
  ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
  AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '2 day'
  INNER JOIN {{ ref('arbigrants_labels_project_metadata') }} m
  ON m.NAME = c.NAME 
  AND m.chain = 'Arbitrum One'
)
SELECT
  ROUND((100 * (past_day_wallets / NULLIF(day_before_wallets, 0)) - 100), 1) AS day_growth
FROM active_wallet_counts
),

actives_7d AS (
SELECT COUNT(DISTINCT FROM_ADDRESS) as week_active_wallets
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN {{ ref('arbigrants_labels_project_contracts') }} c
ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
AND BLOCK_TIMESTAMP < CURRENT_DATE
AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '7 day'
INNER JOIN {{ ref('arbigrants_labels_project_metadata') }} m
ON m.NAME = c.NAME 
AND m.chain = 'Arbitrum One'
),

actives_growth_7d AS (
WITH active_wallet_counts AS (
  SELECT
      COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP >= CURRENT_DATE - interval '7 day' AND BLOCK_TIMESTAMP < CURRENT_DATE THEN FROM_ADDRESS END) as past_day_wallets,
      COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP < CURRENT_DATE - interval '7 day' AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '14 day' THEN FROM_ADDRESS END) as day_before_wallets
  FROM {{ source('arbitrum_raw', 'transactions') }} t
  INNER JOIN {{ ref('arbigrants_labels_project_contracts') }} c
  ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
  AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '14 day'
  INNER JOIN {{ ref('arbigrants_labels_project_metadata') }} m
  ON m.NAME = c.NAME 
  AND m.chain = 'Arbitrum One'
)
SELECT
  ROUND((100 * (past_day_wallets / NULLIF(day_before_wallets, 0)) - 100), 1) AS week_growth
FROM active_wallet_counts
),

actives_1m AS (
SELECT COUNT(DISTINCT FROM_ADDRESS) as month_active_wallets 
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN {{ ref('arbigrants_labels_project_contracts') }} c
ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
AND BLOCK_TIMESTAMP < CURRENT_DATE
AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '1 month'
INNER JOIN {{ ref('arbigrants_labels_project_metadata') }} m
ON m.NAME = c.NAME 
AND m.chain = 'Arbitrum One'
),

actives_growth_1m AS (
WITH active_wallet_counts AS (
  SELECT
      COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP >= CURRENT_DATE - interval '1 month' AND BLOCK_TIMESTAMP < CURRENT_DATE THEN FROM_ADDRESS END) as past_day_wallets,
      COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP < CURRENT_DATE - interval '1 month' AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '2 month' THEN FROM_ADDRESS END) as day_before_wallets
  FROM {{ source('arbitrum_raw', 'transactions') }} t
  INNER JOIN {{ ref('arbigrants_labels_project_contracts') }} c
  ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
  AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '2 month'
  INNER JOIN {{ ref('arbigrants_labels_project_metadata') }} m
  ON m.NAME = c.NAME 
  AND m.chain = 'Arbitrum One'
)
SELECT
  ROUND((100 * (past_day_wallets / NULLIF(day_before_wallets, 0)) - 100), 1) AS month_growth
FROM active_wallet_counts
)

SELECT * FROM actives_24h, actives_growth_24h, actives_7d, actives_growth_7d, actives_1m, actives_growth_1m