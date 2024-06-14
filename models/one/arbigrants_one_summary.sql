{{ config
(
    materialized = 'table'
)
}}

WITH stats_24h AS (
WITH all_txns AS (
SELECT 
COUNT(DISTINCT FROM_ADDRESS) as all_day_active_wallets,
SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS all_day_gas_spend
FROM {{ source('arbitrum_raw', 'transactions') }} t   
WHERE BLOCK_TIMESTAMP < CURRENT_DATE
AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '1 day'
),

grantee_txns AS (
SELECT 
COUNT(DISTINCT FROM_ADDRESS) as grantee_day_active_wallets,
SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS grantee_day_gas_spend
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
AND BLOCK_TIMESTAMP < CURRENT_DATE
AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '1 day'
INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
ON m.NAME = c.NAME 
AND m.chain = 'Arbitrum One')

SELECT 
grantee_day_active_wallets AS day_active_wallets,
grantee_day_active_wallets/all_day_active_wallets AS pct_day_active_wallets,
grantee_day_gas_spend as day_gas_spend,
grantee_day_gas_spend/all_day_gas_spend as pct_day_gas_spend
FROM all_txns, grantee_txns
),

stats_7d AS (
WITH all_txns AS (
SELECT 
COUNT(DISTINCT FROM_ADDRESS) as all_week_active_wallets,
SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS all_week_gas_spend
FROM {{ source('arbitrum_raw', 'transactions') }} t   
WHERE BLOCK_TIMESTAMP < CURRENT_DATE
AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '7 day'
),

grantee_txns AS (
SELECT 
COUNT(DISTINCT FROM_ADDRESS) as grantee_week_active_wallets,
SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS grantee_week_gas_spend
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
AND BLOCK_TIMESTAMP < CURRENT_DATE
AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '7 day'
INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
ON m.NAME = c.NAME 
AND m.chain = 'Arbitrum One')

SELECT 
grantee_week_active_wallets AS week_active_wallets,
grantee_week_active_wallets/all_week_active_wallets AS pct_week_active_wallets,
grantee_week_gas_spend as week_gas_spend,
grantee_week_gas_spend/all_week_gas_spend as pct_week_gas_spend
FROM all_txns, grantee_txns
),

stats_1m AS (
WITH all_txns AS (
SELECT 
COUNT(DISTINCT FROM_ADDRESS) as all_month_active_wallets,
SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS all_month_gas_spend
FROM {{ source('arbitrum_raw', 'transactions') }} t   
WHERE BLOCK_TIMESTAMP < CURRENT_DATE
AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '1 month'
),

grantee_txns AS (
SELECT 
COUNT(DISTINCT FROM_ADDRESS) as grantee_month_active_wallets,
SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS grantee_month_gas_spend
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
AND BLOCK_TIMESTAMP < CURRENT_DATE
AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '1 month'
INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
ON m.NAME = c.NAME 
AND m.chain = 'Arbitrum One')

SELECT 
grantee_month_active_wallets AS month_active_wallets,
grantee_month_active_wallets/all_month_active_wallets AS pct_month_active_wallets,
grantee_month_gas_spend as month_gas_spend,
grantee_month_gas_spend/all_month_gas_spend as pct_month_gas_spend
FROM all_txns, grantee_txns
),

stats_tvl AS (
WITH all_tvl AS (
SELECT 
TVL AS tvl_all
FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_TOTAL_TVL
WHERE DATE = current_date
),

grantee_tvl AS (
SELECT 
SUM(h.TOTAL_LIQUIDITY_USD) AS tvl_grantees
FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
LEFT JOIN DEFILLAMA.TVL.HISTORICAL_TVL_PER_CHAIN h
ON h.CHAIN = 'Arbitrum'
AND h.DATE = current_date
AND h.PROTOCOL_NAME = LLAMA_NAME
)

SELECT 
tvl_grantees,
tvl_grantees/tvl_all as pct_tvl
FROM all_tvl, grantee_tvl
)

SELECT * FROM stats_24h, stats_7d, stats_1m, stats_tvl