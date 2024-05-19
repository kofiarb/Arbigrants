{{ config
(
    materialized = 'table'
)
}}

WITH stats_24h AS (
WITH all_txns AS (
SELECT 
COUNT(DISTINCT FROM_ADDRESS) as all_day_active_wallets,
COUNT(*) as all_day_transactions,
SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS all_day_gas_spend
FROM {{ source('arbitrum_raw', 'transactions') }} t   
WHERE BLOCK_TIMESTAMP < CURRENT_DATE
AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '1 day'
),

grantee_txns AS (
SELECT 
COUNT(DISTINCT FROM_ADDRESS) as grantee_day_active_wallets,
COUNT(*) as grantee_day_transactions,
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
grantee_day_transactions as day_transactions,
grantee_day_transactions/all_day_transactions as pct_day_transactions,
grantee_day_gas_spend as day_gas_spend,
grantee_day_gas_spend/all_day_gas_spend as pct_day_gas_spend
FROM all_txns, grantee_txns
),

stats_7d AS (
WITH all_txns AS (
SELECT 
COUNT(DISTINCT FROM_ADDRESS) as all_week_active_wallets,
COUNT(*) as all_week_transactions,
SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS all_week_gas_spend
FROM {{ source('arbitrum_raw', 'transactions') }} t   
WHERE BLOCK_TIMESTAMP < CURRENT_DATE
AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '7 day'
),

grantee_txns AS (
SELECT 
COUNT(DISTINCT FROM_ADDRESS) as grantee_week_active_wallets,
COUNT(*) as grantee_week_transactions,
SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS grantee_week_gas_spend
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
AND BLOCK_TIMESTAMP < CURRENT_DATE
AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '7 day'
INNER JOINARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
ON m.NAME = c.NAME 
AND m.chain = 'Arbitrum One')

SELECT 
grantee_week_active_wallets AS week_active_wallets,
grantee_week_active_wallets/all_week_active_wallets AS pct_week_active_wallets,
grantee_week_transactions as week_transactions,
grantee_week_transactions/all_week_transactions as pct_week_transactions,
grantee_week_gas_spend as week_gas_spend,
grantee_week_gas_spend/all_week_gas_spend as pct_week_gas_spend
FROM all_txns, grantee_txns
),

stats_1m AS (
WITH all_txns AS (
SELECT 
COUNT(DISTINCT FROM_ADDRESS) as all_month_active_wallets,
COUNT(*) as all_month_transactions,
SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS all_month_gas_spend
FROM {{ source('arbitrum_raw', 'transactions') }} t   
WHERE BLOCK_TIMESTAMP < CURRENT_DATE
AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '1 month'
),

grantee_txns AS (
SELECT 
COUNT(DISTINCT FROM_ADDRESS) as grantee_month_active_wallets,
COUNT(*) as grantee_month_transactions,
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
grantee_week_active_wallets AS week_active_wallets,
grantee_week_active_wallets/all_week_active_wallets AS pct_week_active_wallets,
grantee_week_transactions as week_transactions,
grantee_week_transactions/all_week_transactions as pct_week_transactions,
grantee_week_gas_spend as week_gas_spend,
grantee_week_gas_spend/all_week_gas_spend as pct_week_gas_spend
FROM all_txns, grantee_txns
),

SELECT * FROM stats_24h, stats_7d, stats_1m