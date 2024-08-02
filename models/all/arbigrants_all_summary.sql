{{ config
(
    materialized = 'table'
)
}}

WITH grantees AS (
SELECT 
SUM(DAY_ACTIVE_WALLETS) AS DAY_ACTIVE_WALLETS,
SUM(DAY_GAS_SPEND) AS DAY_GAS_SPEND,
SUM(WEEK_ACTIVE_WALLETS) AS WEEK_ACTIVE_WALLETS,
SUM(WEEK_GAS_SPEND) AS WEEK_GAS_SPEND,
SUM(MONTH_ACTIVE_WALLETS) AS MONTH_ACTIVE_WALLETS,
SUM(MONTH_GAS_SPEND) AS MONTH_GAS_SPEND,
SUM(TVL_GRANTEES) AS TVL_GRANTEES
FROM 
(
    SELECT * FROM {{ ref('arbigrants_one_summary') }}
    UNION ALL 
    SELECT * FROM {{ ref('arbigrants_nova_summary') }}
)
)

, total AS (
    WITH day_stats AS (
    SELECT 
    COUNT(DISTINCT FROM_ADDRESS) as arbone_day_active_wallets,
    SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS arbone_day_gas_spend
    FROM {{ source('arbitrum_raw', 'transactions') }} t   
    WHERE BLOCK_TIMESTAMP < CURRENT_DATE
    AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '1 day')
   
    , week_stats AS (
    SELECT 
    COUNT(DISTINCT FROM_ADDRESS) as arbone_week_active_wallets,
    SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS arbone_week_gas_spend
    FROM {{ source('arbitrum_raw', 'transactions') }} t   
    WHERE BLOCK_TIMESTAMP < CURRENT_DATE
    AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '7 day')

    , month_stats AS (
    SELECT 
    COUNT(DISTINCT FROM_ADDRESS) as arbone_month_active_wallets,
    SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS arbone_month_gas_spend
    FROM {{ source('arbitrum_raw', 'transactions') }} t   
    WHERE BLOCK_TIMESTAMP < CURRENT_DATE
    AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '1 month')

    , tvl_stats AS (
    SELECT 
    TVL AS arbone_tvl
    FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_TOTAL_TVL
    WHERE DATE = current_date
    )

    SELECT * FROM day_stats, week_stats, month_stats, tvl_stats
)

SELECT 
DAY_ACTIVE_WALLETS,
(SELECT arbone_day_active_wallets FROM total) / DAY_ACTIVE_WALLETS AS PCT_DAY_ACTIVE_WALLETS,
DAY_GAS_SPEND,
(SELECT arbone_day_gas_spend FROM total) / DAY_GAS_SPEND AS PCT_DAY_GAS_SPEND,
WEEK_ACTIVE_WALLETS,
(SELECT arbone_week_active_wallets FROM total) / WEEK_ACTIVE_WALLETS AS PCT_WEEK_ACTIVE_WALLETS,
WEEK_GAS_SPEND,
(SELECT arbone_week_gas_spend FROM total) / WEEK_GAS_SPEND AS PCT_WEEK_GAS_SPEND,
MONTH_ACTIVE_WALLETS,
(SELECT arbone_month_active_wallets FROM total) / MONTH_ACTIVE_WALLETS AS PCT_MONTH_ACTIVE_WALLETS,
MONTH_GAS_SPEND,
(SELECT arbone_month_gas_spend FROM total) / MONTH_GAS_SPEND AS PCT_MONTH_GAS_SPEND,
TVL_GRANTEES,
(SELECT arbone_tvl FROM total) / TVL_GRANTEES AS PCT_TVL
FROM grantees