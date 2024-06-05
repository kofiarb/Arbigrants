{{ config
(
    materialized = 'table'
)
}}


WITH time_settings AS (
    SELECT 
        CURRENT_DATE - INTERVAL '1 MONTH' AS one_period_ago,
        CURRENT_DATE - INTERVAL '2 MONTH' AS two_period_ago
),
aggregated_data AS (
    SELECT 
        l.NAME AS project,
        m.CATEGORY,
        m.LLAMA_SLUG AS slug,
        m.LOGO,
        COALESCE(h.TOTAL_LIQUIDITY_USD,0) AS TVL,
        COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP >= ts.one_period_ago AND t.BLOCK_TIMESTAMP < CURRENT_DATE THEN t.HASH END) AS txns_current,
        COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP < ts.one_period_ago AND t.BLOCK_TIMESTAMP >= ts.two_period_ago THEN t.HASH END) AS txns_previous,
        COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP >= ts.one_period_ago AND t.BLOCK_TIMESTAMP < CURRENT_DATE THEN t.FROM_ADDRESS END) AS active_accounts_current,
        COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP < ts.one_period_ago AND t.BLOCK_TIMESTAMP >= ts.two_period_ago THEN t.FROM_ADDRESS END) AS active_accounts_previous,
        SUM(CASE WHEN t.BLOCK_TIMESTAMP >= ts.one_period_ago THEN ((t.RECEIPT_EFFECTIVE_GAS_PRICE * t.RECEIPT_GAS_USED)/1e18) END) AS gas_spend_current,
        SUM(CASE WHEN t.BLOCK_TIMESTAMP < ts.one_period_ago AND t.BLOCK_TIMESTAMP >= ts.two_period_ago THEN ((t.RECEIPT_EFFECTIVE_GAS_PRICE * t.RECEIPT_GAS_USED)/1e18) END) AS gas_spend_previous
    FROM {{ source('arbitrum_raw', 'transactions') }} t  
    INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS l
    ON t.TO_ADDRESS = l.CONTRACT_ADDRESS
    INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
    ON m.NAME = l.NAME 
    AND m.CHAIN = 'Arbitrum One'
    LEFT JOIN DEFILLAMA.TVL.HISTORICAL_TVL_PER_CHAIN h
    ON h.CHAIN = 'Arbitrum'
    AND h.DATE = current_date
    AND h.PROTOCOL_NAME = LLAMA_NAME
    CROSS JOIN time_settings ts
    WHERE t.BLOCK_TIMESTAMP >= ts.two_period_ago
    GROUP BY 1,2,3,4,5
)

SELECT
project,
category,
slug,
logo,
COALESCE(ad.gas_spend_current,0) as ETH_FEES,
CASE 
    WHEN ad.gas_spend_previous > 0 THEN (100 * (COALESCE(ad.gas_spend_current,0) - COALESCE(ad.gas_spend_previous,0)) / COALESCE(ad.gas_spend_previous,0)) 
    ELSE 0 
END as ETH_FEES_GROWTH,
ad.txns_current as TRANSACTIONS,
CASE 
    WHEN ad.txns_previous > 0 THEN (100 * (ad.txns_current - ad.txns_previous) / ad.txns_previous) 
    ELSE 0 
END as TRANSACTIONS_GROWTH,
ad.active_accounts_current as WALLETS,
CASE 
    WHEN ad.active_accounts_previous > 0 THEN (100 * (ad.active_accounts_current - ad.active_accounts_previous) / ad.active_accounts_previous) 
    ELSE 0 
END as WALLETS_GROWTH,
tvl
FROM aggregated_data ad  
ORDER BY ad.gas_spend_current DESC