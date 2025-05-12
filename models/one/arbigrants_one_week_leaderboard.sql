{{ config
(
    materialized = 'table'
)
}}


WITH time_settings AS (
    SELECT 
        CURRENT_DATE - INTERVAL '1 WEEK' AS one_period_ago,
        CURRENT_DATE - INTERVAL '2 WEEK' AS two_period_ago
)

, aggregated_data AS (
    SELECT 
        m.NAME AS project,
        m.CATEGORY,
        m.LLAMA_SLUG AS slug,
        m.LOGO,
        m.CHAIN, 
        m.GRANT_DATE,
        COUNT(l.CONTRACT_ADDRESS) AS NUM_CONTRACTS,
        COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP >= (SELECT one_period_ago FROM time_settings) AND t.BLOCK_TIMESTAMP < CURRENT_DATE THEN t.HASH END) AS txns_current,
        COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP < (SELECT one_period_ago FROM time_settings) AND t.BLOCK_TIMESTAMP >= (SELECT two_period_ago FROM time_settings) THEN t.HASH END) AS txns_previous,
        COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP >= (SELECT one_period_ago FROM time_settings) AND t.BLOCK_TIMESTAMP < CURRENT_DATE THEN t.FROM_ADDRESS END) AS active_accounts_current,
        COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP < (SELECT one_period_ago FROM time_settings) AND t.BLOCK_TIMESTAMP >= (SELECT two_period_ago FROM time_settings) THEN t.FROM_ADDRESS END) AS active_accounts_previous,
        SUM(CASE WHEN t.BLOCK_TIMESTAMP >= (SELECT one_period_ago FROM time_settings) AND t.BLOCK_TIMESTAMP < CURRENT_DATE THEN ((t.RECEIPT_EFFECTIVE_GAS_PRICE * t.RECEIPT_GAS_USED)/1e18) END) AS gas_spend_current,
        SUM(CASE WHEN t.BLOCK_TIMESTAMP < (SELECT one_period_ago FROM time_settings) AND t.BLOCK_TIMESTAMP >= (SELECT two_period_ago FROM time_settings) THEN ((t.RECEIPT_EFFECTIVE_GAS_PRICE * t.RECEIPT_GAS_USED)/1e18) END) AS gas_spend_previous,
        COUNT(DISTINCT t.HASH) AS total_transactions,
        COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP >= DATE(m.GRANT_DATE) THEN t.HASH END) AS transactions_since_grant,
        SUM((t.RECEIPT_EFFECTIVE_GAS_PRICE * t.RECEIPT_GAS_USED)/1e18) AS total_eth_fees,
        SUM(CASE WHEN t.BLOCK_TIMESTAMP >= DATE(m.GRANT_DATE) THEN ((t.RECEIPT_EFFECTIVE_GAS_PRICE * t.RECEIPT_GAS_USED)/1e18) END) AS eth_fees_since_grant
        
    FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m  
    LEFT JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS l
    ON m.NAME = l.NAME 
    LEFT JOIN ARBITRUM.RAW.TRANSACTIONS t  
    ON t.TO_ADDRESS = l.CONTRACT_ADDRESS
    WHERE m.CHAIN IN ('Arbitrum One', 'Offchain', 'Arbitrum Orbit')
    AND GRANT_DATE != ''
    GROUP BY 1,2,3,4,5,6
)

, tvl_data AS (
    SELECT 
    project,
    SUM(TVL) AS TVL
    FROM (
    SELECT 
    m.NAME AS project,
    h.TOTAL_LIQUIDITY_USD AS TVL,
    ROW_NUMBER() OVER (PARTITION BY h.PROTOCOL_NAME ORDER BY h.NEAREST_DATE DESC) AS rn
    FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m  
    INNER JOIN DEFILLAMA.TVL.HISTORICAL_TVL_PER_CHAIN h
    ON h.CHAIN = 'Arbitrum'
    AND date_trunc('day',h.NEAREST_DATE) = current_date
    AND LLAMA_NAME != ''
    AND h.PROTOCOL_NAME LIKE LLAMA_NAME || '%'
    AND m.CHAIN IN ('Arbitrum One', 'Offchain', 'Arbitrum Orbit')
    )
    WHERE rn = 1
    GROUP BY 1
)

, tvl_at_grant AS (
    SELECT 
    project,
    SUM(TVL) AS tvl_at_grant_date
    FROM (
    SELECT 
    m.NAME AS project,
    h.TOTAL_LIQUIDITY_USD AS TVL,
    ROW_NUMBER() OVER (PARTITION BY h.PROTOCOL_NAME, m.NAME ORDER BY ABS(DATEDIFF('day', h.NEAREST_DATE, DATE(m.GRANT_DATE)))) AS rn
    FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m  
    INNER JOIN DEFILLAMA.TVL.HISTORICAL_TVL_PER_CHAIN h
    ON h.CHAIN = 'Arbitrum'
    AND LLAMA_NAME != ''
    AND h.PROTOCOL_NAME LIKE LLAMA_NAME || '%'
    AND m.CHAIN IN ('Arbitrum One', 'Offchain', 'Arbitrum Orbit')
    AND h.NEAREST_DATE = DATE(m.GRANT_DATE)
    )
    WHERE rn = 1
    GROUP BY 1
)

SELECT
ad.project,
category,
slug,
logo,
chain,
grant_date,
CASE WHEN num_contracts = 0 THEN false ELSE true END AS has_contracts,
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
COALESCE(tvl,0) as tvl,
COALESCE(tag.tvl_at_grant_date, 0) as tvl_at_grant_date,
ad.total_transactions,
ad.transactions_since_grant,
COALESCE(ad.total_eth_fees, 0) as total_eth_fees,
COALESCE(ad.eth_fees_since_grant, 0) as eth_fees_since_grant

FROM aggregated_data ad  
LEFT JOIN tvl_data tv ON tv.project = ad.project
LEFT JOIN tvl_at_grant tag ON tag.project = ad.project

