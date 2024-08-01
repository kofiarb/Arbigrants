{{ config
(
    materialized = 'table'
)
}}

SELECT 
TO_VARCHAR(DATE_TRUNC('week',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
COUNT(DISTINCT FROM_ADDRESS) AS active_wallets
FROM {{ source('arbitrum_raw', 'transactions') }}
WHERE BLOCK_TIMESTAMP < DATE_TRUNC('day',CURRENT_DATE())
AND BLOCK_TIMESTAMP >= CURRENT_DATE() - interval '13 months'
GROUP BY 1