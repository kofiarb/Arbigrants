{{ config
(
    materialized = 'table'
)
}}

with total AS (
SELECT 
TO_VARCHAR(DATE_TRUNC('day',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
'total' as category,
COUNT(DISTINCT FROM_ADDRESS) AS active_wallets
FROM {{ source('arbitrum_raw', 'transactions') }}
WHERE BLOCK_TIMESTAMP < DATE_TRUNC('day',CURRENT_DATE())
AND BLOCK_TIMESTAMP >= CURRENT_DATE() - interval '13 months'
GROUP BY 1,2
)

, grantees AS (
SELECT 
TO_VARCHAR(DATE_TRUNC('day',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
'grantees' as category,
COUNT(DISTINCT FROM_ADDRESS) AS active_wallets
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
AND BLOCK_TIMESTAMP < DATE_TRUNC('day',CURRENT_DATE())
AND BLOCK_TIMESTAMP >= CURRENT_DATE() - interval '13 months'
GROUP BY 1,2
)

SELECT * FROM total
UNION ALL 
SELECT * FROM grantees