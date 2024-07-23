{{ config
(
    materialized = 'table'
)
}}

SELECT 
TO_VARCHAR(DATE_TRUNC('month',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
c.NAME,
COUNT(DISTINCT FROM_ADDRESS) AS active_wallets
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
AND BLOCK_TIMESTAMP < DATE_TRUNC('month',CURRENT_DATE())
AND BLOCK_TIMESTAMP >= to_timestamp('2023-03-01', 'yyyy-MM-dd')
GROUP BY 1,2