{{ config
(
    materialized = 'incremental',
    unique_key = ['date', 'category']
)
}}

with total AS (
SELECT 
TO_VARCHAR(DATE_TRUNC('week',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
'total' as category,
COUNT(DISTINCT FROM_ADDRESS) AS active_wallets
FROM {{ source('arbitrum_raw', 'transactions') }}
WHERE BLOCK_TIMESTAMP < DATE_TRUNC('week',CURRENT_DATE())
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= DATE_TRUNC('week',CURRENT_DATE()) - interval '2 week' 
{% endif %}
{% if not is_incremental() %}
AND BLOCK_TIMESTAMP >= to_timestamp('2023-03-01', 'yyyy-MM-dd')
{% endif %}
GROUP BY 1,2
)

, grantees AS (
SELECT 
TO_VARCHAR(DATE_TRUNC('week',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
'grantees' as category,
COUNT(DISTINCT FROM_ADDRESS) AS active_wallets
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
AND BLOCK_TIMESTAMP < DATE_TRUNC('week',CURRENT_DATE())
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= DATE_TRUNC('week',CURRENT_DATE()) - interval '2 week' 
{% endif %}
{% if not is_incremental() %}
AND BLOCK_TIMESTAMP >= to_timestamp('2023-03-01', 'yyyy-MM-dd')
{% endif %}
INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
ON c.NAME = m.NAME
AND t.BLOCK_TIMESTAMP >= to_timestamp(COALESCE(m.GRANT_DATE,'01/01/2023'), 'DD/MM/YYYY')
GROUP BY 1,2
)

SELECT * FROM total
UNION ALL 
SELECT * FROM grantees