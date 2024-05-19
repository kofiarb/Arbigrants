{{ config
(
    materialized = 'incremental',
    unique_key = ['date', 'category']
)
}}

with total AS (
SELECT 
TO_VARCHAR(DATE_TRUNC('day',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
'total' as category,
SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS gas_spend
FROM {{ source('arbitrum_raw', 'transactions') }}
WHERE BLOCK_TIMESTAMP < DATE_TRUNC('day',CURRENT_DATE())
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_DATE() - interval '3 day' 
{% endif %}
GROUP BY 1,2
)

, grantees AS (
SELECT 
TO_VARCHAR(DATE_TRUNC('day',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
'grantees' as category,
SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS gas_spend
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
AND BLOCK_TIMESTAMP < DATE_TRUNC('day',CURRENT_DATE())
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_DATE() - interval '3 day' 
{% endif %}
GROUP BY 1,2
)

SELECT * FROM total
UNION ALL 
SELECT * FROM grantees