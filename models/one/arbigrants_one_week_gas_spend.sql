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
SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS gas_spend
FROM {{ source('arbitrum_raw', 'transactions') }}
WHERE BLOCK_TIMESTAMP < DATE_TRUNC('week',CURRENT_DATE())
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= DATE_TRUNC('week',CURRENT_DATE()) - interval '2 week' 
{% endif %}
GROUP BY 1,2
)

, grantees AS (
SELECT 
TO_VARCHAR(DATE_TRUNC('week',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
'grantees' as category,
SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS gas_spend
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN {{ ref('arbigrants_labels_project_contracts') }} c
ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
AND BLOCK_TIMESTAMP < DATE_TRUNC('week',CURRENT_DATE())
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= DATE_TRUNC('week',CURRENT_DATE()) - interval '2 week' 
{% endif %}
GROUP BY 1,2
)

SELECT * FROM total
UNION ALL 
SELECT * FROM grantees