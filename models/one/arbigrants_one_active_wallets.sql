{{ config
(
    materialized = 'incremental',
    unique_key = ['day', 'category']
)
}}

with total AS (
SELECT 
TO_VARCHAR(DATE_TRUNC('day',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS day,
'total' as category,
COUNT(DISTINCT FROM_ADDRESS) AS active_wallets
FROM {{ source('arbitrum_raw', 'transactions') }}
WHERE BLOCK_TIMESTAMP < DATE_TRUNC('day',CURRENT_DATE())
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_DATE() - interval '3 day' 
{% endif %}
GROUP BY 1,2
)

, grantees AS (
SELECT 
TO_VARCHAR(DATE_TRUNC('day',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS day,
'grantees' as category,
COUNT(DISTINCT FROM_ADDRESS) AS active_wallets
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN {{ ref('arbigrants_labels_project_contracts') }} c
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