{{ config
(
    materialized = 'incremental',
    unique_key = ['date', 'category']
)
}}

with total AS (
SELECT 
TO_VARCHAR(DATE_TRUNC('day',DATE), 'YYYY-MM-DD') AS date,
'total' as category,
TVL 
FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_TOTAL_TVL
WHERE DATE < DATE_TRUNC('day',CURRENT_DATE())
{% if is_incremental() %}
AND DATE >= CURRENT_DATE() - interval '3 day' 
{% endif %}
{% if not is_incremental() %}
AND DATE >= to_timestamp('2023-03-01', 'yyyy-MM-dd')
{% endif %}
)

, grantees AS (
SELECT 
TO_VARCHAR(DATE_TRUNC('day',DATE), 'YYYY-MM-DD') AS date,
'grantees' as category,
SUM(h.TOTAL_LIQUIDITY_USD) AS TVL
FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
INNER JOIN DEFILLAMA.TVL.HISTORICAL_TVL_PER_CHAIN h
ON h.CHAIN = 'Arbitrum'
AND h.PROTOCOL_NAME = LLAMA_NAME
AND DATE < DATE_TRUNC('day',CURRENT_DATE())
AND h.DATE >= to_timestamp(COALESCE(m.GRANT_DATE,'01/01/2023'), 'DD/MM/YYYY')
{% if is_incremental() %}
AND DATE >= CURRENT_DATE() - interval '3 day' 
{% endif %}
{% if not is_incremental() %}
AND DATE >= to_timestamp('2023-03-01', 'yyyy-MM-dd')
{% endif %}
GROUP BY 1,2
)

SELECT * FROM total
UNION ALL 
SELECT * FROM grantees