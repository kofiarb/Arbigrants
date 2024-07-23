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
AND LLAMA_NAME != ''
AND h.PROTOCOL_NAME LIKE LLAMA_NAME || '%'
AND DATE < DATE_TRUNC('day',CURRENT_DATE())
AND h.DATE >= CASE
    WHEN TRY_TO_TIMESTAMP(m.GRANT_DATE, 'M/D/YYYY') IS NOT NULL THEN TRY_TO_TIMESTAMP(m.GRANT_DATE, 'M/D/YYYY')
    ELSE TO_TIMESTAMP('2023-03-01', 'YYYY-MM-DD')
END
{% if is_incremental() %}
AND DATE >= CURRENT_DATE() - interval '3 day' 
{% endif %}
{% if not is_incremental() %}
AND DATE >= to_timestamp('2023-03-01', 'yyyy-MM-dd')
{% endif %}
GROUP BY 1,2
)

, prices AS (
SELECT 
DATE_TRUNC('day',HOUR) AS date,
LAST_VALUE(USD_PRICE) OVER (PARTITION BY DATE_TRUNC('day', HOUR) ORDER BY HOUR) AS USD_PRICE
FROM COMMON.PRICES.TOKEN_PRICES_HOURLY_EASY
WHERE SYMBOL = 'ETH'
{% if is_incremental() %}
AND HOUR >= CURRENT_DATE() - interval '3 day' 
{% endif %}
{% if not is_incremental() %}
AND HOUR >= to_timestamp('2023-03-01', 'yyyy-MM-dd')
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('week', DATE) ORDER BY HOUR DESC) = 1
)

, merged AS (
SELECT * FROM total
UNION ALL 
SELECT * FROM grantees
)

SELECT 
m.DATE,
m.CATEGORY,
m.TVL,
m.TVL/p.USD_PRICE AS TVL_ETH
FROM merged m
LEFT JOIN prices p
ON m.DATE = p.DATE