{{ config
(
    materialized = 'table'
)
}}

with total AS (
SELECT 
TO_VARCHAR(DATE_TRUNC('month',DATE), 'YYYY-MM-DD') AS date,
'total' as category,
LAST_VALUE(TVL) OVER (PARTITION BY DATE_TRUNC('month', DATE) ORDER BY DATE) AS TVL
FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_TOTAL_TVL
WHERE DATE < DATE_TRUNC('day',CURRENT_DATE())
AND DATE >= to_timestamp('2023-03-01', 'yyyy-MM-dd')
QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', DATE) ORDER BY DATE DESC) = 1
)

, grantees AS (
SELECT 
TO_VARCHAR(DATE_TRUNC('month',DATE), 'YYYY-MM-DD') AS date,
category,
LAST_VALUE(TVL) OVER (PARTITION BY DATE_TRUNC('month', DATE) ORDER BY DATE) AS TVL
FROM (
SELECT 
DATE,
'grantees' as category,
SUM(h.TOTAL_LIQUIDITY_USD) AS TVL
FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
INNER JOIN DEFILLAMA.TVL.HISTORICAL_TVL_PER_CHAIN h
ON h.CHAIN = 'Arbitrum'
AND h.PROTOCOL_NAME = LLAMA_NAME
AND DATE < DATE_TRUNC('day',CURRENT_DATE())
AND DATE >= to_timestamp('2023-03-01', 'yyyy-MM-dd')
AND h.DATE >= to_timestamp(COALESCE(m.GRANT_DATE,'01/01/2023'), 'DD/MM/YYYY')
GROUP BY 1,2)
QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', DATE) ORDER BY DATE DESC) = 1
)

SELECT * FROM total
UNION ALL 
SELECT * FROM grantees