{{ config
(
    materialized = 'table'
)
}}

with grantees AS (
SELECT 
DATE,
SUM(TVL) AS TVL
FROM {{ ref('arbigrants_one_month_tvl_by_project') }}
GROUP BY 1
)

, prices AS (
SELECT 
DATE_TRUNC('month',HOUR) AS date,
LAST_VALUE(USD_PRICE) OVER (PARTITION BY DATE_TRUNC('month', HOUR) ORDER BY HOUR) AS USD_PRICE
FROM COMMON.PRICES.TOKEN_PRICES_HOURLY_EASY
WHERE SYMBOL = 'ETH'
AND HOUR >= CURRENT_DATE() - interval '13 months'
QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', DATE) ORDER BY HOUR DESC) = 1
)

SELECT 
m.DATE,
m.TVL,
m.TVL/p.USD_PRICE AS TVL_ETH
FROM grantees m
LEFT JOIN prices p
ON m.DATE = p.DATE