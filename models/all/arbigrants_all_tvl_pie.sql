{{ config
(
    materialized = 'table'
)
}}

SELECT
NAME,
TVL,
ROUND((100* TVL / SUM(TVL) OVER ()),2) AS PCT_TVL
FROM (
SELECT 
NAME,
SUM(TVL) AS TVL
FROM 
(
    SELECT NAME, TVL FROM {{ ref('arbigrants_one_tvl_pie') }}
    UNION ALL 
    SELECT NAME, TVL FROM {{ ref('arbigrants_nova_tvl_pie') }}
)
GROUP BY 1)