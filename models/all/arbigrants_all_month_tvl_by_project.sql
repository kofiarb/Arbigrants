{{ config
(
    materialized = 'table'
)
}}

SELECT 
DATE,
NAME,
SUM(TVL) AS TVL,
SUM(TVL_ETH) AS TVL_ETH
FROM 
(
    SELECT * FROM {{ ref('arbigrants_one_month_tvl_by_project') }}
    UNION ALL 
    SELECT * FROM {{ ref('arbigrants_nova_month_tvl_by_project') }}
)
GROUP BY 1,2