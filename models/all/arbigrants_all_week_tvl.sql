{{ config
(
    materialized = 'table'
)
}}

SELECT 
DATE,
SUM(TVL) AS TVL,
SUM(TVL_ETH) AS TVL_ETH
FROM 
(
    SELECT * FROM {{ ref('arbigrants_one_week_tvl') }}
    UNION ALL 
    SELECT * FROM {{ ref('arbigrants_nova_week_tvl') }}
)
GROUP BY 1
