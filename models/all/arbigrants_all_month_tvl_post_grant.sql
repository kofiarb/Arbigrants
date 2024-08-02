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
    SELECT * FROM {{ ref('arbigrants_one_month_tvl_post_grant') }}
    UNION ALL 
    SELECT * FROM {{ ref('arbigrants_nova_month_tvl_post_grant') }}
)
GROUP BY 1