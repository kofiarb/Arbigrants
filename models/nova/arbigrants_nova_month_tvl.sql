{{ config
(
    materialized = 'table'
)
}}

SELECT 
DATE,
SUM(TVL) AS TVL,
SUM(TVL_ETH) AS TVL_ETH
FROM {{ ref('arbigrants_nova_month_tvl_by_project') }}
GROUP BY 1