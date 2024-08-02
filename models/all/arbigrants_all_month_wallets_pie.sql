{{ config
(
    materialized = 'table'
)
}}

SELECT
NAME,
ACTIVE_WALLETS,
ROUND( ( 100* ACTIVE_WALLETS / SUM(ACTIVE_WALLETS) OVER () ), 2 ) AS PCT_WALLETS
FROM (
SELECT 
NAME,
SUM(ACTIVE_WALLETS) AS ACTIVE_WALLETS
FROM 
(
    SELECT NAME, ACTIVE_WALLETS FROM {{ ref('arbigrants_one_month_wallets_pie') }}
    UNION ALL 
    SELECT NAME, ACTIVE_WALLETS FROM {{ ref('arbigrants_nova_month_wallets_pie') }}
)
GROUP BY 1)

