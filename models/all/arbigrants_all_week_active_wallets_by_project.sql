{{ config
(
    materialized = 'table'
)
}}


SELECT 
DATE,
NAME,
SUM(ACTIVE_WALLETS) AS ACTIVE_WALLETS
FROM 
(
    SELECT * FROM {{ ref('arbigrants_one_week_active_wallets_by_project') }}
    UNION ALL 
    SELECT * FROM {{ ref('arbigrants_nova_week_active_wallets_by_project') }}
)
GROUP BY 1,2