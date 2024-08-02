{{ config
(
    materialized = 'table'
)
}}

SELECT 
DATE,
SUM(ACTIVE_WALLETS) AS ACTIVE_WALLETS
FROM {{ ref('arbigrants_nova_week_active_wallets_by_project') }}
GROUP BY 1