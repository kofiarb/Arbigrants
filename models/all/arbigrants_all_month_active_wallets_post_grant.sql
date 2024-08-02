{{ config
(
    materialized = 'table'
)
}}

SELECT 
DATE,
SUM(ACTIVE_WALLETS) AS ACTIVE_WALLETS
FROM 
(
    SELECT * FROM {{ ref('arbigrants_one_month_active_wallets_post_grant') }}
    UNION ALL 
    SELECT * FROM {{ ref('arbigrants_nova_month_active_wallets_post_grant') }}
)
GROUP BY 1