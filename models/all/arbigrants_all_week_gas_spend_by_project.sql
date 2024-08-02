{{ config
(
    materialized = 'table'
)
}}

SELECT 
DATE,
NAME,
SUM(GAS_SPEND) AS GAS_SPEND
FROM 
(
    SELECT * FROM {{ ref('arbigrants_one_week_gas_spend_by_project') }}
    UNION ALL 
    SELECT * FROM {{ ref('arbigrants_nova_week_gas_spend_by_project') }}
)
GROUP BY 1,2