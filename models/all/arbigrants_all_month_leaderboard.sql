{{ config
(
    materialized = 'table'
)
}}

SELECT * FROM {{ ref('arbigrants_one_month_leaderboard') }}
UNION ALL 
SELECT * FROM {{ ref('arbigrants_nova_month_leaderboard') }}