{{ config
(
    materialized = 'table'
)
}}

SELECT * FROM {{ ref('arbigrants_one_week_leaderboard') }}
UNION ALL 
SELECT * FROM {{ ref('arbigrants_nova_week_leaderboard') }}