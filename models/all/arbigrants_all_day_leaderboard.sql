{{ config
(
    materialized = 'table'
)
}}

SELECT * FROM {{ ref('arbigrants_one_day_leaderboard') }}
UNION ALL 
SELECT * FROM {{ ref('arbigrants_nova_day_leaderboard') }}