{{ config
(
    materialized = 'table'
)
}}

WITH cte AS (
SELECT 
c.NAME,
COUNT(DISTINCT FROM_ADDRESS) AS active_wallets,
SUM(COUNT(DISTINCT FROM_ADDRESS)) OVER () AS total_wallets
FROM {{ source('arbitrum_raw', 'transactions') }} t
INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
AND BLOCK_TIMESTAMP < CURRENT_DATE
AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '1 month'
GROUP BY 1
),
ranked_cte AS (
  SELECT 
    NAME,
    active_wallets,
    total_wallets,
    ROUND(active_wallets / total_wallets * 100, 2) AS PCT_TVL,
    RANK() OVER (ORDER BY active_wallets DESC) AS rnk
  FROM cte
)
SELECT 
  CASE WHEN rnk <= 5 THEN NAME ELSE 'Other' END AS NAME,
  SUM(active_wallets) AS active_wallets,
  ROUND(SUM(active_wallets) / MAX(total_wallets) * 100, 2) AS PCT_WALLETS
FROM ranked_cte
GROUP BY CASE WHEN rnk <= 5 THEN NAME ELSE 'Other' END
ORDER BY active_wallets DESC