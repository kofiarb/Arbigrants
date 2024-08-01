{{ config
(
    materialized = 'table'
)
}}

SELECT 
TO_VARCHAR(DATE_TRUNC('day',DATE), 'YYYY-MM-DD') AS date,
TVL 
FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_TOTAL_TVL
WHERE DATE < DATE_TRUNC('day',CURRENT_DATE())
AND DATE >= CURRENT_DATE() - interval '13 months'