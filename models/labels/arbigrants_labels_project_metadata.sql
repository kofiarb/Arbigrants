{{ config
(
    materialized = 'table'
)
}}

SELECT name, category, grant_date, llama_slug, llama_name, description
FROM (VALUES
('Example1', 'yield', '2024-01-01', 'hmx', 'HMX', 'Lorem ipsum lo menos'), 
('Example2', 'derivatives', '2024-01-01', 'pendle', 'Pendle', 'Lorem ipsum lo menos')
) AS x (name, category, grant_date, llama_slug, llama_name, description)