{{ config
(
    materialized = 'table'
)
}}

SELECT name, category, grant_date, llama_slug, llama_name, chain, description
FROM (VALUES
('Example1', 'yield', '2024-01-01', 'hmx', 'HMX', 'Arbitrum One', 'Lorem ipsum lo menos'), 
('Example2', 'derivatives', '2024-01-01', 'pendle', 'Pendle', 'Arbitrum One', 'Lorem ipsum lo menos')
) AS x (name, category, grant_date, llama_slug, llama_name, chain, description)