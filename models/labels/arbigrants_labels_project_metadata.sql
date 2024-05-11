{{ config
(
    materialized = 'table'
)
}}

SELECT name, category, grant_date, llama_slug, chain, description, logo
FROM (VALUES
('Example Project 1', 'DeFi', '2024-01-01', 'eigenlayer-protocols-part', 'Arbitrum One', 'Lorem ipsum lo menos', 'https://i.imgur.com/EALllxl.png'), 
('Example Project 2', 'DeFi', '2024-01-01', 'eigenlayer-protocols-part', 'Arbitrum Orbit', 'A vertically-integrated DEX on Arbitrum featuring spot, perpetual, and integrated money markets. Universal cross-margin with lightning-fast performance. Take back control.', 'https://i.imgur.com/EALllxl.png')
) AS x (name, category, grant_date, llama_slug, chain, description, logo)