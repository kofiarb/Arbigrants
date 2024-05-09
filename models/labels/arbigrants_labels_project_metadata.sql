{{ config
(
    materialized = 'table'
)
}}

SELECT name, category, grant_date, llama_slug, llama_name, chain, description, logo
FROM (VALUES
('Example Project 1', 'yield', '2024-01-01', 'eigenlayer-protocols-part', 'https://defillama.com/protocol/radiant#informations two', 'Arbitrum One', 'Lorem ipsum lo menos', 'https://i.imgur.com/EALllxl.png'), 
('Example Project 2', 'derivatives', '2024-01-01', 'eigenlayer-protocols-part', 'Eigenlayers protocls part 7', 'Arbitrum One', 'A vertically-integrated DEX on Arbitrum featuring spot, perpetual, and integrated money markets. Universal cross-margin with lightning-fast performance. Take back control.', 'https://i.imgur.com/EALllxl.png')
) AS x (name, category, grant_date, llama_slug, llama_name, chain, description, logo)