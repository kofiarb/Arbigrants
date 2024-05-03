{{ config
(
    materialized = 'table'
)
}}

SELECT name, contract_address
FROM (VALUES
('Example1', '0x00000000005bbb0ef59571e58418f9a4357b68a0'), 
('Example2', '0x83d6c8C06ac276465e4C92E7aC8C23740F435140')
) AS x (name, contract_address)