-- Falha se houver transacao com valor nulo ou nao positivo.
with transacoes as (
    select * from {{ ref('stg_transacoes') }}
)

select id
from transacoes
where valor is null or valor <= 0