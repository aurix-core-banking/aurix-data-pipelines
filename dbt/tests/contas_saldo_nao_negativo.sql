-- Falha se houver conta com saldo negativo (saldo nunca deve ser menor que zero
-- no modelo relacional de contas do core banking).
with contas as (
    select * from {{ ref('stg_contas') }}
)

select id
from contas
where saldo < 0