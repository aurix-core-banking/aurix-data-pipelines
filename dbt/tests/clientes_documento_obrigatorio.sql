-- Falha se um cliente PF nao tiver CPF ou PJ nao tiver CNPJ.
with clientes as (
    select * from {{ ref('stg_clientes') }}
)

select cliente_id
from clientes
where (tipo_pessoa = 'FISICA' and (cpf is null or length(trim(cpf)) = 0))
   or (tipo_pessoa = 'JURIDICA' and (cnpj is null or length(trim(cnpj)) = 0))