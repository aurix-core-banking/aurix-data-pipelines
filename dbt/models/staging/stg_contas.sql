{{ config(materialized='view', schema='aurix') }}
select
    id,
    tenant_id,
    numero_conta,
    cliente_id,
    tipo_conta,
    saldo,
    limite_credito,
    limite_utilizado,
    status,
    data_abertura,
    data_fechamento,
    data_criacao,
    data_atualizacao
from {{ source('aurix', 'contas') }}
